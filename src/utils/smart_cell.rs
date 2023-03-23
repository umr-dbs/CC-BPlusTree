use std::fmt::{Display, Formatter};
use std::{hint, mem, ptr};
use std::mem::{transmute, transmute_copy};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use parking_lot::lock_api::{MutexGuard, RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{Mutex, RawMutex, RawRwLock, RwLock, RwLockUpgradableReadGuard};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Unexpected::Seq;
use crate::record_model::{AtomicVersion, Version};
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::SmartFlavor::{ExclusiveCell, FreeCell, HybridCell, OLCCell, ReadersWriterCell};
use crate::utils::smart_cell::SmartGuard::{HybridWriter, LockFree, MutExclusive, OLCReader, OLCReaderPin, OLCWriter, RwReader, RwWriter};

pub const CPU_THREADS: bool = true;
pub const ENABLE_YIELD: bool = !CPU_THREADS;

pub(crate) const OBSOLETE_FLAG_VERSION: LatchVersion = 0x8_000000000000000;
const WRITE_FLAG_VERSION: LatchVersion = 0x4_000000000000000;
const PIN_FLAG_VERSION: LatchVersion = 0x2_000000000000000;

const WRITE_OBSOLETE_FLAG_VERSION: LatchVersion = 0xC_000000000000000;
const WRITE_PIN_FLAG_VERSION: LatchVersion = 0x6_000000000000000;
const WRITE_PIN_OBSOLETE_FLAG_VERSION: LatchVersion = 0xE_000000000000000;

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum LatchType {
    Exclusive,
    ReadersWriter,
    Optimistic,
    Hybrid,
    None,
}

#[inline(always)]
#[cfg(target_os = "linux")]
pub fn sched_yield(attempt: usize) {
    if attempt > 3 {
        unsafe { libc::sched_yield(); }
    } else {
        hint::spin_loop();
    }
}

pub const FORCE_YIELD: usize = 4;

#[inline(always)]
#[cfg(not(target_os = "linux"))]
pub fn sched_yield(attempt: usize) {
    if attempt > 3 {
        std::thread::yield_now();
    } else {
        hint::spin_loop();
    }
}

type LatchVersion = Version;
type IsRead = bool;

pub struct OptCell<E: Default> {
    pub(crate) cell: SafeCell<E>,
    pub(crate) cell_version: AtomicVersion,
}

impl<E: Default + Serialize> Serialize for OptCell<E> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        self.cell.serialize(serializer)
    }
}

impl<'de, E: Default + Deserialize<'de>> Deserialize<'de> for OptCell<E> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        Ok(OptCell::new(E::deserialize(deserializer)?))
    }
}

impl<E: Default + Display> Display for OptCell<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "OptCell {{\ncell: {}\n\t\tcell_version: {}\n\t}}", self.cell.get_mut(), self.load_version())
    }
}

impl<E: Default> Default for OptCell<E> {
    fn default() -> Self {
        Self::new(E::default())
    }
}

impl<E: Default> OptCell<E> {
    const CELL_START_VERSION: LatchVersion = 0;

    #[inline(always)]
    pub const fn new(data: E) -> Self {
        Self {
            cell: SafeCell::new(data),
            cell_version: AtomicVersion::new(Self::CELL_START_VERSION),
        }
    }

    #[inline(always)]
    pub fn load_version(&self) -> LatchVersion {
        self.cell_version.load(Acquire)
    }

    #[inline(always)]
    pub fn read_lock(&self) -> (IsRead, LatchVersion) {
        let read_version
            = self.load_version();

        (read_version & WRITE_OBSOLETE_FLAG_VERSION == 0, read_version & !PIN_FLAG_VERSION)
    }

    #[inline(always)]
    pub fn pin_lock(&self) -> Result<LatchVersion, (IsRead, LatchVersion)> {
        let read_version
            = self.load_version();

        if read_version & PIN_FLAG_VERSION != 0 {
            return Err((true, read_version & !PIN_FLAG_VERSION));
        }

        if read_version & WRITE_OBSOLETE_FLAG_VERSION != 0 {
            return Err((false, read_version));
        }

        match self.cell_version.compare_exchange_weak(
            read_version,
            read_version | PIN_FLAG_VERSION,
            AcqRel,
            Relaxed)
        {
            Ok(_) => Ok(read_version | PIN_FLAG_VERSION),
            Err(_) => Err((true, read_version))
        }
    }

    #[inline(always)]
    pub fn write_unpin(&self, pin_lock: LatchVersion) {
        debug_assert!(pin_lock & PIN_FLAG_VERSION == PIN_FLAG_VERSION &&
            pin_lock & WRITE_OBSOLETE_FLAG_VERSION == 0);

        self.cell_version.store(pin_lock ^ PIN_FLAG_VERSION, Release)
    }

    #[inline(always)]
    pub fn is_read_valid(&self, read_latch: LatchVersion) -> IsRead {
        let load_version
            = self.load_version();

        read_latch == load_version & !PIN_FLAG_VERSION && load_version & WRITE_OBSOLETE_FLAG_VERSION == 0
    }

    #[inline(always)]
    pub fn pin_write_lock(&self, read_version_pin: LatchVersion) -> LatchVersion {
        let pin_write
            = WRITE_FLAG_VERSION | (read_version_pin & !PIN_FLAG_VERSION);

        self.cell_version.store(pin_write, Release);

        pin_write
    }

    #[inline(always)]
    pub fn write_lock(&self, read_version: LatchVersion) -> Option<LatchVersion> {
        match self.cell_version.compare_exchange_weak(
            read_version,
            WRITE_FLAG_VERSION | read_version,
            AcqRel,
            Relaxed)
        {
            Ok(..) => Some(WRITE_FLAG_VERSION | read_version),
            Err(..) => None
        }
    }

    #[inline(always)]
    pub fn write_lock_strong(&self, read_version: LatchVersion) -> Option<LatchVersion> {
        match self.cell_version.compare_exchange(
            read_version,
            WRITE_FLAG_VERSION | read_version,
            AcqRel,
            Relaxed)
        {
            Ok(..) => Some(WRITE_FLAG_VERSION | read_version),
            Err(..) => None
        }
    }

    #[inline(always)]
    pub fn write_unlock(&self, write_version: LatchVersion) {
        debug_assert!(write_version & WRITE_PIN_FLAG_VERSION == WRITE_FLAG_VERSION);

        self.cell_version.store((write_version + 1) ^ WRITE_FLAG_VERSION, Release)
    }

    #[inline(always)]
    pub fn write_obsolete(&self, write_version: LatchVersion) {
        // debug_assert!(write_version & WRITE_OBSOLETE_FLAG_VERSION == WRITE_FLAG_VERSION);

        self.cell_version.store(OBSOLETE_FLAG_VERSION | write_version, Release)
    }

    #[inline(always)]
    pub fn is_obsolete(&self) -> bool {
        self.load_version() & OBSOLETE_FLAG_VERSION == OBSOLETE_FLAG_VERSION
    }

    #[inline(always)]
    pub fn is_read_obsolete(&self) -> bool {
        let load = self.load_version();
        load & WRITE_FLAG_VERSION == 0 && load & OBSOLETE_FLAG_VERSION == OBSOLETE_FLAG_VERSION
    }

    #[inline(always)]
    pub fn is_write(&self) -> bool {
        self.load_version() & WRITE_FLAG_VERSION == WRITE_FLAG_VERSION
    }

    #[inline(always)]
    pub fn is_read_not_obsolete(&self) -> bool {
        self.load_version() & WRITE_OBSOLETE_FLAG_VERSION == 0
    }

    #[inline(always)]
    pub fn is_read_not_obsolete_result(&self) -> (bool, LatchVersion) {
        if ENABLE_YIELD {
            sched_yield(FORCE_YIELD);
        }

        let read_version
            = self.load_version();

        (read_version & WRITE_OBSOLETE_FLAG_VERSION == 0, read_version)
    }
}

#[derive(Default)]
pub struct SmartCell<E: Default>(pub Arc<SmartFlavor<E>>);

impl<E: Default> Clone for SmartCell<E> {
    #[inline(always)]
    fn clone(&self) -> Self {
        SmartCell(self.0.clone())
    }
}

pub enum SmartFlavor<E: Default> {
    FreeCell(SafeCell<E>),
    ExclusiveCell(Mutex<()>, SafeCell<E>),
    ReadersWriterCell(RwLock<()>, SafeCell<E>),
    OLCCell(OptCell<E>),
    HybridCell(OptCell<E>, RwLock<()>),
}

impl<E: Default> Default for SmartFlavor<E> {
    fn default() -> Self {
        FreeCell(SafeCell::new(unsafe {
            mem::MaybeUninit::uninit().assume_init()
        }))
    }
}

impl<E: Default> SmartFlavor<E> {
    #[inline(always)]
    fn is_read_valid(&self, read_version: LatchVersion) -> bool {
        match self {
            OLCCell(opt) => opt.is_read_valid(read_version),
            HybridCell(opt, rw) =>
                rw.try_read().is_some() && opt.is_read_valid(read_version),
            _ => true
        }
    }

    #[inline(always)]
    fn is_obsolete(&self) -> bool {
        match self {
            OLCCell(opt) => opt.is_obsolete(),
            HybridCell(opt, ..) => opt.is_obsolete(),
            _ => false
        }
    }

    #[inline(always)]
    fn is_read_not_obsolete(&self) -> bool {
        match self {
            OLCCell(opt) => opt.is_read_not_obsolete(),
            HybridCell(opt, rw) =>
                rw.try_read().is_some() && opt.is_read_not_obsolete(),
            _ => true
        }
    }

    #[inline(always)]
    fn is_read_not_obsolete_result(&self) -> (bool, LatchVersion) {
        match self {
            OLCCell(opt) => opt.is_read_not_obsolete_result(),
            HybridCell(opt, rw) =>
                (rw.try_read().is_some(), opt.load_version()),
            _ => (true, LatchVersion::MIN)
        }
    }

    #[inline(always)]
    pub fn as_mut(&self) -> &mut E {
        match self {
            ExclusiveCell(.., ptr) => ptr.get_mut(),
            OLCCell(opt) => opt.cell.get_mut(),
            HybridCell(opt, ..) => opt.cell.get_mut(),
            FreeCell(ptr) => ptr.get_mut(),
            ReadersWriterCell(.., ptr) => ptr.get_mut()
        }
    }
}

impl<E: Default + 'static> Deref for SmartFlavor<E> {
    type Target = E;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        match self {
            ExclusiveCell(.., ptr) => ptr.get_mut(),
            OLCCell(opt) => opt.cell.as_ref(),
            HybridCell(opt, _) => opt.cell.as_ref(),
            FreeCell(ptr) => ptr.get_mut(),
            ReadersWriterCell(.., ptr) => ptr.get_mut()
        }
    }
}

impl<E: Default + 'static> DerefMut for SmartFlavor<E> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            ExclusiveCell(.., ptr) => ptr.get_mut(),
            OLCCell(opt) => opt.cell.get_mut(),
            HybridCell(opt, _) => opt.cell.get_mut(),
            FreeCell(ptr) => ptr.get_mut(),
            ReadersWriterCell(.., ptr) => ptr.get_mut()
        }
    }
}

pub enum SmartGuard<'a, E: Default> {
    LockFree(*mut E),
    RwReader(RwLockReadGuard<'a, RawRwLock, ()>, *const E),
    RwWriter(RwLockWriteGuard<'a, RawRwLock, ()>, *mut E),
    MutExclusive(MutexGuard<'a, RawMutex, ()>, *mut E),
    OLCReader(Option<(SmartCell<E>, LatchVersion)>),
    OLCWriter(SmartCell<E>, LatchVersion),
    OLCReaderPin(SmartCell<E>, LatchVersion),
    HybridWriter(RwLockWriteGuard<'a, RawRwLock, ()>, &'a OptCell<E>, LatchVersion),
}

impl<'a, E: Default + 'static> Clone for SmartGuard<'_, E> {
    fn clone(&self) -> Self {
        match self {
            OLCReader(inner) => OLCReader(inner.clone()),
            OLCReaderPin(inner, read_latch) =>
                OLCReader(Some((inner.clone(), (*read_latch & !PIN_FLAG_VERSION)))),
            RwReader(guard, ptr) => RwReader(
                RwLockReadGuard::rwlock(guard)
                    .read(),
                *ptr),
            LockFree(ptr) => LockFree(*ptr),
            _ => OLCReader(None)
        }
    }
}

impl<'a, E: Default + 'static> SmartGuard<'_, E> {
    #[inline(always)]
    pub fn mark_obsolete(&self) {
        match self {
            OLCWriter(cell, latch) => match cell.0.as_ref() {
                OLCCell(opt) | HybridCell(opt, ..) => opt.write_obsolete(*latch),
                _ => {}
            }
            HybridWriter(.., opt, latch) => opt.write_obsolete(*latch),
            _ => {}
        }
    }

    #[inline(always)]
    pub fn upgrade_write_lock(&mut self) -> bool {
        match self {
            LockFree(_) => true,
            RwWriter(..) => true,
            MutExclusive(..) => true,
            OLCWriter(..) => true,
            OLCReaderPin(cell, pin_latch) => unsafe {
                if let OLCCell(opt) = cell.0.as_ref() {
                    let writer = OLCWriter(
                        transmute_copy(cell),
                        opt.pin_write_lock(*pin_latch));

                    ptr::write(self, writer);
                    return true;
                }

                unreachable!()
            }
            OLCReader(Some((cell, latch))) => unsafe {
                if let OLCCell(opt) = cell.0.as_ref() {
                    if let Some(write_latch) = opt.write_lock(*latch) {
                        let writer = OLCWriter(transmute_copy(cell), write_latch);
                        ptr::write(self, writer);
                        return true;
                    }
                }

                false
            }
            HybridWriter(..) => true,
            _ => false
        }
    }

    #[inline(always)]
    pub const fn is_write_lock(&self) -> bool {
        match self {
            RwWriter(..) => true,
            MutExclusive(..) => true,
            OLCWriter(..) => true,
            HybridWriter(..) => true,
            _ => false
        }
    }

    #[inline(always)]
    pub const fn is_reader_lock(&self) -> bool {
        !self.is_write_lock()
    }

    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        match self {
            OLCReader(Some((cell, latch))) => cell.0
                .is_read_valid(*latch),
            OLCReader(None) => false,
            _ => true
        }
    }

    #[inline(always)]
    pub fn is_obsolete(&self) -> bool {
        match self {
            OLCReader(Some((cell, ..))) => cell.0.is_obsolete(),
            OLCReader(None) => true,
            _ => false
        }
    }

    #[inline(always)]
    pub fn is_read_not_obsolete(&self) -> bool {
        match self {
            OLCReader(Some((cell, ..))) => cell.0.is_read_not_obsolete(),
            OLCReader(None) => false,
            _ => true
        }
    }

    #[inline(always)]
    pub unsafe fn update_read_latch(&mut self, read_latch: LatchVersion) {
        if let OLCReader(Some((.., latched))) = self {
            *latched = read_latch
        }
    }

    #[inline(always)]
    pub fn is_read_not_obsolete_result(&self) -> (IsRead, LatchVersion) {
        match self {
            OLCReader(Some((cell, ..))) => cell.0.is_read_not_obsolete_result(),
            OLCReader(None) => (false, LatchVersion::MIN),
            OLCReaderPin(.., latch) => (true, *latch & !PIN_FLAG_VERSION),
            _ => (true, LatchVersion::MIN)
        }
    }

    #[inline(always)]
    pub fn deref(&self) -> Option<&'_ E> {
        match self {
            LockFree(ptr) => unsafe { ptr.as_ref() },
            RwReader(.., ptr) => unsafe { ptr.as_ref() },
            RwWriter(.., ptr) => unsafe { ptr.as_ref() },
            MutExclusive(.., ptr) => unsafe { ptr.as_ref() },
            OLCReader(Some((cell, latch))) if cell.0.is_read_valid(*latch) =>
                Some(cell.0.as_ref()),
            OLCWriter(cell, ..) =>
                Some(cell.0.as_ref()),
            OLCReaderPin(cell, ..) =>
                Some(cell.0.as_ref()),
            HybridWriter(.., opt, _) => Some(opt.cell.as_ref()),
            _ => None
        }
    }

    #[inline(always)]
    pub unsafe fn deref_unsafe(&self) -> Option<&'_ E> {
        match self {
            LockFree(ptr) => ptr.as_ref(),
            RwReader(.., ptr) => ptr.as_ref(),
            RwWriter(.., ptr) => ptr.as_ref(),
            MutExclusive(.., ptr) => ptr.as_ref(),
            OLCReader(Some((cell, ..))) => Some(cell.0.as_ref()),
            OLCWriter(cell, ..) => Some(cell.0.as_ref()),
            OLCReaderPin(cell, ..) => Some(cell.0.as_ref()),
            HybridWriter(.., opt, _) => Some(opt.cell.as_ref()),
            _ => None
        }
    }

    #[inline(always)]
    pub fn deref_mut(&self) -> Option<&mut E> {
        match self {
            LockFree(ptr) => unsafe { ptr.as_mut() },
            RwWriter(.., ptr) => unsafe { ptr.as_mut() },
            MutExclusive(.., ptr) => unsafe { ptr.as_mut() },
            OLCWriter(cell, ..) => Some(cell.0.as_mut()),
            HybridWriter(.., opt, _) => Some(opt.cell.get_mut()),
            _ => None
        }
    }
}

impl<E: Default> SmartCell<E> {
    #[inline(always)]
    pub fn unsafe_borrow(&self) -> &E {
        match self.0.as_ref() {
            ExclusiveCell(.., ptr) => ptr.as_ref(),
            OLCCell(opt) => opt.cell.as_ref(),
            HybridCell(opt, _) => opt.cell.as_ref(),
            FreeCell(ptr) => ptr.as_ref(),
            ReadersWriterCell(.., ptr) => ptr.as_ref()
        }
    }

    #[inline(always)]
    pub fn unsafe_borrow_mut(&self) -> &mut E {
        match self.0.as_ref() {
            ExclusiveCell(.., ptr) => ptr.get_mut(),
            OLCCell(opt) => opt.cell.get_mut(),
            HybridCell(opt, ..) => opt.cell.get_mut(),
            FreeCell(ptr) => ptr.get_mut(),
            ReadersWriterCell(.., ptr) => ptr.get_mut()
        }
    }

    #[inline(always)]
    pub fn borrow_free(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            FreeCell(ptr) => LockFree(ptr.get_mut()),
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn borrow_read_hybrid(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            HybridCell(opt, rw) => unsafe {
                let reader
                    = rw.read();

                if opt.cell_version.load(SeqCst) & WRITE_OBSOLETE_FLAG_VERSION != 0 {
                    mem::drop(reader);
                    OLCReader(None)
                } else {
                    transmute(RwReader(
                        reader,
                        opt.cell.as_ref()))
                }
            }
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn borrow_read(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            OLCCell(opt) | HybridCell(opt, ..) => {
                let (success, read)
                    = opt.read_lock();

                OLCReader(success.then(|| (self.clone(), read)))
            }
            ExclusiveCell(mutex, ptr) => unsafe {
                MutExclusive(transmute(mutex.lock()),
                             ptr.get_mut())
            },
            ReadersWriterCell(rw, ptr) => unsafe {
                RwReader(transmute(rw.read()), ptr.as_ref())
            },
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn borrow_pin(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            OLCCell(opt) => match opt.pin_lock() {
                Ok(pin_latch) =>
                    OLCReaderPin(self.clone(), pin_latch),
                Err((true, read_latch)) =>
                    OLCReader(Some((self.clone(), read_latch))),
                _ => OLCReader(None)
            },
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn borrow_mut(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            FreeCell(ptr) => LockFree(ptr.get_mut()),
            ReadersWriterCell(rw, ptr) => unsafe {
                transmute(RwWriter(
                    transmute(rw.write()),
                    ptr.get_mut(),
                ))
            },
            OLCCell(opt) => {
                let read_version
                    = opt.load_version();

                if read_version & WRITE_PIN_OBSOLETE_FLAG_VERSION != 0 {
                    OLCReader(None)
                } else if let Some(latched) = opt.write_lock(read_version) {
                    OLCWriter(self.clone(), latched)
                } else {
                    OLCReader(None)
                }
            }
            HybridCell(opt, rw) => unsafe {
                let (read, read_version)
                    = opt.read_lock();

                match read {
                    true => match opt.write_lock_strong(read_version) {
                        Some(write_version) => return transmute(
                            OLCWriter(self.clone(), write_version)),
                        _ => {}
                    }
                    _ => {}
                };

                let writer
                    = rw.write();

                let version
                    = opt.load_version();

                match version & WRITE_OBSOLETE_FLAG_VERSION == 0 {
                    true if opt.write_lock_strong(version).is_some() => transmute(
                        HybridWriter(writer,
                                     opt,
                                     version | WRITE_FLAG_VERSION)),
                    _ => {
                        mem::drop(writer);

                        OLCReader(None)
                    }
                }
            },
            ExclusiveCell(mutex, ptr) => unsafe {
                transmute(MutExclusive(
                    transmute(mutex.lock()),
                    ptr.get_mut(),
                ))
            }
        }
    }
}

impl<'a, E: Default> Drop for SmartGuard<'a, E> {
    fn drop(&mut self) {
        match self {
            OLCWriter(cell, write_version) =>
                if let OLCCell(opt) = cell.0.as_ref() {
                    opt.write_unlock(*write_version)
                }
                else if let HybridCell(opt, ..) = cell.0.as_ref() {
                    opt.write_unlock(*write_version)
                }
            OLCReaderPin(cell, pin_version) =>
                if let OLCCell(opt) = cell.0.as_ref() {
                    opt.write_unpin(*pin_version)
                }
            HybridWriter(.., opt, latch) =>
                opt.write_unlock(*latch),
            _ => {}
        }
    }
}

unsafe impl<'a, E: Default + 'a> Sync for SmartGuard<'a, E> {}

unsafe impl<'a, E: Default + 'a> Send for SmartGuard<'a, E> {}