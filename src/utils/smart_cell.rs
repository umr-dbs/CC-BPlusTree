use std::fmt::{Display, Formatter};
use std::{hint, mem};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};
use parking_lot::lock_api::{MutexGuard, RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawMutex, RawRwLock};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use crate::record_model::{AtomicVersion, Version};
use crate::utils::cc_cell::CCCell;
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::SmartFlavor::{ControlCell, OLCCell};
use crate::utils::smart_cell::SmartGuard::{LockFree, MutExclusive, OLCReader, OLCWriter, RwReader, RwWriter};

// pub union NewCell<E: Default> {
//     pub cccell: ManuallyDrop<CCCell<E>>,
//     pub opt_cell: ManuallyDrop<OptCell<E>>,
// }

pub const OBSOLETE_FLAG_VERSION: Version = 0x8_000000000000000;
pub const WRITE_FLAG_VERSION: Version = 0x4_000000000000000;
pub const WRITE_OBSOLETE_FLAG_VERSION: Version = 0xC_000000000000000;
const READ_FLAG_VERSION: Version = 0x0_000000000000000;
// const LOCK_FREE_FLAG_VERSION: Version = 0x00_00_00_00_00_00_00_00;
// const LOCKING_FLAG_VERSION: Version = OBSOLETE_FLAG_VERSION;

// const READERS_NUM_BITS: Version = 6 + 8;
// 0xC_0 + 0x_00 free bits
// const MAX_READERS: Version = (1 << READERS_NUM_BITS) - 1;
// const LOCKING_BITS_OFFSET: Version = 2;
// const VERSIONING_COUNTER_BITS: Version = (8 * mem::size_of::<Version>() as Version) - READERS_NUM_BITS - LOCKING_BITS_OFFSET;

#[inline(always)]
#[cfg(target_os = "linux")]
pub fn sched_yield(attempt: usize) {
    if attempt > 3 {
        unsafe { libc::sched_yield(); }
    } else {
        hint::spin_loop();
    }
}

#[inline(always)]
#[cfg(not(target_os = "linux"))]
pub fn sched_yield(attempt: usize) {
    if attempt > 3 {
        std::thread::sleep(std::time::Duration::from_nanos(0))
    } else {
        hint::spin_loop();
    }
}

pub type LatchVersion = Version;

// pub const fn num_readers(version: Version) -> Version {
//     (((version << LOCKING_BITS_OFFSET) >> LOCKING_BITS_OFFSET) & MAX_READERS) >> (mem::size_of::<Version>() as Version - READERS_NUM_BITS - LOCKING_BITS_OFFSET)
// }

pub struct OptCell<E: Default> {
    pub(crate) cell: SafeCell<E>,
    cell_version: AtomicVersion,
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
    const CELL_START_VERSION: Version = 0;

    #[inline(always)]
    pub const fn new(data: E) -> Self {
        Self {
            cell: SafeCell::new(data),
            cell_version: AtomicVersion::new(Self::CELL_START_VERSION),
        }
    }

    #[inline(always)]
    pub fn load_version(&self) -> Version {
        self.cell_version.load(Relaxed)
    }

    #[inline(always)]
    pub unsafe fn load_version_force(&self) -> Version {
        self.cell_version.load(SeqCst)
    }

    #[inline(always)]
    pub fn read_lock(&self) -> (bool, LatchVersion) {
        let version = self.load_version();
        if version & WRITE_OBSOLETE_FLAG_VERSION != 0 {
            // hint::spin_loop();
            (false, version)
        } else {
            (true, version)
        }
    }

    // fn is_any_valid(&self, v: Version) -> bool {
    //     let load = self.load_version();
    //     v == load && load & OBSOLETE_FLAG_VERSION == 0
    // }

    #[inline(always)]
    pub fn is_read_valid(&self, v: LatchVersion) -> bool {
        let load = self.load_version();
        v == load && load & WRITE_OBSOLETE_FLAG_VERSION == 0
    }

    #[inline(always)]
    pub fn write_lock(&self, read_version: LatchVersion) -> Option<LatchVersion> {
        // if read_version & WRITE_OBSOLETE_FLAG_VERSION != 0 {
        //     return None;
        // }

        match self.cell_version.compare_exchange(
            read_version,
            WRITE_FLAG_VERSION | read_version,
            SeqCst,
            Relaxed)
        {
            Ok(..) => Some(WRITE_FLAG_VERSION | read_version),
            Err(..) => {
                // hint::spin_loop();
                None
            }
        }
    }

    #[inline(always)]
    pub fn write_unlock(&self, write_version: LatchVersion) {
        debug_assert!(write_version & WRITE_FLAG_VERSION == WRITE_FLAG_VERSION);

        self.cell_version.store((write_version + 1) ^ WRITE_FLAG_VERSION, SeqCst)
    }

    #[inline(always)]
    pub fn write_obsolete(&self, write_version: LatchVersion) {
        debug_assert!(write_version & WRITE_OBSOLETE_FLAG_VERSION == WRITE_FLAG_VERSION);

        self.cell_version.store(OBSOLETE_FLAG_VERSION | write_version, SeqCst);
    }

    #[inline(always)]
    pub fn is_obsolete(&self) -> bool {
        self.load_version() & OBSOLETE_FLAG_VERSION == OBSOLETE_FLAG_VERSION
        // self.cell_version.load(Relaxed) & OBSOLETE_FLAG_VERSION == OBSOLETE_FLAG_VERSION
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
        let load = self.load_version();
        (load & WRITE_OBSOLETE_FLAG_VERSION == 0, load)
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
    ControlCell(CCCell<E>),
    OLCCell(OptCell<E>),
}

impl<E: Default> Default for SmartFlavor<E> {
    fn default() -> Self {
        ControlCell(CCCell::default())
    }
}

impl<E: Default> SmartFlavor<E> {
    #[inline(always)]
    fn is_read_valid(&self, read_version: LatchVersion) -> bool {
        match self {
            OLCCell(opt) => opt.is_read_valid(read_version),
            _ => true
        }
    }

    #[inline(always)]
    fn is_obsolete(&self) -> bool {
        match self {
            OLCCell(opt) => opt.is_obsolete(),
            _ => false
        }
    }

    #[inline(always)]
    fn is_read_obsolete(&self) -> bool {
        match self {
            OLCCell(opt) => opt.is_read_obsolete(),
            _ => false
        }
    }

    #[inline(always)]
    fn is_write(&self) -> bool {
        match self {
            OLCCell(opt) => opt.is_write(),
            _ => false
        }
    }

    #[inline(always)]
    fn is_read_not_obsolete(&self) -> bool {
        match self {
            OLCCell(opt) => opt.is_read_not_obsolete(),
            _ => true
        }
    }

    #[inline(always)]
    fn is_read_not_obsolete_result(&self) -> (bool, LatchVersion) {
        match self {
            OLCCell(opt) => opt.is_read_not_obsolete_result(),
            _ => (true, LatchVersion::MIN)
        }
    }

    #[inline(always)]
    pub fn as_mut(&self) -> &mut E {
        match self {
            ControlCell(cell) => cell.unsafe_borrow_mut(),
            OLCCell(opt) => opt.cell.get_mut(),
        }
    }
}

impl<E: Default + 'static> Deref for SmartFlavor<E> {
    type Target = E;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        match self {
            ControlCell(cell) => cell.unsafe_borrow_static(),
            OLCCell(opt) => opt.cell.as_ref()
        }
    }
}

impl<E: Default + 'static> DerefMut for SmartFlavor<E> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            ControlCell(cell) => cell.unsafe_borrow_mut_static(),
            OLCCell(opt) => opt.cell.get_mut(),
            _ => unreachable!()
        }
    }
}

pub enum SmartGuard<'a, E: Default> {
    LockFree(*mut E),
    RwReader(RwLockReadGuard<'a, RawRwLock, ()>, *const E),
    RwWriter(RwLockWriteGuard<'a, RawRwLock, ()>, *mut E),
    MutExclusive(MutexGuard<'a, RawMutex, ()>, *mut E),
    OLCReader(Option<(SmartCell<E>, LatchVersion)>),
    OLCWriter(Option<(SmartCell<E>, LatchVersion)>),
}

impl<'a, E: Default + 'static> Clone for SmartGuard<'_, E> {
    fn clone(&self) -> Self {
        match self {
            LockFree(p) => LockFree(*p),
            OLCReader(inner) => OLCReader(inner.clone()),
            _ => unreachable!()
        }
    }
}

impl<'a, E: Default + 'static> SmartGuard<'_, E> {
    #[inline(always)]
    pub fn mark_obsolete(&self) -> bool {
        if let OLCWriter(Some((cell, latch))) = self {
            if let OLCCell(opt) = cell.0.as_ref() {
                opt.write_obsolete(*latch);
                return true;
            }
        }

        false
    }

    #[inline(always)]
    pub const fn is_olc(&self) -> bool {
        match self {
            OLCReader(_) => true,
            OLCWriter(_) => true,
            _ => false
        }
    }

    #[inline(always)]
    pub fn upgrade_write_lock(&mut self) -> bool {
        match self {
            LockFree(_) => true,
            RwWriter(..) => true,
            MutExclusive(..) => true,
            OLCWriter(Some(..)) => true,
            OLCReader(Some((cell, latch))) => {
                if let OLCCell(opt) = cell.0.as_ref() {
                    if let Some(write_latch) = opt.write_lock(*latch) {
                        *self = OLCWriter(Some((cell.clone(), write_latch)));
                        return true;
                    }
                }

                false
            }
            _ => false
        }
    }

    #[inline(always)]
    pub const fn is_write_lock(&self) -> bool {
        match self {
            RwWriter(..) => true,
            MutExclusive(..) => true,
            OLCWriter(Some(..)) => true,
            _ => false
        }
    }

    #[inline(always)]
    pub const fn is_reader_lock(&self) -> bool {
        match self {
            LockFree(..) => true,
            RwReader(..) => true,
            OLCReader(..) => true,
            _ => false
        }
    }

    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        match self {
            OLCReader(Some((cell, latch))) => cell.0
                .is_read_valid(*latch),
            OLCWriter(None) => false,
            OLCReader(None) => false,
            _ => true
        }
    }

    #[inline(always)]
    pub fn is_obsolete(&self) -> bool {
        match self {
            OLCReader(Some((cell, ..))) => cell.0.is_obsolete(),
            OLCWriter(None) => true,
            OLCReader(None) => true,
            _ => false
        }
    }

    #[inline(always)]
    pub fn is_read_not_obsolete(&self) -> bool {
        match self {
            OLCReader(Some((cell, ..))) => cell.0.is_read_not_obsolete(),
            OLCWriter(None) => false,
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
    pub fn is_read_not_obsolete_result(&self) -> (bool, LatchVersion) {
        match self {
            OLCReader(Some((cell, ..))) => cell.0.is_read_not_obsolete_result(),
            OLCWriter(None) => (false, LatchVersion::MIN),
            OLCReader(None) => (false, LatchVersion::MIN),
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
            OLCReader(Some((cell, latch)))
            if cell.0.is_read_valid(*latch) =>
                Some(cell.0.as_ref()),
            OLCWriter(Some((cell, ..))) =>
                Some(cell.0.as_ref()),
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
            OLCWriter(Some((cell, ..))) => Some(cell.0.as_ref()),
            _ => None
        }
    }

    #[inline(always)]
    pub unsafe fn deref_unsafe_static(&self) -> Option<&'static E> {
        match self {
            LockFree(ptr) => ptr.as_ref(),
            RwReader(.., ptr) => ptr.as_ref(),
            RwWriter(.., ptr) => ptr.as_ref(),
            MutExclusive(.., ptr) => ptr.as_ref(),
            OLCReader(Some((cell, ..))) => unsafe {
                mem::transmute(Some(cell.0.as_ref()))
            },
            OLCWriter(Some((cell, ..))) => unsafe {
                mem::transmute(Some(cell.0.as_ref()))
            },
            _ => None
        }
    }

    #[inline(always)]
    pub fn deref_mut(&self) -> Option<&mut E> {
        match self {
            LockFree(ptr) => unsafe { ptr.as_mut() },
            RwWriter(.., ptr) => unsafe { ptr.as_mut() },
            MutExclusive(.., ptr) => unsafe { ptr.as_mut() },
            OLCWriter(Some((cell, ..))) => Some(cell.0.as_mut()),
            _ => None
        }
    }
}

impl<E: Default> SmartCell<E> {
    #[inline(always)]
    pub fn unsafe_borrow(&self) -> &E {
        match self.0.as_ref() {
            ControlCell(cell) => cell.unsafe_borrow(),
            OLCCell(opt) => opt.cell.as_ref()
        }
    }

    #[inline(always)]
    pub fn unsafe_borrow_mut(&self) -> &mut E {
        match self.0.as_ref() {
            ControlCell(cell) => cell.unsafe_borrow_mut(),
            OLCCell(opt) => opt.cell.get_mut()
        }
    }

    #[inline(always)]
    pub fn borrow_free(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            ControlCell(cell) =>
                LockFree(cell.unsafe_borrow_mut()),
            _ => self.borrow_read()
        }
    }

    #[inline(always)]
    pub fn borrow_read(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            ControlCell(cell) => unsafe {
                mem::transmute(RwReader(
                    cell.rwlock.read(),
                    cell.unsafe_borrow(),
                ))
            },
            OLCCell(opt) => {
                let (success, read)
                    = opt.read_lock();

                OLCReader(success.then(|| (self.clone(), read)))
            }
        }
    }

    #[inline(always)]
    pub fn borrow_mut_exclusive(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            ControlCell(cell) => unsafe {
                mem::transmute(MutExclusive(
                    cell.mutex.lock(),
                    cell.unsafe_borrow_mut(),
                ))
            },
            _ => self.borrow_mut()
        }
    }

    #[inline(always)]
    pub fn borrow_mut(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            ControlCell(cell) => unsafe {
                mem::transmute(RwWriter(
                    mem::transmute(cell.rwlock.write()),
                    cell.unsafe_borrow_mut(),
                ))
            },
            OLCCell(opt) => {
                let (success, read)
                    = opt.read_lock();

                if !success {
                    OLCWriter(None)
                } else {
                    if let Some(latched) = opt.write_lock(read) {
                        OLCWriter(Some((self.clone(), latched)))
                    } else {
                        OLCWriter(None)
                    }
                }
            }
        }
    }
}

impl<'a, E: Default> Drop for SmartGuard<'a, E> {
    fn drop(&mut self) {
        if let OLCWriter(Some((cell, write_version))) = self {
            // if *read_version & OBSOLETE_FLAG_VERSION == 0 {
                if let OLCCell(opt) = cell.0.as_ref() {
                    opt.write_unlock(*write_version);
                }
            // }
        }
    }
}

unsafe impl<'a, E: Default + 'a> Sync for SmartGuard<'a, E> {}

unsafe impl<'a, E: Default + 'a> Send for SmartGuard<'a, E> {}