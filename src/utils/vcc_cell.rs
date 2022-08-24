use std::cell::Cell;
use std::{hint, mem};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};
use std::sync::{Arc, Weak};
use chronicle_db::tools::safe_cell::SafeCell;
use mvcc_bplustree::index::version_info::{AtomicVersion, Version};
use mvcc_bplustree::utils::cc_cell::{CCCell, CCCellGuard};
use crate::index::node::NodeRef;
use crate::utils::vcc_cell::ConcurrentCell::{ConcurrencyControlCell, OptimisticCell};
use crate::utils::vcc_cell::ConcurrentGuard::{ConcurrencyControlGuard, OptimisticGuard};
use crate::utils::vcc_cell::GuardDerefResult::{ReadHolder, Null, Ref, WriteHolder, RefMut};

const OBSOLETE_FLAG_VERSION: Version = 0x8_000000000000000;
const WRITE_FLAG_VERSION: Version = 0x4_000000000000000;
const WRITE_OBSOLETE_FLAG_VERSION: Version = 0xC_000000000000000;
const READ_FLAG_VERSION: Version = 0x0_000000000000000;
// const LOCK_FREE_FLAG_VERSION: Version = 0x00_00_00_00_00_00_00_00;
// const LOCKING_FLAG_VERSION: Version = OBSOLETE_FLAG_VERSION;

// const READERS_NUM_BITS: Version = 6 + 8;
// 0xC_0 + 0x_00 free bits
// const MAX_READERS: Version = (1 << READERS_NUM_BITS) - 1;
// const LOCKING_BITS_OFFSET: Version = 2;
// const VERSIONING_COUNTER_BITS: Version = (8 * mem::size_of::<Version>() as Version) - READERS_NUM_BITS - LOCKING_BITS_OFFSET;

pub type ConcurrencyCell<E> = CCCell<E>;
pub type LatchVersion = Version;

// pub const fn num_readers(version: Version) -> Version {
//     (((version << LOCKING_BITS_OFFSET) >> LOCKING_BITS_OFFSET) & MAX_READERS) >> (mem::size_of::<Version>() as Version - READERS_NUM_BITS - LOCKING_BITS_OFFSET)
// }

pub struct OptCell<E: Default> {
    cell: SafeCell<E>,
    cell_version: AtomicVersion,
}

impl<E: Default> Default for OptCell<E> {
    fn default() -> Self {
        Self::new(E::default())
    }
}

impl<E: Default> OptCell<E> {
    const CELL_START_VERSION: Version = 0;

    pub fn new(data: E) -> Self {
        Self {
            cell: SafeCell::new(data),
            cell_version: AtomicVersion::new(Self::CELL_START_VERSION),
        }
    }

    fn load_version(&self) -> Version {
        self.cell_version.load(Relaxed)
    }

    fn read_lock(&self) -> (bool, Version) {
        let version = self.cell_version.load(Relaxed);
        if version & WRITE_OBSOLETE_FLAG_VERSION != 0 {
            hint::spin_loop();
            (false, version)
        } else {
            (true, version)
        }
    }

    fn is_read_valid(&self, v: Version) -> bool {
        debug_assert!(v & WRITE_OBSOLETE_FLAG_VERSION == 0);

        v == self.cell_version.load(Relaxed)
    }

    fn write_lock(&self, read_version: Version) -> Option<Version> {
        debug_assert!(read_version & WRITE_OBSOLETE_FLAG_VERSION == 0);

        match self.cell_version.compare_exchange(
            read_version,
            WRITE_FLAG_VERSION | (read_version + 1),
            Acquire,
            Relaxed)
        {
            Ok(version) => Some(version),
            Err(..) => {
                hint::spin_loop();
                None
            }
        }
    }

    fn write_unlock(&self, write_version: Version) {
        debug_assert!(write_version & WRITE_OBSOLETE_FLAG_VERSION == WRITE_FLAG_VERSION);

        let flag = write_version ^ WRITE_FLAG_VERSION;
        self.cell_version.store(flag | READ_FLAG_VERSION, SeqCst)
    }

    pub fn write_obsolete_unlock(&self) {
        self.cell_version.store(WRITE_OBSOLETE_FLAG_VERSION, SeqCst)
    }
}

#[repr(u8)]
#[derive(Clone)]
pub enum ConcurrentCell<E: Default> {
    ConcurrencyControlCell(Arc<CCCell<E>>),
    OptimisticCell(Arc<OptCell<E>>),
}

impl<E: Default> Default for ConcurrentCell<E> {
    fn default() -> Self {
        ConcurrencyControlCell(Arc::new(CCCell::default()))
    }
}

impl<E: Default> Into<ConcurrentCell<E>> for Arc<CCCell<E>> {
    fn into(self) -> ConcurrentCell<E> {
        ConcurrencyControlCell(self)
    }
}

impl<E: Default> Into<ConcurrentCell<E>> for Arc<OptCell<E>> {
    fn into(self) -> ConcurrentCell<E> {
        OptimisticCell(self)
    }
}

unsafe impl<E: Default> Sync for ConcurrentCell<E> {}

unsafe impl<E: Default> Send for ConcurrentCell<E> {}

#[repr(u8)]
pub enum ConcurrentGuard<'a, E: Default + 'a> {
    ConcurrencyControlGuard {
        cell: Arc<CCCell<E>>,
        guard: CCCellGuard<'a, E>,
    },
    OptimisticGuard {
        cell: Weak<OptCell<E>>,
        latch_version: Cell<LatchVersion>,
    },
}

unsafe impl<'a, E: Default + 'a> Sync for ConcurrentGuard<'a, E> {}

unsafe impl<'a, E: Default + 'a> Send for ConcurrentGuard<'a, E> {}

impl<'a, E: Default> Default for ConcurrentGuard<'a, E> {
    fn default() -> Self {
        ConcurrencyControlGuard {
            cell: Arc::new(CCCell::default()),
            guard: CCCellGuard::default(),
        }
    }
}

#[repr(u8)]
#[derive(Default)] // Idea: Maybe add invariant here, instead of separation?
pub enum GuardDerefResult<'a, E: Default> {
    #[default]
    Null,
    Ref(&'a E),
    RefMut(*mut E),
    ReadHolder((Arc<OptCell<E>>, LatchVersion)),
    WriteHolder(Arc<OptCell<E>>),
}

impl<'a, E: Default> Drop for GuardDerefResult<'a, E> {
    fn drop(&mut self) {
        match self {
            WriteHolder(cell) => cell.write_unlock(cell.load_version()),
            _ => {}
        }
    }
}

impl<'a, E: Default> GuardDerefResult<'a, E> {
    pub const fn is_mut(&self) -> bool {
        match self {
            RefMut(_) => true,
            WriteHolder(_) => true,
            _ => false,
        }
    }

    pub const fn can_mut(&self) -> bool {
        match self {
            RefMut(_) => true,
            WriteHolder(_) => true,
            ReadHolder(..) => true,
            _ => false,
        }
    }

    pub fn as_ref(&self) -> Option<&E> {
        match self {
            Ref(e) => Some(e),
            RefMut(e) => unsafe { e.as_ref() },
            WriteHolder(e) => Some(e.cell.deref()),
            ReadHolder((e, latch_version)) if e.is_read_valid(*latch_version) =>
                Some(e.cell.deref()),
            _ => None
        }
    }

    pub fn assume_mut(&self) -> Option<&'a mut E> { // note: illegal unboxing NOT allowed, or invariant is broken!!
        match self {
            RefMut(e) => unsafe { e.as_mut() },
            WriteHolder(e) => unsafe { mem::transmute(Some(e.cell.get_mut())) },
            _ => None
        }
    }

    pub fn force_mut(&mut self) -> Option<&'a mut E> {
        self.assume_mut().or_else(|| match self {
            ReadHolder((cell, latch_version)) => match cell.write_lock(*latch_version) {
                Some(..) => {
                    *self = WriteHolder(mem::take(cell));
                    self.assume_mut()
                },
                _ => None
            }
            _ => None
        })
    }

    pub fn is_valid(&self) -> bool {
        match self {
            Ref(..) => true,
            RefMut(..) => true,
            WriteHolder(..) => true,
            ReadHolder((e, latch_version)) if e.is_read_valid(*latch_version) => true,
            _ => false
        }
    }
}

impl<'a, E: Default + 'a> ConcurrentGuard<'a, E> {
    #[inline(always)]
    pub const fn boxed(cell: Arc<CCCell<E>>, guard: CCCellGuard<'a, E>) -> Self {
        ConcurrencyControlGuard {
            guard,
            cell,
        }
    }

    /// Returns true, if the CCCellGuard is locked via mutex or rwlock write lock.
    /// Returns false, otherwise.
    #[inline]
    pub fn is_write_lock(&self) -> bool {
        match self {
            ConcurrencyControlGuard {
                guard,
                ..
            } => guard.is_write_lock(),
            OptimisticGuard {
                latch_version,
                ..
            } => latch_version.get() & WRITE_FLAG_VERSION == WRITE_FLAG_VERSION
        }
    }

    /// Returns true, if the CCCellGuard is locked via rwlock read lock.
    /// Returns false, otherwise.
    #[inline]
    pub fn is_reader_lock(&self) -> bool {
        match self {
            ConcurrencyControlGuard {
                guard,
                ..
            } => guard.is_reader_lock(),
            OptimisticGuard {
                latch_version,
                ..
            } => latch_version.get() & WRITE_FLAG_VERSION == 0
        }
    }

    /// Returns true, if the CCCellGuard is locked via mutex exclusive lock.
    /// Returns false, otherwise.
    #[inline]
    pub fn is_exclusive_lock(&self) -> bool {
        match self {
            ConcurrencyControlGuard {
                guard,
                ..
            } => guard.is_exclusive_lock(),
            OptimisticGuard {
                latch_version,
                ..
            } => latch_version.get() & WRITE_FLAG_VERSION == WRITE_FLAG_VERSION
        }
    }

    /// Returns true, if the CCCellGuard is locked no lock.
    /// Returns false, otherwise.
    #[inline]
    pub fn is_lock_free_lock(&self) -> bool {
        match self {
            ConcurrencyControlGuard {
                guard,
                ..
            } => guard.is_lock_free_lock(),
            OptimisticGuard {
                latch_version,
                ..
            } => latch_version.get() & WRITE_FLAG_VERSION == 0
        }
    }

    pub fn is_valid(&self) -> bool {
        match self {
            ConcurrencyControlGuard { .. } => true,
            OptimisticGuard {
                cell,
                latch_version
            } => cell.upgrade()
                .map_or(false, |adult| adult.is_read_valid(latch_version.get()))
        }
    }

    pub fn latch_version(&self) -> Option<Version> {
        match self {
            OptimisticGuard {
                latch_version,
                ..
            } => latch_version.get().into(),
            _ => None,
        }
    }

    // pub fn unbox(self) -> Result<CCCellGuard<'a, E>, ConcurrentGuard<'a, E>> {
    //     match self {
    //         ConcurrencyControlGuard { guard, .. } => Ok(guard),
    //         _ => Err(self)
    //     }
    // }

    pub fn try_deref(&self) -> GuardDerefResult<'a, E> {
        match self {
            ConcurrencyControlGuard { guard, .. } => match guard {
                CCCellGuard::Reader(_, e) => Ref(*e),
                CCCellGuard::LockFree(e) => unsafe {
                    let p: *mut *mut E = mem::transmute(e);
                    RefMut(*p)
                }
                CCCellGuard::Writer(_, e) => unsafe {
                    let p: *mut *mut E = mem::transmute(e);
                    RefMut(*p)
                },
                CCCellGuard::Exclusive(_, e) => unsafe {
                    let p: *mut *mut E = mem::transmute(e);
                    RefMut(*p)
                },
            },
            OptimisticGuard {
                cell,
                latch_version
            } => match cell.upgrade() {
                Some(adult) => {
                    let (can_read, read_version)
                        = adult.read_lock();

                    if !can_read || read_version != latch_version.get() {
                        Null
                    } else {
                        ReadHolder((adult, read_version))
                    }
                }
                _ => Null
            }
        }
    }

    pub fn try_deref_mut(&mut self) -> GuardDerefResult<'a, E> {
        match self {
            ConcurrencyControlGuard { guard, .. } => match guard {
                CCCellGuard::LockFree(e) => RefMut(*e),
                CCCellGuard::Writer(_, e) => RefMut(*e),
                CCCellGuard::Exclusive(_, e) => RefMut(*e),
                CCCellGuard::Reader(..) => Null,
            }
            OptimisticGuard {
                cell,
                latch_version
            } => match cell.upgrade() {
                Some(adult) => {
                    let (can_read, read_version)
                        = adult.read_lock();

                    if !can_read || read_version != latch_version.get() {
                        Null
                    } else {
                        match adult.write_lock(read_version) {
                            Some(write_version) => {
                                latch_version.replace(write_version);
                                WriteHolder(adult)
                            }
                            _ => Null
                        }
                    }
                }
                _ => Null
            }
        }
    }
}

impl<'a, E: Default + 'a> ConcurrentCell<E> {
    pub fn new_optimistic(data: E) -> Self {
        OptimisticCell(Arc::new(OptCell::new(data)))
    }

    pub fn new_concurrent(data: E) -> Self {
        ConcurrencyControlCell(Arc::new(CCCell::new(data)))
    }

    #[inline(always)]
    fn as_optimistic_guard(&self) -> ConcurrentGuard<'a, E> {
        match self {
            OptimisticCell(cell) => OptimisticGuard {
                cell: Arc::downgrade(cell),
                latch_version: Cell::new(cell.load_version()),
            },
            _ => unreachable!("Bruhh.. this aint no optimistic guard here dam!!")
        }
    }

    pub fn borrow_free(&self) -> ConcurrentGuard<'a, E> {
        match self {
            ConcurrencyControlCell(cell) => ConcurrentGuard::boxed(
                cell.clone(),
                unsafe { mem::transmute(cell.borrow_free()) }),
            _ => self.as_optimistic_guard(),
        }
    }


    pub fn borrow_free_static(&self) -> ConcurrentGuard<'static, E> {
        unsafe {
            mem::transmute(self.borrow_free())
        }
    }

    /// Read access.
    pub fn borrow_read(&self) -> ConcurrentGuard<'_, E> {
        match self {
            ConcurrencyControlCell(cell) => ConcurrentGuard::boxed(
                cell.clone(),
                cell.borrow_read()),
            _ => self.as_optimistic_guard(),
        }
    }

    pub fn borrow_read_static(&self) -> ConcurrentGuard<'static, E> {
        unsafe { mem::transmute(self.borrow_read()) }
    }

    /// Lock-less access.
    pub fn unsafe_borrow(&self) -> &E {
        match self {
            ConcurrencyControlCell(cell) => cell.unsafe_borrow(),
            OptimisticCell(cell) => cell.cell.deref()
        }
    }

    /// Lock-less read access via static life-time.
    pub fn unsafe_borrow_static(&self) -> &'static E {
        unsafe { mem::transmute(self.unsafe_borrow()) }
    }

    /// Lock-less write access via static life-time.
    pub fn unsafe_borrow_mut(&self) -> &mut E {
        match self {
            ConcurrencyControlCell(cell) => cell.unsafe_borrow_mut(),
            OptimisticCell(cell) => cell.cell.get_mut()
        }
    }

    /// Lock-less write access via static life-time.
    pub fn unsafe_borrow_mut_static(&self) -> &'static mut E {
        unsafe { mem::transmute(self.unsafe_borrow_mut()) }
    }

    /// Write access.
    pub fn borrow_mut(&self) -> ConcurrentGuard<'_, E> {
        match self {
            ConcurrencyControlCell(cell) => ConcurrentGuard::boxed(
                cell.clone(),
                cell.borrow_mut()),
            _ => self.as_optimistic_guard(),
        }
    }

    /// Write access via static life-time.
    pub fn borrow_mut_static(&self) -> ConcurrentGuard<'static, E> {
        unsafe { mem::transmute(self.borrow_mut()) }
    }


    /// Exclusive write access.
    pub fn borrow_mut_exclusive(&self) -> ConcurrentGuard<'_, E> {
        match self {
            ConcurrencyControlCell(cell) => ConcurrentGuard::boxed(
                cell.clone(),
                cell.borrow_mut_exclusive()),
            _ => self.as_optimistic_guard(),
        }
    }

    /// Exclusive write access.
    pub fn borrow_mut_exclusive_static(&self) -> ConcurrentGuard<'static, E> {
        unsafe { mem::transmute(self.borrow_mut_exclusive()) }
    }
}