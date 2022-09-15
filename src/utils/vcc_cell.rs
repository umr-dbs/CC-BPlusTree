use std::{hint, mem};
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};
use std::sync::Arc;
use chronicle_db::tools::safe_cell::SafeCell;
use mvcc_bplustree::index::version_info::{AtomicVersion, Version};
use mvcc_bplustree::locking::locking_strategy::Attempts;
use mvcc_bplustree::utils::cc_cell::{CCCell, CCCellGuard};
use crate::utils::vcc_cell::ConcurrentCell::{ConcurrencyControlCell, OptimisticCell};
use crate::utils::vcc_cell::ConcurrentGuard::{ConcurrencyControlGuard, OptimisticGuard};
use crate::utils::vcc_cell::GuardDerefResult::{ReadHolder, Null, Ref, WriteHolder, RefMut};

pub const OBSOLETE_FLAG_VERSION: Version = 0x8_000000000000000;
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

pub(crate) fn sched_yield(attempt: Attempts) {
    if attempt > 3 {
        unsafe { libc::sched_yield(); }
    } else {
        hint::spin_loop();
    }
}

pub type LatchVersion = Version;

// pub const fn num_readers(version: Version) -> Version {
//     (((version << LOCKING_BITS_OFFSET) >> LOCKING_BITS_OFFSET) & MAX_READERS) >> (mem::size_of::<Version>() as Version - READERS_NUM_BITS - LOCKING_BITS_OFFSET)
// }

pub struct OptCell<E: Default> {
    cell: SafeCell<E>,
    cell_version: AtomicVersion,
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

    pub const fn new(data: E) -> Self {
        Self {
            cell: SafeCell::new(data),
            cell_version: AtomicVersion::new(Self::CELL_START_VERSION),
        }
    }

    #[inline(always)]
    fn load_version(&self) -> Version {
        self.cell_version.load(Relaxed)
    }

    fn read_lock(&self) -> (bool, Version) {
        let version = self.load_version();
        if version & WRITE_OBSOLETE_FLAG_VERSION != 0 {
            hint::spin_loop();
            (false, version)
        } else {
            (true, version)
        }
    }

    // fn is_any_valid(&self, v: Version) -> bool {
    //     let load = self.load_version();
    //     v == load && load & OBSOLETE_FLAG_VERSION == 0
    // }

    fn is_read_valid(&self, v: Version) -> bool {
        let load = self.load_version();
        v == load && load & WRITE_OBSOLETE_FLAG_VERSION == 0
    }

    fn write_lock(&self, read_version: Version) -> Option<Version> {
        if read_version & WRITE_OBSOLETE_FLAG_VERSION != 0 {
            return None;
        }

        match self.cell_version.compare_exchange(
            read_version,
            WRITE_FLAG_VERSION | (read_version + 1),
            Acquire,
            Relaxed)
        {
            Ok(..) => Some(WRITE_FLAG_VERSION | (read_version + 1)),
            Err(..) => {
                hint::spin_loop();
                None
            }
        }
    }

    #[inline(always)]
    fn write_unlock(&self, write_version: Version) {
        // if write_version & WRITE_FLAG_VERSION == WRITE_FLAG_VERSION {
            // println!("Dropping {} to {}", write_version, write_version ^ WRITE_FLAG_VERSION);
        debug_assert!(write_version & WRITE_FLAG_VERSION == WRITE_FLAG_VERSION);
        self.cell_version.store(write_version ^ WRITE_FLAG_VERSION, SeqCst)
        // }
    }

    #[inline(always)]
    fn write_obsolete(&self, write_version: Version) {
        debug_assert!(write_version & WRITE_OBSOLETE_FLAG_VERSION == WRITE_FLAG_VERSION);

        self.cell_version.store(OBSOLETE_FLAG_VERSION | write_version, SeqCst);
        // self.cell_version.compare_exchange(
        //     write_version,
        //     OBSOLETE_FLAG_VERSION | write_version,
        //     Acquire,
        //     Relaxed,
        // ).is_ok()
    }

    // pub fn is_obsolete(&self) -> bool {
    //     self.load_version() & OBSOLETE_FLAG_VERSION == OBSOLETE_FLAG_VERSION
    //     // self.cell_version.load(Relaxed) & OBSOLETE_FLAG_VERSION == OBSOLETE_FLAG_VERSION
    // }
    //
    // pub fn is_write(&self) -> bool {
    //     self.load_version() & WRITE_FLAG_VERSION == WRITE_FLAG_VERSION
    // }
}

#[repr(u8)]
pub enum ConcurrentCell<E: Default> {
    ConcurrencyControlCell(Arc<CCCell<E>>),
    OptimisticCell(Arc<OptCell<E>>),
}

impl<E: Default> Clone for ConcurrentCell<E> {
    fn clone(&self) -> Self {
        match self {
            ConcurrencyControlCell(cell) => ConcurrencyControlCell(cell.clone()),
            OptimisticCell(cell) => OptimisticCell(cell.clone())
        }
    }
}

impl<E: Default + Display> Display for ConcurrentCell<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConcurrencyControlCell(cell) =>
                write!(f, "ConcurrencyControlCell(CCCell({}))", cell.unsafe_borrow()),
            OptimisticCell(cell) =>
                write!(f, "OptimisticCell({})", cell)
        }
    }
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
        cell: Option<Arc<OptCell<E>>>,
        guard_deref: GuardDerefResult<'a, E>,
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

impl<'a, E: Default> Drop for ConcurrentGuard<'a, E> {
    fn drop(&mut self) {
        match self {
            OptimisticGuard {
                guard_deref: WriteHolder((cell, latch_version)),
                ..
            } => cell.write_unlock(*latch_version),
            _ => {}
        }
    }
}

#[repr(u8)]
#[derive(Default)]
pub enum GuardDerefResult<'a, E: Default> {
    #[default]
    Null,
    Ref(&'a E),
    RefMut(*mut E),
    ReadHolder((&'a OptCell<E>, LatchVersion)),
    WriteHolder((&'a OptCell<E>, LatchVersion)),
}

impl<'a, E: Default> Clone for GuardDerefResult<'a, E> {
    fn clone(&self) -> Self {
        match self {
            Null => Null,
            Ref(e) => Ref(*e),
            RefMut(e) => RefMut(*e),
            ReadHolder((e, latch_version)) => ReadHolder((*e, *latch_version)),
            WriteHolder((cell, latch_version)) => WriteHolder((*cell, *latch_version))
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

    pub const fn is_reader(&self) -> bool {
        match self {
            Ref(_) => true,
            ReadHolder(_) => true,
            _ => false,
        }
    }

    pub const fn is_mut_optimistic(&self) -> bool {
        match self {
            WriteHolder(..) => true,
            _ => false
        }
    }

    pub const fn is_null(&self) -> bool {
        match self {
            Null => true,
            _ => false
        }
    }

    pub fn can_mut(&self) -> bool {
        match self {
            RefMut(_) => true,
            WriteHolder(_) => true,
            ReadHolder((cell, latch_version)) => cell.is_read_valid(*latch_version),
            _ => false,
        }
    }

    pub fn as_ref(&self) -> Option<&E> {
        match self {
            Ref(e) => Some(e),
            RefMut(e) => unsafe { e.as_ref() },
            WriteHolder((e, _)) => Some(e.cell.deref()),
            ReadHolder((e, latch_version)) if e.is_read_valid(*latch_version) =>
                Some(e.cell.deref()),
            _ => None
        }
    }

    pub fn assume_mut(&self) -> Option<&'a mut E> {
        match self {
            RefMut(e) => unsafe { e.as_mut() },
            WriteHolder((e, _)) => Some(e.cell.get_mut()),
            _ => None
        }
    }

    fn force_mut(&mut self) -> Option<&'a mut E> {
        self.assume_mut().or_else(|| match self {
            ReadHolder((cell, latch_version)) =>
                match cell.write_lock(*latch_version) {
                    Some(latch_version) => {
                        *self = WriteHolder((*cell, latch_version));
                        self.assume_mut()
                    }
                    _ => None
                }
            _ => None
        })
    }

    // pub fn is_valid(&self) -> bool {
    //     match self {
    //         Ref(..) => true,
    //         RefMut(..) => true,
    //         WriteHolder(..) => true,
    //         ReadHolder((e, latch_version)) if e.is_any_valid(*latch_version) => true,
    //         _ => false
    //     }
    // }

    pub fn mark_obsolete(&self) {
        match self {
            WriteHolder((cell, latch_version)) => cell
                .write_obsolete(*latch_version),
            _ => {}
        }
    }
}

// impl<'a, E: Default + Clone + 'a> ConcurrentGuard<'a, E> {
//     pub(crate) fn data_copy(&self) -> Option<E> {
//         match self {
//             ConcurrencyControlGuard { guard, .. } => Some(guard.deref().clone()),
//             OptimisticGuard { cell: Some(cell), .. } => {
//                 let result
//                     = Some(cell.cell.deref().clone());
//
//                 if self.is_valid() {
//                     result
//                 }
//                 else {
//                     None
//                 }
//             },
//             _ => None
//         }
//     }
// }

impl<'a, E: Default + 'a> ConcurrentGuard<'a, E> {
    // pub(crate) fn data(&self) -> Option<&'a E> {
    //     match self {
    //         ConcurrencyControlGuard {
    //             cell,
    //             ..
    //         } => Some(unsafe { mem::transmute(cell.unsafe_borrow()) }),
    //         OptimisticGuard {
    //             cell: Some(cell),
    //             ..
    //         } if self.is_valid() => Some(unsafe { mem::transmute(cell.cell.deref()) }),
    //         _ => None
    //     }
    // }

    // pub(crate) fn refresh(&mut self) {
    //     match self {
    //         ConcurrencyControlGuard {
    //             cell,
    //             guard
    //         } => *guard = unsafe {
    //             mem::transmute(match mem::take(guard) {
    //                 CCCellGuard::LockFree(_) =>
    //                     CCCellGuard::LockFree(cell.unsafe_borrow_mut()),
    //                 CCCellGuard::Reader(rl, _) =>
    //                     CCCellGuard::Reader(rl, cell.unsafe_borrow()),
    //                 CCCellGuard::Writer(wl, _) =>
    //                     CCCellGuard::Writer(wl, cell.unsafe_borrow_mut()),
    //                 CCCellGuard::Exclusive(el, _) =>
    //                     CCCellGuard::Exclusive(el, cell.unsafe_borrow_mut())
    //             })
    //         },
    //         OptimisticGuard {
    //             cell,
    //             guard_deref
    //         } => {
    //             *guard_deref = unsafe {
    //                 mem::transmute(match mem::take(guard_deref) {
    //                     Ref(_) => Ref(cell.as_ref().unwrap().cell.as_ref()),
    //                     RefMut(_) => RefMut(cell.as_ref().unwrap().cell.get_mut()),
    //                     ReadHolder((_, rl)) if cell.as_ref().unwrap().is_read_valid(rl) =>
    //                         ReadHolder((cell.as_ref().unwrap().as_ref(), rl)),
    //                     WriteHolder((_, wl)) => WriteHolder((cell.as_ref().unwrap(), wl)),
    //                     _ => Null,
    //                 })
    //             }
    //         }
    //     }
    // }

    pub fn upgrade_write_lock(&mut self) -> bool {
        match self {
            ConcurrencyControlGuard { guard, .. } => guard
                .is_write_lock(),
            OptimisticGuard { guard_deref, .. } => guard_deref
                .force_mut()
                .is_some()
        }
    }

    pub fn is_valid(&self) -> bool {
        match self {
            ConcurrencyControlGuard { .. } => true,
            OptimisticGuard {
                cell: Some(..),
                guard_deref: ReadHolder((cell, latch_version))
            } => cell.is_read_valid(*latch_version),
            OptimisticGuard {
                guard_deref: Null,
                ..
            } => false,
            _ => true
        }
    }

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
                guard_deref: guard,
                ..
            } => guard.is_mut()
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
                guard_deref: guard,
                ..
            } => !guard.is_mut()
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
                guard_deref: guard,
                ..
            } => guard.is_mut()
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
                ..
            } => false
        }
    }

    // pub fn cell(&self) -> Option<ConcurrentCell<E>> {
    //     match self {
    //         ConcurrencyControlGuard { cell, .. } =>
    //             Some(ConcurrencyControlCell(cell.clone())),
    //         OptimisticGuard { cell: Some(cell), .. } =>
    //             Some(OptimisticCell(cell.clone())),
    //         _ => None
    //     }
    // }

    pub fn guard_result(&self) -> GuardDerefResult<'a, E> {
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
                guard_deref: ReadHolder((cell, latch_version)),
                ..
            } => if cell.is_read_valid(*latch_version) {
                ReadHolder((*cell, *latch_version))
            } else {
                Null
            }
            OptimisticGuard { guard_deref, .. } => guard_deref.clone(),
        }
    }

    // pub fn try_deref_mut(&self) -> GuardDerefResult<'a, E> {
    //     match self {
    //         ConcurrencyControlGuard { guard, .. } => match guard {
    //             CCCellGuard::LockFree(e) => unsafe {
    //                 let p: *mut *mut E = mem::transmute(e);
    //                 RefMut(*p)
    //             },
    //             CCCellGuard::Writer(_, e) => unsafe {
    //                 let p: *mut *mut E = mem::transmute(e);
    //                 RefMut(*p)
    //             },
    //             CCCellGuard::Exclusive(_, e) => unsafe {
    //                 let p: *mut *mut E = mem::transmute(e);
    //                 RefMut(*p)
    //             },
    //             CCCellGuard::Reader(..) => Null,
    //         }
    //         OptimisticGuard { guard_deref: guard, .. } => guard.clone(),
    //     }
    // }
}

impl<'a, E: Default + 'a> ConcurrentCell<E> {
    pub fn new_optimistic(data: E) -> Self {
        OptimisticCell(Arc::new(OptCell::new(data)))
    }

    pub fn new_concurrent(data: E) -> Self {
        ConcurrencyControlCell(Arc::new(CCCell::new(data)))
    }

    #[inline(always)]
    fn borrow_optimistic_reader(&self) -> ConcurrentGuard<'_, E> {
        match self {
            OptimisticCell(cell) => {
                let (can_read, read_version)
                    = cell.read_lock();

                match can_read {
                    false => OptimisticGuard {
                        cell: None,
                        guard_deref: Null,
                    },
                    true => OptimisticGuard {
                        cell: Some(cell.clone()),
                        guard_deref: ReadHolder((cell.as_ref(), read_version)),
                    }
                }
            }
            _ => unreachable!("Bruhh.. this aint no optimistic guard here dam!!")
        }
    }

    #[inline(always)]
    fn borrow_optimistic_writer(&self) -> ConcurrentGuard<'_, E> {
        match self {
            OptimisticCell(cell) => {
                let (can_read, read_version)
                    = cell.read_lock();

                match can_read {
                    false => OptimisticGuard {
                        cell: None,
                        guard_deref: Null,
                    },
                    true => match cell.write_lock(read_version) {
                        None => OptimisticGuard {
                            cell: None,
                            guard_deref: Null,
                        },
                        Some(latch_version) => OptimisticGuard {
                            cell: Some(cell.clone()),
                            guard_deref: WriteHolder((cell.as_ref(), latch_version)),
                        }
                    }
                }
            }
            _ => unreachable!("Bruhh.. this aint no optimistic guard here dam!!")
        }
    }

    pub fn borrow_free(&self) -> ConcurrentGuard<'_, E> {
        match self {
            ConcurrencyControlCell(cell) => ConcurrentGuard::boxed(
                cell.clone(),
                unsafe { mem::transmute(cell.borrow_free()) }),
            _ => self.borrow_optimistic_reader(),
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
            _ => self.borrow_optimistic_reader(),
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
            _ => self.borrow_optimistic_writer()
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
            _ => self.borrow_optimistic_writer(),
        }
    }

    /// Exclusive write access.
    pub fn borrow_mut_exclusive_static(&self) -> ConcurrentGuard<'static, E> {
        unsafe { mem::transmute(self.borrow_mut_exclusive()) }
    }
}