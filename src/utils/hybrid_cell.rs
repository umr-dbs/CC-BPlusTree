use std::{hint, mem};
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};
use std::sync::Arc;
use crate::utils::hybrid_cell::HybridCell::{ConcurrencyControlCell, OptimisticCell};
use crate::utils::hybrid_cell::ConcurrentGuard::{ConcurrencyControlGuard, OptimisticGuard};
use crate::utils::hybrid_cell::GuardDerefResult::{ReadHolder, Null, Ref, WriteHolder, RefMut};
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use crate::record_model::{AtomicVersion, Version};
// use crate::record_model::{AtomicVersion, Version};
use crate::utils::cc_cell::{CCCell, CCCellGuard};
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::{LatchVersion, OptCell};

pub enum HybridCell<E: Default> {
    ConcurrencyControlCell(Arc<CCCell<E>>),
    OptimisticCell(Arc<OptCell<E>>),
}

impl<E: Default> Clone for HybridCell<E> {
    #[inline(always)]
    fn clone(&self) -> Self {
        match self {
            ConcurrencyControlCell(cell) => ConcurrencyControlCell(cell.clone()),
            OptimisticCell(cell) => OptimisticCell(cell.clone())
        }
    }
}

impl<E: Default + Display> Display for HybridCell<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConcurrencyControlCell(cell) =>
                write!(f, "ConcurrencyControlCell(CCCell({}))", cell.unsafe_borrow()),
            OptimisticCell(cell) =>
                write!(f, "OptimisticCell({})", cell.cell.get_mut())
        }
    }
}

impl<E: Default> Default for HybridCell<E> {
    fn default() -> Self {
        ConcurrencyControlCell(Arc::new(CCCell::default()))
    }
}

impl<E: Default> Into<HybridCell<E>> for Arc<CCCell<E>> {
    fn into(self) -> HybridCell<E> {
        ConcurrencyControlCell(self)
    }
}

impl<E: Default> Into<HybridCell<E>> for Arc<OptCell<E>> {
    fn into(self) -> HybridCell<E> {
        OptimisticCell(self)
    }
}

unsafe impl<E: Default> Sync for HybridCell<E> {}

unsafe impl<E: Default> Send for HybridCell<E> {}

pub enum ConcurrentGuard<'a, E: Default + 'a> {
    ConcurrencyControlGuard {
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
            guard: CCCellGuard::default(),
        }
    }
}

impl<'a, E: Default> Drop for ConcurrentGuard<'a, E> {
    fn drop(&mut self) {
        match self {
            OptimisticGuard {
                guard_deref: WriteHolder(cell, latch_version),
                ..
            } => cell.write_unlock(*latch_version),
            _ => {}
        }
    }
}

#[derive(Default)]
pub enum GuardDerefResult<'a, E: Default> {
    #[default]
    Null,
    Ref(&'a E),
    RefMut(*mut E),
    ReadHolder(&'a OptCell<E>, LatchVersion),
    WriteHolder(&'a OptCell<E>, LatchVersion),
}

impl<'a, E: Default> Clone for GuardDerefResult<'a, E> {
    #[inline(always)]
    fn clone(&self) -> Self {
        match self {
            Null =>
                Null,
            Ref(e) =>
                Ref(*e),
            RefMut(e) =>
                RefMut(*e),
            ReadHolder(e, latch_version) =>
                ReadHolder(*e, *latch_version),
            WriteHolder(cell, latch_version) =>
                WriteHolder(*cell, *latch_version)
        }
    }
}

impl<'a, E: Default> GuardDerefResult<'a, E> {
    #[inline(always)]
    pub const fn is_mut(&self) -> bool {
        match self {
            RefMut(..) => true,
            WriteHolder(..) => true,
            _ => false,
        }
    }

    #[inline(always)]
    pub const fn is_reader(&self) -> bool {
        match self {
            Ref(..) => true,
            ReadHolder(..) => true,
            _ => false,
        }
    }

    #[inline(always)]
    pub const fn is_mut_optimistic(&self) -> bool {
        match self {
            WriteHolder(..) => true,
            _ => false
        }
    }

    #[inline(always)]
    pub const fn is_null(&self) -> bool {
        match self {
            Null => true,
            _ => false
        }
    }

    #[inline(always)]
    pub fn can_mut(&self) -> bool {
        match self {
            RefMut(..) => true,
            WriteHolder(..) => true,
            ReadHolder(cell, latch_version) =>
                cell.is_read_valid(*latch_version),
            _ => false,
        }
    }

    #[inline(always)]
    pub fn as_ref(&self) -> Option<&E> {
        match self {
            Ref(e) =>
                Some(e),
            RefMut(e) =>
                unsafe { e.as_ref() },
            WriteHolder(e, _) =>
                Some(e.cell.deref()),
            ReadHolder(e, latch_version) if e.is_read_valid(*latch_version) =>
                Some(e.cell.deref()),
            _ => None
        }
    }

    #[inline(always)]
    fn latch_version(&self) -> Option<LatchVersion> {
        match self {
            ReadHolder(.., latch_version) => Some(*latch_version),
            WriteHolder(.., latch_version) => Some(*latch_version),
            _ => None
        }
    }

    #[inline(always)]
    pub unsafe fn as_reader(&self) -> Option<&E> {
        match self {
            Ref(e) =>
                Some(e),
            RefMut(e) =>
                e.as_ref(),
            WriteHolder(e, _) =>
                Some(e.cell.deref()),
            ReadHolder(e, ..) =>
                Some(e.cell.deref()),
            _ => None
        }
    }

    #[inline(always)]
    pub fn assume_mut(&self) -> Option<&'a mut E> {
        match self {
            RefMut(e) =>
                unsafe { e.as_mut() },
            WriteHolder(e, ..) =>
                Some(e.cell.get_mut()),
            _ => None
        }
    }

    // pub unsafe fn as_mut(&self) -> &'a mut E {
    //     match self {
    //         RefMut(e) => &mut **e,
    //         WriteHolder((e, _)) => e.cell.get_mut(),
    //         _ => unreachable!("Sleepy joe hit me -> .as_mut() on non-mut GuardDerefResult")
    //     }
    // }

    #[inline(always)]
    fn force_mut(&mut self) -> Option<&'a mut E> {
        self.assume_mut().or_else(|| match self {
            ReadHolder(cell, latch_version) =>
                match cell.write_lock(*latch_version) {
                    Some(latch_version) => {
                        *self = WriteHolder(*cell, latch_version);
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

    #[inline(always)]
    pub fn mark_obsolete(&self) {
        match self {
            WriteHolder(cell, latch_version) =>
                cell.write_obsolete(*latch_version),
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
    #[inline(always)]
    pub(crate) fn guard_latch_version(&self) -> Option<LatchVersion> {
        match self {
            OptimisticGuard {
                guard_deref,
                ..
            } => guard_deref.latch_version(),
            _ => None
        }
    }

    #[inline(always)]
    pub(crate) fn cell_version(&self) -> Option<Version> {
        match self {
            OptimisticGuard {
                cell: Some(cell),
                ..
            } => Some(cell.load_version()),
            _ => None
        }
    }

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
    //
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

    #[inline(always)]
    pub fn upgrade_write_lock(&mut self) -> bool {
        match self {
            ConcurrencyControlGuard { guard, .. } => guard
                .is_write_lock(),
            OptimisticGuard { guard_deref, .. } => guard_deref
                .force_mut()
                .is_some()
        }
    }

    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        match self {
            ConcurrencyControlGuard { .. } => true,
            OptimisticGuard {
                cell: Some(..),
                guard_deref: ReadHolder(cell, latch_version)
            } => cell.is_read_valid(*latch_version),
            OptimisticGuard {
                guard_deref: Null,
                ..
            } => false,
            _ => true
        }
    }

    // #[inline(always)]
    // pub const fn boxed(cell: Arc<CCCell<E>>, guard: CCCellGuard<'a, E>) -> Self {
    //     ConcurrencyControlGuard {
    //         guard,
    //         cell,
    //     }
    // }
    /// Returns true, if the CCCellGuard is locked via mutex or rwlock write lock.
    /// Returns false, otherwise.
    #[inline(always)]
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
    #[inline(always)]
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
    #[inline(always)]
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
    #[inline(always)]
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

    // pub unsafe fn as_reader(&self) -> &'a E {
    //     mem::transmute(match self {
    //         ConcurrencyControlGuard { guard, .. } => match guard {
    //             CCCellGuard::Reader(_, e) => *e,
    //             CCCellGuard::LockFree(e) => *e,
    //             CCCellGuard::Writer(_, e) => *e,
    //             CCCellGuard::Exclusive(_, e) => *e,
    //         },
    //         OptimisticGuard {
    //             cell: Some(cell),
    //             ..
    //         } => cell.cell.as_ref(),
    //         _ => unreachable!("Sleepy Joe hit me -> guard_as_reader on invalid guard!")
    //     })
    // }

    // pub unsafe fn as_mut(&self) -> &'a mut E {
    //     mem::transmute(match self {
    //         ConcurrencyControlGuard { guard, .. } => match guard {
    //             CCCellGuard::LockFree(e) => {
    //                 let p: *const *mut E = mem::transmute(e);
    //                 *p
    //             },
    //             CCCellGuard::Writer(_, e) => {
    //                 let p: *const *mut E = mem::transmute(e);
    //                 *p
    //             },
    //             CCCellGuard::Exclusive(_, e) => {
    //                 let p: *const *mut E = mem::transmute(e);
    //                 *p
    //             },
    //             _ => unreachable!("Sleepy joe hit me -> .as_mut() on invalid guard!")
    //         },
    //         OptimisticGuard {
    //             cell: Some(cell),
    //             ..
    //         } => cell.cell.get_mut(),
    //         _ => unreachable!("Sleepy Joe hit me -> guard_as_reader on invalid guard!")
    //     })
    // }

    #[inline(always)]
    pub unsafe fn guard_result_reader(&self) -> GuardDerefResult<'a, E> {
        match self {
            ConcurrencyControlGuard { guard, .. } => match guard {
                CCCellGuard::Reader(_, e) => Ref(*e),
                CCCellGuard::LockFree(e) => {
                    let p: *mut *mut E = mem::transmute(e);
                    RefMut(*p)
                }
                CCCellGuard::Writer(_, e) => {
                    let p: *mut *mut E = mem::transmute(e);
                    RefMut(*p)
                }
                CCCellGuard::Exclusive(_, e) => {
                    let p: *mut *mut E = mem::transmute(e);
                    RefMut(*p)
                }
            },
            OptimisticGuard {
                cell: Some(cell),
                ..
            } => Ref(mem::transmute(cell.as_ref())),
            _ => Null
        }
    }

    #[inline(always)]
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
                guard_deref: ReadHolder(cell, latch_version),
                ..
            } => if cell.is_read_valid(*latch_version) {
                ReadHolder(*cell, *latch_version)
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

impl<'a, E: Default + 'a> HybridCell<E> {
    #[inline(always)]
    pub fn new_optimistic(data: E) -> Self {
        OptimisticCell(Arc::new(OptCell::new(data)))
    }

    #[inline(always)]
    pub fn new_concurrent(data: E) -> Self {
        ConcurrencyControlCell(Arc::new(CCCell::new(data)))
    }

    #[inline(always)]
    pub fn degrade(&mut self) -> Result<&Arc<CCCell<E>>, &Arc<OptCell<E>>> {
        match self {
            ConcurrencyControlCell(inner) => Ok(inner),
            OptimisticCell(inner) => Err(inner)
        }
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
                        guard_deref: ReadHolder(cell.as_ref(), read_version),
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
                            guard_deref: WriteHolder(cell.as_ref(), latch_version),
                        }
                    }
                }
            }
            _ => unreachable!("Bruhh.. this aint no optimistic guard here dam!!")
        }
    }

    #[inline(always)]
    pub fn borrow_free(&self) -> ConcurrentGuard<'_, E> {
        match self {
            ConcurrencyControlCell(cell) => ConcurrencyControlGuard {
                guard: unsafe { mem::transmute(cell.borrow_free()) }
            },
            _ => self.borrow_optimistic_reader(),
        }
    }

    #[inline(always)]
    pub fn borrow_free_static(&self) -> ConcurrentGuard<'static, E> {
        unsafe {
            mem::transmute(self.borrow_free())
        }
    }

    /// Read access.
    #[inline(always)]
    pub fn borrow_read(&self) -> ConcurrentGuard<'_, E> {
        match self {
            ConcurrencyControlCell(cell) => ConcurrencyControlGuard {
                guard: cell.borrow_read()
            },
            _ => self.borrow_optimistic_reader(),
        }
    }

    #[inline(always)]
    pub fn borrow_read_static(&self) -> ConcurrentGuard<'static, E> {
        unsafe { mem::transmute(self.borrow_read()) }
    }

    /// Lock-less access.
    #[inline(always)]
    pub fn unsafe_borrow(&self) -> &E {
        match self {
            ConcurrencyControlCell(cell) => cell.unsafe_borrow(),
            OptimisticCell(cell) => cell.cell.deref()
        }
    }

    /// Lock-less read access via static life-time.
    #[inline(always)]
    pub fn unsafe_borrow_static(&self) -> &'static E {
        unsafe { mem::transmute(self.unsafe_borrow()) }
    }

    /// Lock-less write access via static life-time.
    #[inline(always)]
    pub fn unsafe_borrow_mut(&self) -> &mut E {
        match self {
            ConcurrencyControlCell(cell) => cell.unsafe_borrow_mut(),
            OptimisticCell(cell) => cell.cell.get_mut()
        }
    }

    /// Lock-less write access via static life-time.
    #[inline(always)]
    pub fn unsafe_borrow_mut_static(&self) -> &'static mut E {
        unsafe { mem::transmute(self.unsafe_borrow_mut()) }
    }

    /// Write access.
    #[inline(always)]
    pub fn borrow_mut(&self) -> ConcurrentGuard<'_, E> {
        match self {
            ConcurrencyControlCell(cell) => ConcurrencyControlGuard {
                guard: cell.borrow_mut()
            },
            _ => self.borrow_optimistic_writer()
        }
    }

    /// Write access via static life-time.
    #[inline(always)]
    pub fn borrow_mut_static(&self) -> ConcurrentGuard<'static, E> {
        unsafe { mem::transmute(self.borrow_mut()) }
    }


    /// Exclusive write access.
    #[inline(always)]
    pub fn borrow_mut_exclusive(&self) -> ConcurrentGuard<'_, E> {
        match self {
            ConcurrencyControlCell(cell) =>  ConcurrencyControlGuard {
                guard: cell.borrow_mut_exclusive()
            },
            _ => self.borrow_optimistic_writer(),
        }
    }

    /// Exclusive write access.
    #[inline(always)]
    pub fn borrow_mut_exclusive_static(&self) -> ConcurrentGuard<'static, E> {
        unsafe { mem::transmute(self.borrow_mut_exclusive()) }
    }
}