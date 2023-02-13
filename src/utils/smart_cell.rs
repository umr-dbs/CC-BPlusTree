use std::hash::Hash;
use std::mem;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Weak};
use parking_lot::lock_api::{MutexGuard, RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{Mutex, RawMutex, RawRwLock, RwLock};
use crate::page_model::block::Block;
// use crate::record_model::AtomicVersion;
use crate::utils::cc_cell::{CCCell, CCCellGuard};
use crate::utils::cc_cell::CCCellGuard::Reader;
use crate::utils::hybrid_cell::{LatchVersion, OBSOLETE_FLAG_VERSION, OptCell, WRITE_FLAG_VERSION};
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::SmartFlavor::{ControlCell, OLCCell};
use crate::utils::smart_cell::SmartGuard::{LockFree, MutExclusive, OLCReader, OLCWriter, RwReader, RwWriter};

// pub union NewCell<E: Default> {
//     pub cccell: ManuallyDrop<CCCell<E>>,
//     pub opt_cell: ManuallyDrop<OptCell<E>>,
// }

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
    fn is_version_valid(&self, read_version: LatchVersion) -> bool {
        match self {
            OLCCell(opt) =>
                opt.load_version() == read_version && read_version & OBSOLETE_FLAG_VERSION == 0,
            _ => true
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

#[derive(Default)]
pub struct SmartCell<E: Default>(pub Arc<SmartFlavor<E>>);

impl<E: Default> Clone for SmartCell<E> {
    #[inline(always)]
    fn clone(&self) -> Self {
        SmartCell(self.0.clone())
    }
}

pub enum SmartGuard<'a, E: Default> {
    LockFree(*mut E),
    RwReader(RwLockReadGuard<'a, RawRwLock, ()>, *const E),
    RwWriter(RwLockWriteGuard<'a, RawRwLock, ()>, *mut E),
    MutExclusive(MutexGuard<'a, RawMutex, ()>, *mut E),
    OLCReader(Option<(Arc<SmartFlavor<E>>, LatchVersion)>),
    OLCWriter(Option<(Arc<SmartFlavor<E>>, LatchVersion)>),
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
            if let OLCCell(opt) = cell.as_ref() {
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
                if let OLCCell(opt) = cell.as_ref() {
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
            OLCReader(Some((cell, latch))) => cell
                .is_version_valid(*latch),
            OLCWriter(None) => false,
            OLCReader(None) => false,
            _ => true
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
            if cell.is_version_valid(*latch) =>
                Some(cell.as_ref()),
            OLCWriter(Some((cell, ..))) =>
                Some(cell.as_ref()),
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
            OLCReader(Some((cell, ..))) => Some(cell.as_ref()),
            OLCWriter(Some((cell, ..))) => Some(cell.as_ref()),
            _ => None
        }
    }

    #[inline(always)]
    pub fn deref_mut(&self) -> Option<&mut E> {
        match self {
            LockFree(ptr) => unsafe { ptr.as_mut() },
            RwWriter(.., ptr) => unsafe { ptr.as_mut() },
            MutExclusive(.., ptr) => unsafe { ptr.as_mut() },
            OLCWriter(Some((cell, ..))) => Some(cell.as_mut()),
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

                OLCReader(success.then(|| (self.0.clone(), read)))
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
                        OLCWriter(Some((self.0.clone(), latched)))
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
        if let OLCWriter(Some((cell, read_version))) = self {
            if *read_version & OBSOLETE_FLAG_VERSION == 0 {
                if let OLCCell(opt) = cell.as_ref() {
                    opt.write_unlock(*read_version);
                }
            }
        }
    }
}

unsafe impl<'a, E: Default + 'a> Sync for SmartGuard<'a, E> {}

unsafe impl<'a, E: Default + 'a> Send for SmartGuard<'a, E> {}




