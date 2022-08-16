use std::mem;
use std::ops::{Deref, DerefMut};
use std::ptr::null_mut;
use std::sync::atomic::Ordering::{AcqRel, Relaxed};
use mvcc_bplustree::index::version_info::{AtomicVersion, Version};
use mvcc_bplustree::utils::cc_cell::{CCCell, CCCellGuard};

#[derive(Default)]
pub struct VCCCell<E: Default> {
    cell: CCCell<E>,
    cell_version: AtomicVersion // TODO: Add locking bits to cell version
}

pub struct VCCCellGuard<'a, E: Default + 'a> {
    cc_guard: CCCellGuard<'a, E>,
    latch_cell_version: Option<(&'a VCCCell<E>, Version)>
}

impl<'a, E: Default + 'a> Drop for VCCCellGuard<'a, E> {
    fn drop(&mut self) {
        self.latch_cell_version.map(|(cell, ..)| cell.cell_version.fetch_add(1, Relaxed));
    }
}

/// Implements sugar for the VCCellGuard, i.e. delegate chain call.
impl<E: Default> Deref for VCCCellGuard<'_, E> {
    type Target = E;

    fn deref(&self) -> &Self::Target {
        self.cc_guard.deref()
    }
}

/// Implements sugar for the CCCellGuard, i.e. delegate chain call.
impl<E: Default> DerefMut for VCCCellGuard<'_, E> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let ptr = unsafe {
            mem::transmute(self.cc_guard.deref_mut())
        };

        if self.is_modified() {
            unsafe { mem::transmute(null_mut() as *mut E) }
        }
        else {
            self.latch_cell_version.map(|(cell, ..)| cell.cell_version.fetch_add(1, Relaxed));
            ptr
        }
    }
}

/// Implements lock checking functions.
impl<'a, E: 'a + Default> VCCCellGuard<'a, E> {
    pub fn latch_version(&self) -> Option<Version> {
        self.latch_cell_version.map(|(_, version)| version)
    }

    pub fn cell_version(&self) -> Option<Version> {
        self.latch_cell_version.map(|(cell, _)| cell.cell_version.fetch_add(0, AcqRel))
    }

    pub fn is_modified(&self) -> bool {
        self.latch_cell_version
            .map(|(cell, latch_version)| latch_version != cell.cell_version.fetch_add(0, AcqRel))
            .unwrap_or(false)
    }

    #[inline]
    pub const fn is_write_lock(&self) -> bool {
        self.cc_guard.is_write_lock()
    }

    #[inline]
    pub const fn is_reader_lock(&self) -> bool {
        self.cc_guard.is_reader_lock()
    }

    #[inline]
    pub const fn is_exclusive_lock(&self) -> bool {
        self.cc_guard.is_exclusive_lock()
    }

    #[inline]
    pub const fn is_lock_free_lock(&self) -> bool {
        self.cc_guard.is_lock_free_lock()
    }
}

impl<E: Default> VCCCell<E> {
    const VC_CELL_START_VERSION: Version = 0;

    pub fn new(data: E) -> Self {
        Self {
            cell: CCCell::new(data),
            cell_version: AtomicVersion::new(Self::VC_CELL_START_VERSION)
        }
    }

    pub fn borrow_versioned<'a>(&'a self) -> VCCCellGuard<'a, E>{
        let cc_guard = self.cell.borrow_free();
        let version = self.cell_version.fetch_add(1, Relaxed);

        VCCCellGuard {
            cc_guard,
            latch_cell_version: Some((self, version))
        }
    }

    pub fn borrow_versioned_static(&self) -> VCCCellGuard<'static, E>{
        let cc_guard
            = self.cell.borrow_free_static();

        let version
            = self.cell_version.fetch_add(1, Relaxed);

        VCCCellGuard {
            cc_guard,
            latch_cell_version: Some((unsafe { mem::transmute(self) }, version))
        }
    }

    /// Read access via static life-time.
    #[inline(always)]
    pub fn borrow_read_static(&self) -> VCCCellGuard<'static, E>  {
        VCCCellGuard {
            cc_guard: self.cell.borrow_read_static(),
            latch_cell_version: None
        }
    }

    /// Read access.
    #[inline(always)]
    pub fn borrow_read(&self) -> VCCCellGuard<'_, E> {
        VCCCellGuard {
            cc_guard: self.cell.borrow_read(),
            latch_cell_version: None
        }
    }

    /// Lock-less access.
    #[inline(always)]
    pub fn unsafe_borrow(&self) -> &E {
        self.cell.unsafe_borrow()
    }

    /// Lock-less read access via static life-time.
    #[inline(always)]
    pub fn unsafe_borrow_static(&self) -> &'static E {
        self.cell.unsafe_borrow_static()
    }

    /// Lock-less write access via static life-time.
    #[inline(always)]
    pub fn unsafe_borrow_mut(&self) -> &mut E {
        self.cell.unsafe_borrow_mut()
    }

    /// Lock-less write access via static life-time.
    #[inline(always)]
    pub fn unsafe_borrow_mut_static(&self) -> &'static mut E {
        self.cell.unsafe_borrow_mut_static()
    }

    /// Write access via static life-time.
    #[inline(always)]
    pub fn borrow_mut_static(&self) -> VCCCellGuard<'static, E> {
        VCCCellGuard {
            cc_guard: self.cell.borrow_mut_static(),
            latch_cell_version: None
        }
    }

    /// Write access.
    #[inline(always)]
    pub fn borrow_mut(&self) -> VCCCellGuard<'_, E> {
        VCCCellGuard {
            cc_guard: self.cell.borrow_mut(),
            latch_cell_version: None
        }
    }

    /// Exclusive write access.
    #[inline(always)]
    pub fn borrow_mut_exclusive(&self) -> VCCCellGuard<'_, E> {
        VCCCellGuard {
            cc_guard: self.cell.borrow_mut_exclusive(),
            latch_cell_version: None
        }
    }

    /// Exclusive write access.
    #[inline(always)]
    pub fn borrow_mut_exclusive_static(&self) -> VCCCellGuard<'static, E> {
        VCCCellGuard {
            cc_guard: self.cell.borrow_mut_exclusive_static(),
            latch_cell_version: None
        }
    }

    /// LockFree access.
    #[inline(always)]
    pub fn borrow_free(&self) -> VCCCellGuard<'_, E> {
        VCCCellGuard {
            cc_guard: self.cell.borrow_free(),
            latch_cell_version: None
        }
    }

    /// LockFree access.
    #[inline(always)]
    pub fn borrow_free_static(&self) -> VCCCellGuard<'static, E> {
        VCCCellGuard {
            cc_guard: self.cell.borrow_free_static(),
            latch_cell_version: None
        }
    }

    /// Structure flatten method.
    /// Retrieves underlying object.
    #[inline(always)]
    pub fn into_inner(self) -> E {
        self.cell.into_inner()
    }
}

impl<E: Default> Deref for VCCCell<E> {
    type Target = CCCell<E>;

    fn deref(&self) -> &Self::Target {
        &self.cell
    }
}