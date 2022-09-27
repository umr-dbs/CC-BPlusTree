use std::mem;
use std::ops::{Deref, DerefMut};
use chronicle_db::tools::aliases::{Key, ObjectCount};
use chronicle_db::tools::arrays::array::FixedArray;
use mvcc_bplustree::index::record::Record;
use crate::block::block::BlockRef;
use crate::index::record_list::RecordList;
use crate::utils::shadow_vec::ShadowVec;

pub(crate) type RecordsPage = LeafPage<Record>;
pub(crate) type RecordListsPage = LeafPage<RecordList>;

impl<E: Default> LeafPage<E> {
    pub(crate) fn new(cap: usize) -> Self {
        let mut vec
            = Vec::with_capacity(cap);

        vec.shrink_to(cap);
        let allocated_units = cap as _;

        LeafPage {
            record_data: vec.into(),
            allocated_units,
        }
    }
}

/// Defines a record page, wrapping aligned Records with an allocation size.
#[derive(Default)]
pub(crate) struct LeafPage<E: Default> {
    pub(crate) record_data: FixedArray<E>,
    pub(crate) allocated_units: ObjectCount,
}

impl<E: Default> Drop for LeafPage<E> {
    fn drop(&mut self) {
        unsafe {
            mem::drop(Vec::from_raw_parts(
                self.as_mut_ptr(),
                self.len(),
                self.cap()));

            self.record_data.set_len(0);
        }
    }
}

/// Sugar implementation, for automatic dereference purposes.
impl<E: Default> Deref for LeafPage<E> {
    type Target = FixedArray<E>;

    fn deref(&self) -> &Self::Target {
        self.record_data.as_ref()
    }
}

/// Sugar implementation, for automatic dereference purposes.
impl<E: Default> DerefMut for LeafPage<E> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.record_data.as_mut()
    }
}


/// Defines a record page, wrapping aligned Records with an allocation size.
#[derive(Default)]
pub(crate) struct IndexPage {
    pub(crate) keys: FixedArray<Key>,
    pub(crate) children: FixedArray<BlockRef>,
    pub(crate) allocated_units: ObjectCount,
}

impl Drop for IndexPage {
    fn drop(&mut self) {
        unsafe {
            mem::drop(
                Vec::from_raw_parts(
                    self.keys.as_mut_ptr(),
                    self.keys_len(),
                    self.keys_cap(),
                ));

            mem::drop(
                Vec::from_raw_parts(
                    self.children.as_mut_ptr(),
                    self.children_len(),
                    self.children_cap(),
                ));

            self.keys.set_len(0);
            self.children.set_len(0);
        }
    }
}

impl IndexPage {
    pub(crate) fn from(keys: Vec<Key>, children: Vec<BlockRef>) -> Self {
        let allocated_units
            = keys.capacity() as _;

        Self {
            keys: keys.into(),
            children: children.into(),
            allocated_units,
        }
    }

    const fn keys_cap(&self) -> usize {
        self.allocated_units as _
    }

    const fn children_cap(&self) -> usize {
        self.allocated_units as usize + 1
    }

    pub(crate) fn keys_len(&self) -> usize {
        self.keys.len()
    }

    pub(crate) fn children_len(&self) -> usize {
        self.children.len()
    }

    pub(crate) fn keys(&self) -> &[Key] {
        self.keys.as_slice()
    }

    pub(crate) fn children(&self) -> &[BlockRef] {
        self.children.as_slice()
    }

    pub(crate) fn keys_mut(&mut self) -> ShadowVec<Key> {
        (self.keys_cap(), self.keys.as_mut()).into()
    }

    pub(crate) fn children_mut(&mut self) -> ShadowVec<BlockRef> {
        (self.children_cap(), self.children.as_mut()).into()
    }
}

impl<E: Default> LeafPage<E> {
    const fn cap(&self) -> usize {
        self.allocated_units as _
    }

    pub(crate) fn as_records(&mut self) -> ShadowVec<E> {
        (self.cap(), self.as_mut()).into()
    }
}