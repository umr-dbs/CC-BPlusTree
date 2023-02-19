use std::hash::Hash;
use std::marker::PhantomData;
use std::{mem, ptr};
use std::mem::{ManuallyDrop, MaybeUninit};
use crate::page_model::{BlockRef, ObjectCount};
use crate::page_model::block::Block;
use crate::record_model::record_point::RecordPoint;
// use crate::record_model::record_like::RecordLike;
use crate::utils::shadow_vec::ShadowVec;

pub struct LeafPage<
    const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default,
> {
    pub(crate) records_len: ObjectCount,
    pub(crate) record_data: [MaybeUninit<RecordPoint<Key, Payload>>; NUM_RECORDS],
    _marker: PhantomData<(Key, Payload)>,
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default,
> Default for LeafPage<NUM_RECORDS, Key, Payload> {
    fn default() -> Self {
        LeafPage::new()
    }
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default
> Drop for LeafPage<NUM_RECORDS, Key, Payload> {
    fn drop(&mut self) {
        self.as_records_mut()
            .clear();
    }
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default,
> LeafPage<NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            records_len: 0,
            record_data: unsafe { mem::MaybeUninit::uninit().assume_init() }, // <[MaybeUninit<Entry>; NUM_RECORDS]>::
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub const fn as_records(&self) -> &[RecordPoint<Key, Payload>] {
        unsafe {
            std::slice::from_raw_parts(self.record_data.as_ptr() as *const RecordPoint<Key, Payload>,
                                       self.records_len as _)
        }
    }

    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.records_len as _
    }

    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub const fn is_full(&self) -> bool {
        self.len() == NUM_RECORDS
    }

    #[inline(always)]
    pub fn as_records_mut(&self) -> ShadowVec<RecordPoint<Key, Payload>> {
        unsafe {
            ShadowVec {
                unreal_vec: ManuallyDrop::new(Vec::from_raw_parts(
                    self.record_data.as_ptr() as *mut RecordPoint<Key, Payload>,
                    self.records_len as _,
                    NUM_RECORDS)),
                obj_cnt: (&self.records_len) as *const _ as *mut _,
                update_len: true,
            }
        }
    }
}