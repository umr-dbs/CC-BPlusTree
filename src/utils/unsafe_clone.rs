use std::{mem, ptr};
use mvcc_bplustree::index::record::Record;

pub(crate) unsafe trait UnsafeClone {
    unsafe fn unsafe_clone(&self) -> Self;
}

unsafe impl UnsafeClone for Record {
    unsafe fn unsafe_clone(&self) -> Self {
        let mut record_copy
            = mem::MaybeUninit::<Record>::uninit().assume_init();

        let raw
            = self as *const Record as *const u8;

        let dst
            = (&mut record_copy) as *mut Record as *mut u8;

        ptr::copy_nonoverlapping(
            raw, dst, mem::size_of::<Record>());

        record_copy
    }
}