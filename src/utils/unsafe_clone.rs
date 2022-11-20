use std::{mem, ptr};
use chronicle_db::backbone::core::event::Event;
use mvcc_bplustree::index::record::Record;

pub(crate) unsafe trait UnsafeClone {
    unsafe fn unsafe_clone(&self) -> Self;
}

unsafe impl UnsafeClone for Event {
    #[inline]
    unsafe fn unsafe_clone(&self) -> Self {
        let event_copy
            = mem::MaybeUninit::zeroed().assume_init();

        let raw
            = self as *const Event as *const u8;

        let dst
            = (&event_copy) as *const Event as *mut u8;

        ptr::copy_nonoverlapping(
            raw, dst, mem::size_of::<Event>());

        event_copy
    }
}

unsafe impl UnsafeClone for Record {
    #[inline]
    unsafe fn unsafe_clone(&self) -> Self {
        let record_copy
            = mem::MaybeUninit::zeroed().assume_init();

        let raw
            = self as *const Record as *const u8;

        let dst
            = (&record_copy) as *const Record as *mut u8;

        ptr::copy_nonoverlapping(
            raw, dst, mem::size_of::<Record>());

        record_copy
    }
}