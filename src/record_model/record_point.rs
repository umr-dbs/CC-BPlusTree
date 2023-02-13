use std::hash::Hash;
use std::{mem, ptr};
use std::fmt::{Display, Formatter};
use std::mem::MaybeUninit;
// use crate::record_model::record_like::RecordLike;
// use crate::record_model::record_list::PayloadVersioned;
use crate::record_model::unsafe_clone::UnsafeClone;
// use crate::record_model::Version;
// use crate::record_model::version_info::VersionInfo;
use serde::{Serialize, Deserialize, Deserializer};

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct RecordPoint<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> {
    pub key: Key,
    pub payload: Payload
}

impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> RecordPoint<Key, Payload> {
    #[inline(always)]
    pub const fn new(key: Key, payload: Payload) -> Self {
        Self {
            key,
            payload
        }
    }
}
// impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> RecordLike<Key, Payload> for RecordPoint<Key, Payload> {
//     #[inline(always)]
//     fn key(&self) -> Key {
//         self.key
//     }
//
//     #[inline(always)]
//     fn payload(&self, version: Option<Version>) -> Option<PayloadVersioned<Payload>> {
//         Some(PayloadVersioned::new(self.payload.clone(), VersionInfo::default()))
//     }
//
//     #[inline(always)]
//     fn version(&self) -> Option<&VersionInfo> {
//         None
//     }
//
//     #[inline(always)]
//     fn into_payload(self) -> Option<Payload> {
//         Some(self.payload)
//     }
//
//     #[inline(always)]
//     fn push_payload(&mut self, payload: Payload, version: Option<Version>) {
//         self.payload = payload
//     }
//
//     #[inline(always)]
//     fn delete(&mut self, delete_version: Version) -> bool {
//         false
//     }
// }

impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> UnsafeClone for RecordPoint<Key, Payload> {
    #[inline(always)]
    unsafe fn unsafe_clone(&self) -> Self {
        mem::transmute_copy(self)
        // let mut copy: MaybeUninit<Self>
        //     = mem::MaybeUninit::uninit();
        //
        // copy.as_mut_ptr()
        //     .copy_from_nonoverlapping(self, 1);
        //
        // copy.assume_init()
    }
}

impl<Key: Display + Ord + Copy + Hash + Default, Payload: Default + Display + Clone> Display for RecordPoint<Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordPoint(Key: {}, Payload: {})", self.key, self.payload)
    }
}

