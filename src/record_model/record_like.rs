use std::fmt::{Display, Formatter};
use std::hash::Hash;
use crate::record_model::record_list::PayloadVersioned;
use crate::record_model::Version;
use crate::record_model::version_info::VersionInfo;

pub trait RecordLike<Key: Ord + Hash + Copy + Default, Payload: Clone + Default>: Clone + Default {
    fn key(&self)
        -> Key;

    fn payload(&self, version: Option<Version>)
        -> Option<PayloadVersioned<Payload>>;

    fn version(&self)
        -> Option<&VersionInfo>;

    fn into_payload(self)
        -> Option<Payload>;

    fn push_payload(
        &mut self,
        payload: Payload,
        version: Option<Version>
    );

    fn delete(
        &mut self,
        delete_version: Version
    ) -> bool;
}