use std::collections::linked_list::Iter;
use std::collections::LinkedList;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use itertools::Itertools;
use serde::{Serialize, Deserialize, Deserializer};
use crate::record_model::record::Record;
use crate::record_model::record_like::RecordLike;
use crate::record_model::Version;
use crate::record_model::version_info::VersionInfo;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct PayloadVersioned<Payload: Clone + Default> {
    pub payload: Payload,
    pub version_info: VersionInfo,
}

impl<Payload: Clone + Default> PayloadVersioned<Payload> {
    #[inline(always)]
    pub const fn new(payload: Payload, version_info: VersionInfo) -> Self {
        Self {
            payload,
            version_info,
        }
    }

    #[inline(always)]
    pub fn as_record<Key: Ord + Hash + Copy + Default>(&self, key: Key) -> Record<Key, Payload> {
        Record::from(key, self.payload.clone(), self.version_info.clone())
    }
}

impl<Payload: Clone + Default> Deref for PayloadVersioned<Payload> {
    type Target = Payload;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl<Payload: Clone + Default> DerefMut for PayloadVersioned<Payload> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.payload
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct RecordList<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> {
    pub key: Key,
    pub payload: LinkedList<PayloadVersioned<Payload>>
}

impl<Payload: Clone + Default + Display> Display for PayloadVersioned<Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PayloadVersioned(Payload: {}, Version: {})", self.payload, self.version_info)
    }
}

impl<Key: Ord + Copy + Hash + Default + Display, Payload: Clone + Default + Display> Display for RecordList<Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordList(Key: {}, VersionList: [{}])",
               self.key,
               self.payload
                   .iter()
                   .join(","))
    }
}

impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> RecordList<Key, Payload> {
    #[inline(always)]
    pub fn new(key: Key, payload: Payload, version_info: VersionInfo) -> Self {
        Self {
            key,
            payload: LinkedList::from_iter([PayloadVersioned::new(payload, version_info)]),
        }
    }

    #[inline(always)]
    pub fn payloads(&self) -> Iter<'_, PayloadVersioned<Payload>> {
        self.payload.iter()
    }

    #[inline(always)]
    pub fn as_records(&self) -> Vec<Record<Key, Payload>> {
        self.payloads()
            .map(|payload_versioned| payload_versioned
                .as_record(self.key))
            .collect()
    }
}

impl<Key: Ord + Hash + Ord + Copy + Default, Payload: Clone + Default> RecordLike<Key, Payload> for RecordList<Key, Payload> {
    #[inline(always)]
    fn key(&self) -> Key {
        self.key
    }

    #[inline(always)]
    fn payload(&self, version: Option<Version>) -> Option<PayloadVersioned<Payload>> {
        match version {
            Some(version) => self
                .payloads()
                .skip_while(|payload_versioned| !payload_versioned
                    .version_info
                    .matches(version))
                .next()
                .cloned(),
            _ => self.payload.front().cloned()
        }
    }

    #[inline(always)]
    fn version(&self) -> Option<&VersionInfo> {
        self.payload.front().map(|found| &found.version_info)
    }

    #[inline(always)]
    fn into_payload(self) -> Option<Payload> {
        self.payload.front().map(|found| found.payload.clone())
    }

    #[inline(always)]
    fn push_payload(&mut self, payload: Payload, version: Option<Version>) {
        self.payload.push_front(PayloadVersioned::new(
            payload, VersionInfo::new(version.unwrap())))
    }

    #[inline(always)]
    fn delete(&mut self, delete_version: Version) -> bool {
        self.payload.front_mut()
            .map(|front| front.version_info.delete(delete_version))
            .unwrap_or(false)
    }
}
