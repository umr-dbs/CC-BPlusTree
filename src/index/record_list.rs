use std::collections::linked_list::Iter;
use std::collections::LinkedList;
use std::ops::{Deref, DerefMut};
use chronicle_db::backbone::core::event::Event;
use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::index::record::{Payload, Record};
use mvcc_bplustree::index::version_info::{Version, VersionInfo};

#[derive(Clone, Default)]
pub struct PayloadVersioned {
    payload: Payload,
    version_info: VersionInfo,
}

impl Into<PayloadVersioned> for Record {
    fn into(self) -> PayloadVersioned {
        PayloadVersioned::new(self.event.payload, self.version)
    }
}

impl Into<PayloadVersioned> for (Payload, Version) {
    fn into(self) -> PayloadVersioned {
        PayloadVersioned::new(self.0, self.1.into())
    }
}

impl Into<PayloadVersioned> for (Payload, VersionInfo) {
    fn into(self) -> PayloadVersioned {
        PayloadVersioned::new(self.0, self.1)
    }
}

impl PayloadVersioned {
    pub const fn new(payload: Payload, version_info: VersionInfo) -> Self {
        Self {
            payload,
            version_info,
        }
    }

    pub fn as_event(&self, key: Key) -> Event {
        Event::new_from_t1(key, self.payload.clone())
    }

    pub fn as_record(&self, key: Key) -> Record {
        (self.as_event(key), self.version_info.clone()).into()
    }

    pub fn payload(&self) -> &Payload {
        &self.payload
    }

    pub fn payload_mut(&mut self) -> &mut Payload {
        &mut self.payload
    }

    pub fn version_info(&self) -> &VersionInfo {
        &self.version_info
    }

    pub fn version_info_mut(&mut self) -> &mut VersionInfo {
        &mut self.version_info
    }
}

impl Deref for PayloadVersioned {
    type Target = Payload;

    fn deref(&self) -> &Self::Target {
        self.payload()
    }
}

impl DerefMut for PayloadVersioned {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.payload_mut()
    }
}

#[derive(Clone, Default)]
pub struct RecordList {
    key: Key,
    payload: LinkedList<PayloadVersioned>,
}

impl RecordList {
    pub fn from_record(record: Record) -> Self {
        Self::new(record.key(), record.event.payload, record.version)
    }

    pub fn from_event(event: Event, version: Version) -> Self {
        Self::new(event.t1(), event.payload, version.into())
    }

    pub fn new(key: Key, payload: Payload, version_info: VersionInfo) -> Self {
        Self {
            key,
            payload: LinkedList::from_iter(vec![(payload, version_info).into()]),
        }
    }

    pub const fn key(&self) -> Key {
        self.key
    }

    pub fn is_deleted(&self) -> bool {
        self.payload_front()
            .map(|front| front.version_info().is_deleted())
            .unwrap_or(true)
    }

    pub fn delete(&mut self, delete_version: Version) -> bool {
        self.payload_front_mut()
            .map(|front| front.version_info.delete(delete_version))
            .unwrap_or(false)
    }

    pub fn push_front(&mut self, record: Record) {
        self.payload.push_front(record.into())
    }

    fn payload_front(&self) -> Option<&PayloadVersioned> {
        self.payload.front()
    }

    fn payload_front_mut(&mut self) -> Option<&mut PayloadVersioned> {
        self.payload.front_mut()
    }

    // fn payload_back(&self) -> Option<&PayloadVersioned> {
    //     self.payload.back()
    // }
    //
    // fn payload_back_mut(&mut self) -> Option<&mut PayloadVersioned> {
    //     self.payload.back_mut()
    // }

    fn payloads(&self) -> Iter<'_, PayloadVersioned> {
        self.payload.iter()
    }
    
    pub fn as_records(&self) -> Vec<Record> {
        self.payloads()
            .map(|payload_versioned| payload_versioned.as_record(self.key))
            .collect()
    }

    fn payload_for_version(&self, version: Version) -> Option<&PayloadVersioned> {
        self.payloads()
            .skip_while(|payload_versioned| !payload_versioned
                .version_info()
                .matches(version))
            .next()
            // .filter(|payload_versioned| payload_versioned
            //     .version_info()
            //     .matches(version))
    }

    pub fn youngest_record(&self) -> Option<Record> {
        self.payload_front()
            .filter(|payload_versioned| !payload_versioned
                .version_info()
                .is_deleted())
            .map(|found| found.as_record(self.key))
    }

    pub fn record_for_version(&self, version: Version) -> Option<Record> {
        self.payload_for_version(version)
            .map(|found| found.as_record(self.key))
    }
}
