use chronicle_db::backbone::core::event::Event;
use chronicle_db::backbone::core::event::EventVariant::Empty;
use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::index::record::Payload;
use crate::index::record_list::RecordList;

const DEFAULT_PAYLOAD: Payload = Empty;

pub trait RecordLike {
    fn key(&self) -> Key;
    fn payload(&self) -> &Payload;
}

impl RecordLike for Event {
    #[inline(always)]
    fn key(&self) -> Key {
        self.t1()
    }

    #[inline(always)]
    fn payload(&self) -> &Payload {
        self.get_event_variant()
    }
}

impl RecordLike for RecordList {
    #[inline(always)]
    fn key(&self) -> Key {
        self.key
    }

    #[inline(always)]
    fn payload(&self) -> &Payload {
        match self.payload_front() {
            Some(payload_versioned) => payload_versioned.payload(),
            _ => &DEFAULT_PAYLOAD,
        }
    }
}