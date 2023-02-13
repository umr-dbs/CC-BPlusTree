use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use serde::{Deserialize, Serialize};
use crate::record_model::record_like::RecordLike;
use crate::record_model::record_list::{PayloadVersioned, RecordList};
use crate::record_model::Version;
use crate::record_model::version_info::VersionInfo;

/// Structure of a record. Wraps an Event including version information.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Record<Key: Ord + Hash + Copy + Default, Payload: Clone + Default> {
    pub key: Key,
    pub payload: Payload,
    pub version: VersionInfo,
}

/// Implementation block for Record, including delegate methods for key and payload access.
impl<Key: Hash + Ord + Copy + Default, Payload: Clone + Default> RecordLike<Key, Payload> for Record<Key, Payload> {
    /// Retrieves the key.
    #[inline(always)]
    fn key(&self) -> Key {
        self.key
    }

    #[inline(always)]
    fn payload(&self, version: Option<Version>) -> Option<PayloadVersioned<Payload>> {
        Some(PayloadVersioned::new(self.payload.clone(), self.version.clone()))
    }

    #[inline(always)]
    fn version(&self) -> Option<&VersionInfo> {
        Some(&self.version)
    }

    fn into_payload(self) -> Option<Payload> {
        Some(self.payload)
    }

    #[inline(always)]
    fn push_payload(&mut self, payload: Payload, version: Option<Version>) {
        self.payload = payload;
        version.map(|version| self.version = VersionInfo::new(version));
    }

    #[inline(always)]
    fn delete(&mut self, delete_version: Version) -> bool {
        self.version.delete(delete_version)
    }
}

/// Implementation block for Record, including delegate methods for key and payload access.
impl<Key: Hash + Ord + Copy + Default, Payload: Clone + Default> Record<Key, Payload> {
    /// Constructor Method by supplying key, payload and insertion version.
    /// This constructor only delegates to field constructors.
    #[inline(always)]
    pub const fn new(key: Key, payload: Payload, insert_version: Version) -> Self {
        Self {
            key,
            payload,
            version: VersionInfo::new(insert_version),
        }
    }

    /// Extended Constructor.
    /// This constructor only delegates to field constructors.
    #[inline(always)]
    pub const fn from(key: Key, payload: Payload, version: VersionInfo) -> Self {
        Self {
            key,
            payload,
            version,
        }
    }

    /// Retrieves VersionInfo.
    #[inline(always)]
    pub const fn version_info(&self) -> &VersionInfo {
        &self.version
    }

    /// Retrieves a reference of the payload.
    #[inline(always)]
    pub const fn payload(&self) -> &Payload {
        &self.payload
    }

    /// Retrieves the insertion version of this record.
    #[inline(always)]
    pub const fn insertion_version(&self) -> Version {
        self.version.insertion_version()
    }

    /// Retrieves the deletion version of this record.
    #[inline(always)]
    pub const fn deletion_version(&self) -> Option<Version> {
        self.version.deletion_version()
    }

    /// Returns true, if supplied version matches this record.
    /// Returns false, otherwise.
    #[inline(always)]
    pub fn match_version(&self, version: Version) -> bool {
        self.version.matches(version)
    }

    /// Returns true, if supplied version is None.
    /// Returns true, if supplied version matches this record via delegate call of match_version.
    /// Returns false, otherwise.
    #[inline(always)]
    pub fn match_version_option(&self, version: Option<Version>) -> bool {
        version
            .map(|version| self.match_version(version))
            .unwrap_or(true)
    }

    /// Returns true, if this return has been deleted.
    #[inline(always)]
    pub const fn is_deleted(&self) -> bool {
        self.deletion_version().is_some()
    }

    /// Mutably sets the delete version, i.e. deletes this record.
    #[inline(always)]
    pub fn delete(&mut self, delete_version: Version) -> bool {
        self.version.delete(delete_version)
    }
}

/// Implements PartialEq for record, i.e. for sorting purposes.
impl<Key: Ord + Hash + Copy + Default, Value: Clone + Default> PartialEq for Record<Key, Value> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

/// Marker Implementation for Record.
impl<Key: Ord + Hash + Copy + Default, Value: Clone + Default> Eq for Record<Key, Value> {}

/// Implements PartialOrd for record, i.e. for sorting purposes.
impl<Key: Ord + Hash + Copy + Default, Value: Clone + Default> PartialOrd for Record<Key, Value> {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key().partial_cmp(&other.key())
    }
}

/// Implements Ord for record, i.e. for sorting purposes.
impl<Key: Ord + Hash + Copy + Default, Value: Clone + Default> Ord for Record<Key, Value> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

/// Implements pretty printers for Record via delegate calls.
impl<Key: Display + Ord + Hash + Copy + Default, Value: Display + Clone + Default> Display for Record<Key, Value> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Record(version: {}, key: {}, value: {})",
               self.version,
               self.key,
               self.payload
        )
    }
}
