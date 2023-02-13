use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Display, Formatter};
use std::sync::atomic::AtomicU64;
use crate::record_model::Version;

/// Defines a deleted version, wrapping with one leading marker bit.
#[derive(Clone, Default)]
struct DeletedVersion(Version);

/// Remove unreadable coding of del version when serializing.
impl Serialize for DeletedVersion {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        match self.get() {
            None => Ok(serializer.serialize_none()?),
            o => Ok(serializer.serialize_some(&o)?),
        }
    }
}

/// Decode readable del version to encoded variant.
impl<'de> Deserialize<'de> for DeletedVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<Version>::deserialize(deserializer)? {
            Some(del) => Ok(DeletedVersion::new(del)),
            None => Ok(DeletedVersion::new_null()),
        }
    }
}

/// Implements mapping for deletion versions.
impl DeletedVersion {
    /// Defines the mask of a non-null Version, i.e. where the left outer most bit is set.
    /// Otherwise defines a null mapping and thus does not exist.
    const NON_NULL_FLAG: Version = 0x80_00000000000000;

    /// Defines the null instance.
    const NULL_FLAG: Version = 0;

    /// The actual mask for selecting the Version.
    const EXTRACTOR: Version = 0x7F_FFFFFFFFFFFFFF;

    /// Standard initializer.
    #[inline(always)]
    const fn new_null() -> Self {
        Self(Self::NULL_FLAG)
    }

    /// Initializer with a deletion version.
    #[inline(always)]
    const fn new(del_version: Version) -> Self {
        Self(del_version | Self::NON_NULL_FLAG)
    }

    /// Retrieves the underlying Delete-Version if present, otherwise None is returned.
    #[inline(always)]
    const fn get(&self) -> Option<Version> {
        match self.0 & Self::NON_NULL_FLAG {
            0 => None,
            _ => Some(self.0 & Self::EXTRACTOR),
        }
    }
}

/// Sugar implementation, wrapping a deletion version.
impl Into<DeletedVersion> for Version {
    fn into(self) -> DeletedVersion {
        DeletedVersion::new(self)
    }
}

/// Defines the version information structure, i.e. insert and delete version.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct VersionInfo {
    insert_version: Version,
    delete_version: DeletedVersion,
}

/// Sugar implementation, wrapping a version into a VersionInfo.
impl Into<VersionInfo> for Version {
    fn into(self) -> VersionInfo {
        VersionInfo::new(self)
    }
}

/// Managing methods implementation for VersionInfo.
impl VersionInfo {
    /// Basic constructor, setting insertion version via supplied version and deletion to None.
    #[inline(always)]
    pub const fn new(insert_version: Version) -> Self {
        Self {
            insert_version,
            delete_version: DeletedVersion::new_null(),
        }
    }

    /// Extended constructor, setting both fields via supplied parameters.
    #[inline(always)]
    pub fn from(insert_version: Version, delete_version: Version) -> Self {
        Self {
            insert_version,
            delete_version: delete_version.into(),
        }
    }

    /// Returns true, if supplied version matches.
    /// Returns false, otherwise.
    #[inline(always)]
    pub fn matches(&self, version: Version) -> bool {
        self.insert_version <= version &&
            self.delete_version
                .get()
                .map(|del| del > version)
                .unwrap_or(true)
    }

    /// Retrieves the insertion version.
    #[inline(always)]
    pub const fn insertion_version(&self) -> Version {
        self.insert_version
    }

    /// Retrieves the deletion version.
    #[inline(always)]
    pub const fn deletion_version(&self) -> Option<Version> {
        self.delete_version.get()
    }

    /// Returns true, if this version has been deleted.
    #[inline(always)]
    pub const fn is_deleted(&self) -> bool {
        self.delete_version.get().is_some()
    }

    /// Actively deletes this version by setting deletion to supplied delete version.
    #[inline(always)]
    pub fn delete(&mut self, delete_version: Version) -> bool {
        debug_assert!(!self.is_deleted());

        if self.is_deleted() {
            false
        } else {
            self.delete_version = delete_version.into();
            true
        }
    }
}

/// Implements standard pretty printers for VersionInfo, displaying both insertion and deletion versions.
impl Display for VersionInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f,
               "[{}, {})",
               self.insert_version,
               self.delete_version
                   .get()
                   .map(|del| del.to_string())
                   .unwrap_or("*".to_string()))
    }
}
