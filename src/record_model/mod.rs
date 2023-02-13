use std::sync::atomic::AtomicU64;

// pub mod record;
// pub mod version_info;
// pub mod record_like;
// pub mod record_list;
pub mod record_point;
pub mod unsafe_clone;

/// Declares the version type.
pub type Version = u64;

/// Declares the atomic version type.
pub type AtomicVersion = AtomicU64;