use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use crate::page_model::block::Block;
// use crate::record_model::record_like::RecordLike;
use serde::{Deserialize, Serialize};
use crate::utils::cc_cell::CCCell;
use crate::utils::smart_cell::{OptCell, SmartCell, SmartFlavor};

pub mod internal_page;
pub mod leaf_page;
pub mod block;
pub mod node;

pub type ObjectCount = u16;
pub type BlockID = u64;
pub type AtomicBlockID = AtomicU64;
pub type Level = u16;
pub type Height = Level;
pub type Attempts = usize;

pub type BlockRef<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> = SmartCell<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Hash + Copy,
    Payload: Default + Clone
> Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub fn into_olc(self) -> SmartCell<Block<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        SmartCell(Arc::new(SmartFlavor::OLCCell(OptCell::new(self))))
    }

    #[inline(always)]
    pub fn into_cc(self) -> SmartCell<Block<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        SmartCell(Arc::new(SmartFlavor::ControlCell(CCCell::new(self))))
    }
}

#[repr(u8)]
#[derive(Clone, Serialize, Deserialize)]
pub enum LevelVariant {
    Height(f32),
    Const(Level),
}

/// Sugar implementation, auto wrapping Level.
impl Into<LevelVariant> for Level {
    fn into(self) -> LevelVariant {
        LevelVariant::Const(self)
    }
}

/// Implements basic functionality methods for checking locking level.
impl LevelVariant {
    /// Basic constructor.
    pub const fn new_const(lock_level: Level) -> Self {
        Self::Const(lock_level)
    }

    /// Basic constructor.
    pub const fn new_height_lock(k: f32) -> Self {
        Self::Height(k)
    }

    /// Returns true, if condition of height is met.
    /// Returns false, otherwise.
    #[inline(always)]
    pub fn is_lock(&self, curr_level: Level, height: Level) -> bool {
        match self {
            LevelVariant::Height(k) => curr_level >= (k * height as f32) as Level,
            LevelVariant::Const(lock_level) => curr_level >= *lock_level,
        }
    }

    /// Retrieves set constant lock level.
    /// Returns None, if variable lock level via height is configured.
    pub fn lock_level(&self) -> Option<Level> {
        match self {
            LevelVariant::Height(..) => None,
            LevelVariant::Const(lock_level) => Some(*lock_level),
        }
    }
}

/// Implements pretty printers for LevelVariant.
impl Display for LevelVariant {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LevelVariant::Height(k) => write!(f, "{}*height", k),
            LevelVariant::Const(c) => write!(f, "{}", c),
        }
    }
}