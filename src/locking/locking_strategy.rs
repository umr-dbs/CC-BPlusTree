use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};
use crate::page_model::{Attempts, Level, LevelVariant};
use crate::tree::root::LEVEL_ROOT;
use crate::utils::smart_cell::LatchType;

pub const fn olc() -> LockingStrategy {
    LockingStrategy::OLC(OLCVariant::Free)
}

pub const fn olc_limited() -> LockingStrategy {
    LockingStrategy::OLC(OLCVariant::Bounded { attempts: 4, level: LevelVariant::Height(1f32) })
}

pub const fn hybrid_lock() -> LockingStrategy {
    LockingStrategy::HybridLocking(LevelVariant::Height(1f32), 4)
}

pub const fn lightweight_hybrid_lock() -> LockingStrategy {
    LockingStrategy::OLC(OLCVariant::Pinned {
        attempts: 0,
        level: LevelVariant::Height(1f32)
    })
}

pub const fn orwc() -> LockingStrategy {
    LockingStrategy::ORWC(LevelVariant::Height(1f32), 4)
}

#[repr(u8)]
#[derive(Serialize, Deserialize, Clone)]
pub enum OLCVariant {
    Free,
    Pinned {
        attempts: Attempts,
        level: LevelVariant,
    },
    Bounded {
        attempts: Attempts,
        level: LevelVariant,
    },
}

impl OLCVariant {
    pub const fn attempts(&self) -> Attempts {
        match self {
            Self::Bounded {
                attempts,
                ..
            } => *attempts,
            Self::Pinned {
                attempts,
                ..
            } => *attempts,
            _ => Attempts::MAX,
        }
    }

    pub fn level_variant(&self) -> LevelVariant {
        match self {
            Self::Bounded {
                level,
                ..
            } => level.clone(),
            Self::Pinned {
                level,
                ..
            } => level.clone(),
            _ => LevelVariant::Const(Level::MAX),
        }
    }
}

#[repr(u8)]
#[derive(Serialize, Deserialize, Default, Clone)]
pub enum LockingStrategy {
    #[default]
    MonoWriter,
    LockCoupling,
    ORWC(LevelVariant, Attempts),
    OLC(OLCVariant),
    HybridLocking(LevelVariant, Attempts)
}

pub type CRUDProtocol = LockingStrategy;

impl Display for LockingStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MonoWriter => write!(f, "MonoWriter"),
            Self::LockCoupling => write!(f, "LockCoupling"),
            Self::ORWC(level, attempts) =>
                write!(f, "ORWC(Attempts={};Level={})", attempts, level),
            Self::OLC(OLCVariant::Free) => write!(f, "OLC"),
            Self::OLC(OLCVariant::Pinned { attempts, level }) =>
                write!(f, "Lightweight-HybridLock(Attempts={};Level={})", attempts, level),
            Self::OLC(OLCVariant::Bounded { attempts, level }) =>
                write!(f, "OLC-Bounded(Attempts={};Level={})", attempts, level),
            LockingStrategy::HybridLocking(level, attempts) =>
                write!(f, "HybridLock(Attempts={};Level={})", attempts, level),
        }
    }
}

impl LockingStrategy {
    const KEY_LOCKING_STRATEGY: &'static str = "lockingstrategy";

    pub(crate) fn load(configs: &HashMap<String, String>) -> LockingStrategy {
        match configs.get(Self::KEY_LOCKING_STRATEGY) {
            None => LockingStrategy::default(),
            Some(inner) => serde_json::from_str(inner)
                .ok()
                .unwrap_or(LockingStrategy::default())
        }
    }

    #[inline(always)]
    pub const fn latch_type(&self) -> LatchType {
        match self {
            LockingStrategy::MonoWriter => LatchType::None,
            LockingStrategy::LockCoupling => LatchType::Exclusive,
            LockingStrategy::ORWC(..) => LatchType::ReadersWriter,
            LockingStrategy::OLC(..) => LatchType::Optimistic,
            LockingStrategy::HybridLocking(..) => LatchType::Hybrid
        }
    }

    #[inline(always)]
    pub(crate) const fn is_optimistic(&self) -> bool {
        match self {
            Self::OLC(_) => true,
            Self::HybridLocking(..) => true,
            _ => false
        }
    }

    #[inline(always)]
    pub(crate) const fn is_mono_writer(&self) -> bool {
        match self {
            Self::MonoWriter => true,
            _ => false
        }
    }

    #[inline(always)]
    pub(crate) const fn is_read_write_lock(&self) -> bool {
        match self {
            Self::ORWC(..) => true,
            _ => false
        }
    }

    pub(crate) const fn is_olc_limited(&self) -> Option<bool> {
        match self {
            Self::OLC(OLCVariant::Free) => Some(false),
            Self::OLC(..) => Some(true),
            _ => None
        }
    }

    #[inline(always)]
    pub const fn additional_lock_required(&self) -> bool {
        match self {
            Self::MonoWriter => false,
            Self::LockCoupling => false,
            _ => true,
        }
    }

    #[inline(always)]
    pub fn is_lock_root(&self, lock_level: Level, attempt: Attempts, height: Level) -> bool {
        self.is_lock(
            LEVEL_ROOT,
            lock_level,
            attempt,
            height)
    }

    #[inline(always)]
    fn is_lock(
        &self,
        curr_level: Level,
        max_level: Level,
        attempt: Attempts,
        height: Level,
    ) -> bool {
        match self {
            // Self::MonoWriter => false,
            Self::LockCoupling => true,
            Self::ORWC(lock_level, attempts) =>
                curr_level >= height
                    || curr_level >= max_level
                    || attempt >= *attempts
                    || lock_level.is_lock(curr_level, height),
            // Self::OLC(OLCVariant::Free) => false,
            Self::OLC(OLCVariant::Bounded { attempts, level }) =>
                curr_level >= height
                    || curr_level >= max_level
                    || attempt >= *attempts
                    || level.is_lock(curr_level, height),
            // LockingStrategy::OLC(OLCVariant::Pinned { attempts, level }) =>
            //     curr_level >= height
            //         || curr_level >= max_level
            //         || attempt >= *attempts
            //         || level.is_lock(curr_level, height),
            LockingStrategy::HybridLocking(level, attempts) =>
                curr_level >= height
                || curr_level >= max_level
                || attempt >= *attempts
                || level.is_lock(curr_level, height),
            _ => false
        }
    }
}