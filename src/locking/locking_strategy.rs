use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};
use crate::page_model::{Attempts, Level, LevelVariant};
use crate::tree::root::LEVEL_ROOT;

#[repr(u8)]
#[derive(Serialize, Deserialize, Clone)]
pub enum OLCVariant {
    Free,
    ReaderLimit {
        attempts: Attempts,
        level: LevelVariant,
    },
    WriterLimit {
        attempts: Attempts,
        level: LevelVariant,
    },
}

impl OLCVariant {
    pub const fn attempts(&self) -> Attempts {
        match self {
            Self::Free => Attempts::MAX,
            Self::WriterLimit {
                attempts,
                ..
            } => *attempts,
            Self::ReaderLimit {
                attempts,
                ..
            } => *attempts,
        }
    }

    pub fn level_variant(&self) -> LevelVariant {
        match self {
            Self::WriterLimit {
                level,
                ..
            } => level.clone(),
            Self::ReaderLimit {
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
    RWLockCoupling(LevelVariant, Attempts),
    OLC(OLCVariant),
}

impl Display for LockingStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MonoWriter => write!(f, "MonoWriter"),
            Self::LockCoupling => write!(f, "LockCoupling"),
            Self::RWLockCoupling(level, attempts) =>
                write!(f, "RWLockCoupling(Attempts={};Level={})", attempts, level),
            Self::OLC(OLCVariant::Free) => write!(f, "OLC-Free"),
            Self::OLC(OLCVariant::ReaderLimit { attempts, level }) =>
                write!(f, "OLC-ReaderLimit(Attempts={};Level={})", attempts, level),
            Self::OLC(OLCVariant::WriterLimit { attempts, level }) =>
                write!(f, "OLC-WriterLimit(Attempts={};Level={})", attempts, level),
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
    pub(crate) const fn is_olc(&self) -> bool {
        match self {
            Self::OLC(_) => true,
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
            Self::RWLockCoupling(..) => true,
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
    pub fn is_lock(
        &self,
        curr_level: Level,
        max_level: Level,
        attempt: Attempts,
        height: Level,
    ) -> bool {
        match self {
            Self::MonoWriter => false,
            Self::LockCoupling => true,
            Self::RWLockCoupling(lock_level, attempts) =>
                curr_level >= height
                    || curr_level >= max_level
                    || attempt >= *attempts
                    || lock_level.is_lock(curr_level, height),
            Self::OLC(OLCVariant::Free) => false,
            Self::OLC(OLCVariant::WriterLimit { attempts, level }) =>
                curr_level >= height
                    || curr_level >= max_level
                    || attempt >= *attempts
                    || level.is_lock(curr_level, height),
            LockingStrategy::OLC(OLCVariant::ReaderLimit { attempts, level }) =>
                curr_level >= height
                    || curr_level >= max_level
                    || attempt >= *attempts
                    || level.is_lock(curr_level, height),
        }
    }
}