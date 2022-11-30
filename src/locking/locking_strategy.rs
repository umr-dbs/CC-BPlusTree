use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};
use TXDataModel::page_model::{Attempts, Level, LevelVariant};
use crate::index::root::LEVEL_ROOT;

#[repr(u8)]
#[derive(Serialize, Deserialize, Clone)]
pub enum LevelConstraints {
    OptimisticLimit {
        attempts: Attempts,
        level: LevelVariant,
    },
    Unlimited
}

impl LevelConstraints {
    pub const fn attempts(&self) -> Attempts {
        match self {
            Self::OptimisticLimit {
                attempts,
                ..
            } => *attempts,
            Self::Unlimited => Attempts::MAX
        }
    }

    pub fn level_variant(&self) -> LevelVariant {
        match self {
            Self::OptimisticLimit {
                level,
                ..
            } => level.clone(),
            Self::Unlimited => LevelVariant::Const(Level::MAX)
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
    OLC(LevelConstraints),
}

impl Display for LockingStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MonoWriter => write!(f, "MonoWriter"),
            Self::LockCoupling => write!(f, "LockCoupling"),
            Self::RWLockCoupling(level, attempts) =>
                write!(f, "RWLockCoupling(Attempts={};Level={})", attempts, level),
            Self::OLC(LevelConstraints::OptimisticLimit { attempts, level }) =>
                write!(f, "OLC(Attempts={};Level={})", attempts, level),
            Self::OLC(LevelConstraints::Unlimited) => write!(f, "OLC(Unlimited)"),
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

    pub(crate) const fn is_olc(&self) -> bool {
        match self {
            Self::OLC(_) => true,
            _ => false
        }
    }

    pub(crate) const fn is_olc_limited(&self) -> Option<bool> {
        match self {
            Self::OLC(LevelConstraints::Unlimited) => Some(false),
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
            Self::RWLockCoupling(lock_level, attempts) => {
                curr_level >= height
                    || curr_level >= max_level
                    || attempt >= *attempts
                    || lock_level.is_lock(curr_level, height)
            }
            Self::OLC(LevelConstraints::Unlimited) => false,
            Self::OLC(LevelConstraints::OptimisticLimit { attempts, level }) => {
                curr_level >= height
                    || curr_level >= max_level
                    || attempt >= *attempts
                    || level.is_lock(curr_level, height)
            }
        }
    }
}