use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use mvcc_bplustree::locking::locking_strategy::{Attempts, Level, LevelVariant, LockingStrategy};
use crate::index::cclocking_strategy::CCLockingStrategy::{OLC, RWLockCoupling};
use crate::index::cclocking_strategy::LevelConstraints::OptimisticLimit;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub enum LevelConstraints {
    OptimisticLimit {
        attempts: Attempts,
        level: LevelVariant,
    },
    None
}

impl LevelConstraints {
    pub const fn attempts(&self) -> Attempts {
        match self {
            LevelConstraints::OptimisticLimit {
                attempts,
                ..
            } => *attempts,
            LevelConstraints::None => Attempts::MAX
        }
    }

    pub const fn level_variant(&self) -> &LevelVariant {
        match self {
            LevelConstraints::OptimisticLimit {
                level,
                ..
            } => level,
            LevelConstraints::None => &LevelVariant::Const(Level::MAX)
        }
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub enum CCLockingStrategy {
    #[default]
    MonoWriter,
    LockCoupling,
    RWLockCoupling(LevelVariant, Attempts),
    OLC(LevelConstraints),
}

impl Display for CCLockingStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MonoWriter => write!(f, "MonoWriter"),
            Self::LockCoupling => write!(f, "LockCoupling"),
            Self::RWLockCoupling(_, _) => write!(f, "RWLockCoupling"),
            Self::OLC(OptimisticLimit { attempts, level }) =>
                write!(f, "OLC(Attempts={},Level={})", attempts, level),
            Self::OLC(LevelConstraints::None) => write!(f, "OLC(Unlimited)"),
        }
    }
}

impl CCLockingStrategy {
    const KEY_LOCKING_STRATEGY: &'static str = "lockingstrategy";

    pub(crate) fn load(configs: &HashMap<String, String>) -> CCLockingStrategy {
        match configs.get(Self::KEY_LOCKING_STRATEGY) {
            None => CCLockingStrategy::default(),
            Some(inner) => serde_json::from_str(inner)
                .ok()
                .unwrap_or(CCLockingStrategy::default())
        }
    }

    pub(crate) const fn is_olc(&self) -> bool {
        match self {
            OLC(_) => true,
            _ => false
        }
    }

    pub(crate) const fn is_olc_limited(&self) -> Option<bool> {
        match self {
            OLC(LevelConstraints::None) => Some(false),
            OLC(..) => Some(true),
            _ => None
        }
    }

    #[inline(always)]
    pub const fn additional_lock_required(&self) -> bool {
        match self {
            CCLockingStrategy::MonoWriter => false,
            CCLockingStrategy::LockCoupling => false,
            _ => true,
        }
    }

    #[inline(always)]
    pub fn is_lock_root(&self, lock_level: Level, attempt: Attempts, height: Level) -> bool {
        self.is_lock(LockingStrategy::LEVEL_ROOT, lock_level, attempt, height)
    }

    pub fn is_lock(
        &self,
        curr_level: Level,
        max_level: Level,
        attempt: Attempts,
        height: Level,
    ) -> bool {
        match self {
            MonoWriter => false,
            LockCoupling => true,
            RWLockCoupling(lock_level, attempts) => {
                curr_level >= height
                    || curr_level >= max_level
                    || attempt >= *attempts
                    || lock_level.is_lock(curr_level, height)
            }
            OLC(LevelConstraints::None) => false,
            OLC(OptimisticLimit { attempts, level }) => {
                curr_level >= height
                    || curr_level >= max_level
                    || attempt >= *attempts
                    || level.is_lock(curr_level, height)
            }
        }
    }
}

// impl Into<CCLockingStrategy> for LockingStrategy {
//     fn into(self) -> CCLockingStrategy {
//         match self {
//             LockingStrategy::SingleWriter => MonoWriter,
//             LockingStrategy::WriteCoupling => LockCoupling,
//             LockingStrategy::Optimistic(level, attempt) =>
//                 RWLockCoupling(level, attempt),
//             LockingStrategy::Dolos(level, attempt) => OLC(OptimisticLimit {
//                 attempts: attempt,
//                 level: level
//             })
//         }
//     }
// }
//
// impl Into<LockingStrategy> for CCLockingStrategy {
//     fn into(self) -> LockingStrategy {
//         match self {
//             MonoWriter => LockingStrategy::SingleWriter,
//             LockCoupling => LockingStrategy::WriteCoupling,
//             RWLockCoupling(max_optimistic_level, attempts) =>
//                 LockingStrategy::Optimistic(max_optimistic_level, attempts),
//             OLC(OptimisticLimit {
//                     attempts,
//                     level: max_optimistic_level
//                 }) => LockingStrategy::Dolos(max_optimistic_level, attempts),
//             OLC(LevelConstraints::None) =>
//                 LockingStrategy::Dolos(LevelVariant::Const(Level::MAX), Attempts::MAX)
//         }
//     }
// }
