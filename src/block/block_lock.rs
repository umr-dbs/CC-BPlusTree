use mvcc_bplustree::index::version_info::Version;
use mvcc_bplustree::locking::locking_strategy::ATTEMPT_START;
use crate::block::block::Block;
use crate::utils::hybrid_cell::{ConcurrentGuard, GuardDerefResult, sched_yield, WRITE_FLAG_VERSION};

pub(crate) type BlockGuard<'a> = ConcurrentGuard<'a, Block>;
pub(crate) type BlockGuardResult<'a> = GuardDerefResult<'a, Block>;

impl BlockGuard<'_> {
    #[inline(always)]
    pub(crate) unsafe fn match_cell_version(&self, version: Version) -> bool {
        self.cell_version_olc() == version
    }

    #[inline(always)]
    unsafe fn cell_version_olc(&self) -> Version {
        self.cell_version().unwrap_or(Version::MIN)
    }

    pub(crate) unsafe fn read_cell_version_as_reader(&self) -> Version {
        let mut attempts
            = ATTEMPT_START;

        loop {
            let version
                = self.cell_version_olc();

            if version & WRITE_FLAG_VERSION != 0 {
                sched_yield(attempts);

                attempts += 1;
            } else {
                break version;
            }
        }
    }
}