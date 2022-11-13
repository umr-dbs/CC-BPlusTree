use std::collections::HashMap;
use std::mem;
use chronicle_db::backbone::core::event::Event;
use chronicle_db::backbone::core::event::EventVariant::F64;
use chronicle_db::configuration::configs::configuration_of;
use chronicle_db::tools::aliases::{Key, ObjectCount};
use mvcc_bplustree::index::record::Payload;
use mvcc_bplustree::index::version_info::AtomicVersion;
use crate::block::block::Block;
use crate::block::block_manager::BlockManager;
use crate::index::record_list::RecordList;
use crate::bplus_tree::BPlusTree;
use crate::index::root::Root;
use crate::locking::locking_strategy::LockingStrategy;
use crate::utils::un_cell::UnCell;

pub const CONFIG_INI_PATH: &'static str = "config.ini";

pub fn init_from_config_ini() -> BPlusTree {
    init_from(load_config(CONFIG_INI_PATH, true))
}

pub fn init_from_file(path: &str, is_file: bool) -> BPlusTree {
    init_from(load_config(path, is_file))
}

pub fn load_config(path: &str, is_file: bool) -> HashMap<String, String> {
    configuration_of(path, is_file)
        .into_iter()
        .collect()
}

fn init_from(config: HashMap<String, String>) -> BPlusTree {
    let locking_strategy
        = LockingStrategy::load(&config);

    let block_manager: BlockManager = BlockSettings::load(&config)
        .into();

    let root = UnCell::new(Root::new(
        block_manager.new_empty_leaf().into_cell(locking_strategy.is_olc()),
        BPlusTree::INIT_TREE_HEIGHT));

    BPlusTree {
        root,
        locking_strategy,
        block_manager,
        version_counter: AtomicVersion::new(BPlusTree::START_VERSION)
    }
}

pub(crate) const DEFAULT_BLOCK_SIZE: usize = mem::size_of::<Block>() + 10 * mem::size_of::<Payload>();
pub(crate) const DEFAULT_IS_MULTI_VERSION: bool = false;
const DEFAULT_PAYLOAD: Payload = F64(0 as _);

#[derive(Clone)]
pub(crate) struct BlockSettings {
    pub(crate) block_size: usize,
    pub(crate) is_multi_version: bool,
    pub(crate) data_entry: Payload,
}

impl Default for BlockSettings {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            is_multi_version: DEFAULT_IS_MULTI_VERSION,
            data_entry: DEFAULT_PAYLOAD
        }
    }
}

impl BlockSettings {
    const KEY_BLOCK_SIZE: &'static str = "blocksize";
    const KEY_MULTI_VERSION: &'static str = "multiversion";
    const KEY_ENTRY_LAYOUT: &'static str = "payload";

    pub(crate) fn new(block_size: usize, is_multi_version: bool, data_entry: Payload) -> Self {
        Self {
            block_size,
            is_multi_version,
            data_entry
        }
    }

    pub(crate) fn load(configs: &HashMap<String, String>) -> BlockSettings {
        let block_size = configs
            .get(Self::KEY_BLOCK_SIZE)
            .unwrap_or(&DEFAULT_BLOCK_SIZE.to_string())
            .parse()
            .unwrap();

        let is_multi_version = configs
            .get(Self::KEY_MULTI_VERSION)
            .unwrap_or(&DEFAULT_IS_MULTI_VERSION.to_string())
            .parse()
            .unwrap();

        let data_entry = serde_json::from_str(configs
            .get(Self::KEY_ENTRY_LAYOUT)
            .unwrap_or(&serde_json::to_string(&DEFAULT_PAYLOAD).unwrap())
        ).unwrap();

        BlockSettings {
            block_size,
            is_multi_version,
            data_entry
        }
    }
}

impl Into<BlockManager> for BlockSettings {
    fn into(self) -> BlockManager {
        let payload_size = mem::size_of::<ObjectCount>() + match self.is_multi_version {
            true => mem::size_of::<RecordList>(),
            false => mem::size_of::<Event>()
        };

        let aligned_block_size = self.block_size - mem::size_of::<Block>();
        let leaf_allocation = aligned_block_size / payload_size;
        let index_allocation = aligned_block_size / mem::size_of::<Key>() / 2 - 1;

        BlockManager::new(
            leaf_allocation,
            index_allocation,
            self.is_multi_version)
    }
}