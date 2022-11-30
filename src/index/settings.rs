use std::collections::HashMap;
use std::hash::Hash;
use std::{fs, mem};
use TXDataModel::page_model::block::Block;
use TXDataModel::page_model::ObjectCount;
use TXDataModel::record_model::AtomicVersion;
use TXDataModel::record_model::record_like::RecordLike;
use TXDataModel::record_model::record_list::RecordList;
use TXDataModel::utils::un_cell::UnCell;
use crate::block::block_manager::BlockManager;
use crate::bplus_tree::BPlusTree;
use crate::index::bplus_tree::{INIT_TREE_HEIGHT, START_VERSION};
use crate::index::root::Root;
use crate::locking::locking_strategy::LockingStrategy;

pub const CONFIG_INI_PATH: &'static str = "config.ini";

/// Retrieves a configuration of a WG formatted ini file.
pub fn configuration_of(path: &str, is_file: bool) -> Vec<(String, String)> {
    match is_file {
        false => path.to_string(),
        _ => fs::read_to_string(path).unwrap_or_default()
    }.lines().filter(|line|
        !line.is_empty() &&
            !line.contains("#") &&
            // !line.contains("[") &&
            !line.as_bytes().iter().all(|b| *b == b'\t' || *b == b' '))
        .map(|line| line.replace("\t", "").replace(" ", ""))
        .map(|line| {
            let configuration = line.split("=").collect::<Vec<_>>();
            (configuration[0].to_lowercase(), configuration[1].to_string())
        }).collect()
}

pub fn init_from_config_ini<
    const KEY_SIZE: usize,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync,
    Entry: Default + RecordLike<Key, Payload> + Sync>() -> BPlusTree<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry>
{
    init_from(load_config(CONFIG_INI_PATH, true))
}

pub fn init_from_file<
    const KEY_SIZE: usize,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync,
    Entry: Default + RecordLike<Key, Payload> + Sync>(path: & str, is_file: bool)
    -> BPlusTree<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry>
{
    init_from(load_config(path, is_file))
}

pub fn load_config(path: &str, is_file: bool) -> HashMap<String, String> {
    configuration_of(path, is_file)
        .into_iter()
        .collect()
}

fn init_from<
    const KEY_SIZE: usize,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync,
    Entry: Default + RecordLike<Key, Payload> + Sync>(config: HashMap<String, String>)
    -> BPlusTree<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry>
{
    let locking_strategy
        = LockingStrategy::load(&config);

    let block_manager: BlockManager<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry>
        = BlockSettings::load(&config).into();

    let root = UnCell::new(Root::new(
        block_manager.new_empty_leaf().into_cell(locking_strategy.is_olc()),
        INIT_TREE_HEIGHT));

    BPlusTree {
        root,
        locking_strategy,
        block_manager,
        version_counter: AtomicVersion::new(START_VERSION),
    }
}

pub(crate) const DEFAULT_IS_MULTI_VERSION: bool = false;

#[derive(Clone)]
pub(crate) struct BlockSettings {
    pub(crate) is_multi_version: bool,
}

impl Default for BlockSettings {
    fn default() -> Self {
        Self {
            is_multi_version: DEFAULT_IS_MULTI_VERSION,
        }
    }
}

impl BlockSettings {
    const KEY_BLOCK_SIZE: &'static str = "blocksize";
    const KEY_MULTI_VERSION: &'static str = "multiversion";
    const KEY_ENTRY_LAYOUT: &'static str = "payload";

    pub(crate) fn new(block_size: usize, is_multi_version: bool) -> Self {
        Self {
            is_multi_version,
        }
    }

    pub(crate) fn load(configs: &HashMap<String, String>) -> BlockSettings {
        let is_multi_version = configs
            .get(Self::KEY_MULTI_VERSION)
            .unwrap_or(&DEFAULT_IS_MULTI_VERSION.to_string())
            .parse()
            .unwrap();

        BlockSettings {
            is_multi_version,
        }
    }
}

impl<const KEY_SIZE: usize,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone,
    Entry: Default + RecordLike<Key, Payload>> Into<BlockManager<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry>> for BlockSettings {
    fn into(self) -> BlockManager<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry> {
        BlockManager::new(self.is_multi_version)
    }
}