use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use chronicle_db::tools::safe_cell::SafeCell;
use mvcc_bplustree::index::version_info::{AtomicVersion, Version};
use mvcc_bplustree::locking::locking_strategy::{Level, LockingStrategy};
use crate::node_manager::NodeManager;
use crate::node::{NodeRef, Node, LeafLinks};

pub(crate) type Index = BPlusTree;

pub struct BPlusTree {
    pub(crate) root: SafeCell<NodeRef>,
    pub(crate) locking_strategy: LockingStrategy,
    pub(crate) node_manager: NodeManager,
    pub(crate) version_manager: AtomicVersion,
    height: AtomicUsize,
}

impl Into<SafeCell<NodeRef>> for Node {
    fn into(self) -> SafeCell<NodeRef> {
        SafeCell::new(self.into())
    }
}

impl BPlusTree {
    pub(crate) const INIT_TREE_HEIGHT: Level = 1;
    pub(crate) const MAX_TREE_HEIGHT: Level = usize::MAX;
    pub(crate) const START_VERSION: Version = 0;

    pub fn new_single_versioned() -> Self {
        Self {
            root: Node::Leaf(vec![], LeafLinks::default()).into(),
            locking_strategy: LockingStrategy::SingleWriter,
            node_manager: Default::default(),
            version_manager: AtomicVersion::new(Self::START_VERSION),
            height: AtomicUsize::new(Self::INIT_TREE_HEIGHT),
        }
    }

    pub fn new_multi_versioned() -> Self {
        Self {
            root: Node::MultiVersionLeaf(vec![], LeafLinks::default()).into(),
            locking_strategy: LockingStrategy::SingleWriter,
            node_manager: Default::default(),
            version_manager: AtomicVersion::new(Self::START_VERSION),
            height: AtomicUsize::new(Self::INIT_TREE_HEIGHT),
        }
    }

    pub fn height(&self) -> usize {
        self.height.load(Relaxed)
    }

    pub(crate) fn next_version(&self) -> Version {
        self.version_manager.fetch_add(1, Relaxed)
    }
}