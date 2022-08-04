use std::ops::Deref;
use crate::node::{LeafLinks, Node, NodeLink};

pub(crate) const DEFAULT_ALLOCATION_LEAF: usize = 10;
pub(crate) const DEFAULT_ALLOCATION_INDEX: usize = 10;

pub struct NodeSettings {
    leaf_allocation: usize,
    index_allocation: usize,
}

impl Default for NodeSettings {
    fn default() -> Self {
        Self {
            leaf_allocation: DEFAULT_ALLOCATION_LEAF,
            index_allocation: DEFAULT_ALLOCATION_INDEX,
        }
    }
}

impl NodeSettings {
    pub const fn new(leaf_allocation: usize, index_allocation: usize) -> Self {
        Self {
            leaf_allocation,
            index_allocation
        }
    }

    pub const fn allocation_leaf(&self) -> usize {
        self.leaf_allocation
    }

    pub const fn allocation_directory(&self) -> usize {
        self.index_allocation
    }
}

impl NodeManager {
    pub const fn leaf_allocation(&self) -> usize {
        match self {
            NodeManager::SingleVersion(node_settings) => node_settings.leaf_allocation,
            NodeManager::MultiVersion(node_settings) => node_settings.leaf_allocation,
        }
    }

    pub const fn index_allocation(&self) -> usize {
        match self {
            NodeManager::SingleVersion(node_settings) => node_settings.index_allocation,
            NodeManager::MultiVersion(node_settings) => node_settings.index_allocation,
        }
    }
}

pub enum NodeManager {
    SingleVersion(NodeSettings),
    MultiVersion(NodeSettings),
}

impl Deref for NodeManager {
    type Target = NodeSettings;

    fn deref(&self) -> &Self::Target {
        match self {
            NodeManager::SingleVersion(node_settings) => node_settings,
            NodeManager::MultiVersion(node_settings) => node_settings
        }
    }
}


impl Default for NodeManager {
    fn default() -> Self {
        Self::SingleVersion(NodeSettings::default())
    }
}

impl NodeManager {
    pub(crate) fn single_version_with(node_settings: NodeSettings) -> Self {
        Self::SingleVersion(node_settings)
    }

    pub(crate) fn multi_version_with(node_settings: NodeSettings) -> Self {
        Self::MultiVersion(node_settings)
    }

    pub const fn node_settings(&self) -> &NodeSettings {
        match self {
            NodeManager::SingleVersion(node_settings) =>
                node_settings,
            NodeManager::MultiVersion(node_settings) =>
                node_settings
        }
    }

    pub(crate) fn make_empty_root(&self) -> Node {
        match self {
            NodeManager::SingleVersion(_) =>
                Node::Leaf(vec![], LeafLinks::none()),
            NodeManager::MultiVersion(_) =>
                Node::MultiVersionLeaf(vec![], LeafLinks::none())
        }
    }
}