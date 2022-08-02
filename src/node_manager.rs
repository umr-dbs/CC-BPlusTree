use std::ops::Deref;

pub struct NodeSettings {
    leaf_allocation: usize,
    index_allocation: usize,
}

impl Default for NodeSettings {
    fn default() -> Self {
        Self {
            leaf_allocation: 10,
            index_allocation: 10,
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
    pub const fn node_settings(&self) -> &NodeSettings {
        match self {
            NodeManager::SingleVersion(node_settings) =>
                node_settings,
            NodeManager::MultiVersion(node_settings) =>
                node_settings
        }
    }
}