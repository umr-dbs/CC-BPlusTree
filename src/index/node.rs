use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use chronicle_db::tools::aliases::Keys;
use mvcc_bplustree::block::block::{AtomicBlockID, BlockID};
use mvcc_bplustree::index::record::Record;
use mvcc_bplustree::utils::cc_cell::{CCCellGuard, CCCell};
use crate::index::record_list::RecordList;

pub(crate) type NodeRef = Arc<CCCell<Node>>;
pub(crate) type NodeGuard<'a> = CCCellGuard<'a, Node>;
pub(crate) type ChildrenRef = Vec<NodeRef>;
pub(crate) type NodeLink = Option<NodeRef>;

// #[derive(Clone)]
// pub struct Block {
//     id: BlockID,
//     node_data: Node
// }
//
// impl Deref for Block {
//     type Target = Node;
//
//     fn deref(&self) -> &Self::Target {
//         &self.node_data
//     }
// }
//
// impl DerefMut for Block {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.node_data
//     }
// }
//
// impl Block {
//     const BLOCK_ID_COUNTER: AtomicBlockID = AtomicBlockID::new(0);
//
//     pub fn new_leaf() -> Self {
//         Self {
//             id: Self::BLOCK_ID_COUNTER.fetch_add(1, Relaxed),
//             node_data: Node::Leaf(vec![])
//         }
//     }
//
//     pub fn new_multi_version_leaf() -> Self {
//         Self {
//             id: Self::BLOCK_ID_COUNTER.fetch_add(1, Relaxed),
//             node_data: Node::MultiVersionLeaf(vec![])
//         }
//     }
// }

#[derive(Clone)]
pub(crate) enum Node {
    Index(Keys, ChildrenRef),
    Leaf(Vec<Record>),
    MultiVersionLeaf(Vec<RecordList>),
}

impl Into<NodeRef> for Node {
    fn into(self) -> NodeRef {
        Arc::new(CCCell::new(self))
    }
}

impl Node {
    pub(crate) fn is_overflow(&self, allocation: usize) -> bool {
        debug_assert!(allocation >= self.len());

        self.len() >= allocation
    }

    pub const fn is_leaf(&self) -> bool {
        match self {
            Node::Index(..) => false,
            _ => true
        }
    }

    pub(crate) fn children_mut(&mut self) -> Option<&mut Vec<NodeRef>> {
        match self {
            Node::Index(_, children) => Some(children),
            _ => None
        }
    }

    pub(crate) fn keys_mut(&mut self) -> Option<&mut Keys> {
        match self {
            Node::Index(keys, _) => Some(keys),
            _ => None
        }
    }

    pub const fn is_directory(&self) -> bool {
        !self.is_leaf()
    }

    pub(crate) fn push_record(&mut self, record: Record, is_update: bool) -> bool {
        match self {
            Node::Leaf(records) => match records.binary_search(&record) {
                Ok(pos) if is_update => records
                    .get_mut(pos)
                    .unwrap()
                    .delete(record.insertion_version())
                    .then(|| {
                        records.insert(pos + 1, record);
                        true
                    }).unwrap_or(false),
                Err(pos) if !is_update => {
                    records.insert(pos, record);
                    true
                }
                _ => false
            }
            Node::MultiVersionLeaf(records_lists, ..) => match records_lists
                .binary_search_by_key(&record.key(), |record_list| record_list.key())
            {
                Ok(pos) if is_update => {
                    let record_list = records_lists
                        .get_mut(pos)
                        .unwrap();

                    record_list.delete(record.insertion_version())
                        .then(|| {
                            record_list.push_front(record);
                            true
                        }).unwrap_or(false)
                }
                Err(pos) if !is_update => {
                    records_lists.insert(pos, RecordList::from_record(record));
                    true
                }
                _ => false
            }
            _ => false
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Node::Index(keys, _) => keys.len(),
            Node::Leaf(records) => records.len(),
            Node::MultiVersionLeaf(record_lists) => record_lists.len()
        }
    }
}

#[derive(Clone)]
pub(crate) struct LeafLinks {
    pub(crate) left: Option<NodeRef>,
    pub(crate) right: Option<NodeRef>,
}

impl LeafLinks {
    pub const fn new(left: NodeLink, right: NodeLink) -> Self {
        Self {
            left,
            right,
        }
    }

    pub const fn none() -> Self {
        Self {
            left: None,
            right: None,
        }
    }
}

impl Default for LeafLinks {
    fn default() -> Self {
        Self::none()
    }
}

impl Default for Node {
    fn default() -> Self {
        Self::Leaf(vec![])
    }
}