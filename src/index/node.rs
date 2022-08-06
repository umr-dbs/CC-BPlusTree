use std::sync::Arc;
use chronicle_db::tools::aliases::Keys;
use mvcc_bplustree::index::record::Record;
use mvcc_bplustree::utils::cc_cell::{CCCellGuard, CCCell};
use crate::index::record_list::RecordList;

pub(crate) type NodeRef = Arc<CCCell<Node>>;
pub(crate) type NodeGuard<'a> = CCCellGuard<'a, Node>;
pub(crate) type ChildrenRef = Vec<NodeRef>;
pub(crate) type NodeLink = Option<NodeRef>;

#[derive(Clone)]
pub(crate) enum Node {
    Index(Keys, ChildrenRef),
    Leaf(Vec<Record>, LeafLinks),
    MultiVersionLeaf(Vec<RecordList>, LeafLinks),
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
            Node::Leaf(records, _) => match records.binary_search(&record) {
                Ok(pos) if is_update => {
                    let old_record = records
                        .get_mut(pos)
                        .unwrap();

                    if !old_record.is_deleted() {
                        old_record.delete(record.insertion_version());
                    }

                    records.insert(pos + 1, record);
                    true
                }
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

                    if !record_list.is_deleted() {
                        record_list.delete(record.insertion_version());
                    }

                    record_list.push_front(record);
                    true
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
            Node::Leaf(records, _) => records.len(),
            Node::MultiVersionLeaf(record_lists, _) => record_lists.len()
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
        Self::Leaf(vec![], LeafLinks::default())
    }
}