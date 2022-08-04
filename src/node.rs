use std::collections::LinkedList;
use std::sync::Arc;
use chronicle_db::tools::aliases::{Key, Keys};
use mvcc_bplustree::index::record::Record;
use mvcc_bplustree::utils::cc_cell_rrw_new::{CCCellGuard, CCCellRRWOPT};

// CCCellRRWOPT is the specialized cell, implementing different access ways in the MVCCBPlusTree, the doc:
/*  // 1. Write via RwLock write lock.
    // 2. Read via RwLock read lock.
    // 3. Write Exclusive via Mutex lock.
    // 4. Lock-free read via unsafe borrow.
    // 5. Lock-free write via unsafe borrow mut.
 */

pub(crate) type NodeRef = Arc<CCCellRRWOPT<Node>>;
pub(crate) type NodeGuard<'a> = CCCellGuard<'a, Node>;
pub(crate) type ChildrenRef = Vec<NodeRef>;
pub(crate) type NodeLink = Option<NodeRef>;

pub(crate) enum Node {
    Index(Keys, ChildrenRef),
    Leaf(Vec<Record>, LeafLinks),
    MultiVersionLeaf(Vec<LinkedList<Record>>, LeafLinks),
}


impl Into<NodeRef> for Node {
    fn into(self) -> NodeRef {
        Arc::new(CCCellRRWOPT::new(self))
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

    // pub(crate) const fn leaf_links(&self) -> Option<&LeafLinks> {
    //     match self {
    //         Node::Leaf(_, leaf_links) => Some(leaf_links),
    //         Node::MultiVersionLeaf(_, leaf_links) => Some(leaf_links),
    //         _ => None
    //     }
    // }
    //
    // pub(crate) fn leaf_links_mut(&mut self) -> Option<&mut LeafLinks> {
    //     match self {
    //         Node::Leaf(_, leaf_links) => Some(leaf_links),
    //         Node::MultiVersionLeaf(_, leaf_links) => Some(leaf_links),
    //         _ => None
    //     }
    // }

    pub const fn is_directory(&self) -> bool {
        !self.is_leaf()
    }

    // pub(crate) fn push_record(&mut self, record: Record) -> bool {
    //     match self {
    //         Node::Leaf(records, _) => match records.binary_search(&record) {
    //             Ok(pos) | Err(pos) => records.insert(pos, record)
    //         }
    //         Node::MultiVersionLeaf(records_version_lists, ..) => match records_version_lists
    //             .binary_search_by_key(&record.key(), |version_list| version_list.front().unwrap().key())
    //         {
    //             Ok(pos) => records_version_lists.get_mut(pos).unwrap().push_front(record),
    //             Err(pos) => records_version_lists.insert(pos, LinkedList::from_iter(vec![record]))
    //         }
    //         _ => return false
    //     }
    //
    //     true
    // }

    pub(crate) fn push_record(&mut self, record: Record, is_update: bool) -> bool {
        match self {
            Node::Leaf(records, _) => match records.binary_search(&record) {
                Ok(pos) => {
                    let old_record = records
                        .get_mut(pos)
                        .unwrap();

                    if !is_update && !old_record.is_deleted() {
                        false
                    }
                    else if is_update {
                        if !old_record.is_deleted() {
                            old_record.delete(record.insertion_version());
                        }

                        records.insert(pos + 1, record);
                        true
                    }
                    else {
                        false
                    }
                },
                Err(pos) => {
                    records.insert(pos, record);
                    true
                }
            }
            Node::MultiVersionLeaf(records_version_lists, ..) => match records_version_lists
                .binary_search_by_key(&record.key(), |version_list| version_list.front().unwrap().key())
            {
                Ok(pos) => {
                    debug_assert_eq!(pos, 0);

                    let version_list = records_version_lists
                        .get_mut(pos)
                        .unwrap();

                    let youngest_record = version_list
                        .front_mut()
                        .unwrap();

                    if !youngest_record.is_deleted() {
                        return false
                    }

                    youngest_record.delete(record.insertion_version());
                    version_list.push_front(record);
                    true
                },
                Err(pos) => {
                    records_version_lists.insert(pos, LinkedList::from_iter(vec![record]));
                    true
                }
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

pub(crate) struct LeafLinks {
    pub(crate) left: Option<NodeRef>,
    pub(crate) right: Option<NodeRef>,
}

impl LeafLinks {
    pub const fn new(left: NodeLink, right: NodeLink) -> Self {
        Self {
            left,
            right
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