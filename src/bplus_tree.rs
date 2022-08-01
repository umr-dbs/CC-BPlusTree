use std::collections::LinkedList;
use std::ops::{Deref, DerefMut};
use mvcc_bplustree::index::record::Record;
use mvcc_bplustree::locking::locking_strategy::LockingStrategy;
use crate::node::{NodeRef, Node, NodeGuard, LeafLinks};

pub struct BPlusTree {
    root: NodeRef,
    locking_strategy: LockingStrategy
}

impl BPlusTree {
    pub fn new_single_versioned() -> Self {
        Self {
            root: Node::Leaf(vec![], LeafLinks::default()).into(),
            locking_strategy: LockingStrategy::SingleWriter
        }
    }

    pub fn new_multi_versioned() -> Self {
        Self {
            root: Node::MultiVersionLeaf(vec![], LeafLinks::default()).into(),
            locking_strategy: LockingStrategy::SingleWriter
        }
    }

    fn lock_node(&self, node: &NodeRef) -> NodeGuard {
        match self.locking_strategy {
            LockingStrategy::SingleWriter => node.borrow_free_static(),
            LockingStrategy::WriteCoupling => node.borrow_mut_exclusive_static(),
            LockingStrategy::Optimistic(..) => node.borrow_read_static(),
            LockingStrategy::Dolos(..) => node.borrow_free_static()
        }
    }

    pub fn insert(&mut self, record: Record) {
        // TODO: Adapt for coupled locking

        let mut current_node = self.root.clone();
        let mut node_guard = self.lock_node(&current_node);

        loop {
            match node_guard.deref() {
                Node::Index(keys, children) => match keys.binary_search(&record.key()) {
                    Ok(pos) | Err(pos) => {
                        // TODO: Preemptive overflow check index

                        current_node = children.get(pos).unwrap().clone();
                        node_guard = self.lock_node(&current_node);
                    }
                }
                Node::Leaf(records, _) => match records.binary_search(&record) {
                    Ok(pos) | Err(pos) => {
                        // TODO: Preemptive overflow check single version leaf

                        node_guard
                            .deref_mut()
                            .as_records_mut()
                            .insert(pos, record);

                        break
                    },
                }
                Node::MultiVersionLeaf(..) => {
                    // TODO: Preemptive overflow check multi version leaf

                    let records_version_lists = node_guard
                        .deref_mut()
                        .as_records_versioned_mut();

                    match records_version_lists
                        .iter_mut()
                        .find(|version_list| version_list
                            .front()
                            .unwrap()
                            .key() == record.key())
                    {
                        Some(version_list) => version_list.push_front(record),
                        _ => records_version_lists.push(LinkedList::from_iter(vec![record]))
                    }

                    break
                }
            }
        }
    }
}