use std::collections::LinkedList;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicUsize;
use mvcc_bplustree::index::record::Record;
use mvcc_bplustree::locking::locking_strategy::LockingStrategy;
use crate::node_manager::NodeManager;
use crate::node::{NodeRef, Node, NodeGuard, LeafLinks};

pub(crate) type Level = usize;

pub struct BPlusTree {
    root: NodeRef,
    locking_strategy: LockingStrategy,
    node_manager: NodeManager,
    height: AtomicUsize,
}

impl BPlusTree {
    pub(crate) const INIT_TREE_HEIGHT: Level = 1;
    pub(crate) const MAX_TREE_HEIGHT: Level = usize::MAX;

    pub fn new_single_versioned() -> Self {
        Self {
            root: Node::Leaf(vec![], LeafLinks::default()).into(),
            locking_strategy: LockingStrategy::SingleWriter,
            node_manager: Default::default(),
            height: AtomicUsize::new(Self::INIT_TREE_HEIGHT),
        }
    }

    pub fn new_multi_versioned() -> Self {
        Self {
            root: Node::MultiVersionLeaf(vec![], LeafLinks::default()).into(),
            locking_strategy: LockingStrategy::SingleWriter,
            node_manager: Default::default(),
            height: AtomicUsize::new(Self::INIT_TREE_HEIGHT),
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

    fn has_overflow(&self, node: &Node) -> bool {
        match node.is_leaf() {
            true => node.is_overflow(self.node_manager.allocation_leaf()),
            false => node.is_overflow(self.node_manager.allocation_directory())
        }
    }

    fn retrieve_root(&self) -> (NodeRef, NodeGuard) {
        match self.locking_strategy {
            LockingStrategy::SingleWriter => (self.root.clone(), self.root.borrow_free_static()),
            LockingStrategy::WriteCoupling => {
                let mut node_guard = self.root.borrow_mut_exclusive_static();
                let mut current_node = self.root.clone();

                (current_node, node_guard)
            }
            LockingStrategy::Optimistic(_, _) => {
                unimplemented!()
            }
            LockingStrategy::Dolos(_, _) => {
                unimplemented!()
            }
        }
    }

    pub fn insert(&mut self, record: Record) {
        let (mut current_node, mut current_guard)
            = self.retrieve_root();

        loop {
            match current_guard.deref() {
                Node::Index(keys, children) => match keys.binary_search(&record.key()) {
                    Ok(pos) | Err(pos) => {
                        let next_node = children
                            .get(pos)
                            .unwrap()
                            .clone();

                        let next_guard
                            = self.lock_node(&next_node);

                        // TODO: Preemptive overflow check index

                        current_guard = next_guard;
                        current_node = next_node;
                    }
                }
                Node::Leaf(records, _) => match records.binary_search(&record) {
                    Ok(pos) | Err(pos) => {
                        // TODO: Preemptive overflow check single version leaf

                        current_guard
                            .deref_mut()
                            .as_records_mut()
                            .insert(pos, record);

                        break;
                    }
                }
                Node::MultiVersionLeaf(..) => {
                    // TODO: Preemptive overflow check multi version leaf

                    let records_version_lists = current_guard
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

                    break;
                }
            }
        }
    }
}