use std::mem;
use std::ops::{Deref, DerefMut};
use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::locking::locking_strategy::{Attempts, Level, LockingStrategy};
use crate::Index;
use crate::index::node::{LeafLinks, Node, NodeGuard, NodeRef};

impl Index {
    fn has_overflow(&self, node: &Node) -> bool {
        match node.is_leaf() {
            true => node.is_overflow(self.node_manager.allocation_leaf()),
            false => node.is_overflow(self.node_manager.allocation_directory())
        }
    }

    // TODO: Check root strong count 1 too much!
    // TODO: maybe relates to double height increase error!
    fn retrieve_root(&self, lock_level: Level, attempt: Attempts) -> (NodeRef, NodeGuard<'static>) {
        let is_root_lock
            = self.locking_strategy.is_lock_root(lock_level, attempt, self.height());

        let (mut current_root, mut root_guard) = match self.locking_strategy {
            LockingStrategy::SingleWriter => (self.root.clone(), self.root.borrow_free_static()),
            LockingStrategy::WriteCoupling => {
                let node_guard = self.root.borrow_mut_exclusive_static();
                let current_node = self.root.clone();

                (current_node, node_guard)
            }
            LockingStrategy::Optimistic(..) if is_root_lock => {
                let node_guard = self.root.borrow_mut_static();
                let current_node = self.root.clone();

                (current_node, node_guard)
            }
            LockingStrategy::Optimistic(..) => {
                let node_guard = self.root.borrow_read_static();
                let current_node = self.root.clone();

                (current_node, node_guard)
            }
            _ => unreachable!("Sleepy joe hit me -> dolos not allowed")
        };

        let force_restart = match self.locking_strategy {
            LockingStrategy::SingleWriter | LockingStrategy::WriteCoupling => false,
            _ => !is_root_lock
        };

        let has_overflow_root = self.has_overflow(root_guard.deref());
        if force_restart && has_overflow_root {
            mem::drop(root_guard);
            mem::drop(current_root);
            mem::drop(is_root_lock);

            return self.retrieve_root(lock_level, attempt + 1);
        }

        if !has_overflow_root {
            return (current_root, root_guard);
        }

        debug_assert!(root_guard.is_write_lock() || !self.locking_strategy.additional_lock_required());

        match root_guard.deref_mut() {
            Node::Index(keys, children) => {
                let keys_mid = keys.len() / 2;
                let k3 = *keys.get(keys_mid).unwrap();
                let new_keys = keys.split_off(keys_mid + 1);
                keys.pop();
                let new_children = children.split_off(keys_mid + 1);
                let new_node: NodeRef = Node::Index(new_keys, new_children).into();

                let new_root: NodeRef
                    = Node::Index(vec![k3], vec![current_root, new_node]).into();

                let new_guard
                    = self.locking_strategy.lock(new_root.deref());

                self.set_root_on_insert(new_root.into());
                current_root = self.root.clone();
                root_guard = new_guard;
            }
            Node::Leaf(records, root_links) => {
                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let new_node: NodeRef = Node::Leaf(
                    records.split_off(records_mid),
                    LeafLinks::new(current_root.clone().into(), None)).into();

                root_links.right = new_node.clone().into();

                let new_root: NodeRef
                    = Node::Index(vec![k3], vec![current_root, new_node]).into();

                let new_guard
                    = self.locking_strategy.lock(new_root.deref());

                self.set_root_on_insert(new_root.into());
                current_root = self.root.clone();
                root_guard = new_guard;
            }
            Node::MultiVersionLeaf(records, root_links) => {
                let k3 = records
                    .get(records.len() / 2)
                    .unwrap()
                    .key();

                let new_node: NodeRef = Node::MultiVersionLeaf(
                    records.split_off(records.len() / 2),
                    LeafLinks::new(current_root.clone().into(), None)).into();

                root_links.right = new_node.clone().into();

                let new_root: NodeRef
                    = Node::Index(vec![k3], vec![current_root, new_node]).into();

                let new_guard
                    = self.locking_strategy.lock(new_root.deref());

                self.set_root_on_insert(new_root.into());
                current_root = self.root.clone();
                root_guard = new_guard;
            }
        }

        (current_root, root_guard)
    }

    fn do_overflow_correction<'a>(
        &self,
        mut parent_guard: NodeGuard<'a>,
        parent_node: NodeRef,
        child_pos: usize,
        mut from_guard: NodeGuard,
        from_node: NodeRef) -> (NodeRef, NodeGuard<'a>)
    {
        match from_guard.deref_mut() {
            Node::Index(keys, children) => {
                let keys_mid = keys.len() / 2;
                let k3 = *keys.get(keys_mid).unwrap();
                let new_keys = keys.split_off(keys_mid + 1);
                keys.pop();
                let new_children = children.split_off(keys_mid + 1);
                let new_node: NodeRef = Node::Index(new_keys, new_children).into();
                // let new_guard = self.locking_strategy.lock(&new_node);

                parent_guard
                    .keys_mut()
                    .unwrap()
                    .insert(child_pos, k3);

                parent_guard
                    .children_mut()
                    .unwrap()
                    .insert(child_pos + 1, new_node.clone());

                mem::drop(from_guard);
                mem::drop(from_node);

                (parent_node, parent_guard)
            }
            Node::Leaf(records, links) => {
                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let new_node: NodeRef = Node::Leaf(
                    records.split_off(records_mid),
                    LeafLinks::new(from_node.into(), links.right.take())).into();

                links.right
                    = new_node.clone().into();

                // let new_guard
                //     = self.locking_strategy.lock(&new_node);

                parent_guard
                    .children_mut()
                    .unwrap()
                    .insert(child_pos + 1, new_node);

                parent_guard
                    .keys_mut()
                    .unwrap()
                    .insert(child_pos, k3);

                // mem::drop(parent_guard);
                mem::drop(from_guard);
                // mem::drop(from_node);
                // mem::drop(parent_node);

                (parent_node, parent_guard)
            }
            Node::MultiVersionLeaf(records, links) => {
                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let new_node: NodeRef = Node::MultiVersionLeaf(
                    records.split_off(records_mid),
                    LeafLinks::new(from_node.into(), links.right.take())).into();

                links.right
                    = new_node.clone().into();

                // let new_guard
                //     = self.locking_strategy.lock(&new_node);

                parent_guard
                    .children_mut()
                    .unwrap()
                    .insert(child_pos + 1, new_node.clone());

                parent_guard
                    .keys_mut()
                    .unwrap()
                    .insert(child_pos, k3);

                // mem::drop(parent_guard);
                mem::drop(from_guard);
                // mem::drop(from_node);
                // mem::drop(parent_node);

                (parent_node, parent_guard)
            }
        }
    }

    fn traversal_write_internal(&self, lock_level: Level, attempt: Attempts, key: Key) -> (NodeRef, NodeGuard) {
        let mut curr_level = Self::INIT_TREE_HEIGHT;

        let (mut current_node, mut current_guard)
            = self.retrieve_root(lock_level, attempt);

        let mut write_n
            = current_guard.len();

        loop {
            match current_guard.deref() {
                Node::Index(keys, children) => {
                    let (next_node, child_pos) = keys
                        .iter()
                        .enumerate()
                        .find(|(_, k)| key.lt(k))
                        .map(|(pos, _)| (children.get(pos).unwrap().clone(), pos))
                        .unwrap_or_else(|| (children.last().unwrap().clone(), keys.len()));

                    curr_level += 1;

                    // Why on earth is height wrong; check root todo tho -.-
                    let next_guard = if next_node.is_leaf() {
                        self.locking_strategy.lock(next_node.deref())
                        // self.locking_strategy.apply_for(
                        //     curr_level,
                        //     lock_level,
                        //     attempt,
                        //     self.height(),
                        //     next_node.deref())
                    } else {
                        self.locking_strategy.apply_for(
                            curr_level,
                            lock_level,
                            attempt,
                            self.height(),
                            next_node.deref())
                    };

                    let has_overflow_next
                        = self.has_overflow(next_guard.deref());

                    if self.locking_strategy.additional_lock_required() &&
                        (has_overflow_next ||
                            (self.locking_strategy.is_dolos() && (write_n != current_guard.len() || self.has_overflow(current_guard.deref())))) &&
                        (!current_guard.is_write_lock() || !next_guard.is_write_lock())
                    {
                        mem::drop(next_guard);
                        mem::drop(current_guard);
                        mem::drop(next_node);
                        mem::drop(current_node);

                        return self.traversal_write_internal(
                            curr_level - 1,
                            attempt + 1,
                            key);
                    } else if has_overflow_next {
                        let (new_next, new_next_guard) = self.do_overflow_correction(
                            current_guard,
                            current_node,
                            child_pos,
                            next_guard,
                            next_node);

                        current_guard = new_next_guard;
                        current_node = new_next;
                        write_n = current_guard.len();
                    } else {
                        current_guard = next_guard;
                        current_node = next_node;
                        write_n = current_guard.len();
                    }
                }
                _ => break (current_node, current_guard)
            }
        }
    }

    pub(crate) fn traversal_write(&self, key: Key) -> (NodeRef, NodeGuard) {
        self.traversal_write_internal(
            Self::MAX_TREE_HEIGHT,
            mvcc_bplustree::locking::locking_strategy::ATTEMPT_START,
            key)
    }

    pub(crate) fn traversal_read(&self, key: Key) -> (NodeRef, NodeGuard) {
        let mut current_node
            = self.root.clone();

        let mut current_guard
            = self.lock_reader(current_node.deref());

        loop {
            match current_guard.deref() {
                Node::Index(keys, children) => {
                    let next_node = keys
                        .iter()
                        .enumerate()
                        .find(|(_, k)| key.lt(k))
                        .map(|(pos, _)| children
                            .get(pos)
                            .unwrap()
                            .clone())
                        .unwrap_or_else(|| children.last().unwrap().clone());

                    let next_guard =
                        self.lock_reader(next_node.deref());

                    current_guard = next_guard;
                    current_node = next_node;
                }
                _ => break (current_node, current_guard)
            }
        }
    }
}