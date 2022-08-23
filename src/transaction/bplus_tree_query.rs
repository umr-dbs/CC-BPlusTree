use std::mem;
use std::ops::{Deref, DerefMut};
use std::ptr::null;
use std::sync::Arc;
use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::locking::locking_strategy::{Attempts, Level, LockingStrategy};
use mvcc_bplustree::utils::cc_cell::CCCell;
use crate::Index;
use crate::index::node::{Node, NodeGuard, NodeGuardResult, NodeRef};
use crate::utils::vcc_cell::{ConcurrentCell, GuardDerefResult, OptCell};

impl Index {
    fn has_overflow(&self, node: &Node) -> bool {
        match node.is_leaf() {
            true => node.is_overflow(self.node_manager.allocation_leaf()),
            false => node.is_overflow(self.node_manager.allocation_directory())
        }
    }

    fn retrieve_root(&self, lock_level: Level, attempt: Attempts) -> NodeGuard<'static> {
        let is_root_lock
            = self.locking_strategy.is_lock_root(lock_level, attempt, self.height());

        let mut root_guard = match self.locking_strategy {
            LockingStrategy::SingleWriter => self.root.borrow_free_static(),
            LockingStrategy::Dolos(..) => self.root.borrow_free_static(),
            LockingStrategy::WriteCoupling => self.root.borrow_mut_exclusive_static(),
            LockingStrategy::Optimistic(..) if is_root_lock => self.root.borrow_mut_static(),
            LockingStrategy::Optimistic(..) => self.root.borrow_read_static(),
        };

        let root_guard_mut = root_guard
            .try_deref_mut();

        if !root_guard_mut.is_valid() {
            mem::drop(root_guard_mut);
            mem::drop(root_guard);
            mem::drop(is_root_lock);

            return self.retrieve_root(lock_level, attempt + 1);
        }

        // Note: checked as valid here, i.e. definitely locked
        // WARNING: UNBOXING not allowed and must keep deref_result alive for invariant!!
        let root_mut = root_guard_mut
            .assume_mut()
            .unwrap();

        let has_overflow_root
            = self.has_overflow(root_mut);

        let force_restart = match self.locking_strategy {
            LockingStrategy::SingleWriter | LockingStrategy::WriteCoupling => false,
            _ => !is_root_lock
        };

        if force_restart && has_overflow_root {
            mem::drop(root_guard_mut);
            mem::drop(root_guard);
            mem::drop(is_root_lock);

            return self.retrieve_root(lock_level, attempt + 1);
        }

        if !has_overflow_root {
            return root_guard;
        }

        match root_mut {
            Node::Index(keys, children) => {
                let keys_mid = keys.len() / 2;
                let k3 = *keys.get(keys_mid).unwrap();
                let new_keys = keys.split_off(keys_mid + 1);
                keys.pop();
                let new_children = children.split_off(keys_mid + 1);

                let new_node_right: NodeRef = Node::Index(new_keys, new_children)
                    .into_node_ref(self.locking_strategy());

                let new_root_left: NodeRef =
                    Node::Index(keys.split_off(0), children.split_off(0))
                        .into_node_ref(self.locking_strategy());

                *root_mut
                    = Node::Index(vec![k3], vec![new_root_left, new_node_right]);
            }
            Node::Leaf(records) => {
                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let new_node_right: NodeRef = Node::Leaf(
                    records.split_off(records_mid), ).into_node_ref(self.locking_strategy());

                let new_node_left: NodeRef = Node::Leaf(
                    records.split_off(0)).into_node_ref(self.locking_strategy());

                *root_mut
                    = Node::Index(vec![k3], vec![new_node_left, new_node_right]);
            }
            Node::MultiVersionLeaf(records) => {
                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let new_node_right: NodeRef = Node::MultiVersionLeaf(
                    records.split_off(records_mid), ).into_node_ref(self.locking_strategy());

                let new_node_left: NodeRef = Node::MultiVersionLeaf(
                    records.split_off(0)).into_node_ref(self.locking_strategy());

                *root_mut
                    = Node::Index(vec![k3], vec![new_node_left, new_node_right]);
            }
        }

        self.inc_height();

        root_guard
    }

    fn do_overflow_correction(
        &self,
        parent_guard: &GuardDerefResult<Node>,
        child_pos: usize,
        from_guard: GuardDerefResult<Node>)
    {
        match from_guard.assume_mut().unwrap() {
            Node::Index(keys, children) => {
                let keys_mid = keys.len() / 2;
                let k3 = *keys.get(keys_mid).unwrap();
                let new_keys = keys.split_off(keys_mid + 1);
                keys.pop();
                let new_children = children.split_off(keys_mid + 1);
                let new_node: NodeRef = Node::Index(new_keys, new_children).into_node_ref(self.locking_strategy());

                let parent_mut
                    = parent_guard.assume_mut().unwrap();

                parent_mut
                    .keys_mut()
                    .unwrap()
                    .insert(child_pos, k3);

                parent_mut
                    .children_mut()
                    .unwrap()
                    .insert(child_pos + 1, new_node.clone());

                mem::drop(from_guard);
            }
            Node::Leaf(records) => {
                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let new_node: NodeRef = Node::Leaf(
                    records.split_off(records_mid), ).into_node_ref(self.locking_strategy());

                let parent_mut
                    = parent_guard.assume_mut().unwrap();

                parent_mut
                    .children_mut()
                    .unwrap()
                    .insert(child_pos + 1, new_node);

                parent_mut
                    .keys_mut()
                    .unwrap()
                    .insert(child_pos, k3);

                mem::drop(from_guard);
            }
            Node::MultiVersionLeaf(records) => {
                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let new_node: NodeRef = Node::MultiVersionLeaf(
                    records.split_off(records_mid), ).into_node_ref(self.locking_strategy());

                let parent_mut
                    = parent_guard.assume_mut().unwrap();

                parent_mut
                    .children_mut()
                    .unwrap()
                    .insert(child_pos + 1, new_node.clone());

                parent_mut
                    .keys_mut()
                    .unwrap()
                    .insert(child_pos, k3);

                mem::drop(from_guard);
            }
        }
    }

    fn traversal_write_internal(&self, lock_level: Level, attempt: Attempts, key: Key) -> NodeGuardResult<'_> {
        let mut curr_level = Self::INIT_TREE_HEIGHT;

        let mut current_guard
            = self.retrieve_root(lock_level, attempt);

        let height
            = self.height();

        let is_dolos
            = self.locking_strategy.is_dolos();

        loop {
            let mut current_node_deref = if self.locking_strategy
                .is_lock(curr_level, lock_level, attempt, height)
            {
                current_guard.try_deref_mut()
            } else {
                current_guard.try_deref()
            };

            let deref_as_ref
                = current_node_deref.as_ref();

            if deref_as_ref.is_none() {
                mem::drop(current_guard);

                return self.traversal_write_internal(
                    curr_level - 1,
                    attempt + 1,
                    key);
            }

            let current
                = deref_as_ref.unwrap();

            match current {
                Node::Index(keys, children) => {
                    let (next_node, child_pos) = keys
                        .iter()
                        .enumerate()
                        .find(|(_, k)| key.lt(k))
                        .map(|(pos, _)| (children.get(pos).unwrap().clone(), pos))
                        .unwrap_or_else(|| (children.last().unwrap().clone(), keys.len()));

                    curr_level += 1;

                    let next_guard = self.apply_for(
                        curr_level,
                        lock_level,
                        attempt,
                        height,
                        &next_node);

                    let mut next_guard_deref
                        = next_guard.try_deref();

                    let next_guard_deref_as_ref
                        = next_guard_deref.as_ref();

                    if next_guard_deref_as_ref.is_none() {
                        mem::drop(current_node_deref);
                        mem::drop(next_guard);
                        mem::drop(current_guard);
                        mem::drop(next_node);

                        return self.traversal_write_internal(
                            curr_level - 1,
                            attempt + 1,
                            key);
                    }

                    let next_guard_ref
                        = next_guard_deref_as_ref.unwrap();

                    let has_overflow_next
                        = self.has_overflow(next_guard_ref);

                    if self.locking_strategy.additional_lock_required() &&
                        (has_overflow_next ||
                            (is_dolos && (!current_guard.is_valid() || self.has_overflow(current)))) &&
                        (!current_node_deref.can_mut() || !next_guard_deref.can_mut())
                    {
                        mem::drop(next_guard_deref);
                        mem::drop(current_node_deref);
                        mem::drop(next_guard);
                        mem::drop(current_guard);
                        mem::drop(next_node);

                        return self.traversal_write_internal(
                            curr_level - 1,
                            attempt + 1,
                            key);
                    } else if has_overflow_next {
                        if current_node_deref.force_mut().is_none() || next_guard_deref.force_mut().is_none() {
                            mem::drop(next_guard_deref);
                            mem::drop(current_node_deref);
                            mem::drop(next_guard);
                            mem::drop(current_guard);
                            mem::drop(next_node);

                            return self.traversal_write_internal(
                                curr_level - 1,
                                attempt + 1,
                                key);
                        }

                        self.do_overflow_correction(
                            &current_node_deref,
                            child_pos,
                            next_guard_deref)
                    } else {
                        current_guard = next_guard;
                    }
                }
                _ => break current_node_deref
            }
        }
    }

    pub(crate) fn traversal_write(&self, key: Key) -> NodeGuardResult<'_> {
        self.traversal_write_internal(
            Self::MAX_TREE_HEIGHT,
            mvcc_bplustree::locking::locking_strategy::ATTEMPT_START,
            key)
    }

    pub(crate) fn traversal_read(&self, key: Key) -> NodeGuardResult<'_> {
        let mut current_guard
            = self.lock_reader(&self.root);


        loop {
            let current_deref_result
                = current_guard.try_deref();

            let current
                = current_deref_result.as_ref();

            if current.is_none() {
                mem::drop(current_deref_result);
                mem::drop(current_guard);
                return self.traversal_read(key);
            }

            match current.unwrap() {
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
                        self.lock_reader(&next_node);

                    current_guard = next_guard;
                }
                _ => break current_deref_result
            }
        }
    }
}