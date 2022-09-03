use std::mem;
use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::locking::locking_strategy::{ATTEMPT_START, Attempts, Level, LockingStrategy};
use crate::Index;
use crate::index::node::{Node, NodeGuard, NodeGuardResult, NodeRef};
use crate::utils::vcc_cell::{GuardDerefResult, sched_yield};

const DEBUG: bool = false;

impl Index {
    fn has_overflow(&self, node: &Node) -> bool {
        match node.is_leaf() {
            true => node.is_overflow(self.node_manager.allocation_leaf()),
            false => node.is_overflow(self.node_manager.allocation_directory())
        }
    }

    fn retrieve_root(&self, mut lock_level: Level, mut attempt: Attempts) -> (NodeGuard, Level, Attempts) {
        loop {
            match self.retrieve_root_internal(lock_level, attempt) {
                Err((n_lock_level, n_attempt)) => {
                    lock_level = n_lock_level;
                    attempt = n_attempt;

                    sched_yield(attempt);
                }
                Ok(guard) => break (guard, lock_level, attempt)
            }
        }
    }

    #[inline]
    fn retrieve_root_internal(&self, lock_level: Level, attempt: Attempts) -> Result<NodeGuard, (Level, Attempts)> {
        let is_root_lock
            = self.locking_strategy.is_lock_root(lock_level, attempt, self.height());

        let mut root_guard = match self.locking_strategy {
            LockingStrategy::SingleWriter => self.root.borrow_free_static(),
            LockingStrategy::Dolos(..) if is_root_lock => {
                let guard
                    = self.root.borrow_mut_static();

                if !guard.is_write_lock() {
                    mem::drop(guard);

                    return Err((lock_level, attempt + 1));
                }

                guard
            }
            LockingStrategy::Dolos(..) => self.root.borrow_free_static(),
            LockingStrategy::WriteCoupling => self.root.borrow_mut_exclusive_static(),
            LockingStrategy::Optimistic(..) if is_root_lock => self.root.borrow_mut_static(),
            LockingStrategy::Optimistic(..) => self.root.borrow_read_static(),
        };

        if !root_guard.is_valid() {
            mem::drop(root_guard);
            return Err((lock_level, attempt + 1));
        }

        let root_guard_result
            = root_guard.guard_result();

        let root_ref
            = root_guard_result.as_ref();

        if root_ref.is_none() {
            mem::drop(root_guard);

            return Err((lock_level, attempt + 1));
        }

        let root_ref = root_ref
            .unwrap();

        let has_overflow_root
            = self.has_overflow(root_ref);

        let force_restart = match self.locking_strategy {
            LockingStrategy::SingleWriter | LockingStrategy::WriteCoupling => false,
            _ => !is_root_lock
        };

        if !root_guard.is_valid() || force_restart && has_overflow_root {
            mem::drop(root_guard);

            return Err((lock_level, attempt + 1));
        }

        if !has_overflow_root {
            return Ok(root_guard);
        }

        debug_assert!(root_guard.is_valid());

        if !root_guard.upgrade_write_lock() && self.locking_strategy().additional_lock_required() {
            mem::drop(root_guard);

            return Err((lock_level, attempt + 1));
        }

        let guard_result
            = root_guard.guard_result();

        debug_assert!(guard_result.is_mut());

        let copy= match guard_result.is_mut_optimistic() {
            true => {
                guard_result.mark_obsolete();
                true
            }
            false => false
        };

        let root_mut
            = guard_result.assume_mut().unwrap();

        match root_mut {
            Node::Index(keys, children) => {
                let keys_mid = keys.len() / 2;
                let k3 = *keys.get(keys_mid).unwrap();

                // let new_keys = keys.split_off(keys_mid + 1);
                let new_keys = if copy {
                    keys[keys_mid + 1..].to_vec()
                } else {
                    let new_keys = keys.split_off(keys_mid + 1);
                    keys.pop();
                    new_keys
                };

                // let new_children = children.split_off(keys_mid + 1);

                let new_children = if copy {
                    children[keys_mid + 1..].to_vec()
                } else {
                    children.split_off(keys_mid + 1)
                };

                let new_node_right: NodeRef = Node::Index(new_keys, new_children)
                    .into_node_ref(self.locking_strategy());

                let new_keys = if copy {
                    keys[..keys_mid + 1].to_vec()
                } else {
                    keys.split_off(0)
                };

                let new_children = if copy {
                    children[..keys_mid + 2].to_vec()
                } else {
                    children.split_off(0)
                };

                let new_root_left: NodeRef =
                    Node::Index(new_keys, new_children)
                        .into_node_ref(self.locking_strategy());

                self.set_new_root(
                    Node::Index(vec![k3], vec![new_root_left, new_node_right]),
                    root_mut,
                ).map(|guard| root_guard = guard);
            }
            Node::Leaf(records) => {
                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let new_records_right = if copy {
                    records[records_mid..].to_vec()
                } else {
                    records.split_off(records_mid)
                };

                let new_node_right: NodeRef = Node::Leaf(new_records_right)
                    .into_node_ref(self.locking_strategy());

                let new_records_left = if copy {
                    records[..records_mid].to_vec()
                } else {
                    records.split_off(0)
                };
                let new_node_left: NodeRef = Node::Leaf(new_records_left)
                    .into_node_ref(self.locking_strategy());

                self.set_new_root(
                    Node::Index(vec![k3], vec![new_node_left, new_node_right]),
                    root_mut,
                ).map(|guard| root_guard = guard);
            }
            Node::MultiVersionLeaf(records) => {
                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let new_records_right = if copy {
                    records[records_mid..].to_vec()
                } else {
                    records.split_off(records_mid)
                };

                let new_node_right: NodeRef = Node::MultiVersionLeaf(new_records_right)
                    .into_node_ref(self.locking_strategy());

                let new_records_left = if copy {
                    records[..records_mid].to_vec()
                } else {
                    records.split_off(0)
                };

                let new_node_left: NodeRef = Node::MultiVersionLeaf(new_records_left)
                    .into_node_ref(self.locking_strategy());

                self.set_new_root(
                    Node::Index(vec![k3], vec![new_node_left, new_node_right]),
                    root_mut,
                ).map(|guard| root_guard = guard);
            }
        }

        self.inc_height();

        Ok(root_guard)
    }

    fn do_overflow_correction(
        &self,
        parent_guard: GuardDerefResult<Node>,
        child_pos: usize,
        from_guard: GuardDerefResult<Node>)
    {
        let copy
            = from_guard.is_mut_optimistic();

        let from_node_deref
            = from_guard.assume_mut().unwrap();

        match from_node_deref {
            Node::Index(keys, children) => {
                let keys_mid = keys.len() / 2;
                let k3 = *keys.get(keys_mid).unwrap();

                // let new_keys = keys.split_off(keys_mid + 1);
                let (new_keys_right, new_keys_from) = if copy {
                    (keys[keys_mid + 1..].to_vec(), keys[..keys_mid].to_vec())
                } else {
                    let new_keys = keys.split_off(keys_mid + 1);
                    keys.pop();
                    (new_keys, keys.split_off(0))
                };
                // keys.pop();

                // let new_children = children.split_off(keys_mid + 1);
                let (new_children_right, new_children_from) = if copy {
                    (children[keys_mid + 1..].to_vec(), children[..keys_mid + 1].to_vec())
                } else {
                    (children.split_off(keys_mid + 1), children.split_off(0))
                };

                let new_node_right: NodeRef = Node::Index(new_keys_right, new_children_right)
                    .into_node_ref(self.locking_strategy());

                let new_node_from: NodeRef = Node::Index(new_keys_from, new_children_from)
                    .into_node_ref(self.locking_strategy());

                let parent_mut
                    = parent_guard.assume_mut().unwrap();

                parent_mut
                    .keys_mut()
                    .unwrap()
                    .insert(child_pos, k3);

                parent_mut
                    .children_mut()
                    .unwrap()
                    .insert(child_pos + 1, new_node_right);

                *parent_mut
                    .children_mut()
                    .unwrap()
                    .get_mut(child_pos)
                    .unwrap() = new_node_from;
            }
            Node::Leaf(records) => {
                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let (new_records, new_records_from) = if copy {
                    (records[records_mid..].to_vec(), records[..records_mid].to_vec())
                } else {
                    (records.split_off(records_mid), records.split_off(0))
                };

                let new_node: NodeRef = Node::Leaf(new_records)
                    .into_node_ref(self.locking_strategy());

                let new_node_from: NodeRef = Node::Leaf(new_records_from)
                    .into_node_ref(self.locking_strategy());

                let parent_mut
                    = parent_guard.assume_mut().unwrap();

                parent_mut
                    .keys_mut()
                    .unwrap()
                    .insert(child_pos, k3);

                parent_mut
                    .children_mut()
                    .unwrap()
                    .insert(child_pos + 1, new_node);

                *parent_mut
                    .children_mut()
                    .unwrap()
                    .get_mut(child_pos)
                    .unwrap() = new_node_from;
            }
            Node::MultiVersionLeaf(records) => {
                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let (new_records, new_records_from) = if copy {
                    (records[records_mid..].to_vec(), records[..records_mid].to_vec())
                } else {
                    (records.split_off(records_mid), records.split_off(0))
                };

                let new_node: NodeRef = Node::MultiVersionLeaf(new_records)
                    .into_node_ref(self.locking_strategy());

                let new_node_from: NodeRef = Node::MultiVersionLeaf(new_records_from)
                    .into_node_ref(self.locking_strategy());

                let parent_mut
                    = parent_guard.assume_mut().unwrap();

                parent_mut
                    .keys_mut()
                    .unwrap()
                    .insert(child_pos, k3);

                parent_mut
                    .children_mut()
                    .unwrap()
                    .insert(child_pos + 1, new_node);

                *parent_mut
                    .children_mut()
                    .unwrap()
                    .get_mut(child_pos)
                    .unwrap() = new_node_from;
            }
        }
    }

    fn traversal_write_internal(&self, lock_level: Level, attempt: Attempts, key: Key) -> Result<NodeGuard, (Level, Attempts)>
    {
        let mut curr_level = Self::INIT_TREE_HEIGHT;

        let (mut current_guard, lock_level, attempt)
            = self.retrieve_root(lock_level, attempt);

        let height
            = self.height();

        loop {
            let current_guard_result
                = current_guard.guard_result();

            let current_ref
                = current_guard_result.as_ref();

            if current_ref.is_none() || !current_guard.is_valid() {
                mem::drop(height);
                mem::drop(current_guard);

                if DEBUG {
                    println!("6 \tAttempt = {}", attempt);
                }

                return Err((curr_level - 1, attempt + 1));
            }

            let current_ref = current_ref
                .unwrap();

            if !current_guard.is_valid() {
                mem::drop(current_guard);

                if DEBUG {
                    println!("7 \tAttempt = {}", attempt);
                }

                return Err((curr_level - 1, attempt + 1));
            }

            match current_ref {
                Node::Index(keys, children) => {
                    let (next_node, child_pos) = keys
                        .iter()
                        .enumerate()
                        .find(|(_, k)| key.lt(k))
                        .map(|(pos, _)| (children.get(pos).cloned(), pos))
                        .unwrap_or_else(|| (children.last().cloned(), keys.len()));

                    if next_node.is_none() || !current_guard.is_valid() {
                        mem::drop(current_guard);

                        if DEBUG {
                            println!("8 \tAttempt = {}", attempt);
                        }

                        return Err((curr_level - 1, attempt + 1));
                    }

                    let next_node = next_node.unwrap();
                    curr_level += 1;

                    let mut next_guard = self.apply_for(
                        curr_level,
                        lock_level,
                        attempt,
                        height,
                        next_node);

                    let next_guard_result
                        = next_guard.guard_result();

                    let next_guard_result_ref
                        = next_guard_result.as_ref();

                    if next_guard_result_ref.is_none() || !current_guard.is_valid() {
                        mem::drop(height);
                        mem::drop(next_guard_result_ref);
                        mem::drop(next_guard);
                        mem::drop(current_guard);

                        if DEBUG {
                            println!("9 \tAttempt = {}", attempt);
                        }

                        return Err((curr_level - 1, attempt + 1));
                    }

                    let next_guard_result_ref
                        = next_guard_result_ref.unwrap();

                    let has_overflow_next
                        = self.has_overflow(next_guard_result_ref);

                    if has_overflow_next {
                        if self.locking_strategy.additional_lock_required() &&
                            (!current_guard.upgrade_write_lock() || !next_guard.upgrade_write_lock())
                        {
                            mem::drop(height);
                            mem::drop(next_guard);
                            mem::drop(current_guard);

                            if DEBUG {
                                println!("10 \tAttempt = {}", attempt);
                            }

                            return Err((curr_level - 1, attempt + 1));
                        }

                        debug_assert!(current_guard.is_write_lock() && next_guard.is_write_lock());

                        self.do_overflow_correction(
                            current_guard.guard_result(),
                            child_pos,
                            next_guard.guard_result())
                    } else if !current_guard.is_valid() || !next_guard.is_valid() {
                        mem::drop(height);
                        mem::drop(next_guard);
                        mem::drop(current_guard);

                        if DEBUG {
                            println!("11 \tAttempt = {}", attempt);
                        }

                        return Err((curr_level - 1, attempt + 1));
                    } else {
                        current_guard = next_guard;
                    }
                }
                _ if current_guard_result.is_mut() => return Ok(current_guard),
                _ => return if !current_guard.upgrade_write_lock() { // is_leaf
                    mem::drop(height);
                    mem::drop(current_guard);

                    if DEBUG {
                        println!("12 \tAttempt = {}", attempt);
                    }

                    Err((curr_level - 1, attempt + 1))
                } else {
                    Ok(current_guard)
                }
            }
        }
    }

    pub(crate) fn traversal_write(&self, key: Key) -> NodeGuard {
        let mut attempt = ATTEMPT_START;
        let mut lock_level = Self::MAX_TREE_HEIGHT;

        loop {
            match self.traversal_write_internal(lock_level, attempt, key) {
                Err((n_lock_level, n_attempt)) => {
                    attempt = n_attempt;
                    lock_level = n_lock_level;

                    sched_yield(attempt);
                }
                Ok(guard) => break guard,
            }
        }
    }

    pub(crate) fn traversal_read_internal(&self, key: Key, attempt: Attempts) -> (NodeGuard, NodeGuardResult) {
        let mut current_guard
            = self.lock_reader(&self.root);

        loop {
            let current_deref_result
                = current_guard.guard_result();

            let current
                = current_deref_result.as_ref();

            if current.is_none() {
                mem::drop(current_deref_result);
                mem::drop(current_guard);

                sched_yield(attempt);
                return self.traversal_read_internal(key, attempt + 1);
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
                _ => break (current_guard, current_deref_result)
            }
        }
    }

    pub(crate) fn traversal_read(&self, key: Key) -> (NodeGuard, NodeGuardResult) {
        self.traversal_read_internal(
            key,
            ATTEMPT_START)
    }
}