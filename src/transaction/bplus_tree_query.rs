use std::mem;
use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::locking::locking_strategy::{ATTEMPT_START, Attempts, Level, LockingStrategy};
use crate::Index;
use crate::index::node::{Node, NodeGuard, NodeGuardResult, NodeRef};
use crate::utils::vcc_cell::{GuardDerefResult, sched_yield};

impl Index {
    fn has_overflow(&self, node: &Node) -> bool {
        match node.is_leaf() {
            true => node.is_overflow(self.node_manager.allocation_leaf()),
            false => node.is_overflow(self.node_manager.allocation_directory())
        }
    }

    fn retrieve_root(&self, lock_level: Level, attempt: Attempts) -> (NodeGuard, NodeGuardResult) {
        let is_root_lock
            = self.locking_strategy.is_lock_root(lock_level, attempt, self.height());

        let (mut root_guard, mut root_guard_result) = match self.locking_strategy {
            LockingStrategy::SingleWriter => {
                let guard
                    = self.root.borrow_free_static();

                let guard_writer
                    = guard.try_deref_mut();

                (guard, guard_writer)
            }
            LockingStrategy::Dolos(..) if is_root_lock => {
                let mut guard
                    = self.root.borrow_free_static();

                let mut guard_writer
                    = guard.try_deref_mut();

                let mut attempt = attempt;
                while !guard_writer.is_mut() {
                    attempt += 1;
                    mem::drop(guard_writer);
                    mem::drop(guard);

                    guard = self.root.borrow_free_static();
                    guard_writer = guard.try_deref_mut();

                    sched_yield(attempt);
                    println!("1 \tAttempt = {}", attempt);
                }

                (guard, guard_writer)
            }
            LockingStrategy::Dolos(..) => {
                let guard
                    = self.root.borrow_free_static();

                let guard_reader
                    = guard.try_deref();

                (guard, guard_reader)
            }
            LockingStrategy::WriteCoupling => {
                let guard
                    = self.root.borrow_mut_exclusive_static();

                let guard_writer
                    = guard.try_deref_mut();

                (guard, guard_writer)
            }
            LockingStrategy::Optimistic(..) if is_root_lock => {
                let guard
                    = self.root.borrow_mut_static();

                let guard_writer
                    = guard.try_deref_mut();

                (guard, guard_writer)
            }
            LockingStrategy::Optimistic(..) => {
                let guard
                    = self.root.borrow_read_static();

                let guard_reader
                    = guard.try_deref();

                (guard, guard_reader)
            }
        };

        let root_deref
            = root_guard_result.as_ref();

        if root_deref.is_none() {
            mem::drop(root_guard_result);
            mem::drop(root_guard);
            mem::drop(is_root_lock);

            println!("2 \tAttempt = {}", attempt);
            return self.retrieve_root(lock_level, attempt + 1);
        }

        let root_deref
            = root_deref.unwrap();

        let has_overflow_root
            = self.has_overflow(root_deref);

        let force_restart = match self.locking_strategy {
            LockingStrategy::SingleWriter | LockingStrategy::WriteCoupling => false,
            _ => !is_root_lock
        };

        if !root_guard_result.is_valid() || force_restart && has_overflow_root {
            mem::drop(root_guard_result);
            mem::drop(root_guard);
            mem::drop(is_root_lock);

            println!("3 \tAttempt = {}", attempt);
            return self.retrieve_root(lock_level, attempt + 1);
        }

        if !has_overflow_root {
            return (root_guard, root_guard_result);
        }

        if root_guard_result.force_mut().is_none() {
            mem::drop(root_guard_result);
            mem::drop(root_guard);
            mem::drop(is_root_lock);

            println!("4 \tAttempt = {}", attempt);
            return self.retrieve_root(lock_level, attempt + 1);
        }

        let (_holder, root_mut) = match root_guard_result.is_optimistic_write() {
            true => {
                root_guard_result.mark_obsolete();

                let mut data_copy = root_guard_result.assume_mut().cloned().unwrap();
                let mut_data: &mut Node = unsafe { mem::transmute(&mut data_copy) };
                (Some(data_copy), mut_data)
            }
            false => (None, root_guard_result.assume_mut().unwrap())
        };

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

                self.set_new_root(
                    Node::Index(vec![k3], vec![new_root_left, new_node_right]),
                    root_mut,
                ).map(|(guard, guard_result)| {
                    root_guard_result = guard_result;
                    root_guard = guard;
                });
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

                self.set_new_root(
                    Node::Index(vec![k3], vec![new_node_left, new_node_right]),
                    root_mut,
                ).map(|(guard, guard_result)| {
                    root_guard_result = guard_result;
                    root_guard = guard;
                });
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

                self.set_new_root(
                    Node::Index(vec![k3], vec![new_node_left, new_node_right]),
                    root_mut,
                ).map(|(guard, guard_result)| {
                    root_guard_result = guard_result;
                    root_guard = guard;
                });
            }
        }

        self.inc_height();

        (root_guard, root_guard_result)
    }

    fn do_overflow_correction(
        &self,
        parent_guard: &GuardDerefResult<Node>,
        child_pos: usize,
        from_guard: GuardDerefResult<Node>)
    {
        let (_holder, from_node_deref) = match from_guard.is_optimistic_write() {
            true => {
                debug_assert!(from_guard.assume_mut().is_some());
                let mut data_copy = from_guard.assume_mut().cloned().unwrap();
                let mut_data: &mut Node = unsafe { mem::transmute(&mut data_copy) };

                from_guard.mark_obsolete();
                (Some(data_copy), mut_data)
            }
            false => (None, from_guard.assume_mut().unwrap())
        };

        match from_node_deref {
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
                    .children_mut()
                    .unwrap()
                    .insert(child_pos + 1, new_node.clone());

                parent_mut
                    .keys_mut()
                    .unwrap()
                    .insert(child_pos, k3);

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

    fn traversal_write_internal(&self, lock_level: Level, attempt: Attempts, key: Key) -> (NodeGuard, NodeGuardResult) {
        let mut curr_level = Self::INIT_TREE_HEIGHT;

        let (mut current_guard, mut current_node_deref)
            = self.retrieve_root(lock_level, attempt);

        let height
            = self.height();

        loop {
            let deref_as_ref
                = current_node_deref.as_ref();

            if deref_as_ref.is_none() {
                mem::drop(height);
                mem::drop(current_node_deref);
                mem::drop(current_guard);

                println!("5 \tAttempt = {}", attempt);
                sched_yield(attempt);
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

                    let mut next_guard_deref = match self
                        .locking_strategy
                        .is_lock(curr_level, lock_level, attempt, height)
                    {
                        true => next_guard.try_deref_mut(),
                        false => next_guard.try_deref()
                    };

                    let next_guard_deref_as_ref
                        = next_guard_deref.as_ref();

                    if next_guard_deref_as_ref.is_none() {
                        mem::drop(height);
                        mem::drop(next_guard_deref_as_ref);
                        mem::drop(current_node_deref);
                        mem::drop(current_guard);
                        mem::drop(next_guard);
                        mem::drop(next_node);

                        println!("6 \tAttempt = {}", attempt);

                        if attempt > 50000 {
                            let x = 13123;
                        }

                        sched_yield(attempt);
                        return self.traversal_write_internal(
                            curr_level - 1,
                            attempt + 1,
                            key);
                    }

                    let next_guard_ref
                        = next_guard_deref_as_ref.unwrap();

                    let has_overflow_next
                        = self.has_overflow(next_guard_ref);

                    if has_overflow_next {
                        if current_node_deref.force_mut().is_none() || next_guard_deref.force_mut().is_none() {
                            mem::drop(height);
                            mem::drop(next_guard_deref);
                            mem::drop(next_guard);
                            mem::drop(next_node);
                            mem::drop(current_node_deref);
                            mem::drop(current_guard);

                            println!("7 \tAttempt = {}", attempt);
                            sched_yield(attempt);
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
                        current_node_deref = next_guard_deref;
                        current_guard = next_guard;
                    }
                }
                _ if current_node_deref.is_mut() => break (current_guard, current_node_deref),
                _ => if current_node_deref.force_mut().is_none() ||
                        self.has_overflow(current_node_deref.as_ref().unwrap())
                {
                    mem::drop(height);
                    mem::drop(current_node_deref);
                    mem::drop(current_guard);

                    println!("8 \tAttempt = {}", attempt);
                    sched_yield(attempt);
                    return self.traversal_write_internal(
                        curr_level - 1,
                        attempt + 1,
                        key);
                }
                else {
                    break (current_guard, current_node_deref)
                }
            }
        }
    }

    pub(crate) fn traversal_write(&self, key: Key) -> (NodeGuard, NodeGuardResult) {
        self.traversal_write_internal(
            Self::MAX_TREE_HEIGHT,
            mvcc_bplustree::locking::locking_strategy::ATTEMPT_START,
            key)
    }

    pub(crate) fn traversal_read_internal(&self, key: Key, attempt: Attempts) -> (NodeGuard, NodeGuardResult) {
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