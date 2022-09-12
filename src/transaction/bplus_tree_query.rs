use std::mem;
use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::locking::locking_strategy::{ATTEMPT_START, Attempts, Level, LockingStrategy};
use crate::Index;
use crate::index::aligned_page::IndexPage;
use crate::index::node::{Node, BlockGuard, BlockGuardResult, BlockRef};
use crate::utils::vcc_cell::sched_yield;

const DEBUG: bool = false;

impl Index {
    // TODO: Introduced aligned pages to avoid readers to read unallocated memory!
    fn has_overflow(&self, node: &Node) -> bool {
        match node.is_leaf() {
            true => node.is_overflow(self.block_manager.allocation_leaf()),
            false => node.is_overflow(self.block_manager.allocation_directory())
        }
    }

    fn retrieve_root(&self, mut lock_level: Level, mut attempt: Attempts) -> (BlockGuard, Level, Attempts) {
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
    fn retrieve_root_internal(&self, lock_level: Level, attempt: Attempts) -> Result<BlockGuard, (Level, Attempts)> {
        let is_root_lock
            = self.locking_strategy.is_lock_root(lock_level, attempt, self.height());

        let mut root_guard = match self.locking_strategy {
            LockingStrategy::SingleWriter => self.root.block().borrow_free_static(),
            LockingStrategy::Dolos(..) if is_root_lock => {
                let guard
                    = self.root.block().borrow_mut_static();

                if !guard.is_write_lock() {
                    mem::drop(guard);

                    return Err((lock_level, attempt + 1));
                }

                guard
            }
            LockingStrategy::Dolos(..) => self.root.block().borrow_free_static(),
            LockingStrategy::WriteCoupling => self.root.block().borrow_mut_exclusive_static(),
            LockingStrategy::Optimistic(..) if is_root_lock => self.root.block().borrow_mut_static(),
            LockingStrategy::Optimistic(..) => self.root.block().borrow_read_static(),
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

        let is_optimistic = match guard_result.is_mut_optimistic() {
            true => {
                guard_result.mark_obsolete();
                true
            }
            false => false
        };

        let root_mut
            = guard_result.assume_mut().unwrap();

        match &mut root_mut.node_data {
            Node::Index(index_page) => {
                let mut keys = index_page.keys_mut();
                let mut children = index_page.children_mut();

                let keys_mid = keys.len() / 2;
                let k3 = *keys.get(keys_mid).unwrap();

                let new_keys = if is_optimistic {
                    keys[keys_mid + 1..].to_vec()
                } else {
                    let new_keys = keys.split_off(keys_mid + 1);
                    keys.pop();
                    new_keys
                };

                let new_children = if is_optimistic {
                    children[keys_mid + 1..].to_vec()
                } else {
                    children.split_off(keys_mid + 1)
                };

                let mut new_node_right =
                    self.block_manager.new_empty_index_block();

                new_node_right.keys_mut().extend(new_keys);
                new_node_right.children_mut().extend(new_children);

                let new_node_right
                    = new_node_right.into_cell(is_optimistic);

                let new_keys = if is_optimistic {
                    keys[..keys_mid + 1].to_vec()
                } else {
                    keys.split_off(0)
                };

                let new_children = if is_optimistic {
                    children[..keys_mid + 2].to_vec()
                } else {
                    children.split_off(0)
                };

                let mut new_root_left
                    = self.block_manager.new_empty_index_block();

                new_root_left.keys_mut().extend(new_keys);
                new_root_left.children_mut().extend(new_children);

                let new_root_left: BlockRef
                    = new_root_left.into_cell(is_optimistic);

                let mut index_block
                    = self.block_manager.new_empty_index_block();

                index_block.keys_mut().push(k3);
                index_block.children_mut().push(new_root_left);
                index_block.children_mut().push(new_node_right);

                self.set_new_root(
                    index_block,
                    self.height() + 1,
                    root_mut,
                ).map(|guard| root_guard = guard);
            }
            Node::Leaf(records) => {
                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let mut records
                    = records.as_records();

                let new_records_right = if is_optimistic {
                    records[records_mid..].to_vec()
                } else {
                    records.split_off(records_mid)
                };

                let mut new_node_right
                    = self.block_manager.new_empty_leaf_single_version_block();

                new_node_right.records_mut().extend(new_records_right);

                let new_records_left = if is_optimistic {
                    records[..records_mid].to_vec()
                } else {
                    records.split_off(0)
                };

                let mut new_node_left = self.block_manager.new_empty_leaf();
                new_node_left.records_mut().extend(new_records_left);

                let mut new_root
                    = self.block_manager.new_empty_index_block();

                new_root.keys_mut().push(k3);
                new_root.children_mut().push(new_node_left.into_cell(is_optimistic));
                new_root.children_mut().push(new_node_right.into_cell(is_optimistic));

                self.set_new_root(
                    new_root,
                    self.height(),
                    root_mut,
                ).map(|guard| root_guard = guard);
            }
            Node::MultiVersionLeaf(records) => {
                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let mut records
                    = records.as_records();

                let new_records_right = if is_optimistic {
                    records[records_mid..].to_vec()
                } else {
                    records.split_off(records_mid)
                };

                let mut new_node_right
                    = self.block_manager.new_empty_leaf_multi_version_block();

                new_node_right.record_lists_mut().extend(new_records_right);

                let new_records_left = if is_optimistic {
                    records[..records_mid].to_vec()
                } else {
                    records.split_off(0)
                };

                let mut new_node_left
                    = self.block_manager.new_empty_leaf_multi_version_block();

                new_node_left.record_lists_mut().extend(new_records_left);

                let mut new_root
                    = self.block_manager.new_empty_index_block();

                new_root.keys_mut().push(k3);
                new_root.children_mut().push(new_node_left.into_cell(is_optimistic));
                new_root.children_mut().push(new_node_right.into_cell(is_optimistic));

                self.set_new_root(
                    new_root,
                    self.height(),
                    root_mut,
                ).map(|guard| root_guard = guard);
            }
        }

        Ok(root_guard)
    }

    fn do_overflow_correction(
        &self,
        parent_guard: BlockGuardResult,
        child_pos: usize,
        from_guard: BlockGuardResult)
    {
        let is_optimistic = match from_guard.is_mut_optimistic() {
            true => {
                from_guard.mark_obsolete();
                true
            }
            false => false
        };

        let from_node_deref
            = from_guard.assume_mut().unwrap();

        match from_node_deref.as_mut() {
            Node::Index(index_page) => {
                let mut keys
                    = index_page.keys_mut();

                let mut children
                    = index_page.children_mut();

                let keys_mid = keys.len() / 2;
                let k3 = *keys.get(keys_mid).unwrap();

                let mut new_node_right
                    = self.block_manager.new_empty_index_block();

                let mut new_node_from
                    = self.block_manager.new_empty_index_block();

                if is_optimistic {
                    new_node_right.keys_mut().extend_from_slice(&keys[keys_mid + 1..]);
                    new_node_right.children_mut().extend_from_slice(&children[keys_mid + 1..]);

                    new_node_from.keys_mut().extend_from_slice(&keys[..keys_mid]);
                    new_node_from.children_mut().extend_from_slice(&children[..keys_mid + 1]);

                } else {
                    new_node_right.keys_mut().extend(keys.split_off(keys_mid + 1));
                    keys.pop();
                    new_node_right.children_mut().extend(children.split_off(keys_mid + 1));

                    new_node_from.keys_mut().extend(keys.split_off(0));
                    new_node_from.children_mut().extend(children.split_off(0));
                };

                let parent_mut
                    = parent_guard.assume_mut().unwrap();

                parent_mut
                    .keys_mut()
                    .insert(child_pos, k3);

                parent_mut
                    .children_mut()
                    .insert(child_pos + 1, new_node_right.into_cell(is_optimistic));

                *parent_mut
                    .children_mut()
                    .get_mut(child_pos)
                    .unwrap() = new_node_from.into_cell(is_optimistic);
            }
            Node::Leaf(records) => {
                let mut records
                    = records.as_records();

                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let mut new_node
                    = self.block_manager.new_empty_leaf_single_version_block();

                let mut new_node_from
                    = self.block_manager.new_empty_leaf_single_version_block();

                if is_optimistic {
                    new_node.records_mut().extend_from_slice(&records[records_mid..]);
                    new_node_from.records_mut().extend_from_slice(&records[..records_mid]);
                } else {
                    new_node.records_mut().extend(records.split_off(records_mid));
                    new_node_from.records_mut().extend(records.split_off(0));
                }

                let parent_mut
                    = parent_guard.assume_mut().unwrap();

                parent_mut
                    .keys_mut()
                    .insert(child_pos, k3);

                parent_mut
                    .children_mut()
                    .insert(child_pos + 1, new_node.into_cell(is_optimistic));

                *parent_mut
                    .children_mut()
                    .get_mut(child_pos)
                    .unwrap() = new_node_from.into_cell(is_optimistic);
            }
            Node::MultiVersionLeaf(records) => {
                let mut records
                    = records.as_records();

                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let mut new_node
                    = self.block_manager.new_empty_leaf_multi_version_block();

                let mut new_node_from
                    = self.block_manager.new_empty_leaf_multi_version_block();

                if is_optimistic {
                    new_node.record_lists_mut().extend_from_slice(&records[records_mid..]);
                    new_node_from.record_lists_mut().extend_from_slice(&records[..records_mid]);
                } else {
                    new_node.record_lists_mut().extend(records.split_off(records_mid));
                    new_node_from.record_lists_mut().extend(records.split_off(0));
                }

                let parent_mut
                    = parent_guard.assume_mut().unwrap();

                parent_mut
                    .keys_mut()
                    .insert(child_pos, k3);

                parent_mut
                    .children_mut()
                    .insert(child_pos + 1, new_node.into_cell(is_optimistic));

                *parent_mut
                    .children_mut()
                    .get_mut(child_pos)
                    .unwrap() = new_node_from.into_cell(is_optimistic);
            }
        }
    }

    fn traversal_write_internal(&self, lock_level: Level, attempt: Attempts, key: Key) -> Result<BlockGuard, (Level, Attempts)>
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

            match current_ref.as_ref() {
                Node::Index(
                    IndexPage {
                        keys,
                        children,
                        ..
                    }) => {
                    let (next_node, child_pos) = keys
                        .iter()
                        .enumerate()
                        .find(|(_, k)| key.lt(k))
                        .map(|(pos, _)| (children.get(pos).cloned(), pos))
                        .unwrap_or_else(|| (children.get(children.len() - 1).cloned(), keys.len()));

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
                _ if current_guard_result.is_mut() || current_guard.upgrade_write_lock() =>
                    return Ok(current_guard),
                _ => return Err((curr_level - 1, attempt + 1))
            }
        }
    }

    pub(crate) fn traversal_write(&self, key: Key) -> BlockGuard {
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

    fn traversal_read_internal(&self, key: Key) -> Option<BlockGuard> {
        let mut current_guard
            = self.lock_reader(&self.root.block());

        loop {
            if !current_guard.is_valid() {
                return None;
            }

            let current_deref_result
                = current_guard.guard_result();

            let current
                = current_deref_result.as_ref();

            if current.is_none() || !current_guard.is_valid() {
                return None;
            }

            match current.unwrap().as_ref() {
                Node::Index(
                    IndexPage {
                        keys,
                        children,
                        ..
                    }) => {
                    let (next_node, _) = keys
                        .iter()
                        .enumerate()
                        .find(|(_, k)| key.lt(k))
                        .map(|(pos, _)| (children.get(pos).cloned(), pos))
                        .unwrap_or_else(|| (children.get(children.len() - 1).cloned(), keys.len()));

                    if next_node.is_none() || !current_guard.is_valid() {
                        return None;
                    }

                    let next_node
                        = next_node.unwrap();

                    current_guard = self.lock_reader(&next_node);
                }
                _ => break Some(current_guard),
            }
        }
    }

    pub(crate) fn traversal_read(&self, key: Key) -> BlockGuard {
        let mut attempt = ATTEMPT_START;

        loop {
            match self.traversal_read_internal(key) {
                Some(guard) => break guard,
                _ => {
                    attempt += 1;
                    sched_yield(attempt)
                }
            }
        }
    }
}