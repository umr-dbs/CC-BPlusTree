use std::mem;
use chronicle_db::tools::aliases::Key;
use itertools::Itertools;
use mvcc_bplustree::locking::locking_strategy::{ATTEMPT_START, Attempts, Level, LockingStrategy};
use crate::block::aligned_page::IndexPage;
use crate::bplus_tree::{Height, LockLevel};
use crate::Index;
use crate::index::node::{Node, BlockGuard, BlockGuardResult, BlockRef};
use crate::utils::vcc_cell::sched_yield;

const DEBUG: bool = false;

impl Index {
    fn has_overflow(&self, node: &Node) -> bool {
        match node.is_leaf() {
            true => node.is_overflow(self.block_manager.allocation_leaf()),
            false => node.is_overflow(self.block_manager.allocation_directory())
        }
    }

    fn retrieve_root(&self, mut lock_level: Level, mut attempt: Attempts) -> (BlockGuard, Height, LockLevel, Attempts) {
        loop {
            match self.retrieve_root_internal(lock_level, attempt) {
                Err((n_lock_level, n_attempt)) => {
                    lock_level = n_lock_level;
                    attempt = n_attempt;

                    sched_yield(attempt);
                }
                Ok((guard, height)) => break (guard, height, lock_level, attempt)
            }
        }
    }

    #[inline]
    fn retrieve_root_internal(&self, lock_level: LockLevel, attempt: Attempts) -> Result<(BlockGuard, Height), (LockLevel, Attempts)> {
        let root
            = self.root.clone();

        let root_block
            = self.root.block();

        let is_root_lock
            = self.locking_strategy.is_lock_root(lock_level, attempt, root.height());

        let mut root_guard = match self.locking_strategy {
            LockingStrategy::SingleWriter => root_block.borrow_free_static(),
            LockingStrategy::Dolos(..) if is_root_lock => {
                let guard
                    = root_block.borrow_mut_static();

                if !guard.is_write_lock() {
                    mem::drop(guard);

                    return Err((lock_level, attempt + 1));
                }

                guard
            }
            LockingStrategy::Dolos(..) => root_block.borrow_free_static(),
            LockingStrategy::WriteCoupling => root_block.borrow_mut_exclusive_static(),
            LockingStrategy::Optimistic(..) if is_root_lock => root_block.borrow_mut_static(),
            LockingStrategy::Optimistic(..) => root_block.borrow_read_static(),
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
            return Ok((root_guard, root.height()));
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

        let root_block_mut
            = guard_result.assume_mut().unwrap();

        let n_height
            = root.height() + 1;

        match &mut root_block_mut.node_data {
            Node::Index(index_page) => {
                let keys = index_page.keys();
                let children = index_page.children();

                let keys_mid = keys.len() / 2;
                let k3 = *keys.get(keys_mid).unwrap();

                let mut index_block
                    = self.block_manager.new_empty_index_block();

                let mut new_node_right =
                    self.block_manager.new_empty_index_block();

                let mut new_root_left
                    = self.block_manager.new_empty_index_block();

                let keys_slice = &keys[keys_mid + 1..];
                new_node_right.keys_mut().extend_from_slice(keys_slice);
                new_node_right.set_keys_len(keys_slice.len());

                let children_slice = &children[keys_mid + 1..];
                new_node_right.children_mut().extend_from_slice(children_slice);
                new_node_right.set_children_len(children_slice.len());

                let keys_slice = &keys[..keys_mid];
                new_root_left.keys_mut().extend_from_slice(keys_slice);
                new_root_left.set_keys_len(keys_slice.len());

                let children_slice = &children[..=keys_mid];
                new_root_left.children_mut().extend_from_slice(children_slice);
                new_root_left.set_children_len(children_slice.len());

                index_block.keys_mut().push(k3);
                index_block.set_keys_len(1);

                index_block.children_mut().extend([
                    new_root_left.into_cell(is_optimistic),
                    new_node_right.into_cell(is_optimistic)
                ]);
                index_block.set_children_len(2);

                self.set_new_root(
                    &mut root_guard,
                    index_block,
                    n_height);
            }
            Node::Leaf(records) => {
                let records
                    = records.as_slice();

                let records_mid
                    = records.len() / 2;

                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let mut new_node_right
                    = self.block_manager.new_empty_leaf_single_version_block();

                let mut new_node_left
                    = self.block_manager.new_empty_leaf_single_version_block();

                let mut new_root
                    = self.block_manager.new_empty_index_block();

                let records_slice = &records[records_mid..];
                new_node_right.records_mut().extend_from_slice(records_slice);
                new_node_right.set_records_len(records_slice.len());

                let records_slice = &records[..records_mid];
                new_node_left.records_mut().extend_from_slice(records_slice);
                new_node_left.set_records_len(records_slice.len());

                new_root.keys_mut().push(k3);
                new_root.set_keys_len(1);

                new_root.children_mut().extend([
                    new_node_left.into_cell(is_optimistic),
                    new_node_right.into_cell(is_optimistic)
                ]);
                new_root.set_children_len(2);

                self.set_new_root(
                    &mut root_guard,
                    new_root,
                    n_height);
            }
            Node::MultiVersionLeaf(records) => {
                let records
                    = records.as_slice();

                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let mut new_node_right
                    = self.block_manager.new_empty_leaf_multi_version_block();

                let mut new_node_left
                    = self.block_manager.new_empty_leaf_multi_version_block();

                let mut new_root
                    = self.block_manager.new_empty_index_block();

                let records_slice = &records[records_mid..];
                new_node_right.record_lists_mut().extend_from_slice(records_slice);
                new_node_right.set_records_len(records_slice.len());

                let records_slice = &records[..records_mid];
                new_node_left.record_lists_mut().extend_from_slice(records_slice);
                new_node_left.set_records_len(records_slice.len());

                new_root.keys_mut().push(k3);
                new_root.set_keys_len(1);

                new_root.children_mut().extend([
                    new_node_left.into_cell(is_optimistic),
                    new_node_right.into_cell(is_optimistic)
                ]);
                new_root.set_children_len(2);

                self.set_new_root(
                    &mut root_guard,
                    new_root,
                    n_height);
            }
        }

        Ok((root_guard, n_height))
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
                let keys
                    = index_page.keys();

                let children
                    = index_page.children();

                let keys_mid = keys.len() / 2;
                let k3 = *keys.get(keys_mid).unwrap();

                let mut new_node_right
                    = self.block_manager.new_empty_index_block();

                let mut new_node_from
                    = self.block_manager.new_empty_index_block();

                let keys_slice = &keys[keys_mid + 1..];
                new_node_right.keys_mut().extend_from_slice(keys_slice);
                new_node_right.set_keys_len(keys_slice.len());

                let children_slice = &children[keys_mid + 1..];
                new_node_right.children_mut().extend_from_slice(children_slice);
                new_node_right.set_children_len(children_slice.len());

                let keys_slice = &keys[..keys_mid];
                new_node_from.keys_mut().extend_from_slice(keys_slice);
                new_node_from.set_keys_len(keys_slice.len());

                let children_slice = &children[..=keys_mid];
                new_node_from.children_mut().extend_from_slice(children_slice);
                new_node_from.set_children_len(children_slice.len());

                let parent_mut
                    = parent_guard.assume_mut().unwrap();

                parent_mut
                    .keys_mut()
                    .insert(child_pos, k3);

                let keys_len = parent_mut.keys_len();
                parent_mut.set_keys_len(keys_len + 1);

                parent_mut
                    .children_mut()
                    .insert(child_pos + 1, new_node_right.into_cell(is_optimistic));

                let children_len = parent_mut.children_len();
                parent_mut.set_children_len(children_len + 1);

                *parent_mut
                    .children_mut()
                    .get_mut(child_pos)
                    .unwrap() = new_node_from.into_cell(is_optimistic);
            }
            Node::Leaf(records) => {
                let records
                    = records.as_slice();

                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let mut new_node
                    = self.block_manager.new_empty_leaf_single_version_block();

                let mut new_node_from
                    = self.block_manager.new_empty_leaf_single_version_block();

                let records_slice = &records[records_mid..];
                new_node.records_mut().extend_from_slice(records_slice);
                new_node.set_records_len(records_slice.len());

                let records_slice = &records[..records_mid];
                new_node_from.records_mut().extend_from_slice(records_slice);
                new_node_from.set_records_len(records_slice.len());

                let parent_mut
                    = parent_guard.assume_mut().unwrap();

                parent_mut
                    .keys_mut()
                    .insert(child_pos, k3);

                let keys_len = parent_mut.keys_len();
                parent_mut.set_keys_len(keys_len + 1);

                parent_mut
                    .children_mut()
                    .insert(child_pos + 1, new_node.into_cell(is_optimistic));

                let children_len = parent_mut.children_len();
                parent_mut.set_children_len(children_len + 1);

                *parent_mut
                    .children_mut()
                    .get_mut(child_pos)
                    .unwrap() = new_node_from.into_cell(is_optimistic);
            }
            Node::MultiVersionLeaf(records) => {
                let records
                    = records.as_slice();

                let records_mid = records.len() / 2;
                let k3 = records
                    .get(records_mid)
                    .unwrap()
                    .key();

                let mut new_node
                    = self.block_manager.new_empty_leaf_multi_version_block();

                let mut new_node_from
                    = self.block_manager.new_empty_leaf_multi_version_block();

                let records_slice = &records[records_mid..];
                new_node.record_lists_mut().extend_from_slice(records_slice);
                new_node.set_records_len(records_slice.len());

                let records_slice = &records[..records_mid];
                new_node_from.record_lists_mut().extend_from_slice(records_slice);
                new_node_from.set_records_len(records_slice.len());

                let parent_mut
                    = parent_guard.assume_mut().unwrap();

                parent_mut
                    .keys_mut()
                    .insert(child_pos, k3);

                let parent_keys_len = parent_mut.keys_len();
                parent_mut.set_keys_len(parent_keys_len + 1);

                parent_mut
                    .children_mut()
                    .insert(child_pos + 1, new_node.into_cell(is_optimistic));

                let parent_children_len = parent_mut.children_len();
                parent_mut.set_children_len(parent_children_len + 1);

                *parent_mut
                    .children_mut()
                    .get_mut(child_pos)
                    .unwrap() = new_node_from.into_cell(is_optimistic);
            }
        }
    }

    fn traversal_write_internal(&self, lock_level: LockLevel, attempt: Attempts, key: Key) -> Result<BlockGuard, (LockLevel, Attempts)>
    {
        let mut curr_level = Self::INIT_TREE_HEIGHT;

        let (mut current_guard, height, lock_level, attempt)
            = self.retrieve_root(lock_level, attempt);

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

                        debug_assert!(self.locking_strategy.additional_lock_required() &&
                            current_guard.is_write_lock() && next_guard.is_write_lock() ||
                            !self.locking_strategy.additional_lock_required());

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
                        .unwrap_or_else(||
                            (children.get(children.len().checked_sub(1).unwrap_or(usize::MAX)).cloned(),
                             keys.len()));

                    if next_node.is_none() || !current_guard.is_valid() {
                        return None;
                    }

                    let next_node
                        = next_node.unwrap();

                    current_guard = self.lock_reader(&next_node);
                }
                _ if current_guard.is_valid() => break Some(current_guard),
                _ => break None
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