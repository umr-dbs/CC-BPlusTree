use std::mem;
use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::locking::locking_strategy::{ATTEMPT_START, Attempts, Level};
use crate::block::aligned_page::IndexPage;
use crate::bplus_tree::{Height, LockLevel};
use crate::Index;
use crate::utils::record_like::RecordLike;
use crate::index::node::{Node, NodeUnsafeDegree};
use crate::locking::block_lock::{BlockGuard, BlockGuardResult};
use crate::locking::locking_strategy::{LevelConstraints, LockingStrategy};
use crate::utils::hybrid_cell::sched_yield;

const DEBUG: bool = false;

impl Index {
    #[inline]
    fn has_overflow(&self, node: &Node) -> bool {
        match node.is_leaf() {
            true => node.is_overflow(self.block_manager.allocation_leaf()),
            false => node.is_overflow(self.block_manager.allocation_directory())
        }
    }

    fn has_underflow(&self, node: &Node) -> bool {
        match node.is_leaf() {
            true => node.is_underflow(self.block_manager.allocation_leaf()),
            false => node.is_underflow(self.block_manager.allocation_directory())
        }
    }

    fn unsafe_degree_of(&self, node: &Node) -> NodeUnsafeDegree {
        match node.is_leaf() {
            true => node.unsafe_degree(self.block_manager.allocation_leaf()),
            false => node.unsafe_degree(self.block_manager.allocation_directory()),
        }
    }

    #[inline]
    fn retrieve_root(&self, mut lock_level: Level, mut attempt: Attempts) -> (BlockGuard, Height, LockLevel, Attempts) {
        let is_olc = self.locking_strategy.is_olc();
        loop {
            match self.retrieve_root_internal(lock_level, attempt) {
                Err((n_lock_level, n_attempt)) => {
                    lock_level = n_lock_level;
                    attempt = n_attempt;

                    if is_olc {
                        sched_yield(attempt);
                    }
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
            = root.block();

        let mut root_guard = match self.locking_strategy {
            LockingStrategy::MonoWriter => root_block.borrow_free_static(),
            LockingStrategy::LockCoupling => root_block.borrow_mut_exclusive_static(),
            LockingStrategy::OLC(LevelConstraints::Unlimited) => root_block.borrow_free_static(),
            LockingStrategy::OLC(LevelConstraints::OptimisticLimit { .. })
            if self.locking_strategy.is_lock_root(lock_level, attempt, root.height()) => {
                let guard
                    = root_block.borrow_mut_static();

                if !guard.is_write_lock() {
                    mem::drop(guard);

                    return Err((lock_level, attempt + 1));
                }

                guard
            }
            LockingStrategy::OLC(..) => root_block.borrow_free_static(),
            LockingStrategy::RWLockCoupling(..)
            if self.locking_strategy.is_lock_root(lock_level, attempt, root.height()) =>
                root_block.borrow_mut_static(),
            LockingStrategy::RWLockCoupling(..) =>
                root_block.borrow_read_static(),
        };

        // if !root_guard.is_valid() {
        //     mem::drop(root_guard);
        //     return Err((lock_level, attempt + 1));
        // }

        let root_guard_result
            = root_guard.guard_result();

        let root_ref = unsafe {
            root_guard_result.as_reader()
        };

        if root_guard_result.is_null() {
            mem::drop(root_guard);

            return Err((lock_level, attempt + 1));
        }

        let root_ref = root_ref.unwrap();

        let has_overflow_root
            = self.has_overflow(root_ref);

        let force_restart = match self.locking_strategy {
            LockingStrategy::MonoWriter | LockingStrategy::LockCoupling => false,
            _ => !root_guard.is_write_lock()
        };

        if  force_restart && has_overflow_root && !root_guard.upgrade_write_lock() { // !root_guard.is_valid() ||
            mem::drop(root_guard);

            return Err((lock_level, attempt + 1));
        }

        if !has_overflow_root {
            return Ok((root_guard, root.height()));
        }

        debug_assert!(root_guard.is_valid());

        if !root_guard.upgrade_write_lock() && self.locking_strategy.additional_lock_required() {
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
            Node::Index(index_page) => unsafe {
                let keys = index_page.keys();
                let children = index_page.children();

                let keys_mid = keys.len() / 2;
                let k3 = *keys.get_unchecked(keys_mid);

                let mut index_block
                    = self.block_manager.new_empty_index_block();

                let mut new_node_right =
                    self.block_manager.new_empty_index_block();

                let mut new_root_left
                    = self.block_manager.new_empty_index_block();

                new_node_right
                    .keys_mut()
                    .extend_from_slice(keys.get_unchecked(keys_mid + 1..));
                new_node_right
                    .children_mut()
                    .extend_from_slice(children.get_unchecked(keys_mid + 1..));

                new_root_left
                    .keys_mut()
                    .extend_from_slice(keys.get_unchecked(..keys_mid));
                new_root_left
                    .children_mut()
                    .extend_from_slice(children.get_unchecked(..=keys_mid));

                index_block.keys_mut().push(k3);
                index_block.children_mut().extend([
                    new_root_left.into_cell(is_optimistic),
                    new_node_right.into_cell(is_optimistic)
                ]);

                self.set_new_root(
                    index_block,
                    n_height);
            }
            Node::Leaf(records) => unsafe {
                let records
                    = records.as_slice();

                let records_mid
                    = records.len() / 2;

                let k3 = records
                    .get_unchecked(records_mid)
                    .key();

                let mut new_node_right
                    = self.block_manager.new_empty_leaf_single_version_block();

                let mut new_node_left
                    = self.block_manager.new_empty_leaf_single_version_block();

                let mut new_root
                    = self.block_manager.new_empty_index_block();

                new_node_right
                    .records_mut()
                    .extend_from_slice(records.get_unchecked(records_mid..));
                new_node_left
                    .records_mut()
                    .extend_from_slice(records.get_unchecked(..records_mid));

                new_root.keys_mut().push(k3);
                new_root.children_mut().extend([
                    new_node_left.into_cell(is_optimistic),
                    new_node_right.into_cell(is_optimistic)
                ]);

                self.set_new_root(
                    new_root,
                    n_height);
            }
            Node::MultiVersionLeaf(records) => unsafe {
                let records
                    = records.as_slice();

                let records_mid = records.len() / 2;
                let k3 = records
                    .get_unchecked(records_mid)
                    .key();

                let mut new_node_right
                    = self.block_manager.new_empty_leaf_multi_version_block();

                let mut new_node_left
                    = self.block_manager.new_empty_leaf_multi_version_block();

                let mut new_root
                    = self.block_manager.new_empty_index_block();

                new_node_right
                    .record_lists_mut()
                    .extend_from_slice(records.get_unchecked(records_mid..));
                new_node_left
                    .record_lists_mut()
                    .extend_from_slice(records.get_unchecked(..records_mid));

                new_root.keys_mut().push(k3);
                new_root.children_mut().extend([
                    new_node_left.into_cell(is_optimistic),
                    new_node_right.into_cell(is_optimistic)
                ]);

                self.set_new_root(
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
            Node::Index(index_page) => unsafe {
                let keys
                    = index_page.keys();

                let children
                    = index_page.children();

                let keys_mid = keys.len() / 2;
                let k3 = *keys
                    .get_unchecked(keys_mid);

                let mut new_node_right
                    = self.block_manager.new_empty_index_block();

                let mut new_node_from
                    = self.block_manager.new_empty_index_block();

                new_node_right
                    .keys_mut()
                    .extend_from_slice(keys.get_unchecked(keys_mid + 1..));
                new_node_right
                    .children_mut()
                    .extend_from_slice(children.get_unchecked(keys_mid + 1..));

                new_node_from
                    .keys_mut()
                    .extend_from_slice(keys.get_unchecked(..keys_mid));
                new_node_from
                    .children_mut()
                    .extend_from_slice(children.get_unchecked(..=keys_mid));

                let parent_mut = parent_guard
                    .assume_mut()
                    .unwrap();

                parent_mut
                    .keys_mut()
                    .insert(child_pos, k3);

                parent_mut
                    .children_mut()
                    .insert(child_pos + 1, new_node_right.into_cell(is_optimistic));

                *parent_mut
                    .children_mut()
                    .get_unchecked_mut(child_pos) = new_node_from.into_cell(is_optimistic);
            }
            Node::Leaf(records) => unsafe {
                let records
                    = records.as_slice();

                let records_mid = records.len() / 2;
                let k3 = records
                    .get_unchecked(records_mid)
                    .key();

                let mut new_node
                    = self.block_manager.new_empty_leaf_single_version_block();

                let mut new_node_from
                    = self.block_manager.new_empty_leaf_single_version_block();

                new_node
                    .records_mut()
                    .extend_from_slice(records.get_unchecked(records_mid..));

                new_node_from
                    .records_mut()
                    .extend_from_slice(records.get_unchecked(..records_mid));

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
                    .get_unchecked_mut(child_pos) = new_node_from.into_cell(is_optimistic);
            }
            Node::MultiVersionLeaf(records) => unsafe {
                let records
                    = records.as_slice();

                let records_mid = records.len() / 2;
                let k3 = records
                    .get_unchecked(records_mid)
                    .key();

                let mut new_node
                    = self.block_manager.new_empty_leaf_multi_version_block();

                let mut new_node_from
                    = self.block_manager.new_empty_leaf_multi_version_block();

                new_node
                    .record_lists_mut()
                    .extend_from_slice(records.get_unchecked(records_mid..));

                new_node_from
                    .record_lists_mut()
                    .extend_from_slice(records.get_unchecked(..records_mid));

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
                    .get_unchecked_mut(child_pos) = new_node_from.into_cell(is_optimistic);
            }
        }
    }

    #[inline]
    fn traversal_write_internal(&self, lock_level: LockLevel, attempt: Attempts, key: Key) -> Result<BlockGuard, (LockLevel, Attempts)>
    {
        let mut curr_level = Self::INIT_TREE_HEIGHT;

        let (mut current_guard, height, lock_level, attempt)
            = self.retrieve_root(lock_level, attempt);

        loop {
            let current_guard_result
                = current_guard.guard_result();

            if current_guard_result.is_null() {
                mem::drop(height);
                mem::drop(current_guard);

                if DEBUG {
                    println!("6 \tAttempt = {}", attempt);
                }

                return Err((curr_level - 1, attempt + 1));
            }

            let current_ref
                = unsafe { current_guard_result.as_reader().unwrap() };

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
        let olc = self.locking_strategy.is_olc();

        loop {
            match self.traversal_write_internal(lock_level, attempt, key) {
                Err((n_lock_level, n_attempt)) => {
                    attempt = n_attempt;
                    lock_level = n_lock_level;

                    if olc {
                        sched_yield(attempt);
                    }
                }
                Ok(guard) => break guard,
            }
        }
    }

    fn traversal_read_internal(&self, key: Key) -> Option<BlockGuard> {
        let root
            = self.root.clone();

        let mut current_guard
            = self.lock_reader(&root.block);

        loop {
            if !current_guard.is_valid() {
                return None;
            }

            let current_deref_result
                = current_guard.guard_result();

            let current
                = current_deref_result.as_ref();

            if current.is_none() {
                return None;
            }

            match current.unwrap().as_ref() {
                Node::Index(
                    IndexPage {
                        keys,
                        children,
                        ..
                    }) => {
                    let next_node = keys
                        .iter()
                        .enumerate()
                        .find(|(_, k)| key.lt(k))
                        .map(|(pos, _)| children.get(pos).cloned())
                        .unwrap_or_else(|| children.get(children.len() - 1).cloned());

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
        let olc = self.locking_strategy.is_olc();
        // let olc_limited = self.locking_strategy.is_olc_limited();

        loop {
            match self.traversal_read_internal(key) {
                Some(guard) => break guard,
                _ => {
                    attempt += 1;

                    if olc {
                        sched_yield(attempt)
                    }
                }
            }
        }
    }
}