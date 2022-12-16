use std::hash::Hash;
use std::{mem, ptr};
use TXDataModel::page_model::{Attempts, Height, Level};
use TXDataModel::page_model::block::BlockGuard;
use TXDataModel::page_model::node::{Node, NodeUnsafeDegree};
use TXDataModel::record_model::record_like::RecordLike;
use TXDataModel::utils::hybrid_cell::sched_yield;
use crate::index::bplus_tree::{BPlusTree, INIT_TREE_HEIGHT, LockLevel, MAX_TREE_HEIGHT};
use crate::locking::locking_strategy::{LevelConstraints, LockingStrategy};

const DEBUG: bool = false;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync
> BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    fn has_overflow(&self, node: &Node<FAN_OUT, NUM_RECORDS, Key, Payload>) -> bool {
        match node.is_leaf() {
            true => node.is_overflow(self.block_manager.allocation_leaf()),
            false => node.is_overflow(self.block_manager.allocation_directory())
        }
    }

    fn has_underflow(&self, node: &Node<FAN_OUT, NUM_RECORDS, Key, Payload>) -> bool {
        match node.is_leaf() {
            true => node.is_underflow(self.block_manager.allocation_leaf()),
            false => node.is_underflow(self.block_manager.allocation_directory())
        }
    }

    fn unsafe_degree_of(&self, node: &Node<FAN_OUT, NUM_RECORDS, Key, Payload>) -> NodeUnsafeDegree {
        match node.is_leaf() {
            true => node.unsafe_degree(self.block_manager.allocation_leaf()),
            false => node.unsafe_degree(self.block_manager.allocation_directory()),
        }
    }

    #[inline]
    fn retrieve_root(&self, mut lock_level: Level, mut attempt: Attempts)
                     -> (BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, Height, LockLevel, Attempts)
    {
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
    fn retrieve_root_internal(&self, lock_level: LockLevel, attempt: Attempts)
                              -> Result<(BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, Height), (LockLevel, Attempts)>
    {
        // let root
        //     = self.root.clone();

        let root
            = self.root.get();

        let mut root_guard = match self.locking_strategy {
            LockingStrategy::MonoWriter => root.block.borrow_free(),
            LockingStrategy::LockCoupling => root.block.borrow_mut_exclusive(),
            LockingStrategy::OLC(LevelConstraints::Unlimited) => root.block.borrow_free(),
            LockingStrategy::OLC(LevelConstraints::OptimisticLimit { .. })
            if self.locking_strategy.is_lock_root(lock_level, attempt, root.height()) => {
                let guard
                    = root.block.borrow_mut();

                if !guard.is_write_lock() {
                    mem::drop(guard);

                    return Err((lock_level, attempt + 1));
                }

                guard
            }
            LockingStrategy::OLC(..) => root.block.borrow_free(),
            LockingStrategy::RWLockCoupling(..)
            if self.locking_strategy.is_lock_root(lock_level, attempt, root.height()) =>
                root.block.borrow_mut(),
            LockingStrategy::RWLockCoupling(..) =>
                root.block.borrow_read(),
        };

        let root_ref
            = root_guard.deref();

        if root_ref.is_none() {
            mem::drop(root_guard);

            return Err((lock_level, attempt + 1));
        }

        let root_ref
            = root_ref.unwrap();

        let has_overflow_root
            = self.has_overflow(root_ref);

        let force_restart = match self.locking_strategy {
            LockingStrategy::MonoWriter | LockingStrategy::LockCoupling => false,
            _ => !root_guard.is_write_lock()
        };

        if force_restart && has_overflow_root && !root_guard.upgrade_write_lock() { // !root_guard.is_valid() ||
            mem::drop(root_guard);

            return Err((lock_level, attempt + 1));
        }

        if !has_overflow_root {
            return Ok((root_guard, root.height()));
        }

        // debug_assert!(root_guard.is_valid());

        // if !root_guard.upgrade_write_lock() && self.locking_strategy.additional_lock_required() {
        //     mem::drop(root_guard);
        //
        //     return Err((lock_level, attempt + 1));
        // }

        let root_ref
            = root_guard.deref_mut().unwrap();

        let is_optimistic
            = root_guard.mark_obsolete();

        let n_height
            = root.height() + 1;

        match root_ref.as_mut() {
            Node::Index(index_page) => unsafe {
                let keys = index_page.keys();
                let children = index_page.children();

                let keys_mid = keys.len() / 2;
                let k3 = *keys.get_unchecked(keys_mid);

                let index_block
                    = self.block_manager.new_empty_index_block();

                let new_node_right =
                    self.block_manager.new_empty_index_block();

                let new_root_left
                    = self.block_manager.new_empty_index_block();

                new_node_right
                    .children_mut()
                    .extend_from_slice(children.get_unchecked(keys_mid + 1..));

                new_node_right
                    .keys_mut()
                    .extend_from_slice(keys.get_unchecked(keys_mid + 1..));

                new_root_left
                    .children_mut()
                    .extend_from_slice(children.get_unchecked(..=keys_mid));

                new_root_left
                    .keys_mut()
                    .extend_from_slice(keys.get_unchecked(..keys_mid));

                index_block.children_mut().extend([
                    new_root_left.into_cell(is_optimistic),
                    new_node_right.into_cell(is_optimistic)
                ]);

                index_block.keys_mut()
                    .push(k3);

                self.set_new_root(
                    index_block,
                    n_height);
            }
            Node::Leaf(records) => unsafe {
                let records
                    = records.as_records();

                let records_mid
                    = records.len() / 2;

                let k3 = records
                    .get_unchecked(records_mid)
                    .key();

                let new_node_right
                    = self.block_manager.new_empty_leaf_single_version_block();

                let new_node_left
                    = self.block_manager.new_empty_leaf_single_version_block();

                let new_root
                    = self.block_manager.new_empty_index_block();

                new_node_right
                    .records_mut()
                    .extend_from_slice(records.get_unchecked(records_mid..));

                new_node_left
                    .records_mut()
                    .extend_from_slice(records.get_unchecked(..records_mid));

                new_root.children_mut().extend([
                    new_node_left.into_cell(is_optimistic),
                    new_node_right.into_cell(is_optimistic)
                ]);

                new_root.keys_mut()
                    .push(k3);

                self.set_new_root(
                    new_root,
                    n_height);
            }
            // Node::MultiVersionLeaf(records) => unsafe {
            //     let records
            //         = records.as_records();
            //
            //     let records_mid = records.len() / 2;
            //     let k3 = records
            //         .get_unchecked(records_mid)
            //         .key();
            //
            //     let new_node_right
            //         = self.block_manager.new_empty_leaf_multi_version_block();
            //
            //     let new_node_left
            //         = self.block_manager.new_empty_leaf_multi_version_block();
            //
            //     let new_root
            //         = self.block_manager.new_empty_index_block();
            //
            //     new_node_right
            //         .record_lists_mut()
            //         .extend_from_slice(records.get_unchecked(records_mid..));
            //
            //     new_node_left
            //         .record_lists_mut()
            //         .extend_from_slice(records.get_unchecked(..records_mid));
            //
            //     new_root.children_mut().extend([
            //         new_node_left.into_cell(is_optimistic),
            //         new_node_right.into_cell(is_optimistic)
            //     ]);
            //
            //     new_root.keys_mut()
            //         .push(k3);
            //
            //     self.set_new_root(
            //         new_root,
            //         n_height);
            // }
        }

        Ok((root_guard, n_height))
    }

    fn do_overflow_correction(
        &self,
        parent_guard: &mut BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        child_pos: usize,
        from_guard: BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)
    {
        let olc
            = from_guard.mark_obsolete();

        match from_guard.deref_mut().unwrap().as_mut() {
            Node::Index(index_page) => unsafe {
                let keys
                    = index_page.keys();

                let children
                    = index_page.children();

                let keys_mid = keys.len() / 2;
                let k3 = *keys
                    .get_unchecked(keys_mid);

                let new_node_right
                    = self.block_manager.new_empty_index_block();

                let new_node_from
                    = self.block_manager.new_empty_index_block();

                new_node_right
                    .children_mut()
                    .extend_from_slice(children.get_unchecked(keys_mid + 1..));

                new_node_right
                    .keys_mut()
                    .extend_from_slice(keys.get_unchecked(keys_mid + 1..));

                new_node_from
                    .children_mut()
                    .extend_from_slice(children.get_unchecked(..=keys_mid));

                new_node_from
                    .keys_mut()
                    .extend_from_slice(keys.get_unchecked(..keys_mid));

                let parent_mut = parent_guard
                    .deref_mut()
                    .unwrap();

                let mut parent_children
                    = parent_mut.children_mut();

                if olc {
                    mem::drop(mem::replace(parent_children.get_unchecked_mut(child_pos),
                              new_node_from.into_olc()))
                } else {
                    ptr::write(parent_children.get_unchecked_mut(child_pos),
                               new_node_from.into_cc())
                }

                parent_children
                    .insert(child_pos + 1, new_node_right.into_cell(olc));

                parent_mut
                    .keys_mut()
                    .insert(child_pos, k3);
            }
            Node::Leaf(records) => unsafe {
                let records
                    = records.as_records();

                let records_mid = records.len() / 2;
                let k3 = records
                    .get_unchecked(records_mid)
                    .key();

                let new_node
                    = self.block_manager.new_empty_leaf_single_version_block();

                let new_node_from
                    = self.block_manager.new_empty_leaf_single_version_block();

                new_node
                    .records_mut()
                    .extend_from_slice(records.get_unchecked(records_mid..));

                new_node_from
                    .records_mut()
                    .extend_from_slice(records.get_unchecked(..records_mid));

                let parent_mut = parent_guard
                    .deref_mut()
                    .unwrap();

                let mut parent_children
                    = parent_mut.children_mut();

                if olc {
                    mem::drop(mem::replace(parent_children.get_unchecked_mut(child_pos),
                                           new_node_from.into_olc()))
                } else {
                    ptr::write(parent_children.get_unchecked_mut(child_pos),
                               new_node_from.into_cc());
                }

                parent_children
                    .insert(child_pos + 1, new_node.into_cell(olc));

                parent_mut
                    .keys_mut()
                    .insert(child_pos, k3);
            }
            // Node::MultiVersionLeaf(records) => unsafe {
            //     let records
            //         = records.as_records();
            //
            //     let records_mid = records.len() / 2;
            //     let k3 = records
            //         .get_unchecked(records_mid)
            //         .key();
            //
            //     let new_node
            //         = self.block_manager.new_empty_leaf_multi_version_block();
            //
            //     let new_node_from
            //         = self.block_manager.new_empty_leaf_multi_version_block();
            //
            //     new_node
            //         .record_lists_mut()
            //         .extend_from_slice(records.get_unchecked(records_mid..));
            //
            //     new_node_from
            //         .record_lists_mut()
            //         .extend_from_slice(records.get_unchecked(..records_mid));
            //
            //     let parent_mut = parent_guard
            //         .deref_mut()
            //         .unwrap();
            //
            //     parent_mut
            //         .children_mut()
            //         .insert(child_pos + 1, new_node.into_cell(olc));
            //
            //     mem::forget(*parent_mut
            //         .children_mut()
            //         .get_unchecked_mut(child_pos) = new_node_from.into_cell(olc));
            //
            //     parent_mut
            //         .keys_mut()
            //         .insert(child_pos, k3);
            // }
        }
    }

    #[inline]
    fn traversal_write_internal(&self, lock_level: LockLevel, attempt: Attempts, key: Key)
                                -> Result<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, (LockLevel, Attempts)>
    {
        let mut curr_level = INIT_TREE_HEIGHT;

        let (mut current_guard, height, lock_level, attempt)
            = self.retrieve_root(lock_level, attempt);

        loop {
            let current_guard_result
                = current_guard.deref();

            if current_guard_result.is_none() {
                mem::drop(height);
                mem::drop(current_guard);

                if DEBUG {
                    println!("6 \tAttempt = {}", attempt);
                }

                return Err((curr_level - 1, attempt + 1));
            }

            match current_guard_result.unwrap().as_ref() {
                Node::Index(index_page) => {
                    let keys = index_page.keys();
                    let children = index_page.children();

                    let (next_node, child_pos) = keys
                        .iter()
                        .enumerate()
                        .find(|(_, k)| key.lt(k))
                        .map(|(pos, _)| (children.get(pos).cloned(), pos))
                        .unwrap_or_else(|| (children.get(keys.len()).cloned(), keys.len()));

                    if next_node.is_none() || !current_guard.is_valid() {
                        mem::drop(next_node);
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
                        = next_guard.deref();

                    if next_guard_result.is_none() || !current_guard.is_valid() {
                        mem::drop(height);
                        mem::drop(next_guard);
                        mem::drop(current_guard);

                        if DEBUG {
                            println!("9 \tAttempt = {}", attempt);
                        }

                        return Err((curr_level - 1, attempt + 1));
                    }

                    let has_overflow_next
                        = self.has_overflow(next_guard_result.unwrap());

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
                            &mut current_guard,
                            child_pos,
                            next_guard)
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
                _ => return if current_guard.upgrade_write_lock() {
                    Ok(current_guard)
                } else {
                    Err((curr_level - 1, attempt + 1))
                },
                // _ if current_guard.upgrade_write_lock() => return Ok(current_guard),
                _ => return Err((curr_level - 1, attempt + 1))
            }
        }
    }

    #[inline]
    pub(crate) fn traversal_write(&self, key: Key) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {
        let mut attempt = 0;
        let mut lock_level = MAX_TREE_HEIGHT;
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

    #[inline]
    fn traversal_read_internal(&self, key: Key) -> Option<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        let root
            = self.root.get();

        // let root
        //     = self.root.clone();

        let mut current_guard
            = self.lock_reader(&root.block);

        loop {
            let current
                = current_guard.deref();

            if current.is_none() {
                mem::drop(current_guard);

                return None;
            }

            match current.unwrap().as_ref() {
                Node::Index(index_page) => {
                    let keys = index_page.keys();
                    let children = index_page.children();

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

    #[inline]
    pub(crate) fn traversal_read(&self, key: Key) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {
        let mut attempt = 0;
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