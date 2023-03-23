use std::collections::VecDeque;
use std::hash::Hash;
use std::{mem, ptr};
use crate::locking::locking_strategy::{OLCVariant, LockingStrategy};
use crate::page_model::{Attempts, BlockID, BlockRef, Height, Level};
use crate::block::block::BlockGuard;
use crate::page_model::node::{Node, NodeUnsafeDegree};
use crate::page_model::node::Node::Index;
use crate::tree::bplus_tree::{BPlusTree, INIT_TREE_HEIGHT, LockLevel, MAX_TREE_HEIGHT};
use crate::utils::interval::Interval;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync
> BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub(crate) fn has_overflow(&self, node: &Node<FAN_OUT, NUM_RECORDS, Key, Payload>) -> bool {
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
    pub(crate) fn retrieve_root(&self, mut lock_level: Level, mut attempt: Attempts)
                     -> (BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, Height, LockLevel, Attempts)
    {
        loop {
            match self.retrieve_root_internal(lock_level, attempt) {
                Err((n_lock_level, n_attempt)) => {
                    lock_level = n_lock_level;
                    attempt = n_attempt;
                }
                Ok((guard, height)) =>
                    break (guard, height, lock_level, attempt)
            }
        }
    }

    #[inline]
    pub(crate) fn retrieve_root_internal(&self, lock_level: LockLevel, attempt: Attempts)
    -> Result<(BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, Height), (LockLevel, Attempts)>
    {
        let root
            = self.root.get();

        let mut root_guard = match self.locking_strategy {
            LockingStrategy::MonoWriter => root.block.borrow_free(),
            LockingStrategy::LockCoupling => root.block.borrow_mut(),
            LockingStrategy::OLC(OLCVariant::Free) => root.block.borrow_read(),
            LockingStrategy::OLC(OLCVariant::Bounded { .. })
            if self.locking_strategy.is_lock_root(lock_level, attempt, root.height()) =>
                root.block.borrow_mut(),
            LockingStrategy::OLC(..) => root.block.borrow_read(),
            LockingStrategy::ORWC(..)
            if self.locking_strategy.is_lock_root(lock_level, attempt, root.height()) =>
                root.block.borrow_mut(),
            LockingStrategy::ORWC(..) =>
                root.block.borrow_read(),
            LockingStrategy::HybridLocking(..)
            if self.locking_strategy.is_lock_root(lock_level, attempt, root.height()) =>
                root.block.borrow_mut(),
            LockingStrategy::HybridLocking(..) =>
                root.block.borrow_read()
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

        let root_ref
            = root_guard.deref_mut().unwrap();

        let latch_type
            = self.locking_strategy.latch_type();

        let n_height
            = root.height() + 1;

        match root_ref.as_mut() {
            Node::Index(index_page) => unsafe {
                let keys = index_page.keys();
                let children = index_page.children();

                let keys_mid = keys.len() / 2;
                let k3 = *keys.get_unchecked(keys_mid);

                let new_node_right =
                    self.block_manager.new_empty_index_block();

                let new_root_left
                    = self.block_manager.new_empty_index_block();

                children
                    .as_ptr()
                    .add(keys_mid + 1)
                    .copy_to_nonoverlapping(new_node_right.children_mut().as_mut_ptr(),
                                            children.get_unchecked(keys_mid + 1..).len());

                children
                    .as_ptr()
                    .copy_to_nonoverlapping(new_root_left.children_mut().as_mut_ptr(),
                                            children.get_unchecked(..=keys_mid).len());

                keys.as_ptr()
                    .copy_to_nonoverlapping(new_root_left.keys_mut().as_mut_ptr(),
                                            keys.get_unchecked(..keys_mid).len());

                keys.as_ptr()
                    .add(keys_mid + 1)
                    .copy_to_nonoverlapping(new_node_right.keys_mut().as_mut_ptr(),
                                            keys.get_unchecked(keys_mid + 1..).len());

                new_root_left.keys_mut().set_len(keys.get_unchecked(..keys_mid).len());
                new_node_right.keys_mut().set_len(keys.get_unchecked(keys_mid + 1..).len());

                root_ref.children_mut()
                    .as_mut_ptr()
                    .write(new_root_left.into_cell(latch_type));

                root_ref.children_mut()
                    .as_mut_ptr()
                    .add(1)
                    .write(new_node_right.into_cell(latch_type));

                root_ref.keys_mut()
                    .as_mut_ptr()
                    .write(k3);

                root_ref.keys_mut().set_len(1);

                self.root.get_mut().height = n_height;
            }
            Node::Leaf(records) => unsafe {
                let records
                    = records.as_records();

                let records_mid
                    = records.len() / 2;

                let k3 = records
                    .get_unchecked(records_mid)
                    .key;

                let new_node_right
                    = self.block_manager.new_empty_leaf_single_version_block();

                let new_node_left
                    = self.block_manager.new_empty_leaf_single_version_block();

                records
                    .as_ptr()
                    .copy_to_nonoverlapping(new_node_left.records_mut().as_mut_ptr(), records_mid - 1);

                records
                    .as_ptr()
                    .add(records_mid)
                    .copy_to_nonoverlapping(new_node_right.records_mut().as_mut_ptr(), records.len() - records_mid);

                new_node_right
                    .records_mut()
                    .set_len(records.len() - records_mid);

                new_node_left
                    .records_mut()
                    .set_len(records_mid);

                ptr::write((root_ref as *mut _ as *mut usize)
                               .add(mem::size_of::<BlockID>() / mem::size_of::<usize>()),
                           0usize);

                root_ref
                    .children_mut()
                    .as_mut_ptr()
                    .write(new_node_left.into_cell(latch_type));
                root_ref
                    .children_mut()
                    .as_mut_ptr()
                    .add(1)
                    .write(new_node_right.into_cell(latch_type));

                root_ref
                    .keys_mut()
                    .as_mut_ptr()
                    .write(k3);

                root_ref
                    .keys_mut()
                    .set_len(1);

                self.root.get_mut().height = n_height;
            }
        }

        Ok((root_guard, n_height))
    }

    pub(crate) fn do_overflow_correction(
        &self,
        parent_guard: &mut BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        child_pos: usize,
        from_guard: BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)
    {
        from_guard.mark_obsolete();

        let latch_type
            = self.locking_strategy.latch_type();

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

                parent_children
                    .insert(child_pos + 1, new_node_right.into_cell(latch_type));

                mem::drop(mem::replace(parent_children.get_unchecked_mut(child_pos),
                                       new_node_from.into_cell(latch_type)));

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
                    .key;

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

                parent_children
                    .insert(child_pos + 1, new_node.into_cell(latch_type));

                mem::drop(mem::replace(parent_children.get_unchecked_mut(child_pos),
                                       new_node_from.into_cell(latch_type)));

                parent_mut
                    .keys_mut()
                    .insert(child_pos, k3);
            }
        }
    }

    #[inline]
    fn traversal_write_internal(&self, lock_level: LockLevel, attempt: Attempts, key: Key)
                                    -> Result<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, (LockLevel, Attempts)>
    {
        let mut curr_level = INIT_TREE_HEIGHT;

        let (mut current_guard, height, lock_level, attempt)
            = self.retrieve_root(lock_level, attempt);

        let key = (self.inc_key)(key);
        loop {
            let current_guard_result
                = current_guard.deref();

            match current_guard_result.unwrap().as_ref() {
                Node::Index(index_page) => unsafe {
                    let (child_pos, next_node)
                        = match index_page.keys().binary_search(&key)
                    {
                        Ok(pos) => (pos, index_page.get_child_unsafe(pos)),
                        Err(pos) => (pos, index_page.get_child_unsafe(pos))
                    };

                    curr_level += 1;

                    let mut next_guard = self.apply_for_ref(
                        curr_level,
                        lock_level,
                        attempt,
                        height,
                        next_node);

                    let next_guard_result
                        = next_guard.deref();

                    let has_overflow_next
                        = self.has_overflow(next_guard_result.unwrap());

                    if has_overflow_next {
                        if self.locking_strategy.additional_lock_required() &&
                            (!current_guard.upgrade_write_lock() || !next_guard.upgrade_write_lock())
                        {
                            mem::drop(next_guard);
                            mem::drop(current_guard);

                            return Err((curr_level - 1, attempt + 1));
                        }

                        debug_assert!(self.locking_strategy.additional_lock_required() &&
                            current_guard.is_write_lock() && next_guard.is_write_lock() ||
                            !self.locking_strategy.additional_lock_required());

                        self.do_overflow_correction(
                            &mut current_guard,
                            child_pos,
                            next_guard)
                    } else {
                        current_guard = next_guard;
                    }
                }
                _ => return if current_guard.upgrade_write_lock() {
                    Ok(current_guard)
                } else {
                    Err((curr_level - 1, attempt + 1))
                },
            }
        }
    }

    #[inline]
    pub(crate) fn traversal_write(&self, key: Key) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {
        let mut attempt = 0;
        let mut lock_level = MAX_TREE_HEIGHT;

        loop {
            match self.traversal_write_internal(lock_level, attempt, key) {
                Err((n_lock_level, n_attempt)) => {
                    attempt = n_attempt;
                    lock_level = n_lock_level;
                }
                Ok(guard) => break guard,
            }
        }
    }

    #[inline]
    pub(crate) fn traversal_read(&self, key: Key) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {
        let mut current_block
            = self.root.block();

        let mut current_guard
            = self.lock_reader(&current_block);

        let key = (self.inc_key)(key);
        loop {
            match current_guard.deref().unwrap().as_ref() {
                Node::Index(index_page) =>
                    match index_page.keys().binary_search(&key) {
                        Ok(pos) => unsafe {
                            let next_block
                                = index_page.get_child_unsafe_cloned(pos);

                            let next_guard
                                = self.lock_reader(&next_block);

                            current_guard = next_guard;
                            current_block = next_block;
                        },
                        Err(pos) => unsafe {
                            let next_block
                                = index_page.get_child_unsafe_cloned(pos);

                            let next_guard
                                = self.lock_reader(&next_block);

                            current_guard = next_guard;
                            current_block = next_block;
                        }
                    },
                _ => break current_guard,
            }
        }
    }

    #[inline]
    pub(crate) fn traversal_read_range(
        &self,
        current_range: &Interval<Key>)
    -> Vec<(BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>, BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>)>
    {
        let mut current_block
            = self.root.block();

        let mut current_guard
            = self.lock_reader(&current_block);

        let mut path
            = VecDeque::new();

        path.push_back((current_block, current_guard));

        let mut results
            = Vec::new();

        while !path.is_empty() {
            let (n_current_block, n_current_guard)
                = path.pop_front().unwrap();

            current_guard = n_current_guard;
            current_block = n_current_block;

            match current_guard.deref().unwrap().as_ref() {
                Node::Index(index_page) => unsafe {
                    let keys
                        = index_page.keys();

                    let first_pos = match keys.binary_search(&current_range.lower) {
                        Ok(pos) => pos,
                        Err(pos) => pos
                    };

                    let last_pos = match keys.binary_search(&(self.inc_key)(current_range.upper)) {
                        Ok(pos) => pos,
                        Err(pos) => pos
                    };

                    path.extend(index_page.children()
                        .get_unchecked(first_pos..=last_pos)
                        .iter()
                        .map(|child| (child.clone(), self.lock_reader(child))));
                }
                _ => results.push((current_block, current_guard)),
            }
        }

        results
    }
}