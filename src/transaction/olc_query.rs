use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use std::mem;
use crate::index::bplus_tree::{BPlusTree, INIT_TREE_HEIGHT, LockLevel, MAX_TREE_HEIGHT};
use crate::page_model::Attempts;
use crate::page_model::block::BlockGuard;
use crate::page_model::node::Node;
use crate::record_model::record_point::RecordPoint;
use crate::record_model::unsafe_clone::UnsafeClone;
use crate::test::{Key, Payload};
use crate::transaction::query::DEBUG;
use crate::tx_model::transaction::Transaction;
use crate::tx_model::transaction_result::TransactionResult;
use crate::utils::interval::Interval;
use crate::utils::smart_cell::sched_yield;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display,
    Payload: Default + Clone + Sync + Display
> BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub(crate) fn range_query_olc(&self,
                                  path: &mut Vec<(Interval<Key>, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)>,
                                  org_key_interval: Interval<Key>) -> TransactionResult<Key, Payload>
    {
        let mut key_interval
            = org_key_interval.clone();

        let mut all_results
            = vec![];

        let mut history_path
            = VecDeque::with_capacity(2);

        loop {
            history_path.push_back(path.clone());

            let local_results =
                self.range_query_leaf_results(path, &key_interval);

            if path.len() >= 2 {
                let prev_path
                    = history_path.pop_front().unwrap();

                match prev_path.get(prev_path.len() - 2) {
                    Some((.., parent_leaf)) if !parent_leaf.is_valid() =>
                        return self.execute(Transaction::Range(org_key_interval)),
                    _ => {}
                };
            }

            let (leaf_space, ..)
                = path.last().unwrap();

            key_interval.set_lower((self.inc_key)(leaf_space.upper()));

            if !local_results.is_empty() {
                all_results.extend(local_results);

                self.next_leaf_page(path,
                                    path.len() - 2,
                                    key_interval.lower());
            } else {
                break;
            }
        }

        TransactionResult::MatchedRecords(all_results)
    }

    #[inline]
    fn next_leaf_page(&self,
                      path: &mut Vec<(Interval<Key>, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)>,
                      mut parent_index: usize,
                      next_key: Key)
    {
        loop {
            if parent_index >= path.len() { // when all path is invalid, we run stacking path function again!
                *path = unsafe {
                    mem::transmute(
                        self.traversal_read_range_olc(next_key))
                };
                return;
            }

            let (curr_interval, curr_parent)
                = path.get(parent_index).unwrap();

            let mut curr_interval
                = curr_interval.clone();

            let mut curr_parent
                = curr_parent;

            while !curr_interval.contains(next_key) {
                parent_index -= 1;
                let (n_curr_interval, n_curr_parent)
                    = path.get(parent_index).unwrap();

                curr_interval = n_curr_interval.clone();
                curr_parent = n_curr_parent;
            }

            let curr_deref
                = unsafe { curr_parent.deref_unsafe() };

            if !curr_parent.is_valid() {
                parent_index -= 1;
                continue;
            }

            match curr_deref.unwrap().as_ref() {
                Node::Index(index_page) => unsafe {
                    let keys
                        = index_page.keys();

                    let children
                        = index_page.children();

                    let (index_of_child, next_page)
                        = match keys.binary_search(&(self.inc_key)(next_key))
                    {
                        Ok(pos) => (pos, children.get(pos).cloned()),
                        Err(pos) => (pos, children.get(pos).cloned())
                    };

                    if next_page.is_none() || !curr_parent.is_valid() {
                        parent_index = parent_index - 1;
                        continue;
                    }

                    let next_page
                        = next_page.unwrap();

                    parent_index = parent_index + 1;

                    curr_interval = Interval::new(
                        keys.get(index_of_child - 1)
                            .cloned()
                            .unwrap_or(curr_interval.lower()),
                        keys.get(index_of_child)
                            .cloned()
                            .map(|max| (self.dec_key)(max))
                            .unwrap_or(curr_interval.upper()));

                    if parent_index == path.len() {
                        parent_index = path.len();

                        path.push((curr_interval,
                                   mem::transmute(self.lock_reader(&next_page))));
                    } else {
                        *path.get_unchecked_mut(parent_index)
                            = (curr_interval, mem::transmute(self.lock_reader(&next_page)));
                    }
                }
                Node::Leaf(..) => {
                    path.truncate(parent_index + 1);

                    if path.last().unwrap().1.is_obsolete() {
                        parent_index -= 1;
                    } else {
                        return;
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn range_query_leaf_results(&self,
                                path: &mut Vec<(Interval<Key>, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)>,
                                key_interval: &Interval<Key>)
                                -> Vec<RecordPoint<Key, Payload>>
    {
        let (.., leaf)
            = path.last().unwrap();

        let leaf_unchecked = unsafe { leaf.deref_unsafe() }.unwrap().as_ref();

        match leaf_unchecked {
            Node::Leaf(leaf_page) => unsafe {
                let (read, current_read_version)
                    = leaf.is_read_not_obsolete_result();

                if read {
                    // println!("Records in Leaf = {}", leaf_page
                    //     .as_records_mut().iter().join(","));
                    let mut potential_results = leaf_page
                        .as_records()
                        .iter()
                        .skip_while(|record| record.key.lt(&key_interval.lower()))
                        .take_while(|record| record.key.le(&key_interval.upper()))
                        .map(|record| record.unsafe_clone())
                        .collect::<Vec<_>>();

                    // println!("Filtered Records = {}", potential_results.iter().join(","));
                    if leaf.cell_version_olc() == current_read_version { // avoid write in-between
                        return potential_results;
                    } else {
                        potential_results.set_len(0);
                    }
                }

                let parent_index = path.len() - 2;
                self.next_leaf_page(path, parent_index, key_interval.lower());
                self.range_query_leaf_results(path, key_interval)
            }
            _ => unreachable!("Found Index but expected leaf = {}", leaf_unchecked)
        }
    }
    #[inline]
    fn traversal_read_olc_internal(&self, key: Key) -> Option<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        let mut current_guard
            = self.lock_reader(&self.root.block);

        let key = (self.inc_key)(key);
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

                    let next_node = match keys.binary_search(&key) {
                        Ok(pos) => children.get(pos).cloned(),
                        Err(pos) => children.get(pos).cloned()
                    };

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
    pub(crate) fn traversal_read_olc(&self, key: Key) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {
        let mut attempt = 0;

        loop {
            match self.traversal_read_olc_internal(key) {
                Some(guard) => break guard,
                _ => {
                    attempt += 1;
                    sched_yield(attempt)
                }
            }
        }
    }

    #[inline]
    fn traversal_read_range_olc_internal(&self, key_low: Key)
                                         -> Vec<(Interval<Key>, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)>
    {
        let root
            = self.root.get();

        let mut path
            = Vec::with_capacity(self.height() as _);

        let mut key_interval
            = Interval::new(self.min_key, self.max_key);

        let mut current_guard
            = self.lock_reader(&root.block);

        loop {
            let current
                = current_guard.deref();

            if current.is_none() {
                mem::drop(current_guard);

                path.clear();
                return path;
            }

            match current.unwrap().as_ref() {
                Node::Index(index_page) => {
                    let keys = index_page.keys();
                    let children = index_page.children();

                    let (index_of_child, next_node)
                        = match keys.binary_search(&(self.inc_key)(key_low))
                    {
                        Ok(pos) => (pos, children.get(pos).cloned()),
                        Err(pos) => (pos, children.get(pos).cloned())
                    };

                    if next_node.is_none() || !current_guard.is_valid() {
                        path.clear();
                        return path;
                    }

                    let next_node
                        = next_node.unwrap();

                    let old_interval = key_interval.clone();
                    key_interval = Interval::new(
                        keys.get(index_of_child - 1)
                            .cloned()
                            .unwrap_or(key_interval.lower()),
                        keys.get(index_of_child)
                            .cloned()
                            .map(|max| (self.dec_key)(max))
                            .unwrap_or(key_interval.upper()));

                    path.push((old_interval, current_guard));
                    current_guard = self.lock_reader(&next_node);
                }
                Node::Leaf(..) => {
                    if current_guard.is_obsolete() {
                        path.clear();
                    } else {
                        path.push((key_interval, current_guard));
                    }

                    break path;
                }
            }
        }
    }

    #[inline]
    pub(crate) fn traversal_read_range_olc(&self, key: Key)
                                           -> Vec<(Interval<Key>, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)>
    {
        let mut attempt = 0;

        loop {
            match self.traversal_read_range_olc_internal(key) {
                path if path.is_empty() => {
                    attempt += 1;
                    sched_yield(attempt)
                }
                path => break path,
            }
        }
    }

    #[inline]
    pub(crate) fn traversal_write_olc(&self, key: Key) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {
        let mut attempt = 0;
        let mut lock_level = MAX_TREE_HEIGHT;
        let olc = self.locking_strategy.is_olc();

        loop {
            match self.traversal_write_olc_internal(lock_level, attempt, key) {
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
    fn traversal_write_olc_internal(&self, lock_level: LockLevel, attempt: Attempts, key: Key)
                                    -> Result<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, (LockLevel, Attempts)>
    {
        let mut curr_level = INIT_TREE_HEIGHT;

        let (mut current_guard, height, lock_level, attempt)
            = self.retrieve_root(lock_level, attempt);

        let key = (self.inc_key)(key);
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

                    let (child_pos, next_node)
                        = match keys.binary_search(&key)
                    {
                        Ok(pos) => (pos, children.get(pos).cloned()),
                        Err(pos) => (pos, children.get(pos).cloned())
                    };

                    if next_node.is_none() || !current_guard.is_valid() {
                        mem::drop(next_node);
                        mem::drop(current_guard);

                        if DEBUG {
                            println!("8 \tAttempt = {}", attempt);
                        }

                        return Err((curr_level - 1, attempt + 1));
                    }

                    let next_node
                        = next_node.unwrap();

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
                        if !current_guard.upgrade_write_lock() || !next_guard.upgrade_write_lock() {
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
            }
        }
    }
}