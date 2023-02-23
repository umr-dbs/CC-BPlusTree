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

            if history_path.len() >= 2 {
                let prev_path
                    = history_path.pop_front().unwrap();

                match prev_path.get(prev_path.len() - 2) {
                    Some((.., parent_leaf)) if !parent_leaf.is_valid() =>
                        return self.execute(Transaction::Range(org_key_interval)),
                    _ => {}
                };
            }

            if !local_results.is_empty() {
                all_results.extend(local_results);

                let (leaf_space, ..)
                    = path.last().unwrap();

                key_interval.set_lower((self.inc_key)(leaf_space.upper()));

                if key_interval.lower > key_interval.upper {
                    break;
                }

                self.next_leaf_page(
                    path,
                    path.len() - 2,
                    key_interval.lower());
            } else {
                break;
            }
        }

        TransactionResult::MatchedRecords(all_results)
    }

    #[inline]
    pub(crate) fn next_leaf_page(&self,
                                 path: &mut Vec<(Interval<Key>, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)>,
                                 mut parent_index: usize,
                                 next_key: Key)
    {
        let mut attempts = 0;
        loop {
            if attempts > 0 {
                sched_yield(attempts);
            }

            if parent_index >= path.len() { // when all path is invalid, we run stacking path function again!
                path.clear();

                let root_read
                    = self.lock_reader(&self.root.block);

                if !root_read.is_read_not_obsolete() {
                    attempts += 1;
                    continue
                }

                path.push((Interval::new(self.min_key, self.max_key),
                           unsafe { mem::transmute(root_read) }));

                attempts = 0;
                parent_index = 0;
            }

            let (curr_interval, curr_parent)
                = path.get_mut(parent_index).unwrap();

            let mut curr_interval
                = curr_interval.clone();

            let mut curr_parent
                = curr_parent;

            while !curr_interval.contains(next_key) {
                path.truncate(parent_index);
                parent_index -= 1;
                let (n_curr_interval, .., n_curr_parent)
                    = path.get_mut(parent_index).unwrap();

                curr_interval = n_curr_interval.clone();
                curr_parent = n_curr_parent;
            }

            let curr_deref
                = unsafe { curr_parent.deref_unsafe() };

            let (read, current_reader_version)
                = curr_parent.is_read_not_obsolete_result();

            if curr_deref.is_none() || !read {
                path.truncate(parent_index);
                attempts += 1;
                parent_index -= 1;
                continue;
            }

            match curr_deref.unwrap().as_ref() {
                Node::Index(index_page) => unsafe {
                    let keys
                        = index_page.keys();

                    let (curr_interval, next_page)
                        = match keys.binary_search(&(self.inc_key)(next_key))
                    {
                        Ok(pos) => (Interval::new(
                            keys.get(pos - 1).cloned()
                                .unwrap_or(curr_interval.lower()),
                            keys.get(pos).cloned()
                                .map(|max| (self.dec_key)(max)).unwrap_or(curr_interval.upper())),
                                    index_page.get_child_result(pos)),
                        Err(pos) => (Interval::new(
                            keys.get(pos - 1).cloned()
                                .unwrap_or(curr_interval.lower()),
                            keys.get(pos).cloned()
                                .map(|max| (self.dec_key)(max)).unwrap_or(curr_interval.upper())),
                                     index_page.get_child_result(pos))
                    };

                    let (read, read_version)
                        = curr_parent.is_read_not_obsolete_result();

                    if !read || read_version != current_reader_version {
                        path.truncate(parent_index);
                        parent_index -= 1;
                        attempts += 1;
                        continue;
                    }

                    let next_page = {
                        let child = next_page.assume_init();
                        mem::forget(child.clone());

                        child
                    };

                    curr_parent.update_read_latch(read_version);

                    attempts = 0;
                    parent_index += 1;
                    path.insert(parent_index,
                                (curr_interval, mem::transmute(self.lock_reader(&next_page))));
                }
                Node::Leaf(..) => return
            }
        }
    }

    #[inline(always)]
    fn range_query_leaf_results(&self,
                                path: &mut Vec<(Interval<Key>, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)>,
                                key_interval: &Interval<Key>)
                                -> Vec<RecordPoint<Key, Payload>>
    {
        loop {
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
                            .skip_while(|record| record.key().lt(&key_interval.lower()))
                            .take_while(|record| record.key().le(&key_interval.upper()))
                            .map(|record| record.unsafe_clone())
                            .collect::<Vec<_>>();

                        // println!("Filtered Records = {}", potential_results.iter().join(","));
                        if leaf.cell_version_olc() == current_read_version { // avoid write in-between
                            return potential_results;
                        } else {
                            potential_results.set_len(0);
                        }
                    }

                    self.next_leaf_page(path, path.len() - 2, key_interval.lower());
                }
                _ => unreachable!("Found Index but expected leaf = {}", leaf_unchecked)
            }
        }
    }

    #[inline]
    fn traversal_read_olc_internal(&self, key: Key) -> Option<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        let mut current_guard
            = self.lock_reader(&self.root.block);

        let key = (self.inc_key)(key);
        loop {
            let current
                = unsafe { current_guard.deref_unsafe() };

            let (read, current_reader_version)
                = current_guard.is_read_not_obsolete_result();

            if current.is_none() || !read {
                mem::drop(current_guard);

                return None;
            }

            match current.unwrap().as_ref() {
                Node::Index(index_page) => {
                    let next_node = match index_page.keys().binary_search(&key) {
                        Ok(pos) => index_page.get_child_result(pos),
                        Err(pos) => index_page.get_child_result(pos)
                    };

                    let (read, read_version)
                        = current_guard.is_read_not_obsolete_result();

                    if !read || read_version != current_reader_version {
                        return None;
                    }

                    let next_node = unsafe {
                        let child = next_node.assume_init();
                        mem::forget(child.clone());

                        child
                    };

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
    pub(crate) fn traversal_write_olc(&self, key: Key) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {
        let mut attempt = 0;
        let mut lock_level = MAX_TREE_HEIGHT;

        loop {
            match self.traversal_write_olc_internal(lock_level, attempt, key) {
                Err((n_lock_level, n_attempt)) => {
                    attempt = n_attempt;
                    lock_level = n_lock_level;

                    sched_yield(attempt);
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
                    let (child_pos, next_node)
                        = match index_page.keys().binary_search(&key)
                    {
                        Ok(pos) => (pos, index_page.get_child_result(pos)),
                        Err(pos) => (pos, index_page.get_child_result(pos))
                    };

                    if !current_guard.is_valid() {
                        mem::drop(next_node);
                        mem::drop(current_guard);

                        if DEBUG {
                            println!("8 \tAttempt = {}", attempt);
                        }

                        return Err((curr_level - 1, attempt + 1));
                    }

                    let next_node = unsafe {
                        let child = next_node.assume_init();
                        mem::forget(child.clone());

                        child
                    };

                    curr_level += 1;

                    let mut next_guard = self.apply_for(
                        curr_level,
                        lock_level,
                        attempt,
                        height,
                        next_node);

                    let next_guard_result
                        = unsafe { next_guard.deref_unsafe() };

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