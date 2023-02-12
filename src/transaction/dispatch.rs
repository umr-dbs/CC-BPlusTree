use std::borrow::BorrowMut;
use std::hash::Hash;
use std::{mem, ptr};
use std::collections::VecDeque;
use std::fmt::Display;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::null;
use itertools::Itertools;
use TXDataModel::page_model::Attempts;
use TXDataModel::page_model::block::{Block, BlockGuard};
use TXDataModel::page_model::node::Node;
// use TXDataModel::record_model::record::Record;
// use TXDataModel::record_model::record_like::RecordLike;
use TXDataModel::record_model::record_point::RecordPoint;
use TXDataModel::record_model::unsafe_clone::UnsafeClone;
use TXDataModel::record_model::Version;
use TXDataModel::tx_model::transaction::Transaction;
use TXDataModel::tx_model::transaction_result::TransactionResult;
use TXDataModel::utils::hybrid_cell::{OBSOLETE_FLAG_VERSION, sched_yield, WRITE_FLAG_VERSION, WRITE_OBSOLETE_FLAG_VERSION};
use TXDataModel::utils::interval;
use TXDataModel::utils::interval::Interval;
use TXDataModel::utils::smart_cell::SmartGuard;
use crate::index::bplus_tree::BPlusTree;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display,
    Payload: Default + Clone + Sync + Display
> BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub fn execute(&self, transaction: Transaction<Key, Payload>) -> TransactionResult<Key, Payload> {
        match transaction {
            Transaction::Range(key_interval)
            if self.locking_strategy.is_olc() => self.range_query_olc(
                &mut self.traversal_read_range_OLC(key_interval.lower()),
                key_interval,
            ),
            Transaction::Delete(key) => {
                let guard
                    = self.traversal_write(key);

                guard.deref_mut()
                    .unwrap()
                    .delete_key(key)
                    .map(|payload| TransactionResult::Deleted(key, payload))
                    .unwrap_or_default()
            }
            Transaction::Insert(key, payload) => {
                let guard
                    = self.traversal_write(key);

                guard.deref_mut()
                    .unwrap()
                    .push_record_point(key, payload)
                    .then(|| TransactionResult::Inserted(key))
                    .unwrap_or_default()
            }
            Transaction::Update(key, payload) => {
                let guard
                    = self.traversal_write(key);

                guard.deref_mut()
                    .unwrap()
                    .update_record_point(key, payload)
                    .map(|old| TransactionResult::Updated(key, old))
                    .unwrap_or_default()
            }
            Transaction::Point(key) if self.locking_strategy.is_olc() => unsafe {
                let guard
                    = self.traversal_read(key);

                let reader = guard
                    .deref();

                if reader.is_none() {
                    return self.execute(Transaction::Point(key));
                }

                let reader
                    = reader.unwrap();

                loop {
                    let reader_cell_version
                        = guard.cell_version_olc();

                    if reader_cell_version & WRITE_OBSOLETE_FLAG_VERSION != 0 {
                        mem::drop(guard);

                        return self.execute(Transaction::Point(key));
                    }

                    let maybe_record = match reader.as_ref() {
                        Node::Leaf(records) => records
                            .as_records()
                            .binary_search_by_key(&key, |record_point| record_point.key)
                            .ok()
                            .map(|pos| records
                                .as_records()
                                .get_unchecked(pos)
                                .unsafe_clone()),
                        _ => None
                    };

                    if guard.cell_version_olc() == reader_cell_version {
                        mem::drop(guard);
                        break maybe_record.into();
                    } else {
                        mem::drop(guard);
                        mem::forget(maybe_record);

                        return self.execute(Transaction::Point(key));
                    }
                }
            }
            Transaction::Point(key) => unsafe {
                let guard
                    = self.traversal_read(key);

                let reader = guard
                    .deref()
                    .unwrap();

                match reader.as_ref() {
                    Node::Leaf(leaf_page) => leaf_page
                        .as_records()
                        .binary_search_by_key(&key, |record| record.key)
                        .ok()
                        .map(|pos| leaf_page.as_records().get_unchecked(pos).clone())
                        .into(),
                    _ => TransactionResult::Error
                }
            }
            Transaction::Range(interval) => {
                unimplemented!("RangeSearch in olc not implemented yet!")
            }
            Transaction::Empty => TransactionResult::Error,
            _ => unimplemented!("Not impl yet!"),
        }
    }

    fn next_leaf_page(&self,
                      path: &mut Vec<(Interval<Key>, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)>,
                      mut parent_index: usize,
                      next_key: Key)
    {
        unsafe {
            let k: u64 = *(&next_key as *const Key as *const u64);
            if k == 22 {
                let mahala = 1231231;
            }
        }
        loop {
            if parent_index >= path.len() { // when all path is invalid, we run stacking path function again!
                *path = unsafe {
                    mem::transmute(
                        self.traversal_read_range_OLC(next_key))
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

            match unsafe { curr_parent.deref_unsafe() }.unwrap().as_ref() {
                Node::Index(index_page) => {
                    let keys = index_page.keys();
                    let children = index_page.children();
                    let (index_of_child, next_node) = keys
                        .iter()
                        .enumerate()
                        .find(|(_, k)| next_key.lt(k))
                        .map(|(pos, _)| (pos, children.get(pos).cloned()))
                        .unwrap_or_else(|| (keys.len(), children.last().cloned()));

                    if next_node.is_none() || !curr_parent.is_valid() {
                        parent_index = parent_index - 1;
                        continue;
                    }

                    let next_page
                        = next_node.unwrap();

                    parent_index = parent_index + 1;

                    curr_interval = Interval::new(
                        keys.get(index_of_child - 1)
                            .cloned()
                            .unwrap_or(curr_interval.lower()),
                        keys.get(index_of_child)
                            .cloned()
                            .map(|max| (self.dec_key)(max))
                            .unwrap_or(curr_interval.upper()));

                    *path.get_mut(parent_index).unwrap() = unsafe {
                        (curr_interval, mem::transmute(self.lock_reader(&next_page)))
                    };
                }
                Node::Leaf(..) => {
                    // let records
                    //     = leaf_page.as_records();

                    // curr_interval = Interval::new(
                    //     records.first().unwrap().key,
                    //     records.last().unwrap().key);

                    path.truncate(parent_index + 1);
                    // let (last_interval, _)
                    //     = path.last_mut().unwrap();
                    //
                    // *last_interval = curr_interval;
                    return;
                }
            }
        }
    }

    fn range_query_leaf_results(&self,
                                path: &mut Vec<(Interval<Key>, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)>,
                                key_interval: &Interval<Key>)
                                -> Vec<RecordPoint<Key, Payload>>
    {
        let (interval, leaf)
            = path.last().unwrap();

        let leaf_unchecked = unsafe { leaf.deref_unsafe() }.unwrap().as_ref();

        match leaf_unchecked {
            Node::Leaf(leaf_page) if interval.overlap(&key_interval) => unsafe {
                println!("Records in Leaf = {}", leaf_page
                    .as_records_mut().iter().join(","));

                let mut potential_results = leaf_page
                    .as_records()
                    .iter()
                    .skip_while(|record| record.key.lt(&key_interval.lower()))
                    .take_while(|record| record.key.le(&key_interval.upper()))
                    .map(|record| record.unsafe_clone())
                    .collect::<Vec<_>>();

                println!("Filtered Records = {}", potential_results.iter().join(","));
                if leaf.is_valid() {
                    potential_results
                } else {
                    potential_results.set_len(0);
                    let parent_index = path.len() - 2;
                    self.next_leaf_page(path, parent_index, key_interval.lower());
                    self.range_query_leaf_results(path, key_interval)
                }
            }
            Node::Leaf(..) => Vec::with_capacity(0),
            _ => {
                println!("Found Index but expected leaf = {}", leaf_unchecked);
                unreachable!()
            }
        }
    }

    #[inline(always)]
    fn range_query_olc(&self,
                       path: &mut Vec<(Interval<Key>, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)>,
                       org_key_interval: Interval<Key>) -> TransactionResult<Key, Payload>
    {
        let mut key_interval = org_key_interval.clone();
        let mut all_results = vec![];
        let mut history_path = VecDeque::new();
        loop {
            history_path.push_back(path.clone());

            let local_results =
                self.range_query_leaf_results(path, &key_interval);

            if !local_results.is_empty() {
                let highest_key = local_results.last().unwrap().key;
                key_interval.set_lower((self.inc_key)(highest_key));

                all_results.extend(local_results);

                if history_path.len() >= 2 {
                    let trace
                        = history_path.pop_front().unwrap();

                    let (_, l_leaf)
                        = trace.last().unwrap();

                    let ll_parent = match trace.get(trace.len() - 2) {
                        Some((_, l)) => Some(l),
                        _ => None
                    };

                    if !l_leaf.is_valid() {
                        return self.execute(Transaction::Range(org_key_interval));
                    }

                    if ll_parent.is_some() && !ll_parent.unwrap().is_valid() {
                        return self.execute(Transaction::Range(org_key_interval));
                    }
                }

                self.next_leaf_page(path,
                                    path.len() - 2,
                                    key_interval.lower());
            } else {
                key_interval.set_lower((self.inc_key)(key_interval.lower()));
                self.next_leaf_page(path,
                                    0,
                                    key_interval.lower());
            }

            if key_interval.lower().gt(&key_interval.upper()) {
                break;
            }
        }

        TransactionResult::MatchedRecords(all_results)
    }
}