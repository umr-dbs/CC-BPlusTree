use std::hash::Hash;
use std::mem;
use std::fmt::Display;
use std::sync::atomic::Ordering::SeqCst;
use crate::index::bplus_tree::BPlusTree;
use crate::page_model::node::Node;
use crate::record_model::unsafe_clone::UnsafeClone;
use crate::tx_model::transaction::Transaction;
use crate::tx_model::transaction_result::TransactionResult;
use crate::utils::interval::Interval;
use crate::utils::smart_cell::{__FENCE, WRITE_OBSOLETE_FLAG_VERSION};

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display,
    Payload: Default + Clone + Sync + Display
> BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub fn execute(&self, transaction: Transaction<Key, Payload>) -> TransactionResult<Key, Payload> {
        let olc = self.locking_strategy.is_olc();
        match transaction {
            Transaction::Delete(key) if olc => {
                let guard
                    = self.traversal_write_olc(key);

                guard.deref_mut()
                    .unwrap()
                    .delete_key(key)
                    .map(|payload| TransactionResult::Deleted(key, payload))
                    .unwrap_or_default()
            }
            Transaction::Delete(key) => {
                let guard
                    = self.traversal_write(key);

                guard.deref_mut()
                    .unwrap()
                    .delete_key(key)
                    .map(|payload| TransactionResult::Deleted(key, payload))
                    .unwrap_or_default()
            }
            Transaction::Insert(key, payload) if olc => {
                let guard
                    = self.traversal_write_olc(key);

                guard.deref_mut()
                    .unwrap()
                    .push_record_point(key, payload)
                    .then(|| TransactionResult::Inserted(key))
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
            Transaction::Update(key, payload) if olc => {
                let guard
                    = self.traversal_write_olc(key);

                guard.deref_mut()
                    .unwrap()
                    .update_record_point(key, payload)
                    .map(|old| TransactionResult::Updated(key, old))
                    .unwrap_or_default()
            }
            Transaction::Update(key, payload) => {
                let guard
                    = self.traversal_write_olc(key);

                guard.deref_mut()
                    .unwrap()
                    .update_record_point(key, payload)
                    .map(|old| TransactionResult::Updated(key, old))
                    .unwrap_or_default()
            }
            Transaction::Point(key) if olc => match self.execute(Transaction::Range((key..=key).into())) {
                TransactionResult::MatchedRecords(mut records) if records.len() <= 1 =>
                    TransactionResult::MatchedRecord(records.pop()),
                _ => TransactionResult::Error
            },
            // Transaction::Point(key) if olc => unsafe {
            //     let guard
            //         = self.traversal_read_olc(key);
            //
            //     __FENCE(SeqCst);
            //     let reader = guard
            //         .deref();
            //
            //     if reader.is_none() {
            //         return self.execute(Transaction::Point(key));
            //     }
            //
            //     let reader
            //         = reader.unwrap();
            //
            //     loop {
            //         __FENCE(SeqCst);
            //         let (read, reader_cell_version)
            //             = guard.is_read_not_obsolete_result();
            //
            //         if !read {
            //             mem::drop(guard);
            //
            //             return self.execute(Transaction::Point(key));
            //         }
            //
            //         __FENCE(SeqCst);
            //         let maybe_record = match reader.as_ref() {
            //             Node::Leaf(records) => records
            //                 .as_records()
            //                 .binary_search_by_key(&key, |record_point| record_point.key)
            //                 .ok()
            //                 .map(|pos| records
            //                     .as_records()
            //                     .get_unchecked(pos)
            //                     .unsafe_clone()),
            //             _ => None
            //         };
            //
            //         __FENCE(SeqCst);
            //         let (read, n_reader_cell_version)
            //             = guard.is_read_not_obsolete_result();
            //
            //         if !read || n_reader_cell_version != reader_cell_version {
            //             mem::drop(guard);
            //             break maybe_record.into();
            //         }
            //         else {
            //             mem::drop(guard);
            //             mem::forget(maybe_record);
            //
            //             return self.execute(Transaction::Point(key));
            //         }
            //     }
            // }
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
            Transaction::Range(key_interval) if olc => {
                let mut path = vec![
                    (Interval::new(self.min_key, self.max_key),
                     self.lock_reader(&self.root.block))
                ];

                self.next_leaf_page(path.as_mut(),
                                    0,
                                    key_interval.lower);

                self.range_query_olc(path.as_mut(), key_interval)
            },
            Transaction::Range(interval) => self.traversal_read_range(&interval)
                .into_iter()
                .flat_map(|(_block, guard)| guard
                    .deref()
                    .unwrap()
                    .as_ref()
                    .as_records()
                    .iter()
                    .skip_while(|record| !interval.contains(record.key))
                    .take_while(|record| interval.contains(record.key))
                    .cloned()
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>()
                .into(),
            Transaction::Empty => TransactionResult::Error,
        }
    }
}