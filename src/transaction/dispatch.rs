use std::hash::Hash;
use std::mem;
use std::fmt::Display;
use crate::index::bplus_tree::BPlusTree;
use crate::page_model::node::Node;
use crate::record_model::unsafe_clone::UnsafeClone;
use crate::tx_model::transaction::Transaction;
use crate::tx_model::transaction_result::TransactionResult;
use crate::utils::smart_cell::WRITE_OBSOLETE_FLAG_VERSION;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display,
    Payload: Default + Clone + Sync + Display
> BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub fn execute(&self, transaction: Transaction<Key, Payload>) -> TransactionResult<Key, Payload> {
        match transaction {
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
            Transaction::Range(key_interval) if self.locking_strategy.is_olc() => self.range_query_olc(
                &mut self.traversal_read_range_OLC(key_interval.lower()),
                key_interval,
            ),
            Transaction::Range(interval) => self
                .traversal_read_range_deterministic(
                    &interval,
                    self.lock_reader(&self.root.get().block()))
                .into_iter()
                .flat_map(|leafs| leafs
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