use std::borrow::BorrowMut;
use std::hash::Hash;
use std::{mem, ptr};
use std::ptr::null;
use TXDataModel::page_model::Attempts;
use TXDataModel::page_model::block::Block;
use TXDataModel::page_model::node::Node;
// use TXDataModel::record_model::record::Record;
// use TXDataModel::record_model::record_like::RecordLike;
use TXDataModel::record_model::record_point::RecordPoint;
use TXDataModel::record_model::unsafe_clone::UnsafeClone;
use TXDataModel::record_model::Version;
use TXDataModel::tx_model::transaction::Transaction;
use TXDataModel::tx_model::transaction_result::TransactionResult;
use TXDataModel::utils::hybrid_cell::{OBSOLETE_FLAG_VERSION, sched_yield, WRITE_FLAG_VERSION, WRITE_OBSOLETE_FLAG_VERSION};
use TXDataModel::utils::smart_cell::SmartGuard;
use crate::index::bplus_tree::BPlusTree;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync
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
            // Transaction::Insert(key, payload) if self.block_manager.is_multi_version => {
            //     let guard
            //         = self.traversal_write(key);
            //
            //     let version
            //         = self.next_version();
            //
            //     guard.deref_mut()
            //         .unwrap()
            //         .push_record(key, payload, version)
            //         .then(|| TransactionResult::Inserted(key, Some(version)))
            //         .unwrap_or_default()
            // }
            Transaction::Insert(key, payload) => {
                let guard
                    = self.traversal_write(key);

                guard.deref_mut()
                    .unwrap()
                    .push_record_point(key, payload)
                    .then(|| TransactionResult::Inserted(key))
                    .unwrap_or_default()
            }
            // Transaction::Update(key, payload) if self.block_manager.is_multi_version => {
            //     let guard
            //         = self.traversal_write(key);
            //
            //     let version
            //         = self.next_version();
            //
            //     guard.deref_mut()
            //         .unwrap()
            //         .update_record(key, payload, version)
            //         .then(|| TransactionResult::Updated(key, Some(version)))
            //         .unwrap_or_default()
            // }
            Transaction::Update(key, payload) => {
                let guard
                    = self.traversal_write(key);

                guard.deref_mut()
                    .unwrap()
                    .update_record_point(key, payload)
                    .map(|old| TransactionResult::Updated(key, old))
                    .unwrap_or_default()
            }
            // Transaction::Point(key, version) if self.locking_strategy.is_olc() => unsafe {
            //     let guard
            //         = self.traversal_read(key);
            //
            //     let reader = guard
            //         .deref_unsafe()
            //         .unwrap();
            //
            //     let mut attempts: Attempts = 0;
            //
            //     loop {
            //         let reader_cell_version
            //             = guard.read_cell_version_as_reader();
            //
            //         let maybe_record = match reader.as_ref() {
            //             Node::Leaf(event_page) => event_page
            //                 .as_records()
            //                 .iter()
            //                 .skip_while(|event| event.key() != key)
            //                 .next()
            //                 .map(|event| event.unsafe_clone())
            //                 .map(|event| Record::new(event.key(), event.payload, Version::MIN)),
            //             // Node::MultiVersionLeaf(leaf_page) => leaf_page
            //             //     .as_records()
            //             //     .iter()
            //             //     .find(|record_list| record_list.key() == key)
            //             //     .map(|version_list| version_list
            //             //         .payload(version)
            //             //         .map(|found| Record::new(
            //             //             version_list.key,
            //             //             found.payload,
            //             //             found.version_info.insertion_version())))
            //             //     .unwrap_or_default(),
            //             _ => None
            //         };
            //
            //         if guard.match_cell_version(reader_cell_version) {
            //             break maybe_record.into();
            //         } else {
            //             maybe_record.map(|mut inner|
            //                 ptr::write(&mut inner.payload, Payload::default()));
            //
            //             attempts += 1;
            //             sched_yield(attempts)
            //         }
            //     }
            // }
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
                        // Node::MultiVersionLeaf(leaf_page) => leaf_page
                        //     .as_records()
                        //     .iter()
                        //     .find(|record_list| record_list.key() == key)
                        //     .map(|version_list| version_list.payload(None)
                        //         .map(|found| Record::new(
                        //             version_list.key,
                        //             found.payload,
                        //             found.version_info.insertion_version())))
                        //     .unwrap_or_default(),
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
                    // Node::MultiVersionLeaf(leaf_page) => leaf_page
                    //     .as_records()
                    //     .iter()
                    //     .find(|record_list| record_list.key() == key)
                    //     .map(|version_list| version_list
                    //         .payload(version)
                    //         .map(|found| Record::from(
                    //             version_list.key,
                    //             found.payload,
                    //             found.version_info.clone())))
                    //     .unwrap_or_default()
                    //     .into(),
                    _ => TransactionResult::Error
                }
            }
            Transaction::Range(interval) if self.locking_strategy.is_olc() => {
                unimplemented!("RangeSearch in olc not implemented yet!")
            }
            // Transaction::RangeSearch(key_interval, version) => {
            //     let (lower, upper)
            //         = (key_interval.lower(), key_interval.upper());
            //
            //     let root
            //         = self.root.clone();
            //
            //     let current_root
            //         = root.block();
            //
            //     let current_guard
            //         = self.lock_reader(&current_root);
            //
            //     let mut lock_level
            //         = vec![(current_root, current_guard)];
            //
            //     loop {
            //         match lock_level.first().map(|(_n, guard)| guard.guard_result().as_ref().unwrap().is_directory()).unwrap_or(false) {
            //             true => lock_level = lock_level
            //                 .drain(..)
            //                 .flat_map(|(_, guard)| match guard.guard_result().as_ref().unwrap().as_ref() {
            //                     Node::Index(
            //                         IndexPage {
            //                             keys,
            //                             children,
            //                             ..
            //                         }) => keys
            //                         .iter()
            //                         .enumerate()
            //                         .skip_while(|(_, k)| !lower.lt(k))
            //                         .take_while(|(pos, k)| upper.ge(k) || *pos == 0)
            //                         .map(|(pos, _)| {
            //                             let child
            //                                 = children.get(pos).unwrap().clone();
            //
            //                             let child_guard
            //                                 = self.lock_reader(&child);
            //
            //                             (child, child_guard)
            //                         }).collect::<Vec<_>>(),
            //                     _ => unreachable!("Sleepy joe hit me -> dude hang on, wtf just happened?!"),
            //                 }).collect(),
            //             false => break TransactionResult::MatchedRecords(lock_level
            //                 .drain(..)
            //                 .flat_map(|(_n, guard)| match guard.guard_result().as_ref().unwrap().as_ref() {
            //                     Node::Leaf(records) => records
            //                         .iter()
            //                         .filter(|event| key_interval.contains(event.key()))
            //                         .map(|event| Record::new(event.key(), event.payload.clone(), Version::MIN))
            //                         .collect(),
            //                     Node::MultiVersionLeaf(record_list) => record_list
            //                         .iter()
            //                         .filter(|record_list| key_interval.contains(record_list.key()))
            //                         .map(|record_list| record_list.record_for_version(version))
            //                         .filter(|record| record.is_some())
            //                         .map(|record| record.unwrap())
            //                         .collect(),
            //                     _ => vec![]
            //                 }).collect())
            //         }
            //     }
            // }
            Transaction::Empty => TransactionResult::Error,
            _ => unimplemented!("Not impl yet!"),
        }
    }
}