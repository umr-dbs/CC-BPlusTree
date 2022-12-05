use std::borrow::BorrowMut;
use std::hash::Hash;
use std::{mem, ptr};
use TXDataModel::page_model::Attempts;
use TXDataModel::page_model::node::Node;
use TXDataModel::record_model::record::Record;
use TXDataModel::record_model::record_like::RecordLike;
use TXDataModel::record_model::record_point::RecordPoint;
use TXDataModel::record_model::unsafe_clone::UnsafeClone;
use TXDataModel::record_model::Version;
use TXDataModel::tx_model::transaction::Transaction;
use TXDataModel::tx_model::transaction_result::TransactionResult;
use TXDataModel::utils::hybrid_cell::sched_yield;
use crate::index::bplus_tree::BPlusTree;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync
> BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub fn execute(&self, transaction: Transaction<Key, Payload>) -> TransactionResult<Key, Payload> {
        match transaction {
            Transaction::Delete(key, version) => {
                let guard
                    = self.traversal_write(key);

                guard.deref_mut()
                    .unwrap()
                    .delete_key(key, version)
                    .then(|| TransactionResult::Deleted(key, version))
                    .unwrap_or_default()
            }
            Transaction::Insert(key, payload) if self.block_manager.is_multi_version => {
                let guard
                    = self.traversal_write(key);

                let version
                    = self.next_version();

                guard.deref_mut()
                    .unwrap()
                    .push_record(key, payload, version)
                    .then(|| TransactionResult::Inserted(key, Some(version)))
                    .unwrap_or_default()
            }
            Transaction::Insert(key, payload) => {
                let guard
                    = self.traversal_write(key);

                guard.deref_mut()
                    .unwrap()
                    .push_record_point(key, payload)
                    .then(|| TransactionResult::Inserted(key, None))
                    .unwrap_or_default()
            }
            Transaction::Update(key, payload) if self.block_manager.is_multi_version => {
                let guard
                    = self.traversal_write(key);

                let version
                    = self.next_version();

                guard.deref_mut()
                    .unwrap()
                    .update_record(key, payload, version)
                    .then(|| TransactionResult::Updated(key, Some(version)))
                    .unwrap_or_default()
            }
            Transaction::Update(key, payload) => {
                let guard
                    = self.traversal_write(key);

                guard.deref_mut()
                    .unwrap()
                    .update_record_point(key, payload)
                    .then(|| TransactionResult::Updated(key, None))
                    .unwrap_or_default()
            }
            Transaction::Point(key, version) if self.locking_strategy.is_olc() => unsafe {
                let guard
                    = self.traversal_read(key);

                let reader = guard
                    .deref_unsafe()
                    .unwrap();

                let mut attempts: Attempts = 0;

                loop {
                    let reader_cell_version
                        = guard.read_cell_version_as_reader();

                    let maybe_record = match reader.as_ref() {
                        Node::Leaf(event_page) => event_page
                            .as_records()
                            .iter()
                            .skip_while(|event| event.key() != key)
                            .next()
                            .map(|event| event.unsafe_clone())
                            .map(|event| Record::new(event.key(), event.payload, Version::MIN)),
                        Node::MultiVersionLeaf(leaf_page) => leaf_page
                            .as_records()
                            .iter()
                            .find(|record_list| record_list.key() == key)
                            .map(|version_list| version_list
                                .payload(version)
                                .map(|found| Record::new(
                                    version_list.key,
                                    found.payload,
                                    found.version_info.insertion_version())))
                            .unwrap_or_default(),
                        _ => None
                    };

                    if guard.match_cell_version(reader_cell_version) {
                        break maybe_record.into();
                    } else {
                        maybe_record.map(|mut inner|
                            ptr::write(&mut inner.payload, Payload::default()));

                        attempts += 1;
                        sched_yield(attempts)
                    }
                }
            }
            Transaction::Point(key, None) if self.locking_strategy.is_olc() => unsafe {
                let guard
                    = self.traversal_read(key);

                let reader = guard
                    .deref_unsafe()
                    .unwrap();

                let mut attempts: Attempts = 0;

                loop {
                    let reader_cell_version
                        = guard.read_cell_version_as_reader();

                    let maybe_record = match reader.as_ref() {
                        Node::Leaf(records) => records
                            .as_records()
                            .iter()
                            .rev()
                            .skip_while(|event| event.key() != key)
                            .next()
                            .map(|event| event.unsafe_clone())
                            .map(|event| Record::new(event.key(), event.payload, Version::MIN)),
                        Node::MultiVersionLeaf(leaf_page) => leaf_page
                            .as_records()
                            .iter()
                            .find(|record_list| record_list.key() == key)
                            .map(|version_list| version_list.payload(None)
                                .map(|found| Record::new(
                                    version_list.key,
                                    found.payload,
                                    found.version_info.insertion_version())))
                            .unwrap_or_default(),
                        _ => None
                    };

                    if guard.match_cell_version(reader_cell_version) {
                        break maybe_record.into();
                    } else {
                        maybe_record.map(|mut inner|
                            ptr::write(&mut inner.payload, Payload::default()));

                        attempts += 1;
                        sched_yield(attempts)
                    }
                }
            }
            Transaction::Point(key, version) => {
                let guard
                    = self.traversal_read(key);

                let reader = guard
                    .deref()
                    .unwrap();

                match reader.as_ref() {
                    Node::Leaf(leaf_page) => leaf_page
                        .as_records()
                        .iter()
                        .skip_while(|event| event.key() != key)
                        .next()
                        .map(|record_point| RecordPoint::new(
                            record_point.key(),
                            record_point.payload.clone()))
                        .into(),
                    Node::MultiVersionLeaf(leaf_page) => leaf_page
                        .as_records()
                        .iter()
                        .find(|record_list| record_list.key() == key)
                        .map(|version_list| version_list
                            .payload(version)
                            .map(|found| Record::from(
                                version_list.key,
                                found.payload,
                                found.version_info.clone())))
                        .unwrap_or_default()
                        .into(),
                    _ => TransactionResult::Error
                }
            }
            Transaction::Range(interval, version) if self.locking_strategy.is_olc() => {
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