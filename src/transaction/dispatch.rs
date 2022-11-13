use std::ptr;
use chronicle_db::backbone::core::event::EventVariant::Empty;
use mvcc_bplustree::index::record::Record;
use mvcc_bplustree::index::version_info::Version;
use mvcc_bplustree::locking::locking_strategy::ATTEMPT_START;
use mvcc_bplustree::transaction::transaction::Transaction;
use mvcc_bplustree::transaction::transaction_result::TransactionResult;
use crate::block::aligned_page::IndexPage;
use crate::bplus_tree::Index;
use crate::index::record_like::RecordLike;
use crate::index::node::Node;
use crate::log_debug;
use crate::utils::unsafe_clone::UnsafeClone;
use crate::utils::hybrid_cell::sched_yield;

impl Index {
    pub fn execute(&self, transaction: Transaction) -> TransactionResult {
        match transaction {
            Transaction::Delete(key, version) => {
                let guard
                    = self.traversal_write(key);

                debug_assert!(guard.guard_result().is_mut());

                guard.guard_result()
                    .assume_mut()
                    .unwrap()
                    .delete_key(key, version)
                    .then(|| TransactionResult::Deleted(key, version))
                    .unwrap_or_default()
            }
            Transaction::Insert(event) if self.block_manager.is_multi_version => {
                let key
                    = event.t1();

                let guard
                    = self.traversal_write(key);

                let version
                    = self.next_version();

                debug_assert!(guard.guard_result().is_mut(), "{}", self.locking_strategy);

                guard.guard_result()
                    .assume_mut()
                    .unwrap()
                    .push_record((event, version).into())
                    .then(|| TransactionResult::Inserted(key, version))
                    .unwrap_or_default()
            }
            Transaction::Insert(event) => {
                let key
                    = event.key();

                let guard
                    = self.traversal_write(key);

                debug_assert!(guard.guard_result().is_mut(), "{}", self.locking_strategy);

                guard.guard_result()
                    .assume_mut()
                    .unwrap()
                    .push_event(event)
                    .then(|| TransactionResult::Inserted(key, Version::MIN))
                    .unwrap_or_default()
            }
            Transaction::Update(event) if self.block_manager.is_multi_version => {
                let key
                    = event.key();

                let guard
                    = self.traversal_write(key);

                let version
                    = self.next_version();

                debug_assert!(guard.guard_result().is_mut());

                guard.guard_result()
                    .assume_mut()
                    .unwrap()
                    .update_record((event, version).into())
                    .then(|| TransactionResult::Updated(key, version))
                    .unwrap_or_default()
            }
            Transaction::Update(event) => {
                let key
                    = event.key();

                let guard
                    = self.traversal_write(key);

                debug_assert!(guard.guard_result().is_mut());

                guard.guard_result()
                    .assume_mut()
                    .unwrap()
                    .update_event(event)
                    .then(|| TransactionResult::Updated(key, Version::MIN))
                    .unwrap_or_default()
            }
            Transaction::ExactSearch(key, version) if self.locking_strategy.is_olc() => unsafe {
                let guard
                    = self.traversal_read(key);

                let guard_result
                    = guard.guard_result_reader();

                let reader
                    = guard_result.as_reader().unwrap();

                let mut attempts
                    = ATTEMPT_START;

                loop {
                    let reader_cell_version
                        = guard.read_cell_version_as_reader();

                    let maybe_record = match reader.as_ref() {
                        Node::Leaf(events) => events
                            .iter()
                            .skip_while(|event| event.t1() != key)
                            .next()
                            .map(|event| event.unsafe_clone())
                            .map(|event| Record::new(event.key(), event.payload, Version::MIN)),
                        Node::MultiVersionLeaf(record_list) => record_list
                            .iter()
                            .find(|record_list| record_list.key() == key)
                            .map(|version_list| version_list.record_for_version(version))
                            .unwrap_or_default(),
                        _ => None
                    };

                    if guard.match_cell_version(reader_cell_version) {
                        break maybe_record.into();
                    } else {
                        maybe_record.map(|mut inner|
                            ptr::write(&mut inner.event.payload, Empty));

                        attempts += 1;
                        sched_yield(attempts)
                    }
                }
            }
            Transaction::ExactSearch(key, version) => {
                let guard
                    = self.traversal_read(key);

                let guard_result
                    = guard.guard_result();

                let reader
                    = guard_result.as_ref().unwrap();

                match reader.as_ref() {
                    Node::Leaf(records) => records
                        .iter()
                        .skip_while(|event| event.key() != key)
                        .next()
                        .map(|event| Record::new(event.key(), event.payload.clone(), Version::MIN))
                        .into(),
                    Node::MultiVersionLeaf(record_list) => record_list
                        .iter()
                        .find(|record_list| record_list.key() == key)
                        .map(|version_list| version_list.record_for_version(version).into())
                        .unwrap_or(None.into()),
                    _ => TransactionResult::Error
                }
            }
            Transaction::ExactSearchLatest(key) if self.locking_strategy.is_olc() => unsafe {
                let guard
                    = self.traversal_read(key);

                let guard_result
                    = guard.guard_result_reader();

                let reader
                    = guard_result.as_reader().unwrap();

                let mut attempts
                    = ATTEMPT_START;

                loop {
                    let reader_cell_version
                        = guard.read_cell_version_as_reader();

                    let maybe_record = match reader.as_ref() {
                        Node::Leaf(records) => records
                            .iter()
                            .rev()
                            .skip_while(|event| event.key() != key)
                            .next()
                            .map(|event| event.unsafe_clone())
                            .map(|event| Record::new(event.key(), event.payload, Version::MIN)),
                        Node::MultiVersionLeaf(record_list) => record_list
                            .iter()
                            .find(|record_list| record_list.key() == key)
                            .map(|version_list| version_list.youngest_record())
                            .unwrap_or_default(),
                        _ => None
                    };

                    if guard.match_cell_version(reader_cell_version) {
                        break maybe_record.into();
                    } else {
                        maybe_record.map(|mut inner|
                            ptr::write(&mut inner.event.payload, Empty));

                        attempts += 1;
                        sched_yield(attempts)
                    }
                }
            }
            Transaction::ExactSearchLatest(key) => {
                let guard
                    = self.traversal_read(key);

                let guard_result
                    = guard.guard_result();

                let reader
                    = guard_result.as_ref().unwrap();

                match reader.as_ref() {
                    Node::Leaf(records) => records
                        .iter()
                        .rev()
                        .skip_while(|event| event.key() != key)
                        .next()
                        .map(|event| Record::new(event.key(), event.payload.clone(), Version::MIN))
                        .into(),
                    Node::MultiVersionLeaf(record_list) => record_list
                        .iter()
                        .skip_while(|record_list| record_list.key() != key)
                        .filter(|version_list| !version_list.is_deleted())
                        .next()
                        .map(|version_list| version_list.youngest_record().into())
                        .unwrap_or(None.into()),
                    _ => TransactionResult::Error
                }
            }
            Transaction::RangeSearch(key_interval, version) if self.locking_strategy.is_olc() => {
                unimplemented!("RangeSearch in olc not implemented yet!")
            }
            Transaction::RangeSearch(key_interval, version) => {
                let (lower, upper)
                    = (key_interval.lower(), key_interval.upper());

                let root
                    = self.root.clone();

                let current_root
                    = root.block();

                let current_guard
                    = self.lock_reader(&current_root);

                let mut lock_level
                    = vec![(current_root, current_guard)];

                loop {
                    match lock_level.first().map(|(_n, guard)| guard.guard_result().as_ref().unwrap().is_directory()).unwrap_or(false) {
                        true => lock_level = lock_level
                            .drain(..)
                            .flat_map(|(_, guard)| match guard.guard_result().as_ref().unwrap().as_ref() {
                                Node::Index(
                                    IndexPage {
                                        keys,
                                        children,
                                        ..
                                    }) => keys
                                    .iter()
                                    .enumerate()
                                    .skip_while(|(_, k)| !lower.lt(k))
                                    .take_while(|(pos, k)| upper.ge(k) || *pos == 0)
                                    .map(|(pos, _)| {
                                        let child
                                            = children.get(pos).unwrap().clone();

                                        let child_guard
                                            = self.lock_reader(&child);

                                        (child, child_guard)
                                    }).collect::<Vec<_>>(),
                                _ => unreachable!("Sleepy joe hit me -> dude hang on, wtf just happened?!"),
                            }).collect(),
                        false => break TransactionResult::MatchedRecords(lock_level
                            .drain(..)
                            .flat_map(|(_n, guard)| match guard.guard_result().as_ref().unwrap().as_ref() {
                                Node::Leaf(records) => records
                                    .iter()
                                    .filter(|event| key_interval.contains(event.key()))
                                    .map(|event| Record::new(event.key(), event.payload.clone(), Version::MIN))
                                    .collect(),
                                Node::MultiVersionLeaf(record_list) => record_list
                                    .iter()
                                    .filter(|record_list| key_interval.contains(record_list.key()))
                                    .map(|record_list| record_list.record_for_version(version))
                                    .filter(|record| record.is_some())
                                    .map(|record| record.unwrap())
                                    .collect(),
                                _ => vec![]
                            }).collect())
                    }
                }
            }
            Transaction::DEBUGExactSearch(key, version) => {
                log_debug(format!("Transaction::DEBUGExactSearch(key: {}, version: {})",
                                  key, version));
                self.execute(Transaction::ExactSearch(key, version))
            }
            Transaction::DEBUGExactSearchLatest(key) => {
                log_debug(format!("Transaction::DEBUGExactSearchLatest(key: {})", key));
                self.execute(Transaction::ExactSearchLatest(key))
            }
            Transaction::Empty => TransactionResult::Error
        }
    }
}