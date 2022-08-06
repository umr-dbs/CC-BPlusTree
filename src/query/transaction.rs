use std::ops::Deref;
use chronicle_db::backbone::core::event::Event;
use mvcc_bplustree::transaction::transaction::Transaction;
use mvcc_bplustree::transaction::transaction_result::TransactionResult;
use crate::bplus_tree::Index;
use crate::index::node::Node;

impl Index {
    pub fn execute(&self, transaction: Transaction) -> TransactionResult {
        match transaction {
            Transaction::Empty => TransactionResult::Error,
            Transaction::Insert(event) => {
                let key
                    = event.t1();

                let (_node, mut guard)
                    = self.traversal_write(key);

                let version
                    = self.next_version();

                guard.push_record((event, version).into(), false)
                    .then(|| TransactionResult::Inserted(key, version))
                    .unwrap_or(TransactionResult::Error)
            }
            Transaction::Update(event) => {
                let key
                    = event.t1();

                let (_node, mut guard)
                    = self.traversal_write(key);

                let version
                    = self.next_version();

                guard.push_record((event, version).into(), true)
                    .then(|| TransactionResult::Updated(key, version))
                    .unwrap_or(TransactionResult::Error)
            }
            Transaction::ExactSearch(key, version) => {
                let (_node_ref, guard)
                    = self.traversal_read(key);

                match guard.deref() {
                    Node::Leaf(records, _) => records
                        .iter()
                        .find(|record| record.key() == key && record.match_version(version))
                        .map(|found| TransactionResult::MatchedRecord(Some(found.clone())))
                        .unwrap_or(TransactionResult::MatchedRecord(None)),
                    Node::MultiVersionLeaf(record_list, _) => record_list
                        .iter()
                        .find(|entry| entry.key() == key)
                        .map(|version_list| version_list.payload_for_version(version))
                        .unwrap_or_default()
                        .map(|found|
                            (Event::new_from_t1(key, found.payload().clone()),
                             found.version_info().clone()).into())
                        .map(|record| TransactionResult::MatchedRecord(Some(record)))
                        .unwrap_or(TransactionResult::MatchedRecord(None)),
                    _ => TransactionResult::Error
                }
            }
            Transaction::ExactSearchLatest(key) => {
                let (_node_ref, guard)
                    = self.traversal_read(key);

                match guard.deref() {
                    Node::Leaf(records, _) => records
                        .iter()
                        .rev()
                        .find(|record| record.key() == key)
                        .map(|found| TransactionResult::MatchedRecord(Some(found.clone())))
                        .unwrap_or(TransactionResult::MatchedRecord(None)),
                    Node::MultiVersionLeaf(record_list, _) => record_list
                        .iter()
                        .find(|entry| entry.key() == key)
                        .map(|version_list| version_list.payload_front())
                        .unwrap_or_default()
                        .map(|found|
                            (Event::new_from_t1(key, found.payload().clone()),
                             found.version_info().clone()).into())
                        .map(|record| TransactionResult::MatchedRecord(Some(record)))
                        .unwrap_or(TransactionResult::MatchedRecord(None)),
                    _ => TransactionResult::Error
                }
            }
            _ => unimplemented!("bro hang on, im working on it..")
        }
    }
}