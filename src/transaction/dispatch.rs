use std::ops::Deref;
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
                    Node::Leaf(records) => records
                        .iter()
                        .find(|record| record.key() == key)
                        .filter(|record| record.match_version(version))
                        .cloned()
                        .into(),
                    Node::MultiVersionLeaf(record_list) => record_list
                        .iter()
                        .find(|record_list| record_list.key() == key)
                        .map(|version_list| version_list.record_for_version(version).into())
                        .unwrap_or(None.into()),
                    _ => TransactionResult::Error
                }
            }
            Transaction::ExactSearchLatest(key) => {
                let (_node_ref, guard)
                    = self.traversal_read(key);

                match guard.deref() {
                    Node::Leaf(records) => records
                        .iter()
                        .rev()
                        .skip_while(|record| record.key() != key)
                        .filter(|record| !record.is_deleted())
                        .next()
                        .cloned()
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
            Transaction::RangeSearch(key_interval, version) => {
                let (lower, upper)
                    = (key_interval.lower(), key_interval.upper());

                let current_guard
                    = self.lock_reader(self.root.deref());

                let current_root
                    = self.root.clone();

                let mut lock_level
                    = vec![(current_root, current_guard)];

                loop {
                    match lock_level.first().map(|(_n, guard)| guard.is_directory()).unwrap_or(false) {
                        true => lock_level = lock_level
                            .drain(..)
                            .flat_map(|(_, guard)| match guard.deref() {
                                Node::Index(keys, children) => keys
                                    .iter()
                                    .enumerate()
                                    .skip_while(|(_, k)| !lower.lt(k))
                                    .take_while(|(pos, k)| upper.ge(k) || *pos == 0)
                                    .map(|(pos, _)| {
                                        let child
                                            = children.get(pos).unwrap().clone();

                                        let child_guard
                                            = self.lock_reader(child.deref());

                                        (child, child_guard)
                                    }).collect::<Vec<_>>(),
                                _ => unreachable!("Sleepy joe hit me -> dude hang on, wtf just happened?!"),
                            }).collect(),
                        false => break TransactionResult::MatchedRecords(lock_level
                            .drain(..)
                            .flat_map(|(_n, guard)| match guard.deref() {
                                Node::Leaf(records) => records
                                    .iter()
                                    .filter(|record| key_interval.contains(record.key()) &&
                                        record.match_version(version))
                                    .cloned()
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
            _ => unimplemented!("bro hang on, im working on it..")
        }
    }
}