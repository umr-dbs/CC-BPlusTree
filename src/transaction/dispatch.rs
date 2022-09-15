use mvcc_bplustree::transaction::transaction::Transaction;
use mvcc_bplustree::transaction::transaction_result::TransactionResult;
use crate::block::aligned_page::IndexPage;
use crate::bplus_tree::Index;
use crate::index::node::Node;

impl Index {
    // pub fn execute_range_query_iter(&self, transaction: Transaction) -> Option<ResultIter> {
    //     let index: &'static Index = unsafe {
    //         mem::transmute(self)
    //     };
    //
    //     let (key_interval, version) = match &transaction {
    //         Transaction::RangeSearch(key_interval, version) => (key_interval.clone(), *version),
    //         _ => return None
    //     };
    //
    //     let father_ref
    //         = index.root.clone();
    //
    //     let father_guard: NodeGuard
    //         = index.lock_reader(father_ref.deref());
    //
    //     let filter = RangeFilter {
    //         key_interval,
    //         version,
    //     };
    //
    //     let fan_out = match father_guard.deref().unwrap() {
    //         Node::Index(keys, children) => {
    //             let fan_out = keys
    //                 .iter()
    //                 .enumerate()
    //                 .skip_while(|(_, k)| !filter.key_interval.lower().lt(k))
    //                 .take_while(|(pos, k)| filter.key_interval.upper().ge(k) || *pos == 0)
    //                 .map(|(pos, _)| children.get(pos).unwrap().clone())
    //                 .collect::<VecDeque<_>>();
    //
    //             Fanout::Fan {
    //                 index,
    //                 father: (father_ref, father_guard),
    //                 filter,
    //                 fan_out,
    //             }
    //         }
    //         _ => Fanout::Filter {
    //             filter,
    //             leaf: (father_ref, father_guard),
    //         },
    //     };
    //
    //     Some(ResultIter(
    //         VecDeque::from(vec![fan_out]),
    //         VecDeque::from(vec![]))
    //     )
    // }

    pub fn execute(&self, transaction: Transaction) -> TransactionResult {
        match transaction {
            Transaction::Empty => TransactionResult::Error,
            Transaction::Insert(event) => {
                // println!("Inserting Event = {}", event);
                let key
                    = event.t1();

                let guard
                    = self.traversal_write(key);

                let version
                    = self.next_version();

                debug_assert!(guard.guard_result().is_mut(), "{}", self.locking_strategy);

                guard.guard_result().assume_mut().unwrap().push_record((event, version).into(), false)
                    .then(|| TransactionResult::Inserted(key, version))
                    .unwrap_or(TransactionResult::Error)
            }
            Transaction::Update(event) => {
                let key
                    = event.t1();

                let guard
                    = self.traversal_write(key);

                let version
                    = self.next_version();

                debug_assert!(guard.guard_result().is_mut());

                guard.guard_result().assume_mut().unwrap().push_record((event, version).into(), true)
                    .then(|| TransactionResult::Updated(key, version))
                    .unwrap_or(TransactionResult::Error)
            }
            Transaction::ExactSearch(key, version) => {
                let guard
                    = self.traversal_read(key);

                match guard.guard_result().as_ref().unwrap().as_ref() {
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
                let guard
                    = self.traversal_read(key);

                match guard.guard_result().as_ref().unwrap().as_ref() {
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
                    = self.lock_reader(&self.root.block());

                let current_root
                    = self.root.block();

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

// pub struct TransactionResultIter {
//     transaction: Transaction,
//     index: &'static Index,
// }
//
// pub struct ResultIter(VecDeque<Fanout>, VecDeque<Record>);
//
// impl Iterator for ResultIter {
//     type Item = Record;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if !self.1.is_empty() {
//             return self.1.pop_front();
//         }
//
//         let fan_out
//             = self.0.borrow_mut();
//
//         if fan_out.is_empty() {
//             return None;
//         }
//
//         let mut next_fan
//             = fan_out.pop_front().unwrap();
//
//         while !next_fan.is_results() {
//             let newer_fans = fan_out.split_off(0);
//             fan_out.extend(next_fan.flatten());
//             fan_out.extend(newer_fans);
//
//             next_fan = fan_out.pop_front().unwrap();
//         }
//
//         self.1.extend(next_fan.into_results());
//
//         self.next()
//     }
// }
//
// #[derive(Clone)]
// struct RangeFilter {
//     key_interval: KeyInterval,
//     version: Version,
// }
//
// enum Fanout {
//     Fan {
//         index: &'static Index,
//         filter: RangeFilter,
//         father: (NodeRef, NodeGuard<'static>),
//         fan_out: VecDeque<NodeRef>,
//     },
//     Filter {
//         filter: RangeFilter,
//         leaf: (NodeRef, NodeGuard<'static>),
//     },
//     Results(Vec<Record>),
// }
//
// impl Fanout {
//     const fn is_results(&self) -> bool {
//         match self {
//             Self::Results(..) => true,
//             _ => false
//         }
//     }
//
//     fn into_results(self) -> Vec<Record> {
//         match self {
//             Self::Results(records) => records,
//             _ => unreachable!()
//         }
//     }
// }
//
// impl Iterator for Fanout {
//     type Item = Vec<Fanout>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         match self {
//             Fanout::Results(..) => None,
//             Fanout::Filter {
//                 filter,
//                 leaf: (.., leaf)
//             } => {
//                 Some(vec![Fanout::Results(match NodeGuard::deref(leaf) {
//                     Node::Index(..) => unreachable!(),
//                     Node::Leaf(records) => records
//                         .iter()
//                         .filter(|record| filter.key_interval
//                             .contains(record.key()) && record
//                             .match_version(filter.version))
//                         .cloned()
//                         .collect::<Vec<_>>(),
//                     Node::MultiVersionLeaf(record_lists) => record_lists
//                         .iter()
//                         .filter(|record_list| filter.key_interval.contains(record_list.key()))
//                         .map(|record_list| record_list.record_for_version(filter.version))
//                         .filter(|record| record.is_some())
//                         .map(|record| record.unwrap())
//                         .collect::<Vec<_>>(),
//                 })])
//             }
//             Fanout::Fan {
//                 index,
//                 father: (father_ref, ..),
//                 filter,
//                 fan_out,
//             } => {
//                 let next_fan = if !fan_out.is_empty() {
//                     let next_child
//                         = fan_out.pop_front().unwrap();
//
//                     let next_guard
//                         = index.lock_reader(next_child.deref());
//
//                      match next_guard.deref() {
//                         Node::Index(keys, children) => Self::Fan {
//                             index,
//                             filter: filter.clone(),
//                             fan_out: keys
//                                 .iter()
//                                 .enumerate()
//                                 .skip_while(|(_, k)| !filter.key_interval.lower().lt(k))
//                                 .take_while(|(pos, k)| filter.key_interval.upper().ge(k) || *pos == 0)
//                                 .map(|(pos, _)| children.get(pos).unwrap().clone())
//                                 .collect(),
//                             father: (next_child, next_guard),
//                         },
//                         _ => Self::Filter {
//                             filter: filter.clone(),
//                             leaf: (next_child, next_guard),
//                         }
//                     }
//                 } else {
//                     Self::Results(vec![])
//                 };
//
//                 if next_fan.is_results() {
//                     Some(vec![next_fan])
//                 }
//                 else {
//                     Some(vec![Self::Fan {
//                         index,
//                         filter: filter.clone(),
//                         father: (father_ref.clone(), index.lock_reader(father_ref.deref())),
//                         fan_out: fan_out.split_off(0),
//                     }, next_fan])
//                 }
//             }
//         }
//     }
// }