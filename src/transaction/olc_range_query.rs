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

// fn read_guard_reader_cell_version(guard: &BlockGuard) -> Version {
//     let mut attempts
//         = Attempts::MIN;
//
//     loop {
//         let version
//             = guard.cell_version().unwrap();
//
//         if version & WRITE_FLAG_VERSION != 0 {
//             sched_yield(attempts);
//
//             attempts += 1;
//         }
//         else {
//             break version
//         }
//     }
// }

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