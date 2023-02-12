use std::borrow::Borrow;
use std::collections::{HashSet, VecDeque};
use std::{mem, thread};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::ops::Add;
use std::time::SystemTime;
use itertools::Itertools;
use parking_lot::Mutex;
use rand::RngCore;
use TXDataModel::page_model::block::{Block, BlockGuard};
use TXDataModel::page_model::{BlockID, BlockRef, ObjectCount};
// use TXDataModel::record_model::record_like::RecordLike;
use TXDataModel::tx_model::transaction::Transaction;
use TXDataModel::tx_model::transaction_result::TransactionResult;
use TXDataModel::utils::cc_cell::CCCell;
use TXDataModel::utils::interval::Interval;
use TXDataModel::utils::safe_cell::SafeCell;
use TXDataModel::utils::smart_cell::{SmartCell, SmartFlavor};
use crate::block::block_manager::{_4KB, bsz_alignment};
use crate::bplus_tree::BPlusTree;
use crate::locking::locking_strategy::{LevelConstraints, LockingStrategy};
use crate::show_alignment_bsz;


pub const BSZ_BASE: usize       = _4KB;
pub const BSZ: usize            = BSZ_BASE - bsz_alignment::<Key, Payload>();
pub const FAN_OUT: usize        = BSZ / 8 / 2;
pub const NUM_RECORDS: usize    = (BSZ - 2) / (8 + 8);

// const FAN_OUT: usize        = BSZ / (8 + 8) - 8;
// const NUM_RECORDS: usize    = BSZ / 16;
// const FAN_OUT: usize        = 3*256;
// const NUM_RECORDS: usize    = 256;

pub type Key                = u64;
pub type Payload            = f64;
pub fn inc_key(k: Key) -> Key {
    k.checked_add(1).unwrap_or(Key::MAX)
}
pub fn dec_key(k: Key) -> Key {
    k.checked_sub(1).unwrap_or(Key::MIN)
}

pub type INDEX              = BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;

pub const MAKE_INDEX: fn(LockingStrategy) -> INDEX
= |ls| INDEX::new_single_version_for(ls,  Key::MIN, Key::MAX, inc_key, dec_key);

pub const MAKE_INDEX_MULTI: fn(LockingStrategy) -> INDEX
= |ls| INDEX::new_multi_version_for(ls,  Key::MIN, Key::MAX, inc_key, dec_key);

pub const EXE_LOOK_UPS: bool = false;

pub fn log_debug_ln(s: String) {
    println!("> {}", s.replace("\n", "\n>"))
}

pub fn log_debug(s: String) {
    print!("> {}", s.replace("\n", "\n>"))
}

pub fn simple_test() {
    const INSERT: fn(u64) -> Transaction<Key, Payload> = |k: Key|
        Transaction::Insert(k, k as _);

    const UPDATE: fn(u64) -> Transaction<Key, Payload> = |k: Key|
        Transaction::Update(k, k as _);

    let keys_insert_org = vec![
        1, 5, 6, 7, 3, 4, 10, 30, 11, 12, 14, 17, 18, 13, 16, 15, 36, 20, 21, 22, 23, 37, 2, 0,
    ];

    let keys_insert_org: Vec<Key> = vec![
        8, 11, 19, 33, 24, 36, 34, 25, 12, 37, 14, 10, 45, 31, 18,
        3, 9, 5, 2, 13, 40, 38, 41, 27, 16, 28, 42, 1, 43, 23, 26,
        44, 17, 29, 39, 20, 6, 4, 7, 30, 21, 35, 8];

    // let mut rand = rand::thread_rng();
    // let mut keys_insert = gen_rand_data(1_000);
    //
    // let dups = rand.next_u32().min(keys_insert.len() as _) as usize;
    // keys_insert.extend(keys_insert.get(..dups).unwrap().to_vec());
    // let mut rng = thread_rng();
    // keys_insert.shuffle(&mut rng);

    let mut already_used: Vec<Key> = vec![];
    let keys_insert = keys_insert_org
        .iter()
        .map(|key| if already_used.contains(key) {
            UPDATE(*key)
        } else {
            already_used.push(*key);
            INSERT(*key)
        }).collect::<Vec<_>>();

    // let keys_insert = vec![ // k = 1
    //     8, 11, 19, 33, 24, 36, 34, 25, 12, 37, 14, 10, 45, 31, 18,
    //     3, 9, 5, 2, 13, 40, 38, 41, 27
    // ];

    // let mut keys_insert = gen_rand_data(10_000_000);

    let tree = MAKE_INDEX_MULTI(LockingStrategy::OLC(LevelConstraints::Unlimited));
    let mut search_queries = vec![];

    for (i, tx) in keys_insert.into_iter().enumerate() {
        log_debug_ln(format!("# {}", i + 1));
        log_debug_ln(format!("############################################\
        ###########################################################"));

        let key = match tree.execute(tx) {
            TransactionResult::Inserted(key) => {
                log_debug_ln(format!("Ingest: {}", TransactionResult::<Key, Payload>::Inserted(key)));
                key
            }
            TransactionResult::Updated(key, payload) => {
                log_debug_ln(format!("Ingest: {}", TransactionResult::<Key, Payload>::Updated(key, payload)));
                key
            }
            joe => panic!("Sleepy Joe -> TransactionResult::{}", joe)
        };

        let search = vec![
            Transaction::Point(key),
            Transaction::Point(key),
            // Transaction::RangeSearch((key..=key).into(), version),
        ];

        search_queries.push(search.clone());
        search.into_iter().for_each(|query| match tree.execute(query.clone()) {
            TransactionResult::Error =>
                panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n{}",
                       query,
                       TransactionResult::<Key, Payload>::Error,
                       level_order(&tree)),
            TransactionResult::MatchedRecords(records) if records.len() != 1 =>
                panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n{}",
                       query,
                       TransactionResult::<Key, Payload>::Error,
                       level_order(&tree)),
            TransactionResult::MatchedRecord(None) =>
                panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n{}",
                       query,
                       TransactionResult::<Key, Payload>::MatchedRecord(None),
                       level_order(&tree)),
            result =>
                log_debug_ln(format!("\t- Query:  {}\n\t- Result: {}", query, result)),
        });
        log_debug_ln(format!("##################################################################################\
        ######################\n"));
    }

    log_debug_ln(format!("--------------------------------\
    ------------------------------------------------------------------------"));
    log_debug_ln(format!("----------------------------------\
    ----------------------------------------------------------------------"));
    log_debug_ln(format!("\n############ Query All via Searches ############\n"));
    for (s, chunk) in search_queries.into_iter().enumerate() {
        log_debug_ln(format!("----------------------------------\
        ----------------------------------------------------------------------"));
        log_debug_ln(format!("\t# [{}]", s));
        // if s == 42 {
        //     let x = 31;
        // }
        for query in chunk {
            // if let Transaction::ExactSearchLatest(..) = transaction {
            //     continue
            // }
            match tree.execute(query.clone()) {
                TransactionResult::Error =>
                    panic!("\n\t- Query: {}\n\t- Result: {}", query, TransactionResult::<Key, Payload>::Error),
                TransactionResult::MatchedRecords(records) if records.len() != 1 =>
                    panic!("\n\t#- Query: {}\n\t- Result: {}", query, TransactionResult::<Key, Payload>::Error),
                TransactionResult::MatchedRecord(None) =>
                    panic!("\n\t#- Query: {}\n\t- Result: {}", query, TransactionResult::<Key, Payload>::MatchedRecord(None)),
                result =>
                    log_debug_ln(format!("\t- Query:  {}\n\t- Result: {}", query, result)),
            }
        }
        log_debug_ln(format!("----------------------------------------------------------\
        ----------------------------------------------\n"));
    }

    show_alignment_bsz();

    let range = Interval::new(
        0,
        100
    );

    let matches = keys_insert_org
        .into_iter()
        .filter(|k| range.contains(*k))
        .unique();

    let results
        = tree.execute(Transaction::Range(range.clone()));

    println!("Results of Range Query:\n{}\n\nExpected: \t{}\nFound: \t\t{}\nRange: {}", results, matches.count(), match results {
        TransactionResult::MatchedRecords(ref records) => records.len(),
        _ => 0
    }, range);

    // json_index(&tree, "simple_tree.json");
}

pub fn gen_rand_data(n: usize) -> Vec<Key> {
    let mut nums = HashSet::new();
    let mut rand = rand::thread_rng();
    loop {
        let next = rand.next_u64() as Key;
        if !nums.contains(&next) {
            nums.insert(next);
        }

        if nums.len() == n as usize {
            break;
        }
    }
    nums.into_iter().collect::<Vec<_>>()
}

pub fn beast_test(num_thread: usize, index: INDEX, t1s: &[u64]) -> u128 {
    let index_o
        = index;

    let mut handles
        = Vec::with_capacity(num_thread);

    let query_buff = t1s
        .iter()
        .map(|key| Transaction::Insert(*key, Payload::default()))
        .collect::<Vec<_>>();

    let mut data_buff = query_buff
        .chunks(t1s.len() / num_thread)
        .into_iter()
        .map(|s| SafeCell::new(s.to_vec()))
        .collect::<Vec<_>>();

    let index: &'static INDEX = unsafe { mem::transmute(&index_o) };
    let start = SystemTime::now();

    for _ in 1..=num_thread {
        let current_chunk
            = data_buff.pop().unwrap();

        handles.push(thread::spawn(move || current_chunk.into_inner().into_iter().for_each(|next_query| {
            match index.execute(next_query) { // index.execute(transaction),
                TransactionResult::Inserted(key, ..) |
                TransactionResult::Updated(key, ..) => if EXE_LOOK_UPS
                {
                    // loop {
                    match index.execute(Transaction::Point(key)) {
                        TransactionResult::MatchedRecord(Some(record))
                        if record.key == key => {}//,
                        // TransactionResult::MatchedRecordVersioned(Some(record))
                        // if record.key() == key => {}//,
                        joe => { //  if !index.locking_strategy().is_dolos()
                            log_debug_ln(format!("\nERROR Search -> Transaction::{}",
                                                 Transaction::<_, Payload>::Point(key)));
                            log_debug_ln(format!("\n****ERROR: {}, TransactionResult::{}", index.locking_strategy, joe));
                            panic!()
                        }
                        // _ => {}
                    };
                    // }

                    // match index.execute(RangeSearch((key..=key).into(), version)) {
                    //     TransactionResult::MatchedRecords(records)
                    //     if records.len() != 1 =>
                    //         panic!("Sleepy Joe => len = {} - {}",
                    //                records.len(),
                    //                records.iter().join("\n")),
                    //     TransactionResult::MatchedRecords(ref records)
                    //     if records[0].key() != key || !records[0].insertion_version() == version =>
                    //         panic!("Sleepy Joe => RangeQuery matched garbage record = {}", records[0]),
                    //     _ => {}
                    // };
                },
                joey => {
                    log_debug_ln(format!("\n#### ERROR: {}, {}", index.locking_strategy, joey));
                    panic!()
                }
            };
        })
        ));
    }

    handles
        .into_iter()
        .for_each(|handle| handle
            .join()
            .unwrap());

    let time = SystemTime::now().duration_since(start).unwrap().as_millis();
    print!(",{},{}", index_o.locking_strategy(), index_o.height());

    time
}

pub fn level_order<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync
>
(tree: &BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>) -> String {
    "".to_string()
    // tree.level_order(None)
    //     .into_iter()
    //     .map(|node| node.unsafe_borrow_static())
    //     .join("\n")
}

// pub fn beast_test2<
//     const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Display + Default + Ord + Copy + Hash + Sync,
//     Payload: Display + Default + Clone + Sync + Default
// >(num_thread: usize, index: BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>, t1s: &[Key])
//     -> (u128, CCCell<BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>>)
// {
//     let index_o
//         = CCCell::new(index);
//
//     let mut handles = vec![];
//
//     let query_buff = Mutex::new(VecDeque::from_iter(
//         t1s.iter().map(|key| Transaction::Insert(*key, Payload::default())))
//     );
//
//     let query_buff_t: &'static Mutex<VecDeque<Transaction<Key, Payload>>>
//         = unsafe { mem::transmute(query_buff.borrow()) };
//
//     let index = index_o.unsafe_borrow_mut_static();
//     let start = SystemTime::now();
//
//     for _ in 1..=num_thread {
//         handles.push(thread::spawn(|| loop {
//             let mut buff = query_buff_t.lock();
//             let next_query = buff.pop_front();
//             mem::drop(buff);
//
//             match next_query {
//                 Some(query) => match index.execute(query) { // index.execute(transaction),
//                     TransactionResult::Inserted(key) |
//                     TransactionResult::Updated(key, ..) => if EXE_LOOK_UPS
//                     {
//                         match index.execute(Transaction::Point(key)) {
//                             TransactionResult::MatchedRecord(Some(record))
//                             if record.key == key =>
//                                 {}
//                             joe => {
//                                 log_debug_ln(format!("\nERROR Search -> Transaction::{}",
//                                                      Transaction::<Key, Payload>::Point(key)));
//                                 log_debug_ln(format!("\n****ERROR: {}, {}", index.locking_strategy, joe));
//                                 panic!()
//                             }
//                         };
//
//                         // match index.execute(RangeSearch((key..=key).into(), version)) {
//                         //     TransactionResult::MatchedRecords(records)
//                         //     if records.len() != 1 =>
//                         //         panic!("Sleepy Joe => len = {} - {}",
//                         //                records.len(),
//                         //                records.iter().join("\n")),
//                         //     TransactionResult::MatchedRecords(ref records)
//                         //     if records[0].key() != key || !records[0].insertion_version() == version =>
//                         //         panic!("Sleepy Joe => RangeQuery matched garbage record = {}", records[0]),
//                         //     _ => {}
//                         // };
//                     },
//                     joey => {
//                         log_debug_ln(format!("\n####ERROR: {}, {}", index.locking_strategy, joey));
//                         panic!()
//                     }
//                 }
//                 None => break
//             };
//         }));
//     }
//
//     handles
//         .into_iter()
//         .for_each(|handle| handle
//             .join()
//             .unwrap());
//
//     let time = SystemTime::now().duration_since(start).unwrap().as_millis();
//     print!(",{},{}", index_o.locking_strategy(), index_o.height());
//
//     (time, index_o)
// }

pub fn simple_test2() {
    let singled_versioned_index = MAKE_INDEX(LockingStrategy::MonoWriter);
    let multi_versioned_index = MAKE_INDEX_MULTI(LockingStrategy::MonoWriter);

    for key in 1..=10_000 as Key {
        singled_versioned_index.execute(Transaction::Insert(key, key as f64));
        multi_versioned_index.execute(Transaction::Insert(key, key as f64));
    }

    log_debug_ln(format!(""));
    log_debug_ln(format!(""));
    log_debug_ln(format!(""));
}

// fn experiment2() {
//     println!("> Preparing data, hold on..");
//
//     let threads_cpu = 24;
//     let insertions: Key = 1_000_000;
//     let data = gen_rand_data(insertions as usize);
//
//     println!("Number Insertions,Number Threads,Locking Strategy,Height,Time");
//     print!("{}", insertions);
//     print!(",{}", threads_cpu);
//
//     let index
//         = MAKE_INDEX(LockingStrategy::LockCoupling);
//
//     let (time, index_o) = beast_test2(
//         threads_cpu,
//         index,
//         data.as_slice());
//
//     println!(",{}", time);
//
//     let index = index_o.unsafe_borrow();
//     for key in data {
//         match index.execute(Transaction::Point(key)) {
//             TransactionResult::MatchedRecord(Some(..)) => {}
//             joe => println!("ERROR: {}", joe)
//         }
//     }
// }

pub fn format_insertsions(i: Key) -> String {
    if i == 100_000_000 {
        "100 Mio".to_string()
    } else if i == 10_000_000 {
        "10 Mio".to_string()
    } else if i == 1_000_000 {
        "1 Mio".to_string()
    } else if i == 100_000 {
        "100 K".to_string()
    } else if i == 10_000 {
        "10 K".to_string()
    } else if i == 1_000 {
        "1 K".to_string()
    } else {
        i.to_string()
    }
}