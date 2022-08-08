use std::collections::{HashSet, VecDeque};
use std::{fs, mem, thread};
use std::borrow::Borrow;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};
use chronicle_db::backbone::core::event::Event;
use chronicle_db::backbone::core::event::EventVariant::F64;
use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::index::record::Record;
use mvcc_bplustree::locking::locking_strategy::{LevelVariant, LockingStrategy};
use mvcc_bplustree::transaction::transaction::Transaction;
use mvcc_bplustree::transaction::transaction_result::TransactionResult;
use chrono::{DateTime, Local};
use mvcc_bplustree::utils::cc_cell::CCCell;
use rand::RngCore;
use crate::index::bplus_tree;
use crate::index::bplus_tree::Index;

mod index;
mod transaction;
mod utils;


fn main() {
    make_splash();

    // simple_test();
    // simple_test2();
    experiment();

    experiment2();
}

/// Essential function.
fn make_splash() {
    let datetime: DateTime<Local> = fs::metadata(std::env::current_exe().unwrap())
        .unwrap().modified().unwrap().into();

    println!("                         _________________________");
    println!("                 _______/                         \\_______");
    println!("                /                                         \\");
    println!(" +-------------+                                           +-------------+");
    println!(" |                                                                       |");
    println!(" |               ----------------------------------                      |");
    println!(" |               # Build:   {}                          |", datetime.format("%d-%m-%Y %T"));
    println!(" |               # Current version: {}                               |", env!("CARGO_PKG_VERSION"));
    println!(" |               ----------------------------------                      |");
    println!(" |                                                                       |");
    println!(" |               Written by: Amir El-Shaikh                              |");
    println!(" |               E-Mail: elshaikh@mathematik.uni-marburg.de              |");
    println!(" |               First released: 03-08-2022                              |");
    println!(" |                                                                       |");
    println!(" |               ...CC-B+Tree Application Launching...                   |");
    println!(" +-------------+                                           +-------------+");
    println!("                \\_______                           _______/");
    println!("                        \\_________________________/");

    println!();
    println!("--> System Log:");
}

fn simple_test() {
    const INSERT: fn(Key) -> Transaction = |k: Key|
        Transaction::Insert(Event::new_single_float_event_t1(k, k as _));

    const UPDATE: fn(Key) -> Transaction = |k: Key|
        Transaction::Update(Event::new_single_float_event_t1(k, k as _));

    let keys_insert = vec![
        1, 5, 6, 7, 3, 4, 10, 30, 11, 12, 14, 17, 18, 13, 16, 15, 36, 20, 21, 22, 23, 37, 2, 0,
    ];

    let keys_insert: Vec<Key> = vec![
        8, 11, 19, 33, 24, 36, 34, 25, 12, 37, 14, 10, 45, 31, 18,
        3, 9, 5, 2, 13, 40, 38, 41, 27, 16, 28, 42, 1, 43, 23, 26,
        44, 17, 29, 39, 20, 6, 4, 7, 30, 21, 35, 8];

    let mut rand = rand::thread_rng();
    let mut keys_insert = gen_rand_data(100_000);

    // let dups = rand.next_u32().min(keys_insert.len() as _) as usize;
    // keys_insert.extend(keys_insert.get(..dups).unwrap().to_vec());
    // let mut rng = thread_rng();
    // keys_insert.shuffle(&mut rng);

    let mut already_used: Vec<Key> = vec![];
    let keys_insert = keys_insert
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

    let tree = Index::new_multi_version_for(LockingStrategy::SingleWriter);
    let mut search_queries = vec![];

    for (i, tx) in keys_insert.into_iter().enumerate() {
        println!("# {}", i + 1);
        println!("########################################################################################################");

        let (key, version) = match tree.execute(tx) {
            TransactionResult::Inserted(key, version) => {
                println!("Ingest: {}", TransactionResult::Inserted(key, version));
                (key, version)
            }
            TransactionResult::Updated(key, version) => {
                println!("Ingest: {}", TransactionResult::Updated(key, version));
                (key, version)
            }
            joe => panic!("Sleepy Joe -> TransactionResult::{}", joe)
        };

        let search = vec![
            Transaction::ExactSearchLatest(key),
            Transaction::ExactSearch(key, version),
            // Transaction::RangeSearch((key..=key).into(), version),
        ];

        search_queries.push(search.clone());
        search.into_iter().for_each(|query| match tree.execute(query.clone()) {
            TransactionResult::Error =>
                panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n{}",
                       query,
                       TransactionResult::Error,
                       level_order(&tree)),
            TransactionResult::MatchedRecords(records) if records.len() != 1 =>
                panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n{}",
                       query,
                       TransactionResult::Error,
                       level_order(&tree)),
            TransactionResult::MatchedRecord(None) =>
                panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n{}",
                       query,
                       TransactionResult::MatchedRecord(None),
                       level_order(&tree)),
            result =>
                println!("\t- Query:  {}\n\t- Result: {}", query, result),
        });
        println!("########################################################################################################\n");
    }

    println!("--------------------------------------------------------------------------------------------------------");
    println!("--------------------------------------------------------------------------------------------------------");
    println!("\n############ Query All via Searches ############\n");
    for chunk in search_queries.into_iter() {
        println!("--------------------------------------------------------------------------------------------------------");

        for query in chunk {
            // if let Transaction::ExactSearchLatest(..) = transaction {
            //     continue
            // }
            match tree.execute(query.clone()) {
                TransactionResult::Error =>
                    panic!("\n\t- Query: {}\n\t- Result: {}", query, TransactionResult::Error),
                TransactionResult::MatchedRecords(records) if records.len() != 1 =>
                    panic!("\n\t- Query: {}\n\t- Result: {}", query, TransactionResult::Error),
                TransactionResult::MatchedRecord(None) =>
                    panic!("\n\t- Query: {}\n\t- Result: {}", query, TransactionResult::MatchedRecord(None)),
                result =>
                    println!("\t- Query:  {}\n\t- Result: {}", query, result),
            }
        }
        println!("--------------------------------------------------------------------------------------------------------\n");
    }

    // json_index(&tree, "simple_tree.json");
}

fn simple_test2() {
    let mut singled_versioned_index = bplus_tree::BPlusTree::new_single_versioned();
    let mut multi_versioned_index = bplus_tree::BPlusTree::new_multi_versioned();


    for key in 1..=10_000 as Key {
        singled_versioned_index.execute(
            Transaction::Insert(Event::new_from_t1(key, F64(key as f64))));

        multi_versioned_index.execute(
            Transaction::Insert(Event::new_from_t1(key, F64(key as f64))));
    }
}

fn gen_rand_data(n: usize) -> Vec<Key> {
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

fn experiment() {
    println!("> Preparing data, hold on..");
    let cpu_threads = false;

    let mut threads_cpu = vec![
        1,
        2,
        3,
        4,
        8,
        16,
        32,
        64,
        128,
        256,
        // 512,
        // 1024,
    ];

    if cpu_threads {
        threads_cpu = (1..=num_cpus::get()).collect();
    }

    let insertions: Vec<Key> = vec![
        10,
        100,
        1_000,
        10_000,
        100_000,
        1_000_000,
        // 2_000_000,
        // 5_000_000,
        10_000_000,
        // 20_000_000,
        // 50_000_000,
        // 100_000_000,
    ];

    let mut strategies = vec![];
    strategies.push(LockingStrategy::WriteCoupling);

    for attempt in 1..=3 {
        // strategies.push(LockingStrategy::optimistic_custom(
        //     LevelVariant::new_height_lock(0.2_f32), attempt));
        // strategies.push(LockingStrategy::dolos_custom(
        //     LevelVariant::new_height_lock(0.2_f32), attempt));
        //
        // strategies.push(LockingStrategy::optimistic_custom(
        //     LevelVariant::new_height_lock(0.5_f32), attempt));
        // strategies.push(LockingStrategy::dolos_custom(
        //     LevelVariant::new_height_lock(0.5_f32), attempt));
        //
        // strategies.push(LockingStrategy::optimistic_custom(
        //     LevelVariant::new_height_lock(0.9_f32), attempt));
        // strategies.push(LockingStrategy::dolos_custom(
        //     LevelVariant::new_height_lock(0.9_f32), attempt));
        //
        strategies.push(LockingStrategy::optimistic_custom(
            LevelVariant::new_height_lock(1_f32), attempt));
        // strategies.push(LockingStrategy::dolos_custom(
        //     LevelVariant::new_height_lock(1_f32), attempt));
    }

    let cases = insertions
        .into_iter()
        .map(|n| {
            println!("> Preparing n = {} data, hold on..", n);

            let data = gen_rand_data(n as usize);
            println!("> Completed n = {} data", n);

            (data, strategies.clone())
        }).collect::<Vec<_>>();

    mem::drop(strategies);

    println!("Number Insertions,Number Threads,Locking Strategy,Height,Time");

    cases.iter().for_each(|(t1s, strats)|
        for num_threads in threads_cpu.iter() {
            if *num_threads == 1 {
                print!("{}", t1s.len());
                print!(",{}", *num_threads);

                let index
                    = Index::new_single_version_for(LockingStrategy::SingleWriter);

                let time = beast_test(
                    1,
                    index,
                    t1s.as_slice());

                println!(",{}", time);

                print!("{}", t1s.len());
                print!(",{}", *num_threads);

                let index
                    = Index::new_single_version_for(LockingStrategy::WriteCoupling);

                let time = beast_test(
                    1,
                    index,
                    t1s.as_slice());

                println!(",{}", time);

                // thread::sleep(Duration::from_millis(200));
            } else {
                for ls in strats.iter() {
                    print!("{}", t1s.len());
                    print!(",{}", *num_threads);
                    let index
                        = Index::new_single_version_for(ls.clone());

                    let time = beast_test(
                        *num_threads,
                        index,
                        t1s.as_slice());

                    println!(",{}", time);

                    // thread::sleep(Duration::from_millis(200));
                }
            }
        });
}

fn experiment2() {
    println!("> Preparing data, hold on..");

    let mut threads_cpu = 24;
    let insertions: Key = 1_000_000;
    let data = gen_rand_data(insertions as usize);

    println!("Number Insertions,Number Threads,Locking Strategy,Height,Time");
    print!("{}", insertions);
    print!(",{}", threads_cpu);

    let index
        = Index::new_single_version_for(LockingStrategy::WriteCoupling);

    let (time, index_o) = beast_test2(
        threads_cpu,
        index,
        data.as_slice());

    println!(",{}", time);

    let index = index_o.unsafe_borrow();
    for key in data {
        match index.execute(Transaction::ExactSearchLatest(key)) {
            TransactionResult::MatchedRecord(Some(..)) => {},
            joe => println!("ERROR: {}", joe)
        }
    }


}

fn beast_test2(num_thread: usize, index: Index, t1s: &[Key]) -> (u128, CCCell<Index>) {
    let index_o
        = CCCell::new(index);

    let mut handles = vec![];

    let mut query_buff = Mutex::new(VecDeque::from_iter(
        t1s.iter().map(|key| Transaction::Insert(
            Event::new_single_float_event_t1(*key, *key as _))))
    );

    let query_buff_t: &'static Mutex<VecDeque<Transaction>>
        = unsafe { mem::transmute(query_buff.borrow()) };

    let index = index_o.unsafe_borrow_mut_static();
    let start = SystemTime::now();

    for _ in 1..=num_thread {
        handles.push(thread::spawn(|| loop {
            let mut buff = query_buff_t.lock().unwrap();
            let next_query = buff.pop_front();
            mem::drop(buff);

            match next_query {
                Some(query) => match index.execute(query) { // index.execute(transaction),
                    TransactionResult::Inserted(key, version) |
                    TransactionResult::Updated(key, version) => if false
                    {
                        match index.execute(Transaction::ExactSearch(key, version)) {
                            TransactionResult::MatchedRecord(Some(record))
                            if record.key() == key && record.match_version(version) =>
                                {}
                            joe => {
                                println!("\nERROR Search -> Transaction::{}", Transaction::ExactSearch(key, version));
                                println!("\n****ERROR: {}, {}", index.locking_strategy, joe);
                                let t
                                    = index.execute(Transaction::ExactSearch(key, version));

                                let x = 123123;
                                // panic!()
                            }
                        };

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
                        println!("\n####ERROR: {}, {}", index.locking_strategy, joey);
                        panic!()
                    }
                }
                None => break
            };
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle
            .join()
            .unwrap());

    let time = SystemTime::now().duration_since(start).unwrap().as_millis();
    print!(",{},{}", index_o.locking_strategy(), index_o.height());

    (time, index_o)
}

fn display_record(record: Option<Record>) -> String {
    record.map(|record| record.to_string()).unwrap_or("None".to_string())
}

fn level_order(tree: &Index) -> String {
    "".to_string()
    // tree.level_order(None)
    //     .into_iter()
    //     .map(|node| node.unsafe_borrow_static())
    //     .join("\n")
}

fn beast_test(num_thread: usize, index: Index, t1s: &[Key]) -> u128 {
    let index_o
        = CCCell::new(index);

    let mut handles = vec![];

    let mut query_buff = Mutex::new(VecDeque::from_iter(
        t1s.iter().map(|key| Transaction::Insert(
            Event::new_single_float_event_t1(*key, *key as _))))
    );

    let query_buff_t: &'static Mutex<VecDeque<Transaction>>
        = unsafe { mem::transmute(query_buff.borrow()) };

    let index = index_o.unsafe_borrow_mut_static();
    let start = SystemTime::now();

    for _ in 1..=num_thread {
        handles.push(thread::spawn(|| loop {
            let mut buff = query_buff_t.lock().unwrap();
            let next_query = buff.pop_front();
            mem::drop(buff);

            match next_query {
                Some(query) => match index.execute(query) { // index.execute(transaction),
                    TransactionResult::Inserted(key, version) |
                    TransactionResult::Updated(key, version) => if true
                    {
                        match index.execute(Transaction::ExactSearch(key, version)) {
                            TransactionResult::MatchedRecord(Some(record))
                            if record.key() == key && record.match_version(version) =>
                                {}
                            joe => {
                                println!("\nERROR Search -> Transaction::{}", Transaction::ExactSearch(key, version));
                                println!("\n****ERROR: {}, {}", index.locking_strategy, joe);
                                panic!()
                            }
                        };

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
                        println!("\n####ERROR: {}, {}", index.locking_strategy, joey);
                        panic!()
                    }
                }
                None => break
            };
        }));
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