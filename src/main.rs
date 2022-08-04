use std::collections::{HashSet, VecDeque};
use std::{fs, mem, thread};
use std::borrow::Borrow;
use std::sync::Mutex;
use std::time::SystemTime;
use chronicle_db::backbone::core::event::Event;
use chronicle_db::backbone::core::event::EventVariant::F64;
use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::index::record::Record;
use mvcc_bplustree::index::version_info::Version;
use mvcc_bplustree::locking::locking_strategy::{DEFAULT_OPTIMISTIC_ATTEMPTS, LevelVariant, LockingStrategy};
use mvcc_bplustree::transaction::transaction::Transaction;
use mvcc_bplustree::transaction::transaction_result::TransactionResult;
use mvcc_bplustree::utils::cc_cell_rrw_new::CCCellRRWOPT;
use chrono::{DateTime, Local};
use rand::RngCore;
use crate::bplus_tree::Index;
use crate::node_manager::NodeSettings;

mod node;
mod bplus_tree;
mod node_manager;
mod bplus_tree_query;
mod transaction;


fn main() {
    make_splash();

    simple_test();
    simple_test2();

    experiment();
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
    println!(" |               # Current version: {}                                |", env!("CARGO_PKG_VERSION"));
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

fn simple_test2() {
    let mut singled_versioned_index = bplus_tree::BPlusTree::new_single_versioned();
    let mut multi_versioned_index = bplus_tree::BPlusTree::new_multi_versioned();


    for key in 1..=10000 as Key {
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
        // 10,
        // 100,
        // 1_000,
        // 10_000,
        // 100_000,
        1_000_000,
        // 2_000_000,
        // 5_000_000,
        // 10_000_000,
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
                    = Index::new_with(LockingStrategy::SingleWriter);

                let time = beast_test(
                    1,
                    index,
                    t1s.as_slice());

                println!(",{}", time);

                print!("{}", t1s.len());
                print!(",{}", *num_threads);

                let index
                    = Index::new_with(LockingStrategy::WriteCoupling);

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
                        = Index::new_with(ls.clone());

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

fn display_record(record: Option<Record>) -> String {
    record.map(|record| record.to_string()).unwrap_or("None".to_string())
}

fn simple_test() {
    let keys_insert = vec![
        1, 5, 6, 7, 3, 4, 10, 30, 11, 12, 14, 17, 18, 13, 16, 15, 36, 20, 21, 22, 23, 37, 2, 0,
    ];

    // let mut keys_insert = (1..10_000_000).collect::<Vec<_>>();
    // let mut rng = thread_rng();
    // keys_insert.shuffle(&mut rng);

    let keys_insert = vec![
        8, 11, 19, 33, 24, 36, 34, 25, 12, 37, 14, 10, 45, 31, 18,
        3, 9, 5, 2, 13, 40, 38, 41, 27, 16, 28, 42, 1, 43, 23, 26,
        44, 17, 29, 39, 20, 6, 4, 7, 30, 21, 35, ];
    // let keys_insert = vec![ // k = 1
    //     8, 11, 19, 33, 24, 36, 34, 25, 12, 37, 14, 10, 45, 31, 18,
    //     3, 9, 5, 2, 13, 40, 38, 41, 27
    // ];

    // let mut keys_insert = gen_rand_data(10_000_000);

    let tree = Index::new_single_versioned();

    // let mut search_queries = vec![];

    for (i, key) in keys_insert.iter().enumerate() {
        if *key == 13 {
            let s = 12312;
        }
        println!("# {}", i + 1);
        println!("########################################################################################################");
        let (key, version) = match tree.execute(Transaction::Insert(Event::new_single_float_event_t1(*key, *key as _))) {
            TransactionResult::Inserted(key, version) => {
                println!("Ingest: {}", TransactionResult::Inserted(key, version));
                (key, version)
            }
            _ => panic!("Sleepy Joe")
        };

        // let search = vec![
        //     Transaction::ExactSearchLatest(key),
        //     Transaction::ExactSearch(key, version),
        //     Transaction::RangeSearch((key..=key).into(), version),
        // ];
        //
        // search_queries.push(search.clone());
        // search.into_iter().for_each(|query| match tree.execute(query.clone()) {
        //     TransactionResult::Error =>
        //         panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n{}",
        //                query,
        //                TransactionResult::Error,
        //                level_order(&tree)),
        //     TransactionResult::MatchedRecords(records) if records.len() != 1 =>
        //         panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n{}",
        //                query,
        //                TransactionResult::Error,
        //                level_order(&tree)),
        //     TransactionResult::MatchedRecord(None) =>
        //         panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n{}",
        //                query,
        //                TransactionResult::MatchedRecord(None),
        //                level_order(&tree)),
        //     result =>
        //         println!("\t- Query:  {}\n\t- Result: {}", query, result),
        // });
        // println!("########################################################################################################\n");
    }

    println!("--------------------------------------------------------------------------------------------------------");
    println!("--------------------------------------------------------------------------------------------------------");
    println!("\n############ Query All via Searches ############\n");
    // for chunk in search_queries.into_iter() {
    //     println!("--------------------------------------------------------------------------------------------------------");
    //
    //     for query in chunk {
    //         // if let Transaction::ExactSearchLatest(..) = query {
    //         //     continue
    //         // }
    //         match tree.execute(query.clone()) {
    //             TransactionResult::Error =>
    //                 panic!("\n\t- Query: {}\n\t- Result: {}", query, TransactionResult::Error),
    //             TransactionResult::MatchedRecords(records) if records.len() != 1 =>
    //                 panic!("\n\t- Query: {}\n\t- Result: {}", query, TransactionResult::Error),
    //             TransactionResult::MatchedRecord(None) =>
    //                 panic!("\n\t- Query: {}\n\t- Result: {}", query, TransactionResult::MatchedRecord(None)),
    //             result =>
    //                 println!("\t- Query:  {}\n\t- Result: {}", query, result),
    //         }
    //     }
    //     println!("--------------------------------------------------------------------------------------------------------\n");
    // }

    // json_index(&tree, "simple_tree.json");
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
        = CCCellRRWOPT::new(index);

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
                Some(query) => index.execute(query), // match index.execute(query) {
                //     Inserted(key, version) => if false {
                //         match index.execute(ExactSearch(key, version)) {
                //             TransactionResult::MatchedRecord(Some(record))
                //             if record.key() == key && version == record.insertion_version() =>
                //                 {}
                //             s => {
                //                 fs::write("tree_error.json",
                //                           serde_json::to_string(index).unwrap())
                //                     .unwrap();
                //
                //
                //                 println!("\n****ERROR: {}", index.locking_strategy);
                //                 let debug_1 = Transaction::DEBUGExactSearch(key, version);
                //                 let debug_2 = Transaction::DEBUGExactSearchLatest(key);
                //
                //                 let exact = index
                //                     .execute(debug_1.clone());
                //
                //                 println!("----------------------\
                //                 --------------------------------\
                //                 --------------------------------\
                //                 --------------------------------");
                //
                //                 let latest = index
                //                     .execute(debug_2.clone());
                //
                //                 println!("{} \n\t= {}", debug_1, exact);
                //                 println!("{} \n\t= {}", debug_2, latest);
                //
                //                 println!("######################\
                //                 ################################\
                //                 ################################\
                //                 ################################\n\n");
                //
                //                 panic!()
                //             }
                //         };
                //
                //         match index.execute(RangeSearch((key..=key).into(), version)) {
                //             TransactionResult::MatchedRecords(records)
                //             if records.len() != 1 =>
                //                 panic!("Sleepy Joe => len = {} - {}",
                //                        records.len(),
                //                        records.iter().join("\n")),
                //             TransactionResult::MatchedRecords(ref records)
                //             if records[0].key() != key || !records[0].insertion_version() == version =>
                //                 panic!("Sleepy Joe => RangeQuery matched garbage record = {}", records[0]),
                //             _ => {}
                //         };
                //     },
                //     _ => panic!("Bad")
                // }
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