use std::borrow::Borrow;
use std::collections::{HashSet, VecDeque};
use std::{mem, thread};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::time::SystemTime;
use parking_lot::Mutex;
use rand::RngCore;
use TXDataModel::page_model::block::{Block, BlockGuard};
use TXDataModel::page_model::{BlockID, BlockRef};
use TXDataModel::record_model::record_like::RecordLike;
use TXDataModel::tx_model::transaction::Transaction;
use TXDataModel::tx_model::transaction_result::TransactionResult;
use TXDataModel::utils::cc_cell::CCCell;
use TXDataModel::utils::safe_cell::SafeCell;
use TXDataModel::utils::smart_cell::{SmartCell, SmartFlavor};
use crate::bplus_tree::BPlusTree;
use crate::locking::locking_strategy::LockingStrategy;

const _1KB: usize   = 1024;
const _2KB: usize   = 2 * _1KB;
const _4KB: usize   = 4 * _1KB;
const _8KB: usize   = 8 * _1KB;
const _16KB: usize  = 16 * _1KB;
const _32KB: usize  = 32 * _1KB;

pub const BSZ_BASE: usize       = _4KB;
pub const BSZ: usize            = BSZ_BASE - bsz_alignment();
pub const FAN_OUT: usize        = BSZ / 8 / 2;
pub const NUM_RECORDS: usize    = (BSZ - 2) / (8 + 8);

pub enum BlockSize {
    _1KB,
    _2KB,
    _4KB,
    _8KB,
    _16KB,
    _32KB
}

impl Display for BlockSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} kb", match self {
            BlockSize::_1KB => "1",
            BlockSize::_2KB => "2",
            BlockSize::_4KB => "4",
            BlockSize::_8KB => "8",
            BlockSize::_16KB => "16",
            BlockSize::_32KB => "32",
        })
    }
}


pub const fn fan_out(bsz: BlockSize) -> usize {
    ((match bsz {
        BlockSize::_1KB => _1KB,
        BlockSize::_2KB => _2KB,
        BlockSize::_4KB => _4KB,
        BlockSize::_8KB => _8KB,
        BlockSize::_16KB => _16KB,
        BlockSize::_32KB => _32KB,
    }) - 2) / (8 + 8)
}

pub const fn num_records(bsz: BlockSize) -> usize {
    (match bsz {
        BlockSize::_1KB => _1KB,
        BlockSize::_2KB => _2KB,
        BlockSize::_4KB => _4KB,
        BlockSize::_8KB => _8KB,
        BlockSize::_16KB => _16KB,
        BlockSize::_32KB => _32KB,
    }) / 8 / 2
}

pub const fn bsz_alignment() -> usize {
    mem::size_of::<BlockID>() +
        mem::size_of::<BlockRef<0, 0, Key, Payload>>() +
        mem::align_of::<Block<0,0,Key, Payload>>() +
        16 + // wc + sc
        mem::size_of::<BlockGuard<0,0, Key, Payload>>()
}
// const FAN_OUT: usize        = BSZ / (8 + 8) - 8;
// const NUM_RECORDS: usize    = BSZ / 16;
// const FAN_OUT: usize        = 3*256;
// const NUM_RECORDS: usize    = 256;

pub type Key                = u64;
pub type Payload            = f64;

pub type INDEX              = BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;

pub const MAKE_INDEX: fn(LockingStrategy) -> INDEX
= INDEX::new_single_version_for;

pub const MAKE_INDEX_MULTI: fn(LockingStrategy) -> INDEX
= INDEX::new_multi_version_for;

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

    let keys_insert = vec![
        1, 5, 6, 7, 3, 4, 10, 30, 11, 12, 14, 17, 18, 13, 16, 15, 36, 20, 21, 22, 23, 37, 2, 0,
    ];

    let keys_insert: Vec<Key> = vec![
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

    let tree = MAKE_INDEX_MULTI(LockingStrategy::default());
    let mut search_queries = vec![];

    for (i, tx) in keys_insert.into_iter().enumerate() {
        log_debug_ln(format!("# {}", i + 1));
        log_debug_ln(format!("############################################\
        ###########################################################"));

        let (key, version) = match tree.execute(tx) {
            TransactionResult::Inserted(key, version) => {
                log_debug_ln(format!("Ingest: {}", TransactionResult::<Key, Payload>::Inserted(key, version)));
                (key, version)
            }
            TransactionResult::Updated(key, version) => {
                log_debug_ln(format!("Ingest: {}", TransactionResult::<Key, Payload>::Updated(key, version)));
                (key, version)
            }
            joe => panic!("Sleepy Joe -> TransactionResult::{}", joe)
        };

        let search = vec![
            Transaction::Point(key, None),
            Transaction::Point(key, version),
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
                TransactionResult::Inserted(key, version) |
                TransactionResult::Updated(key, version) => if EXE_LOOK_UPS
                {
                    // loop {
                    match index.execute(Transaction::Point(key, version)) {
                        TransactionResult::MatchedRecord(Some(record))
                        if record.key() == key => {}//,
                        joe => { //  if !index.locking_strategy().is_dolos()
                            log_debug_ln(format!("\nERROR Search -> Transaction::{}",
                                                 Transaction::<Key, Payload>::Point(key, version)));
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

pub fn beast_test2<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync,
    Payload: Display + Default + Clone + Sync
>(num_thread: usize, index: BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>, t1s: &[Key])
    -> (u128, CCCell<BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>>)
{
    let index_o
        = CCCell::new(index);

    let mut handles = vec![];

    let query_buff = Mutex::new(VecDeque::from_iter(
        t1s.iter().map(|key| Transaction::Insert(*key, Payload::default())))
    );

    let query_buff_t: &'static Mutex<VecDeque<Transaction<Key, Payload>>>
        = unsafe { mem::transmute(query_buff.borrow()) };

    let index = index_o.unsafe_borrow_mut_static();
    let start = SystemTime::now();

    for _ in 1..=num_thread {
        handles.push(thread::spawn(|| loop {
            let mut buff = query_buff_t.lock();
            let next_query = buff.pop_front();
            mem::drop(buff);

            match next_query {
                Some(query) => match index.execute(query) { // index.execute(transaction),
                    TransactionResult::Inserted(key, version) |
                    TransactionResult::Updated(key, version) => if EXE_LOOK_UPS
                    {
                        match index.execute(Transaction::Point(key, version)) {
                            TransactionResult::MatchedRecord(Some(record))
                            if record.key() == key =>
                                {}
                            joe => {
                                log_debug_ln(format!("\nERROR Search -> Transaction::{}",
                                                     Transaction::<Key, Payload>::Point(key, version)));
                                log_debug_ln(format!("\n****ERROR: {}, {}", index.locking_strategy, joe));
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
                        log_debug_ln(format!("\n####ERROR: {}, {}", index.locking_strategy, joey));
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

pub fn simple_test2() {
    let singled_versioned_index = INDEX::new_single_versioned();
    let multi_versioned_index = INDEX::new_multi_versioned();

    for key in 1..=10_000 as Key {
        singled_versioned_index.execute(Transaction::Insert(key, key as f64));
        multi_versioned_index.execute(Transaction::Insert(key, key as f64));
    }

    log_debug_ln(format!(""));
    log_debug_ln(format!(""));
    log_debug_ln(format!(""));
}

fn experiment2() {
    println!("> Preparing data, hold on..");

    let threads_cpu = 24;
    let insertions: Key = 1_000_000;
    let data = gen_rand_data(insertions as usize);

    println!("Number Insertions,Number Threads,Locking Strategy,Height,Time");
    print!("{}", insertions);
    print!(",{}", threads_cpu);

    let index
        = MAKE_INDEX(LockingStrategy::LockCoupling);

    let (time, index_o) = beast_test2(
        threads_cpu,
        index,
        data.as_slice());

    println!(",{}", time);

    let index = index_o.unsafe_borrow();
    for key in data {
        match index.execute(Transaction::Point(key, None)) {
            TransactionResult::MatchedRecord(Some(..)) => {}
            joe => println!("ERROR: {}", joe)
        }
    }
}

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