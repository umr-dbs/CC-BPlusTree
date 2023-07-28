use std::collections::{HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::{Add, Deref, DerefMut, Div, RangeInclusive, Sub};
use std::sync::Arc;
use std::thread::spawn;
use std::time::{Duration, SystemTime};
use crossbeam::channel::TryRecvError;
use itertools::Itertools;
use rand::{Rng, RngCore, SeedableRng, thread_rng};
use rand::distributions::{Standard, Uniform};
use rand::rngs::StdRng;
use crate::block::block_manager::{_4KB, bsz_alignment};
use crate::bplus_tree::BPlusTree;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::locking::locking_strategy::{CRUDProtocol, hybrid_lock, lightweight_hybrid_lock, LHL_read, lightweight_hybrid_lock_unlimited, LHL_write, LHL_read_write, LockingStrategy, OLC, orwc, orwc_attempts};
use crate::{TREE, Tree};
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::locking::locking_strategy::LockingStrategy::{LockCoupling, MonoWriter};

pub const VALIDATE_OPERATION_RESULT: bool = false;
pub const EXE_LOOK_UPS: bool = false;
pub const EXE_RANGE_LOOK_UPS: bool = false;

pub const BSZ_BASE: usize = _4KB;
pub const BSZ: usize = BSZ_BASE - bsz_alignment::<Key, Payload>();
pub const FAN_OUT: usize = BSZ / 8 / 2;
pub const NUM_RECORDS: usize = (BSZ - 2) / (8 + 8);

pub type Key = u64;
pub type Payload = f64;

pub fn inc_key(k: Key) -> Key {
    k.checked_add(1).unwrap_or(Key::MAX)
}

pub fn dec_key(k: Key) -> Key {
    k.checked_sub(1).unwrap_or(Key::MIN)
}

pub type INDEX = BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;

pub const MAKE_INDEX: fn(LockingStrategy) -> INDEX
= |ls| INDEX::new_with(ls, Key::MIN, Key::MAX, inc_key, dec_key);

#[inline(always)]
pub fn insert(create_threads: usize, tree: Tree, operations: &[CRUDOperation<Key, Payload>]) -> (u128, u64) {
    let mut data_buff = operations
        .iter()
        .chunks(operations.len() / create_threads)
        .into_iter()
        .map(|s| s.into_iter().cloned().collect::<Vec<_>>())
        .collect::<VecDeque<_>>();

    if data_buff.len() > create_threads {
        let back = data_buff.pop_back().unwrap();
        data_buff.front_mut().unwrap().extend(back);
    }

    let mut handles
        = Vec::with_capacity(create_threads);

    let start = SystemTime::now();
    for _ in 1..=create_threads {
        let current_chunk
            = data_buff.pop_front().unwrap();

        let index = tree.clone();
        handles.push(spawn(move || {
            let mut counter_dups = 0;
            current_chunk
                .into_iter()
                .for_each(|next_query| match index.dispatch(next_query) { // tree.execute(operation),
                    CRUDOperationResult::Inserted(..) => {}
                    _ => counter_dups += 1
                });
            counter_dups
        }));
    }

    let dups = handles
        .into_iter()
        .map(|handle| handle
            .join()
            .unwrap())
        .sum();

    (SystemTime::now().duration_since(start).unwrap().as_millis(), dups)
}

pub fn start_paper_tests() {
    println!("Records,Threads,Protocol,Create Time,Dupes,Lambda,Run,Mixed Time,U-TH,Updates,Reads,Total");
    let number_records
        = 1_000_000;

    let key_range
        = 1..=number_records;

    let repeats
        = 10_usize;

    let updates
        = [0.1, 0.3, 0.5, 0.7, 0.9];

    let threads
        = [1, 2, 4, 5, 8, 16, 20, 25, 32, 40, 50, 64, 80, 100, 125];

    let lambdas
        = [0.1_f64, 16_f64, 32_f64, 64_f64, 128_f64, 256_f64, 512_f64, 1024_f64];

    let data_lambdas = lambdas
        .iter()
        .map(|lambda| {
            let mut rnd = StdRng::seed_from_u64(90501960);
            gen_data_exp(number_records, *lambda, &mut rnd)
                .into_iter().map(|key| CRUDOperation::Insert(key, Payload::default()))
                .collect::<Vec<_>>()
        }).collect::<Vec<_>>();

    let protocols = [
        OLC(),

        LHL_read(0),
        LHL_read(1),
        LHL_read(4),
        LHL_read(16),
        LHL_read(64),

        LHL_write(0),
        LHL_write(1),
        LHL_write(4),
        LHL_write(16),
        LHL_write(64),
        LHL_write(128),

        LHL_read_write(0, 0),
        LHL_read_write(1, 1),
        LHL_read_write(4, 4),
        LHL_read_write(16, 16),
        LHL_read_write(64, 64),
        LHL_read_write(128, 128),
    ];

    for protocol in protocols {
        for thread in threads {
            for lambda in 0..lambdas.len() {
                for updates_thresh_hold in 0..updates.len() {
                    mixed_test_new(
                        protocol.clone(),
                        data_lambdas[lambda].as_slice(),
                        key_range.clone(),
                        thread,
                        lambdas[lambda],
                        repeats,
                        updates[updates_thresh_hold])
                }
            }
        }
    }
}

fn mixed_test_new(
    ls: CRUDProtocol,
    data: &[CRUDOperation<Key, Payload>],
    key_range: RangeInclusive<Key>,
    threads: usize,
    lambda: f64,
    runs: usize,
    updates_thresh_hold: f64,
) {
    let operations_count
        = data.len();

    let operation_per_thread
        = operations_count / threads;

    let tree
        = TREE(ls.clone());

    let (create_time, dups) = if ls.is_mono_writer() {
        insert(1, tree.clone(), data)
    } else {
        insert(16, tree.clone(), data)
    };

    let mut rnd = StdRng::seed_from_u64(90501960);
    let operations = thread_rng()
        .sample_iter(Uniform::new(0_f64, 1_f64))
        .take(operations_count)
        .map(|t| {
            let key = gen_rand_key(
                data.len() as _,
                *key_range.start(),
                *key_range.end(),
                lambda,
                &mut rnd);

            if t <= updates_thresh_hold {
                CRUDOperation::Update(key, Payload::default())
            } else {
                CRUDOperation::Point(key)
            }
        })
        .chunks(operation_per_thread)
        .into_iter()
        .map(|chunk| Arc::new(chunk.collect::<Vec<_>>()))
        .collect::<Vec<_>>();

    let actual_updates_count = operations
        .iter()
        .map(|u| u
            .iter()
            .map(|op|
                if let CRUDOperation::Update(..) = op { 1 } else { 0 })
            .sum::<usize>())
        .sum::<usize>();

    let actual_reads_count = operations
        .iter()
        .map(|u| u
            .iter()
            .map(|op|
                if let CRUDOperation::Point(..) = op { 1 } else { 0 })
            .sum::<usize>())
        .sum::<usize>();

    let worker = |which: usize| {
        let u_tree
            = tree.clone();

        let working_queue
            = operations.get(which).unwrap().clone();

        spawn(move || working_queue
            .iter()
            .for_each(|op| { u_tree.dispatch(op.clone()); })
        )
    };

    for run in 1..=runs {
        let start = SystemTime::now();
        (0..threads)
            .map(|which| (worker)(which))
            .collect::<Vec<_>>()
            .drain(..)
            .for_each(|handle| handle.join().unwrap());

        // assert_eq!(data.len(), actual_reads_count + actual_updates_count);
        println!("{},{},{},{},{},{},{},{},{},{},{},{}",
                 operations_count,
                 threads,
                 ls.clone(),
                 create_time,
                 dups,
                 lambda,
                 run,
                 SystemTime::now().duration_since(start).unwrap().as_millis(),
                 updates_thresh_hold,
                 actual_updates_count,
                 actual_reads_count,
                 actual_reads_count + actual_updates_count);
    }
}

pub fn gen_data_exp(limit: u64, lambda: f64, rnd: &mut StdRng) -> Vec<u64> {
    (1..=limit)
        .map(|i|
            gen_rand_key(i, 0, i, lambda, rnd))
        .collect()
}

pub fn gen_rand_key(i: u64, range_start: u64, range_end: u64, lambda: f64, rnd: &mut StdRng) -> u64 {
    #[inline(always)]
    fn sample_next(lambda: f64, rnd: &mut StdRng) -> f64 {
        let num
            = rnd.gen_range(0_f64..1_f64);

        (1_f64 - num)
            .ln()
            .div(-lambda)
    }

    let range = range_end - range_start;

    (((loop {
        let key = i as f64 * (1_f64 - sample_next(lambda, rnd));
        if key >= 0_f64 {
            break key;
        }
    }) / range as f64) * u64::MAX as f64) as _
}