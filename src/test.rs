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
use crate::utils::interval::Interval;

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
pub fn bulk_crud(worker_threads: usize, tree: Tree, operations_queue: &[CRUDOperation<Key, Payload>]) -> (u128, u64) {
    let mut data_buff = operations_queue
        .iter()
        .chunks(operations_queue.len() / worker_threads)
        .into_iter()
        .map(|s| s.into_iter().cloned().collect::<Vec<_>>())
        .collect::<VecDeque<_>>();

    if data_buff.len() > worker_threads {
        let back = data_buff.pop_back().unwrap();
        data_buff.front_mut().unwrap().extend(back);
    }

    let mut handles
        = Vec::with_capacity(worker_threads);

    let start = SystemTime::now();
    for _ in 1..=worker_threads {
        let current_chunk
            = data_buff.pop_front().unwrap();

        let index = tree.clone();
        handles.push(spawn(move || {
            let mut counter_errs = 0;
            current_chunk
                .into_iter()
                .for_each(|next_query| match index.dispatch(next_query) { // tree.execute(operation),
                    CRUDOperationResult::Error => counter_errs += 1,
                    _ => {}
                });
            counter_errs
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
    println!("Records,Threads,Protocol,Create Time,Dupes,Lambda,Run,Mixed Time,U-TH,Updates,Reads,Ranges,Range Offset,RQ-TH,Total");

    const ENABLE_CASES: bool
    = true;

    const N: u64
    = 100_000;

    const KEY_RANGE: RangeInclusive<Key>
    = 1..=N;

    const REPEATS: usize
    = 3;

    const UPDATES_THRESHOLD: [f64; 5]
    = [0.1, 0.3, 0.5, 0.7, 0.9];

    const THREADS: [usize; 15]
    = [1, 2, 4, 5, 8, 16, 20, 25, 32, 40, 50, 64, 80, 100, 125];

    const LAMBDAS: [f64; 8]
    = [0.1_f64, 16_f64, 32_f64, 64_f64, 128_f64, 256_f64, 512_f64, 1024_f64];

    const RQ_PROBABILITY: [f64; 5]
    = [0.0, 0.1, 0.5, 0.9, 1.0];

    const RQ_OFFSET: [u64; 2] = [
        4 * (NUM_RECORDS as u64 + 1_u64),
        64 * (NUM_RECORDS as u64 + 1_u64),
    ];

    let data_lambdas = LAMBDAS
        .iter()
        .map(|lambda| {
            let mut rnd = StdRng::seed_from_u64(90501960);
            gen_data_exp(N, *lambda, &mut rnd)
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

    let cases = vec![
        (&LAMBDAS[0], &UPDATES_THRESHOLD[2], &RQ_PROBABILITY[0], RQ_OFFSET[0]),
        (&LAMBDAS[0], &UPDATES_THRESHOLD[2], &RQ_PROBABILITY[1], RQ_OFFSET[0]),
        (&LAMBDAS[0], &UPDATES_THRESHOLD[2], &RQ_PROBABILITY[0], RQ_OFFSET[1]),
        (&LAMBDAS[0], &UPDATES_THRESHOLD[2], &RQ_PROBABILITY[3], RQ_OFFSET[0]),
        (&LAMBDAS[0], &UPDATES_THRESHOLD[3], &RQ_PROBABILITY[2], RQ_OFFSET[1]),
        (&LAMBDAS[0], &UPDATES_THRESHOLD[4], &RQ_PROBABILITY[3], RQ_OFFSET[0]),
        (&LAMBDAS[4], &UPDATES_THRESHOLD[0], &RQ_PROBABILITY[1], RQ_OFFSET[0]),
        (&LAMBDAS[6], &UPDATES_THRESHOLD[1], &RQ_PROBABILITY[2], RQ_OFFSET[1]),
        (&LAMBDAS[7], &UPDATES_THRESHOLD[0], &RQ_PROBABILITY[1], RQ_OFFSET[0]),
        (&LAMBDAS[7], &UPDATES_THRESHOLD[1], &RQ_PROBABILITY[3], RQ_OFFSET[0]),
    ];

    for protocol in protocols {
        for lambda in 0..LAMBDAS.len() {
            if cases.iter().find(|(l, ..)| &LAMBDAS[lambda] == *l).is_none() {
                continue
            }
            let tree
                = TREE(protocol.clone());

            let (create_time, errs) = if protocol.is_mono_writer() {
                bulk_crud(1,
                          tree.clone(),
                          data_lambdas[lambda].as_slice())
            } else {
                bulk_crud(16,
                          tree.clone(),
                          data_lambdas[lambda].as_slice())
            };

            for thread in THREADS {
                for ut in UPDATES_THRESHOLD {
                    for rq in RQ_PROBABILITY {
                        for rq_off in RQ_OFFSET {
                            if ENABLE_CASES && cases.iter().find(|(_, uth, rqt, off, ..)|
                                &ut == *uth && &rq == *rqt && rq_off == *off).is_none()
                            {
                                continue
                            }

                            mixed_test_new(
                                create_time,
                                errs,
                                protocol.clone(),
                                tree.clone(),
                                N,
                                KEY_RANGE.clone(),
                                thread,
                                LAMBDAS[lambda],
                                REPEATS,
                                ut,
                                rq,
                                rq_off)
                        }
                    }
                }
            }
        }
    }
}

fn mixed_test_new(
    create_time: u128,
    dups: u64,
    ls: CRUDProtocol,
    tree: Tree,
    n: u64,
    key_range: RangeInclusive<Key>,
    threads: usize,
    lambda: f64,
    runs: usize,
    updates_thresh_hold: f64,
    rq_probability: f64,
    rq_offset: Key,
) {
    let operations_count
        = n as usize;

    let operation_per_thread
        = operations_count / threads;

    let mut rnd
        = StdRng::seed_from_u64(0x3A5F72B9C81D4EF2);

    let mut gen_key = || gen_rand_key(
        n,
        *key_range.start(),
        *key_range.end(),
        lambda,
        &mut rnd);

    let operations = thread_rng()
        .sample_iter(Uniform::new(0_f64, 1_f64))
        .take(operations_count)
        .collect::<Vec<_>>()
        .into_iter()
        .map(|t| {
            let key = gen_key();

            if t <= updates_thresh_hold {
                CRUDOperation::Update(key, Payload::default())
            } else {
                if thread_rng().gen_bool(rq_probability) {
                    match key.checked_add(rq_offset) {
                        None => {
                            let key1 = key.sub(rq_offset);
                            CRUDOperation::Range(Interval::new(
                                key1,
                                key))
                        }
                        Some(key1) => CRUDOperation::Range(Interval::new(
                            key,
                            key1))
                    }
                } else {
                    CRUDOperation::Point(key)
                }
            }
        })
        .chunks(operation_per_thread)
        .into_iter()
        .map(|chunk| Arc::new(chunk.collect::<Vec<_>>()))
        .collect::<Vec<_>>();

    let (actual_reads_count, actual_rq_count, actual_updates_count) = operations
        .iter()
        .fold((0usize, 0usize, 0usize), |(p, r, u), inner| {
            let (n_p, n_r, n_u) = inner
                .iter()
                .fold((0usize, 0usize, 0usize), |(p, r, u), op|
                    match op {
                        CRUDOperation::Point(..) => (p + 1, r, u),
                        CRUDOperation::Range(..) => (p, r + 1, u),
                        _ => (p, r, u + 1)
                    });
            (n_p + p, n_r + r, n_u + u)
        });
    let worker = |which: usize| {
        let u_tree
            = tree.clone();

        let working_queue
            = operations.get(which).unwrap().clone();

        spawn(move || working_queue
            .iter()
            .for_each(|op| { u_tree.dispatch(op.clone()); }))
    };

    for run in 1..=runs {
        let start = SystemTime::now();
        (0..threads)
            .map(|which| (worker)(which))
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|handle| handle.join().unwrap());

        println!("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
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
                 actual_rq_count,
                 rq_offset,
                 rq_probability,
                 actual_reads_count + actual_rq_count + actual_updates_count);
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