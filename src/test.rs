use std::collections::{HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::{Add, Deref, DerefMut, Div, RangeInclusive, Sub};
use std::thread::spawn;
use std::time::{Duration, SystemTime};
use itertools::Itertools;
use rand::{Rng, RngCore, SeedableRng, thread_rng};
use rand::rngs::StdRng;
use crate::block::block_manager::{_4KB, bsz_alignment};
use crate::bplus_tree::BPlusTree;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::locking::locking_strategy::{CRUDProtocol, hybrid_lock, lightweight_hybrid_lock, lightweight_hybrid_lock_read_attempts, lightweight_hybrid_lock_unlimited, lightweight_hybrid_lock_write_attempts, lightweight_hybrid_lock_write_read_attempts, LockingStrategy, olc, orwc, orwc_attempts};
use crate::{TREE, Tree};
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;

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
pub fn beast_test2(num_thread: usize, p_index: Tree, t1s: &[CRUDOperation<Key, Payload>]) -> (u128, u64) {
    let mut data_buff = t1s
        .iter()
        .chunks(t1s.len() / num_thread)
        .into_iter()
        .map(|s| s.into_iter().cloned().collect::<Vec<_>>())
        .collect::<VecDeque<_>>();

    if data_buff.len() > num_thread {
        let back = data_buff.pop_back().unwrap();
        data_buff.front_mut().unwrap().extend(back);
    }

    let mut handles
        = Vec::with_capacity(num_thread);

    let start = SystemTime::now();
    for _ in 1..=num_thread {
        let current_chunk
            = data_buff.pop_front().unwrap();

        let index = p_index.clone();
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
    println!("Number Records,Number Threads,Locking Strategy,Create Time,Duplicates Count,Lambda,Run");

    let number_records
        = 10_000_000;

    let repeats
        = 10_usize;

    let threads
        = [1, 2, 4, 6, 8, 16, 20, 24, 32, 40, 56, 64, 72, 80, 96, 128];

    let lambdas
        = [0.1_f64, 16_f64, 32_f64, 64_f64, 128_f64, 256_f64, 512_f64, 1024_f64];

    let locking_protocols = [
        // MonoWriter,
        // LockCoupling,
        // orwc_attempts(0),
        // orwc_attempts(1),
        // orwc_attempts(4),
        // orwc_attempts(16),
        // orwc_attempts(64),
        // orwc_attempts(128),
        // olc(),
        // // lightweight_hybrid_lock_unlimited(),
        // // lightweight_hybrid_lock_read_attempts(0),
        // // lightweight_hybrid_lock_read_attempts(1),
        // // lightweight_hybrid_lock_read_attempts(4),
        // // lightweight_hybrid_lock_read_attempts(16),
        // // lightweight_hybrid_lock_read_attempts(64),
        // lightweight_hybrid_lock_write_attempts(0),
        // lightweight_hybrid_lock_write_attempts(1),
        // lightweight_hybrid_lock_write_attempts(4),
        // lightweight_hybrid_lock_write_attempts(16),
        // lightweight_hybrid_lock_write_attempts(64),
        // lightweight_hybrid_lock_write_attempts(128),
        // lightweight_hybrid_lock_write_read_attempts(0, 0),
        hybrid_lock(),
    ];

    let data_lambdas = lambdas.iter().map(|lambda| {
        let mut rnd = StdRng::seed_from_u64(90501960);
        gen_data_exp(number_records, *lambda, &mut rnd)
            .into_iter()
            .map(|key|
                CRUDOperation::Insert(key, Payload::default()))
            .collect::<Vec<_>>()
    }).collect::<Vec<_>>();

    for protocol in locking_protocols {
        for create_threads in threads {
            for lambda in 0..lambdas.len() {
                for run in 1..=repeats {
                    print!("{}", number_records);
                    print!(",{}", create_threads);
                    print!(",{}", protocol);

                    let (time, dups) = beast_test2(
                        create_threads,
                        TREE(protocol.clone()),
                        data_lambdas[lambda].as_slice());

                    println!(",{},{},{},{}", time, dups, lambdas[lambda], run);
                }
            }
        }
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