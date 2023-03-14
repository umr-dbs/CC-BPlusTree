use std::{fs, mem};
use chrono::{DateTime, Local};
use itertools::Itertools;
use crate::block::block_manager::bsz_alignment;
use crate::tree::bplus_tree;
use crate::locking::locking_strategy::{OLCVariant, LockingStrategy};
use block::block::Block;
use crate::page_model::LevelVariant;
use crate::test::{beast_test, BSZ_BASE, EXE_LOOK_UPS, EXE_RANGE_LOOK_UPS, FAN_OUT, format_insertsions, gen_rand_data, Key, log_debug, log_debug_ln, MAKE_INDEX, NUM_RECORDS, Payload, simple_test};
use crate::utils::smart_cell::CPU_THREADS;

mod block;
mod crud_model;
mod locking;
mod page_model;
mod record_model;
mod tree;
mod utils;
mod test;

fn main() {
    make_splash();
    // show_alignment_bsz();

    // simple_test();
    experiment();
}

fn show_alignment_bsz() {
    log_debug_ln(format!("\t- Block Size: \t\t{} bytes\n\t\
        - Block Align-Size: \t{} bytes\n\t\
        - Block/Delta: \t\t{}/{} bytes\n\t\
        - Num Keys: \t\t{}\n\t\
        - Fan Out: \t\t{}\n\t\
        - Num Records: \t\t{}\n",
                         BSZ_BASE,
                         bsz_alignment::<Key, Payload>(),
                         mem::size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>(),
                         BSZ_BASE - mem::size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>(),
                         FAN_OUT - 1,
                         FAN_OUT,
                         NUM_RECORDS)
    );
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

fn experiment() {
    let mut threads_cpu = vec![
        // 1,
        // 2,
        // 3,
        // 4,
        // 8,
        // 10,
        // 12,
        16,
        24,
        32,
        64,
        128,
        256,
        512,
        1024,
    ];

    if CPU_THREADS {
        let cpu = num_cpus::get();
        threads_cpu = threads_cpu
            .into_iter()
            .take_while(|t| cpu >= *t)
            .collect();
    }

    let insertions: Vec<Key> = vec![
        // 10,
        // 100,
        // 1_000,
        10_000,
        // 100_000,
        // 1_000_000,
        // 2_000_000,
        // 5_000_000,
        10_000_000,
        20_000_000,
        // 50_000_000,
        // 100_000_000,
    ];

    log_debug_ln(format!("Preparing {} Experiments, hold on..", insertions.len()));

    let mut strategies = vec![];
    // strategies.push(LockingStrategy::LockCoupling);
    //
    // strategies.push(LockingStrategy::optimistic_custom(
    //     LevelVariant::new_height_lock(1_f32), 1));
    // strategies.push(LockingStrategy::optimistic_custom(
    //     LevelVariant::new_height_lock(1_f32), 3));
    // strategies.push(LockingStrategy::optimistic_custom(
    //     LevelVariant::new_height_lock(1_f32), 10));

    // strategies.push(LockingStrategy::OLC(
    //     LevelConstraints::OptimisticLimit { attempts: 1, level: LevelVariant::new_height_lock(1_f32) }));
    //
    // strategies.push(LockingStrategy::OLC(
    //     LevelConstraints::OptimisticLimit { attempts: 3, level: LevelVariant::new_height_lock(1_f32) }));
    //
    // strategies.push(LockingStrategy::RWLockCoupling(
    //     LevelVariant::new_height_lock(1 as _),
    //     4));
    // //
    // strategies.push(LockingStrategy::RWLockCoupling(
    //     LevelVariant::new_height_lock(0.8 as _),
    //     2));

    // strategies.push(LockingStrategy::OLC(OLCVariant::WriterLimit {
    //     attempts: 4,
    //     level: LevelVariant::default(),
    // }));


    // strategies.push(LockingStrategy::OLC(OLCVariant::Free));
    strategies.push(LockingStrategy::OLC(OLCVariant::Pinned {
        attempts: 0,
        level: LevelVariant::default()
    }));

    // strategies.push(LockingStrategy::RWLockCoupling(
    //     LevelVariant::new_height_lock(1 as _),
    //     1_00));

    insertions.iter().enumerate().for_each(|(i, insertion)| {
        log_debug_ln(format!("# {}\n\t\
        - Records: \t\t{}\n\t\
        - Threads: \t\t{}",
                             i + 1,
                             format_insertsions(*insertion),
                             threads_cpu.iter().join(",")));

        log_debug(format!("\t- Strategy:"));
        if threads_cpu.contains(&1) {
            println!("\t\t{}", LockingStrategy::MonoWriter);
            strategies
                .iter()
                .for_each(|st| log_debug_ln(format!("\t\t\t\t{}", st)))
        } else {
            println!("\t\t{}", strategies[0]);
            (&strategies[1..])
                .iter()
                .for_each(|st| log_debug_ln(format!("\t\t\t\t{}", st)))
        }
    });


    log_debug_ln(format!("Preparing data, hold on.."));
    let cases = insertions
        .into_iter()
        .map(|n| {
            println!("> Preparing n = {} data, hold on..", n);

            let data = gen_rand_data(n as usize);
            println!("> Completed n = {} data", n);

            (data, strategies.clone())
        }).collect::<Vec<_>>();

    mem::drop(strategies);

    // thread::sleep(Duration::from_secs(4));
    println!("Number Insertions,Number Threads,Locking Strategy,Height,Time,Fan Out,Leaf Records,Block Size");

    cases.into_iter().for_each(|(t1s, strats)|
        for num_threads in threads_cpu.iter() {
            if *num_threads > t1s.len() {
                log_debug_ln("WARNING: Number of Threads larger than number of Transactions!".to_string());
                log_debug_ln(format!("WARNING: Skipping Transactions = {}, Threads = {}!", t1s.len(), num_threads));
                continue;
            }
            if *num_threads == 1 {
                if EXE_LOOK_UPS {
                    log_debug_ln(format!("Warning: Look-up queries enabled!"))
                }
                if EXE_RANGE_LOOK_UPS {
                    log_debug_ln(format!("Warning: Range queries enabled!"))
                }

                print!("{}", t1s.len());
                print!(",{}", *num_threads);

                let index = MAKE_INDEX(LockingStrategy::MonoWriter);

                let time = beast_test(
                    1,
                    index,
                    t1s.as_slice());

                print!(",{}", time);
                print!(",{}", FAN_OUT);
                print!(",{}", NUM_RECORDS);
                println!(",{}", BSZ_BASE);
            }

            for ls in strats.iter() {
                if EXE_LOOK_UPS {
                    log_debug_ln(format!("Warning: Look-up queries enabled!"))
                }
                if EXE_RANGE_LOOK_UPS {
                    log_debug_ln(format!("Warning: Range queries enabled!"))
                }

                print!("{}", t1s.len());
                print!(",{}", *num_threads);

                let index = MAKE_INDEX(ls.clone());

                let time = beast_test(
                    *num_threads,
                    index,
                    t1s.as_slice());

                print!(",{}", time);
                print!(",{}", FAN_OUT);
                print!(",{}", NUM_RECORDS);
                println!(",{}", BSZ_BASE);

                // thread::sleep(Duration::from_millis(200));
            }
        });
}