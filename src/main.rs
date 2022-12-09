use std::{fs, mem};
use chrono::{DateTime, Local};
use itertools::Itertools;
use TXDataModel::page_model::LevelVariant;
use crate::index::bplus_tree;
use crate::index::settings::{CONFIG_INI_PATH, init_from_config_ini, load_config};
use crate::locking::locking_strategy::{LevelConstraints, LockingStrategy};
use crate::test::{beast_test, EXE_LOOK_UPS, format_insertsions, gen_rand_data, Key, level_order, log_debug, log_debug_ln, MAKE_INDEX, Payload, simple_test, simple_test2};

mod index;
mod transaction;
mod block;
mod test;
mod locking;

fn main() {
    make_splash();

    simple_test();
    // simple_test2();
    experiment();
    //
    // experiment2();
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
    let cpu_threads = true;
    test::show_bsz_alignment();
    let threads_cpu = vec![
        1,
        2,
        // 3,
        4,
        8,
        16,
        24,
        32,
        64,
        128,
        // 256,
        // 512,
        // 1024,
    ];

    // let mut threads_cpu = (1..=usize::max(num_cpus::get(), *threads_cpu.last().unwrap()))
    //     .collect::<Vec<_>>();
    //
    // if cpu_threads {
    //     threads_cpu = (1..=num_cpus::get()).collect();
    // }

    let insertions: Vec<Key> = vec![
        // 10,
        // 100,
        // 1_000,
        // 10_000,
        // 100_000,
        // 1_000_000,
        // 2_000_000,
        // 5_000_000,
        // 10_000_000,
        // 20_000_000,
        // 50_000_000,
        100_000_000,
    ];

    let bszs = vec![
        // 1,
        // 2,
        // 3,
        4,
        // 8,
        // 10,
        // 12,
        // 14,
        // 16,
        // 32,
    ].into_iter().map(|bsz| bsz * 1024);

    log_debug_ln(format!("Preparing {} Experiments, hold on..", insertions.len() * bszs.clone().len()));

    let mut strategies = vec![];
    strategies.push(LockingStrategy::LockCoupling);
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
    // strategies.push(LockingStrategy::OLC(
    //     LevelConstraints::OptimisticLimit { attempts: 10, level: LevelVariant::new_height_lock(1_f32) }));

    strategies.push(LockingStrategy::OLC(LevelConstraints::Unlimited));

    strategies.push(LockingStrategy::RWLockCoupling(
        LevelVariant::new_height_lock(1 as _),
        4));

    // strategies.push(LockingStrategy::RWLockCoupling(
    //     LevelVariant::new_height_lock(1 as _),
    //     10));

    // strategies.push(LockingStrategy::RWLockCoupling(
    //     LevelVariant::new_height_lock(1 as _),
    //     1_00));

    bszs.clone().enumerate().for_each(|(b_i, bsz)| {
        insertions.iter().enumerate().for_each(|(i, insertion)| {
            log_debug_ln(format!("# {}.{}\n\t- Records: \t{}\n\t- Threads: \t{}\n\t- Block Size: \t{} kb",
                                 b_i + 1,
                                 i + 1,
                                 format_insertsions(*insertion),
                                 threads_cpu.iter().join(","),
                                 bsz / 1024)
            );

            log_debug(format!("\t- Strategy:"));
            if threads_cpu.contains(&1) {
                println!("\t{}", LockingStrategy::MonoWriter);
                strategies
                    .iter()
                    .for_each(|st| log_debug_ln(format!("\t\t\t{}", st)))
            } else {
                log_debug_ln(format!("\t{}", strategies[0]));
                (&strategies[1..])
                    .iter()
                    .for_each(|st| log_debug_ln(format!("\t\t\t{}", st)))
            }
        });
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
    println!("Number Insertions,Number Threads,Locking Strategy,Height,Time,Block Size");

    bszs.for_each(|bsz| {
        cases.iter().for_each(|(t1s, strats)|
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

                    print!("{}", t1s.len());
                    print!(",{}", *num_threads);

                    let index = MAKE_INDEX(LockingStrategy::MonoWriter);

                    let time = beast_test(
                        1,
                        index,
                        t1s.as_slice());

                    print!(",{}", time);
                    println!(",{}", bsz);
                }

                for ls in strats.iter() {
                    if EXE_LOOK_UPS {
                        log_debug_ln(format!("Warning: Look-up queries enabled!"))
                    }

                    print!("{}", t1s.len());
                    print!(",{}", *num_threads);

                    let index = MAKE_INDEX(ls.clone());

                    let time = beast_test(
                        *num_threads,
                        index,
                        t1s.as_slice());

                    print!(",{}", time);
                    println!(",{}", bsz);

                    // thread::sleep(Duration::from_millis(200));
                }
            });
    });
}