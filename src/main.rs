use std::{fs, mem};
use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::locking::locking_strategy::{Attempts, LevelVariant, LockingStrategy};
use chrono::{DateTime, Local};
use itertools::Itertools;
use mvcc_bplustree::index::record::Payload;
use crate::index::bplus_tree;
use crate::index::bplus_tree::Index;
use crate::index::cclocking_strategy::{CCLockingStrategy, LevelConstraints};
use crate::index::settings::{BlockSettings, CONFIG_INI_PATH, init_from_config_ini, load_config};
use crate::test::{beast_test, EXE_LOOK_UPS, format_insertsions, gen_rand_data, level_order, log_debug, log_debug_ln, simple_test};

mod index;
mod transaction;
mod utils;
mod block;
mod test;

fn main() {
    make_splash();

    // simple_test();
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

    let threads_cpu = vec![
        1,
        2,
        3,
        4,
        8,
        16,
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
    // strategies.push(LockingStrategy::WriteCoupling);
    //
    // strategies.push(LockingStrategy::optimistic_custom(
    //     LevelVariant::new_height_lock(1_f32), 1));
    // strategies.push(LockingStrategy::optimistic_custom(
    //     LevelVariant::new_height_lock(1_f32), 3));
    // strategies.push(LockingStrategy::optimistic_custom(
    //     LevelVariant::new_height_lock(1_f32), 10));

    strategies.push(CCLockingStrategy::OLC(
        LevelConstraints::OptimisticLimit { attempts: 1, level: LevelVariant::new_height_lock(1_f32) }));

    strategies.push(CCLockingStrategy::OLC(
        LevelConstraints::OptimisticLimit { attempts: 3, level: LevelVariant::new_height_lock(1_f32) }));

    strategies.push(CCLockingStrategy::OLC(
        LevelConstraints::OptimisticLimit { attempts: 10, level: LevelVariant::new_height_lock(1_f32) }));

    strategies.push(CCLockingStrategy::OLC(LevelConstraints::None));

    bszs.clone().enumerate().for_each(|(b_i, bsz)| {
        insertions.iter().enumerate().for_each(|(i, insertion)| {
            log_debug_ln(format!("# {}.{}\n\t- Records: \t{}\n\t- Threads: \t{}\n\t- Block Size: \t{} kb",
                                 i + 1, b_i + 1,
                                 format_insertsions(*insertion),
                                 threads_cpu.iter().join(","),
                                 bsz / 1024)
            );

            log_debug(format!("\t- Strategy:"));
            if threads_cpu.contains(&1) {
                println!("\t{}", CCLockingStrategy::MonoWriter);
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

    println!("Number Insertions,Number Threads,Locking Strategy,Height,Time,Block Size");

    let index_anker
        = init_from_config_ini(); // anker

    let payload_anker: Payload = serde_json::from_str(
        load_config(CONFIG_INI_PATH, true)
            .get("payload")
            .unwrap()
    ).unwrap(); // anker layout

    bszs.for_each(|bsz| {
        cases.iter().for_each(|(t1s, strats)|
            for num_threads in threads_cpu.iter() {
                if *num_threads == 1 {
                    if EXE_LOOK_UPS {
                        log_debug_ln(format!("Warning: Look-up queries enabled!"))
                    }

                    print!("{}", t1s.len());
                    print!(",{}", *num_threads);

                    let index = Index::make(BlockSettings::new(
                        bsz,
                        index_anker.block_manager.is_multi_version,
                        payload_anker.clone(),
                    ).into(), CCLockingStrategy::MonoWriter);

                    let time = beast_test(
                        1,
                        index,
                        t1s.as_slice());

                    print!(",{}", time);
                    println!(",{}", bsz);
                } else {
                    for ls in strats.iter() {
                        if EXE_LOOK_UPS {
                            log_debug_ln(format!("Warning: Look-up queries enabled!"))
                        }

                        print!("{}", t1s.len());
                        print!(",{}", *num_threads);

                        let mut index = Index::make(BlockSettings::new(
                            bsz,
                            index_anker.block_manager.is_multi_version,
                            payload_anker.clone(),
                        ).into(), ls.clone());

                        let time = beast_test(
                            *num_threads,
                            index,
                            t1s.as_slice());

                        print!(",{}", time);
                        println!(",{}", bsz);

                        // thread::sleep(Duration::from_millis(200));
                    }
                }
            });
    });
}