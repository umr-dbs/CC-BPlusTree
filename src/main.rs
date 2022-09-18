use std::{fs, mem};
use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::locking::locking_strategy::{LevelVariant, LockingStrategy};
use chrono::{DateTime, Local};
use itertools::Itertools;
use crate::index::bplus_tree;
use crate::index::bplus_tree::Index;
use crate::test::{beast_test, EXE_LOOP_UPS, format_insertsions, gen_rand_data, level_order, log_debug, log_debug_ln, simple_test};

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
        512,
        1024,
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
        // 1_000_000,
        // 2_000_000,
        // 5_000_000,
        10_000_000,
        // 20_000_000,
        // 50_000_000,
        // 100_000_000,
    ];

    log_debug_ln(format!("Preparing {} Experiments, hold on..", insertions.len()));

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

        strategies.push(LockingStrategy::optimistic_custom(
            LevelVariant::new_height_lock(0.9_f32), attempt));
        strategies.push(LockingStrategy::dolos_custom(
            LevelVariant::new_height_lock(0.9_f32), attempt));

        strategies.push(LockingStrategy::optimistic_custom(
            LevelVariant::new_height_lock(1_f32), attempt));
        strategies.push(LockingStrategy::dolos_custom(
            LevelVariant::new_height_lock(1_f32), attempt));
    }

    insertions.iter().enumerate().for_each(|(i, insertion)| {
        log_debug_ln(format!("# {} - Experiment:\n\t- Records: \t{}\n\t- Threads: \t{}",
                             i + 1,
                             format_insertsions(*insertion),
                             threads_cpu.iter().join(","))
        );

        log_debug(format!("\t- Strategy:"));
        if threads_cpu.contains(&1) {
            println!("\t{}", LockingStrategy::SingleWriter);
            strategies
                .iter()
                .for_each(|st| log_debug_ln(format!("\t\t\t{}", st)))
        }
        else {
            log_debug_ln(format!("\t{}", strategies[0]));
            (&strategies[1..])
                .iter()
                .for_each(|st| log_debug_ln(format!("\t\t\t{}", st)))
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

    println!("Number Insertions,Number Threads,Locking Strategy,Height,Time");

    cases.iter().for_each(|(t1s, strats)|
        for num_threads in threads_cpu.iter() {
            if *num_threads == 1 {
                if EXE_LOOP_UPS {
                    log_debug_ln(format!("Warning: Look-up queries enabled!"))
                }

                print!("{}", t1s.len());
                print!(",{}", *num_threads);

                let index
                    = Index::new_single_version_for(LockingStrategy::SingleWriter);

                let time = beast_test(
                    1,
                    index,
                    t1s.as_slice());

                println!(",{}", time);

                if EXE_LOOP_UPS {
                    log_debug_ln(format!("Warning: Look-up queries enabled!"))
                }

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
                    if EXE_LOOP_UPS {
                        log_debug_ln(format!("Warning: Look-up queries enabled!"))
                    }

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