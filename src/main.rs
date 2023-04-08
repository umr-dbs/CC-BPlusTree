use std::{env, fs, mem, path, thread};
use std::ops::{Deref, Not};
use std::ptr::null;
use std::sync::Arc;
use std::time::SystemTime;
use chrono::{DateTime, Local};
use itertools::Itertools;
use parking_lot::Mutex;
use rand::prelude::SliceRandom;
use serde::{Deserialize, Serialize};
use crate::block::block_manager::bsz_alignment;
use crate::tree::bplus_tree;
use crate::locking::locking_strategy::{CRUDProtocol, LockingStrategy};
use block::block::Block;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::locking::locking_strategy::LockingStrategy::{LockCoupling, MonoWriter};
use crate::test::{beast_test, BSZ_BASE, EXE_LOOK_UPS, EXE_RANGE_LOOK_UPS, FAN_OUT, format_insertions, gen_rand_data, get_system_info, INDEX, Key, log_debug, log_debug_ln, MAKE_INDEX, NUM_RECORDS, Payload, S_INSERTIONS, S_STRATEGIES, S_THREADS_CPU, simple_test, SyncIndex};
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::{CPU_THREADS, ENABLE_YIELD};

mod block;
mod crud_model;
mod locking;
mod page_model;
mod record_model;
mod tree;
mod utils;
mod test;

fn main() {
    // println!("size = {}", mem::size_of::<SmartFlavor<()>>());
    // make_splash();
    // show_alignment_bsz();
    do_tests();
    // simple_test();
    // experiment();
}

fn do_tests() {
    let mut args: Vec<String>
        = env::args().collect();

    if args.len() > 1 {
        let raw = args
            .pop()
            .unwrap()
            .split_whitespace()
            .collect::<String>();

        let (params, command) = if raw.contains("=") {
            let mut command_salad = raw.split("=").collect::<Vec<_>>();
            (command_salad.pop().unwrap(), command_salad.pop().unwrap())
        } else {
            ("", raw.as_str())
        };

        match command {
            "crud_protocol" | "crud_protocols" | "crud" | "cruds" | "protocol" | "protocols" =>
                println!("{}", S_STRATEGIES
                    .as_slice()
                    .iter()
                    .map(|s| format!("Name: \t`{}`\nObject: `{}`",
                                     s,
                                     serde_json::to_string(s).unwrap()))
                    .join("\n******************************************************************\n")),
            "info" | "system" | "sys" => println!("{}", get_system_info()),
            "make_splash" | "splash" =>
                make_splash(),
            "yield_enabled" | "yield" =>
                println!("yield_enabled: {}", ENABLE_YIELD),
            "cpu_cores" | "cpu_threads" | "cpu" =>
                println!("Cores/Threads: {}/{}", num_cpus::get_physical(), num_cpus::get()),
            "simple_test" | "st" =>
                simple_test(),
            "create" | "c" => {
                let mut p = params.split("+");
                let inserts = serde_json::from_str::<Vec<Key>>(p.next().unwrap_or(""))
                    .unwrap_or(S_INSERTIONS.to_vec());

                let mut crud_str
                    = p.next().unwrap_or_default().to_string();

                if crud_str.contains("MonoWriter") && !crud_str.contains("\"MonoWriter\"") {
                    crud_str = crud_str.replace("MonoWriter", "\"MonoWriter\"");
                }
                if crud_str.contains("LockCoupling") && !crud_str.contains("\"LockCoupling\"") {
                    crud_str = crud_str.replace("LockCoupling", "\"LockCoupling\"");
                }

                let threads
                    = serde_json::from_str::<Vec<usize>>(p.next().unwrap_or_default())
                    .unwrap_or(S_THREADS_CPU.to_vec());

                let crud = serde_json::from_str::<Vec<CRUDProtocol>>(crud_str.as_str())
                    .unwrap_or(S_STRATEGIES.to_vec());

                experiment(threads,
                           inserts,
                           crud, )
            },
            "update_read" | "ur" => { //update=
                // tree_records+
                // update_records+
                // [CRUD,..]+
                // [t0,..]

                log_debug_ln(format!("Running `{}={}`", command, params));

                let mut params
                    = params.split("+");

                let tree_records
                    = params.next().unwrap().parse::<usize>().unwrap();

                let update_records
                    = params.next().unwrap().parse::<f32>().unwrap();

                let mut crud_str
                    = params.next().unwrap_or_default().to_string();

                let threads
                    = serde_json::from_str::<Vec<usize>>(params.next().unwrap_or_default())
                    .unwrap_or(S_THREADS_CPU.to_vec());

                if crud_str.contains("MonoWriter") && !crud_str.contains("\"MonoWriter\"") {
                    crud_str = crud_str.replace("MonoWriter", "\"MonoWriter\"");
                }

                if crud_str.contains("LockCoupling") && !crud_str.contains("\"LockCoupling\"") {
                    crud_str = crud_str.replace("LockCoupling", "\"LockCoupling\"");
                }

                let crud = serde_json::from_str::<Vec<CRUDProtocol>>(crud_str.as_str())
                    .unwrap_or(S_STRATEGIES.to_vec());

                log_debug_ln(format!("CRUD = `{}` ", crud.as_slice().iter().join(",")));
                log_debug_ln(format!("Threads = `{}` ", threads.as_slice().iter().join(",")));

                let update_records
                    = (update_records * tree_records as f32) as usize;

                log_debug_ln(format!("Records = `{}`, Updates = `{}` ",
                                     format_insertions(tree_records as _),
                                     format_insertions(update_records as _)));

                let data_file
                    = data_file_name(tree_records);

                let read_file
                    = read_data_file_name(tree_records, update_records);

                let from_file = path::Path::new(data_file.as_str())
                    .exists() && path::Path::new(read_file.as_str())
                    .exists();

                let (create_data, read_data) = if from_file {
                    log_debug_ln(format!("Using `{}` for data, `{}` for reads", data_file, read_file));

                    (serde_json::from_str::<Vec<Key>>(fs::read_to_string(data_file).unwrap()
                        .as_str()
                    ).unwrap(), serde_json::from_str::<Vec<Key>>(fs::read_to_string(read_file).unwrap()
                        .as_str()
                    ).unwrap())
                } else {
                    log_debug_ln(format!("Generating `{}` for data", data_file));

                    let c_data = gen_rand_data(tree_records);

                    let mut read_data
                        = (0 as Key..tree_records as Key).collect::<Vec<_>>();

                    read_data.shuffle(&mut rand::thread_rng());
                    read_data.truncate(update_records);

                    read_data
                        .iter_mut()
                        .for_each(|index| *index = c_data[(*index) as usize]);

                    fs::write(data_file, serde_json::to_string(c_data.as_slice()).unwrap())
                        .unwrap();

                    fs::write(read_file, serde_json::to_string(read_data.as_slice()).unwrap())
                        .unwrap();

                    (c_data, read_data)
                };

                crud.into_iter().for_each(|crud| {
                    let index = Arc::new(MAKE_INDEX(crud.clone()));

                    log_debug_ln("Creating index...".to_string());
                    let create_time = if crud.is_mono_writer() {
                        beast_test(1, index.clone(), create_data.as_slice(), false)
                    }
                    else {
                        beast_test(4, index.clone(), create_data.as_slice(), false)
                    };

                    log_debug_ln(format!("Created index in `{}` ms", create_time));

                    let read_data: &'static [_] = unsafe { mem::transmute(read_data.as_slice()) };

                    log_debug_ln("UPDATE + READ BENCHMARK; Each Thread = [Updater Thread + Reader Thread]".to_string());
                    println!("Locking Strategy,Threads,Time");
                    threads.iter().for_each(|spawns| {
                            if crud.is_mono_writer() {
                                let index = Arc::new(SyncIndex(Mutex::new(index.clone())));
                                let start = SystemTime::now();
                                (0..=*spawns).map(|_| {
                                    let i1 = index.clone();
                                    let i2 = index.clone();
                                    [thread::spawn(move || {
                                        read_data
                                            .iter()
                                            .for_each(|read_key| if let CRUDOperationResult::Error =
                                                i1.dispatch(CRUDOperation::Point(*read_key))
                                            {
                                                log_debug_ln(format!("Error reading key = {}", read_key));
                                            });
                                    }), thread::spawn(move || {
                                        read_data
                                            .iter()
                                            .for_each(|read_key| if let CRUDOperationResult::Error
                                                = i2.dispatch(
                                                CRUDOperation::Update(*read_key, Payload::default()))
                                            {
                                                log_debug_ln(format!("Error reading key = {}", read_key));
                                            });
                                    })]
                                }).collect::<Vec<_>>()
                                    .into_iter()
                                    .for_each(|h| h.into_iter().for_each(|sh| sh.join().unwrap()));

                                println!("{},{},{}", crud, *spawns,
                                         SystemTime::now().duration_since(start).unwrap().as_millis());
                            }
                            else {
                                let read_data = read_data.clone();
                                let start = SystemTime::now();
                                (0..=*spawns).map(|_| {
                                    let index1 = index.clone();
                                    let index2 = index.clone();
                                    [thread::spawn(move || {
                                        read_data
                                            .iter()
                                            .for_each(|read_key| if let CRUDOperationResult::Error =
                                                index1.dispatch(CRUDOperation::Point(*read_key))
                                            {
                                                log_debug_ln(format!("Error reading key = {}", read_key));
                                            });
                                    }), thread::spawn(move || {
                                        read_data
                                            .iter()
                                            .for_each(|read_key| if let CRUDOperationResult::Error
                                                = index2.dispatch(
                                                CRUDOperation::Update(*read_key, Payload::default()))
                                            {
                                                log_debug_ln(format!("Error reading key = {}", read_key));
                                            });
                                    })]
                                }).collect::<Vec<_>>()
                                    .into_iter()
                                    .for_each(|h| h.into_iter().for_each(|sh| sh.join().unwrap()));

                                println!("{},{},{}", crud, *spawns,
                                         SystemTime::now().duration_since(start).unwrap().as_millis());
                            }
                    });
                });
            }
            "generate" | "gen" => fs::write(
                data_file_name(params.parse::<usize>().unwrap()),
                serde_json::to_string(
                    gen_rand_data(params.parse::<usize>().unwrap()).as_slice()).unwrap()
            ).unwrap(),
            "block_alignment" | "bsz_aln" | "alignment" | "aln" | "block" | "bsz" =>
                show_alignment_bsz(),
            "hardware_lock_elision" | "hle" =>
                println!("OLC hardware_lock_elision: {}", hle()),
            "x86_64" | "x86" =>
                println!("x86_64 or x86: {}", cfg!(any(target_arch = "x86", target_arch = "x86_64"))),
            _ => make_splash(),
        }
    }
    else {
        make_splash()
    }
}

fn data_file_name(n_records: usize) -> String {
    format!("create_{}.create", format_insertions(n_records as _))
}

fn read_data_file_name(n_records: usize, read_records: usize) -> String {
    format!("{}__read_{}.read",
            data_file_name(n_records).replace(".create", ""),
            format_insertions(read_records as _))
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

fn hle() -> &'static str {
    if cfg!(feature = "hardware-lock-elision") {
        if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
            "ON    "
        }
        else {
            "NO HTL"
        }
    }
    else {
        "OFF   "
    }
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
    println!(" |               ------------------------------                          |");
    println!(" |               # Build:   {}                          |", datetime.format("%d-%m-%Y %T"));
    println!(" |               # Current version: {}                               |", env!("CARGO_PKG_VERSION"));
    println!(" |               -------------------------                               |");
    println!(" |               # OLC-HLE:   {}                                     |", hle());
    println!(" |               # RW-HLE:    AUTO                                       |");
    println!(" |               # SYS-YIELD: {}                                       |",
             if ENABLE_YIELD { "ON  " } else { "OFF " });
    println!(" |               -----------------                                       |");
    println!(" |                                                                       |");
    println!(" |               --------------------------------------------            |");
    println!(" |               # E-Mail: elshaikh@mathematik.uni-marburg.de            |");
    println!(" |               # Written by: Amir El-Shaikh                            |");
    println!(" |               # First released: 03-08-2022                            |");
    println!(" |               ----------------------------                            |");
    println!(" |                                                                       |");
    println!(" |               ...CC-B+Tree Application Launching...                   |");
    println!(" +-------------+                                           +-------------+");
    println!("                \\_______                           _______/");
    println!("                        \\_________________________/");

    println!();
    println!("--> System Log:");
}

fn experiment(mut threads_cpu: Vec<usize>,
              insertions: Vec<Key>,
              strategies: Vec<LockingStrategy>)
{
    if CPU_THREADS {
        let cpu = num_cpus::get();
        threads_cpu = threads_cpu
            .into_iter()
            .take_while(|t| cpu >= *t)
            .collect();
    }

    log_debug_ln(format!("Preparing {} Experiments, hold on..", insertions.len()));

    insertions.iter().enumerate().for_each(|(i, insertion)| {
        log_debug_ln(format!("# {}\n\t\
        - Records: \t\t{}\n\t\
        - Threads: \t\t{}", i + 1, format_insertions(*insertion), threads_cpu.iter().join(",")));

        log_debug(format!("\t- Strategy:"));

            println!("\t\t{}", strategies[0]);
            (&strategies[1..])
                .iter()
                .for_each(|st| log_debug_ln(format!("\t\t\t\t{}", st)))

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
    println!("Number Insertions,Number Threads,Locking Strategy,Time,Fan Out,Leaf Records,Block Size");

    cases.into_iter().for_each(|(t1s, strats)|
        for num_threads in threads_cpu.iter() {
            if *num_threads > t1s.len() {
                log_debug_ln("WARNING: Number of threads larger than number of operations!".to_string());
                log_debug_ln(format!("WARNING: Skipping operations = {}, threads = {}!", t1s.len(), num_threads));
                continue;
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

                let time = beast_test(
                    *num_threads,
                    Arc::new(MAKE_INDEX(ls.clone())),
                    t1s.as_slice(), true);

                print!(",{}", time);
                print!(",{}", FAN_OUT);
                print!(",{}", NUM_RECORDS);
                println!(",{}", BSZ_BASE);
            }
        });
}