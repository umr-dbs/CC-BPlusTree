use std::{env, fs, mem, path, thread};
use std::collections::VecDeque;
use std::io::BufReader;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use chrono::{DateTime, Local};
use itertools::Itertools;
use parking_lot::Mutex;
use rand::prelude::SliceRandom;
use crate::block::block_manager::bsz_alignment;
use crate::tree::bplus_tree;
use crate::locking::locking_strategy::{CRUDProtocol, LockingStrategy, olc};
use block::block::Block;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::locking::locking_strategy::LockingStrategy::MonoWriter;
use crate::test::{beast_test, beast_test2, BSZ_BASE, FAN_OUT, format_insertions, gen_rand_data, get_system_info, INDEX, Key, log_debug, log_debug_ln, MAKE_INDEX, NUM_RECORDS, Payload, S_INSERTIONS, S_STRATEGIES, S_THREADS_CPU, simple_test, SyncIndex};
use crate::utils::smart_cell::{CPU_THREADS, ENABLE_YIELD};

mod block;
mod crud_model;
mod locking;
mod page_model;
mod record_model;
mod tree;
mod utils;
mod test;

const TERMINAL: bool = false;

fn main() {
    println!("Number Insertions,Number Threads,Locking Strategy,Create Time,Fan Out,Leaf Records,Block Size,Scan Time");

    for n in S_INSERTIONS {
        let file_suffix = format_insertions(n as _);
        let create_file = format!("create_{}.bin", file_suffix);
        let create_file = create_file.as_str();

        let scan_file = format!("scan_{}.bin", file_suffix);
        let scan_file = scan_file.as_str();

        // log_debug_ln(format!("Trying to use pre-generated data for n = '{}' ...", n));
        let (create, scan) = if let (Ok(mut create), Ok(mut scan))
            = (fs::read(create_file), fs::read(scan_file))
        {
            // log_debug_ln(format!("Found pre-generated data CREATE = '{}', SCAN = '{}'", create_file, scan_file));
            unsafe {
                create.set_len(create.len() / 8);
                scan.set_len(scan.len() / 8);
                (mem::transmute(create), mem::transmute(scan))
            }
        } else {
            // log_debug_ln(format!("Generating CREATE = '{}', SCAN = '{}' ...", create_file, scan_file));
            let t1s
                = gen_rand_data(n as usize);

            let mut scans = t1s.clone();
            scans.shuffle(&mut rand::thread_rng());

            fs::write(create_file, unsafe {
                std::slice::from_raw_parts(t1s.as_ptr() as _, t1s.len() * mem::size_of::<Key>())
            }).unwrap();

            fs::write(scan_file, unsafe {
                std::slice::from_raw_parts(scans.as_ptr() as _, scans.len() * mem::size_of::<Key>())
            }).unwrap();

            (t1s, scans)
        };

        create_scan_test(create, scan);
    }
}

fn create_scan_test(t1s: Vec<Key>, scans: Vec<Key>) {
    let threads_cpu
        = S_THREADS_CPU.to_vec();

    let strategies
        = S_STRATEGIES.to_vec();

    for num_threads in threads_cpu.iter() {
        for ls in strategies.iter() {
            print!("{}", t1s.len());
            print!(",{}", *num_threads);

            let (create_time, index) = beast_test(
                *num_threads,
                MAKE_INDEX(ls.clone()),
                t1s.as_slice(), true);

            print!(",{}", create_time);
            print!(",{}", FAN_OUT);
            print!(",{}", NUM_RECORDS);
            print!(",{}", BSZ_BASE);

            let chunk_size = scans.len() / *num_threads;
            let mut slices = (0..*num_threads).map(|i| unsafe {
                std::slice::from_raw_parts(
                    scans.as_ptr().add(i * chunk_size),
                    chunk_size)
            }).collect::<VecDeque<_>>();

            let index: &'static INDEX = unsafe { mem::transmute(&index) };

            let start = SystemTime::now();
            let read_handles = (0..*num_threads).map(|_| {
                let chunk
                    = slices.pop_front().unwrap();

                thread::spawn(move ||
                    for key in chunk {
                        match index.dispatch(CRUDOperation::Point(*key)) {
                            CRUDOperationResult::MatchedRecord(_) => {}
                            CRUDOperationResult::Error => log_debug(format!("Not found key = {}", key)),
                            cor =>
                                log_debug(format!("sleepy joe hit me -> {}", cor))
                        }})
            }).collect::<Vec<_>>();

            read_handles
                .into_iter()
                .for_each(|handle|
                    handle.join().unwrap());

            let read_time
                = SystemTime::now().duration_since(start).unwrap().as_millis();

            println!(",{}", read_time);
        }
    }
}

fn create_filter_params(params: &str) -> (Vec<usize>, Vec<Key>, Vec<CRUDProtocol>) {
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

    (threads, inserts, crud)
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
            "all" => experiment(S_THREADS_CPU.to_vec(),
                                S_INSERTIONS.as_slice(),
                                S_STRATEGIES.as_slice()),
            "t1" => println!("Time = {}ms",
                             beast_test(24, MAKE_INDEX(MonoWriter), gen_rand_data(200_000).as_slice(), true).0),
            "t2" => println!("Time = {}ms",
                             beast_test(24, MAKE_INDEX(olc()), gen_rand_data(20_000_000).as_slice(), true).0),
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
                let (threads, inserts, crud)
                    = create_filter_params(params);

                experiment(threads,
                           inserts.as_slice(),
                           crud.as_slice())
            }
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

                crud.into_iter().for_each(|crud| unsafe {
                    log_debug_ln("Creating index...".to_string());
                    let (create_time, index) = if crud.is_mono_writer() {
                        beast_test(1, MAKE_INDEX(crud.clone()), create_data.as_slice(), false)
                    } else {
                        beast_test(4, MAKE_INDEX(crud.clone()), create_data.as_slice(), false)
                    };

                    log_debug_ln(format!("Created index in `{}` ms", create_time));

                    let read_data: &'static [_] = unsafe { mem::transmute(read_data.as_slice()) };

                    log_debug_ln("UPDATE + READ BENCHMARK; Each Thread = [Updater Thread + Reader Thread]".to_string());
                    println!("Locking Strategy,Threads,Time");
                    threads.iter().for_each(|spawns| unsafe {
                        if crud.is_mono_writer() {
                            let index_r: &'static INDEX = mem::transmute(&index);
                            let index = Arc::new(SyncIndex(Mutex::new(index_r)));
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
                        } else {
                            let read_data = read_data.clone();
                            let index_r: &'static INDEX = mem::transmute(&index);
                            let start = SystemTime::now();
                            (0..=*spawns).map(|_| {
                                [thread::spawn(move || {
                                    read_data
                                        .iter()
                                        .for_each(|read_key| if let CRUDOperationResult::Error =
                                            index_r.dispatch(CRUDOperation::Point(*read_key))
                                        {
                                            log_debug_ln(format!("Error reading key = {}", read_key));
                                        });
                                }), thread::spawn(move || {
                                    read_data
                                        .iter()
                                        .for_each(|read_key| if let CRUDOperationResult::Error
                                            = index_r.dispatch(
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
                    gen_rand_data(params.parse::<usize>().unwrap()).as_slice()).unwrap(),
            ).unwrap(),
            "block_alignment" | "bsz_aln" | "alignment" | "aln" | "block" | "bsz" =>
                show_alignment_bsz(),
            "hardware_lock_elision" | "hle" =>
                println!("OLC hardware_lock_elision: {}", hle()),
            "x86_64" | "x86" =>
                println!("x86_64 or x86: {}", cfg!(any(target_arch = "x86", target_arch = "x86_64"))),
            _ => make_splash(),
        }
    } else {
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
        } else {
            "NO HTL"
        }
    } else {
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

fn experiment(threads_cpu: Vec<usize>,
              insertions: &[Key],
              strategies: &[LockingStrategy])
{
    // if CPU_THREADS {
    //     let cpu = num_cpus::get();
    //     threads_cpu.truncate(threads_cpu
    //         .iter()
    //         .enumerate()
    //         .find(|(_, t)| **t > cpu)
    //         .unwrap()
    //         .0)
    // }

    println!("Number Insertions,Number Threads,Locking Strategy,Time,Fan Out,Leaf Records,Block Size");

    for insertion_n in insertions {
        let t1s = gen_rand_data(*insertion_n as usize);
        for num_threads in threads_cpu.iter() {
            // if *num_threads > t1s.len() {
            //     continue;
            // }

            for ls in strategies {
                print!("{}", t1s.len());
                print!(",{}", *num_threads);

                let time = beast_test2(
                    *num_threads,
                    MAKE_INDEX(ls.clone()),
                    t1s.as_slice());

                print!(",{}", time);
                print!(",{}", FAN_OUT);
                print!(",{}", NUM_RECORDS);
                println!(",{}", BSZ_BASE);
            }
        }
    }
}