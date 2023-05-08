use std::{env, fs, mem, thread};
use std::collections::VecDeque;
use std::time::SystemTime;
use chrono::{DateTime, Local};
use rand::prelude::SliceRandom;
use crate::tree::bplus_tree;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::test::{beast_test, BSZ_BASE, FAN_OUT, format_insertions, gen_rand_data, hle, INDEX, Key, log_debug, MAKE_INDEX, NUM_RECORDS, Payload, S_INSERTIONS, S_STRATEGIES, S_THREADS_CPU};
use crate::utils::smart_cell::ENABLE_YIELD;

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

    // create_scan
    // println!("Number Insertions,Number Threads,Locking Strategy,Create Time,Fan Out,Leaf Records,Block Size,Scan Time");

    // update
    println!("Number Insertions,Number Threads,Locking Strategy,Update Time,Fan Out,Leaf Records,Block Size");
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

        update_test(create, scan);
        // create_scan_test(create, scan);
    }
}

fn update_test(t1s: Vec<Key>, updates: Vec<Key>) {
    let threads_cpu
        = S_THREADS_CPU.to_vec();

    let strategies
        = S_STRATEGIES.to_vec();

    for num_threads in threads_cpu.iter() {
        for ls in strategies.iter() {
            print!("{}", t1s.len());
            print!(",{}", *num_threads);

            let (_, index) = beast_test(
                *num_threads,
                MAKE_INDEX(ls.clone()),
                t1s.as_slice(), true);

            let chunk_size = updates.len() / *num_threads;
            let mut slices = (0..*num_threads).map(|i| unsafe {
                std::slice::from_raw_parts(
                    updates.as_ptr().add(i * chunk_size),
                    chunk_size)
            }).collect::<VecDeque<_>>();

            let index: &'static INDEX = unsafe { mem::transmute(&index) };

            let start = SystemTime::now();
            let update_handles = (0..*num_threads).map(|_| {
                let chunk
                    = slices.pop_front().unwrap();

                thread::spawn(move ||
                    for key in chunk {
                        match index.dispatch(CRUDOperation::Update(*key, Payload::default())) {
                            CRUDOperationResult::Updated(..) => {}
                            CRUDOperationResult::Error => log_debug(format!("Not found key = {}", key)),
                            cor =>
                                log_debug(format!("sleepy joe hit me -> {}", cor))
                        }})
            }).collect::<Vec<_>>();

            update_handles
                .into_iter()
                .for_each(|handle|
                    handle.join().unwrap());

            let update_time
                = SystemTime::now().duration_since(start).unwrap().as_millis();

            print!(",{}", update_time);
            print!(",{}", FAN_OUT);
            print!(",{}", NUM_RECORDS);
            println!(",{}", BSZ_BASE);
        }
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
