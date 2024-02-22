use std::{env, fs};
use std::sync::Arc;
use chrono::{DateTime, Local};
use parking_lot::RwLock;
use crate::tree::bplus_tree;
use crate::crud_model::crud_api::{CRUDDispatcher, NodeVisits};
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::locking::locking_strategy::CRUDProtocol;
use crate::locking::locking_strategy::LockingStrategy::*;
use crate::test::{INDEX, Key, MAKE_INDEX, Payload, start_paper_tests};
use crate::utils::smart_cell::ENABLE_YIELD;

mod block;
mod crud_model;
mod locking;
mod page_model;
mod record_model;
mod tree;
mod utils;
mod test;
// @Bernhard: Manually reduce InternalNode size:
// Check whether that makes a difference.



fn main() {
    // make_splash();
    // show_alignment_bsz();

    start_paper_tests();
    //
    // const THREADS: usize        = 24;
    // const INSERTIONS: usize     = 10_000_000;
    // const VALIDATE_CRUD: bool   = true;
    // const CRUD: CRUDProtocol    = olc();
    // // const CRUD: CRUDProtocol = LockCoupling;
    // // const CRUD: CRUDProtocol = LockCoupling;
    // // const CRUD: CRUDProtocol = LockCoupling;
    // // const CRUD: CRUDProtocol = LockCoupling;
    //
    // let tree = TREE(CRUD);
    // // End Init B-Tree FREE
    //
    // let data_org
    //     = gen_rand_data(INSERTIONS);
    //
    // let data = data_org
    //     .chunks(data_org.len() / THREADS)
    //     .map(|c| c.to_vec())
    //     .collect::<Vec<_>>();
    //
    // println!("Number Insertions,Number Threads,Locking Strategy,Create Time,Fan Out,Leaf Records,Block Size,Scan Time");
    // print!("{}", INSERTIONS);
    // print!(",{}", THREADS);
    // print!(",{}", CRUD);
    //
    // let mut handles
    //     = Vec::with_capacity(THREADS);
    //
    // let insert_data = data
    //     .iter()
    //     .map(|inner_insert| inner_insert
    //         .iter()
    //         .map(|k| CRUDOperation::Insert(*k, Payload::default()))
    //         .collect::<Vec<_>>())
    //     .collect::<Vec<_>>();
    //
    // let start = SystemTime::now();
    // for chunk in insert_data {
    //     let tree = tree.clone();
    //     handles.push(thread::spawn(move ||
    //         for insertion in chunk {
    //             if VALIDATE_CRUD {
    //                 match tree.dispatch(insertion) {
    //                     CRUDOperationResult::Inserted(..) => {},
    //                     _ => assert!(false)
    //                 }
    //             }
    //             else {
    //                 tree.dispatch(insertion);
    //             }
    //     }));
    // }
    //
    // for thread in handles.drain(..) {
    //     thread.join().unwrap();
    // }
    //
    // print!(",{}", SystemTime::now().duration_since(start).unwrap().as_millis());
    // print!(",{}", FAN_OUT);
    // print!(",{}", NUM_RECORDS);
    // print!(",{}", BSZ_BASE);
    //
    // let search_data = data
    //     .into_iter()
    //     .map(|inner_search| inner_search
    //         .into_iter()
    //         .map(|k| CRUDOperation::Point(k))
    //         .collect::<Vec<_>>())
    //     .collect::<Vec<_>>();
    //
    // let start = SystemTime::now();
    // for chunk in search_data {
    //     let tree = tree.clone();
    //     handles.push(thread::spawn(move ||
    //         for search_op in chunk {
    //             if VALIDATE_CRUD {
    //                 match tree.dispatch(search_op) {
    //                     CRUDOperationResult::MatchedRecord(Some(..))
    //                     => {}
    //                     _ => assert!(false)
    //                 }
    //             }
    //             else {
    //                 tree.dispatch(search_op);
    //             }
    //         }
    //     ));
    // }
    //
    // println!(",{}", SystemTime::now().duration_since(start).unwrap().as_millis());
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


pub fn hle() -> &'static str {
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