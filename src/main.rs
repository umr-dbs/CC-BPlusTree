use std::{env, fs, mem, thread};
use std::ops::{Deref, Index};
use std::time::SystemTime;
use chrono::{DateTime, Local};
use parking_lot::RwLock;
use crate::tree::bplus_tree;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::locking::locking_strategy::CRUDProtocol;
use crate::locking::locking_strategy::LockingStrategy::*;
use crate::test::{BSZ_BASE, FAN_OUT, gen_rand_data, hle, INDEX, Key, log_debug, log_debug_ln, MAKE_INDEX, NUM_RECORDS, Payload, S_INSERTIONS, S_STRATEGIES, S_THREADS_CPU, show_alignment_bsz, SyncIndex, UnsafeIndex};
use crate::utils::smart_cell::{ENABLE_YIELD, LatchType};

mod block;
mod crud_model;
mod locking;
mod page_model;
mod record_model;
mod tree;
mod utils;
mod test;

pub const TREE: fn(Box<INDEX>) -> TreeDispatcher = |index| {
    if let MonoWriter = index.locking_strategy {
        TreeDispatcher::Wrapper(Box::leak(Box::new(SyncIndex(RwLock::new(*index)))))
    }
    else {
        TreeDispatcher::Ref(Box::leak(index))
    }
};

fn main() {
    make_splash();
    show_alignment_bsz();

    const THREADS: usize        = 24;
    const INSERTIONS: usize     = 1_000_000;
    const VALIDATE_CRUD: bool   = true;

    const CRUD: CRUDProtocol = OLC;
    // const CRUD: CRUDProtocol = LockCoupling;
    // const CRUD: CRUDProtocol = LockCoupling;
    // const CRUD: CRUDProtocol = LockCoupling;
    // const CRUD: CRUDProtocol = LockCoupling;

    let tree = TREE(Box::new(MAKE_INDEX(CRUD)));
    // End Init B-Tree FREE

    let data_org
        = gen_rand_data(INSERTIONS);

    let data
        = data_org.chunks(THREADS).map(|c| c.to_vec()).collect::<Vec<_>>();

    println!("Number Insertions,Number Threads,Locking Strategy,Create Time,Fan Out,Leaf Records,Block Size,Scan Time");
    print!("{}", INSERTIONS);
    print!(",{}", THREADS);
    print!(",{}", CRUD);

    let mut handles
        = Vec::with_capacity(THREADS);

    let insert_data = data
        .iter()
        .map(|inner_insert| inner_insert
            .iter()
            .map(|k|CRUDOperation::Insert(*k, Payload::default()))
            .collect::<Vec<_>>())
        .collect::<Vec<_>>();

    let start = SystemTime::now();
    for chunk in insert_data {
        let tree = tree.clone();
        handles.push(thread::spawn(move ||
            for insertion in chunk {
                if VALIDATE_CRUD {
                    match tree.dispatch(insertion) {
                        CRUDOperationResult::Inserted(..) => {},
                        _ => assert!(false)
                    }
                }
                else {
                    tree.dispatch(insertion);
                }
        }));
    }

    for thread in handles.drain(..) {
        thread.join().unwrap();
    }

    print!(",{}", SystemTime::now().duration_since(start).unwrap().as_millis());
    print!(",{}", FAN_OUT);
    print!(",{}", NUM_RECORDS);
    print!(",{}", BSZ_BASE);

    let search_data = data
        .iter()
        .map(|inner_search| inner_search
            .iter()
            .map(|k|CRUDOperation::Point(*k))
            .collect::<Vec<_>>())
        .collect::<Vec<_>>();

    let start = SystemTime::now();
    for chunk in search_data {
        let tree = tree.clone();
        handles.push(thread::spawn(move ||
            for search_op in chunk {
                if VALIDATE_CRUD {
                    match tree.dispatch(search_op) {
                        CRUDOperationResult::MatchedRecord(Some(..))
                        => {}
                        _ => assert!(false)
                    }
                }
                else {
                    tree.dispatch(search_op);
                }
            }
        ));
    }

    println!(",{}", SystemTime::now().duration_since(start).unwrap().as_millis());
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

// #[derive(Clone)]
pub enum TreeDispatcher {
    Wrapper(&'static SyncIndex),
    Ref(&'static INDEX)
}

impl Clone for TreeDispatcher {

    fn clone(&self) -> Self {
        match self {
            TreeDispatcher::Wrapper(inner) =>
                TreeDispatcher::Wrapper(*inner),
            TreeDispatcher::Ref(inner) =>
                TreeDispatcher::Ref(*inner)
        }
    }
}

impl CRUDDispatcher<Key, Payload> for TreeDispatcher {

    fn dispatch(&self, crud: CRUDOperation<Key, Payload>) -> CRUDOperationResult<Key, Payload> {
        match self {
            TreeDispatcher::Wrapper(sync)=>
                sync.dispatch(crud),
            TreeDispatcher::Ref(inner) =>
                inner.dispatch(crud)
        }
    }
}