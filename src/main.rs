use chronicle_db::backbone::core::event::Event;
use chronicle_db::backbone::core::event::EventVariant::F64;
use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::index::version_info::Version;
use mvcc_bplustree::transaction::transaction::Transaction;

mod node;
mod bplus_tree;
mod node_manager;
mod bplus_tree_query;
mod transaction;


fn main() {
    println!("Hola!");

    let mut singled_versioned_index = bplus_tree::BPlusTree::new_single_versioned();
    let mut multi_versioned_index = bplus_tree::BPlusTree::new_multi_versioned();


    for key in 1..=10000 as Key {
        singled_versioned_index.execute(
            Transaction::Insert(Event::new_from_t1(key, F64(key as f64))));

        multi_versioned_index.execute(
            Transaction::Insert(Event::new_from_t1(key, F64(key as f64))));
    }

}



