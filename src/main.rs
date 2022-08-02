use chronicle_db::backbone::core::event::Event;
use chronicle_db::backbone::core::event::EventVariant::F64;
use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::index::version_info::Version;

mod node;
mod bplus_tree;
mod node_manager;


fn main() {
    println!("Hola!");

    let mut singled_versioned_index = bplus_tree::BPlusTree::new_single_versioned();
    let mut multi_versioned_index = bplus_tree::BPlusTree::new_multi_versioned();


    for key in 1..=10 as Key {
        singled_versioned_index.insert(
            (Event::new_from_t1(key, F64(key as f64)), key as Version).into());

        multi_versioned_index.insert(
            (Event::new_from_t1(key, F64(key as f64)), key as Version).into())
    }

}



