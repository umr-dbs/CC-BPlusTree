use chronicle_db::tools::aliases::Key;
use mvcc_bplustree::index::version_info::Version;
use mvcc_bplustree::transaction::transaction::Transaction;
use mvcc_bplustree::utils::interval::KeyInterval;

pub const fn exact_search(key: Key) -> Transaction {
    Transaction::ExactSearch(key, Version::MIN)
}

pub const fn delete(key: Key) -> Transaction {
    Transaction::Delete(key, Version::MIN)
}

pub const fn range_search(interval: KeyInterval) -> Transaction {
    Transaction::RangeSearch(interval, Version::MIN)
}