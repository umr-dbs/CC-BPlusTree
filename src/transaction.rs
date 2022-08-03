use mvcc_bplustree::transaction::transaction::Transaction;
use mvcc_bplustree::transaction::transaction_result::TransactionResult;
use crate::bplus_tree::Index;

impl Index {
    pub fn execute(&self, transaction: Transaction) -> TransactionResult {
        match transaction {
            Transaction::Empty => TransactionResult::Error,
            Transaction::Insert(event) => {
                let key
                    = event.t1();

                let (_node, mut guard)
                    = self.traversal_insert(key);

                let version
                    = self.next_version();

                guard.push_record((event, version).into())
                    .then(|| TransactionResult::Inserted(key, version))
                    .unwrap_or(TransactionResult::Error)
            }
            _ => unimplemented!("bro hang on, im working on it..")
        }
    }
}