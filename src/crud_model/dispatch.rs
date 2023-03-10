use std::hash::Hash;
use std::fmt::Display;
use crate::page_model::node::Node;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::tree::bplus_tree::BPlusTree;
use crate::utils::interval::Interval;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display,
    Payload: Default + Clone + Sync + Display
> BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub fn execute(&self, crud_operation: CRUDOperation<Key, Payload>) -> CRUDOperationResult<Key, Payload> {
        let olc
            = self.locking_strategy.is_olc();

        match crud_operation {
            CRUDOperation::Delete(key) if olc => self
                .traversal_write_olc(key)
                .deref_mut()
                .unwrap()
                .delete_key(key)
                .map(|payload| CRUDOperationResult::Deleted(key, payload))
                .unwrap_or_default(),
            CRUDOperation::Delete(key) => self
                .traversal_write(key)
                .deref_mut()
                .unwrap()
                .delete_key(key)
                .map(|payload| CRUDOperationResult::Deleted(key, payload))
                .unwrap_or_default(),
            CRUDOperation::Insert(key, payload) if olc => self
                .traversal_write_olc(key)
                .deref_mut()
                .unwrap()
                .push_record_point(key, payload)
                .then(|| CRUDOperationResult::Inserted(key))
                .unwrap_or_default(),
            CRUDOperation::Insert(key, payload) => self
                .traversal_write(key).deref_mut()
                .unwrap()
                .push_record_point(key, payload)
                .then(|| CRUDOperationResult::Inserted(key))
                .unwrap_or_default(),
            CRUDOperation::Update(key, payload) if olc => self
                .traversal_write_olc(key)
                .deref_mut()
                .unwrap()
                .update_record_point(key, payload)
                .map(|old| CRUDOperationResult::Updated(key, old))
                .unwrap_or_default(),
            CRUDOperation::Update(key, payload) => self
                .traversal_write_olc(key)
                .deref_mut()
                .unwrap()
                .update_record_point(key, payload)
                .map(|old| CRUDOperationResult::Updated(key, old))
                .unwrap_or_default(),
            CRUDOperation::Point(key) if olc => match self.execute(CRUDOperation::Range((key..=key).into())) {
                CRUDOperationResult::MatchedRecords(mut records) if records.len() <= 1 =>
                    CRUDOperationResult::MatchedRecord(records.pop()),
                _ => CRUDOperationResult::Error
            },
            CRUDOperation::Point(key) => match self
                .traversal_read(key)
                .deref()
                .unwrap()
                .as_ref()
            {
                    Node::Leaf(leaf_page) => leaf_page
                        .as_records()
                        .binary_search_by_key(&key, |record| record.key)
                        .ok()
                        .map(|pos| unsafe { leaf_page.as_records().get_unchecked(pos) }.clone())
                        .into(),
                    _ => CRUDOperationResult::Error
            },
            CRUDOperation::Range(key_interval) if olc => {
                let mut path = vec![
                    (Interval::new(self.min_key, self.max_key),
                     self.lock_reader(&self.root.block))
                ];

                self.next_leaf_page(path.as_mut(),
                                    0,
                                    key_interval.lower);

                self.range_query_olc(path.as_mut(), key_interval)
            },
            CRUDOperation::Range(interval) => self.traversal_read_range(&interval)
                .into_iter()
                .flat_map(|(_block, guard)| guard
                    .deref()
                    .unwrap()
                    .as_ref()
                    .as_records()
                    .iter()
                    .skip_while(|record| !interval.contains(record.key))
                    .take_while(|record| interval.contains(record.key))
                    .cloned()
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>()
                .into(),
            CRUDOperation::Empty => CRUDOperationResult::Error,
        }
    }
}