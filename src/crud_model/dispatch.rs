use std::hash::Hash;
use std::fmt::Display;
use crate::crud_model::crud_api::{CRUDDispatcher, NodeVisits};
use crate::page_model::node::Node;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::tree::bplus_tree::BPlusTree;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display,
    Payload: Default + Clone + Sync + Display
> CRUDDispatcher<Key, Payload> for BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    fn dispatch(&self, crud_operation: CRUDOperation<Key, Payload>)
                -> (NodeVisits, CRUDOperationResult<Key, Payload>) {
        let olc
            = self.locking_strategy.is_optimistic();

        match crud_operation {
            CRUDOperation::Delete(key) if olc => {
                let (node_visits, guard) = self
                    .traversal_write_olc(key);

                (node_visits,
                 guard.deref_mut()
                     .unwrap()
                     .delete_key(key)
                     .map(|payload| CRUDOperationResult::Deleted(key, payload))
                     .unwrap_or_default())
            }
            CRUDOperation::Delete(key) => {
                let (node_visits, guard) = self
                    .traversal_write(key);

                (node_visits,
                 guard.deref_mut()
                     .unwrap()
                     .delete_key(key)
                     .map(|payload| CRUDOperationResult::Deleted(key, payload))
                     .unwrap_or_default())
            }
            CRUDOperation::Insert(key, payload) if olc => {
                let (node_visits, guard) = self
                    .traversal_write_olc(key);

                (node_visits, guard.deref_mut()
                    .unwrap()
                    .push_record_point(key, payload)
                    .then(|| CRUDOperationResult::Inserted(key))
                    .unwrap_or_default())
            }
            CRUDOperation::Insert(key, payload) => {
                let (node_visits, guard) = self
                    .traversal_write(key);

                (node_visits, guard.deref_mut()
                    .unwrap()
                    .push_record_point(key, payload)
                    .then(|| CRUDOperationResult::Inserted(key))
                    .unwrap_or_default())
            }
            CRUDOperation::Update(key, payload) if olc => {
                let (node_visits, guard) = self
                    .traversal_write_olc(key);

                (node_visits, guard
                    .deref_mut()
                    .unwrap()
                    .update_record_point(key, payload)
                    .map(|old| CRUDOperationResult::Updated(key, old))
                    .unwrap_or_default())
            }
            CRUDOperation::Update(key, payload) => {
                let (node_visits, guard) = self
                    .traversal_write(key);

                (node_visits, guard.deref_mut()
                    .unwrap()
                    .update_record_point(key, payload)
                    .map(|old| CRUDOperationResult::Updated(key, old))
                    .unwrap_or_default())
            }
            CRUDOperation::Point(key) if olc => match self.dispatch(
                CRUDOperation::Range((key..=key).into()))
            {
                (node_visits,
                    CRUDOperationResult::MatchedRecords(mut records))
                if records.len() <= 1 => (node_visits, records.pop().into()),
                (node_visits, ..) => (node_visits, CRUDOperationResult::Error)
            },
            CRUDOperation::Point(key) => match self.traversal_read(key) {
                (node_visits, leaf_guard) => {
                    let leaf_page =  leaf_guard
                        .deref()
                        .unwrap()
                        .as_ref();

                    (node_visits, leaf_page
                        .as_records()
                        .binary_search_by_key(&key, |record| record.key)
                        .ok()
                        .map(|pos| unsafe { leaf_page.as_records().get_unchecked(pos) }.clone())
                        .into())
                }
                (node_visits, ..) => (node_visits, CRUDOperationResult::Error)
            }
            ,
            CRUDOperation::Range(key_interval) if olc => {
                let mut path
                    = Vec::with_capacity(self.root.height() as _);

                let node_visits = self.next_leaf_page(path.as_mut(),
                                                      0,
                                                      key_interval.lower);

                self.range_query_olc(path.as_mut(), key_interval, node_visits)
            }
            CRUDOperation::Range(interval) => {
                let (node_visits, guards)
                    =  self.traversal_read_range(&interval);

                (node_visits,
                 guards.into_iter()
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
                    .into())
            },
            CRUDOperation::Empty => (NodeVisits::MIN, CRUDOperationResult::Error),
        }
    }
}