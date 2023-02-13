pub mod record_model;
pub mod tx_model;
pub mod page_model;
pub mod utils;

#[cfg(test)]
mod tests {
    use std::mem;
    use crate::page_model::block::Block;
    use crate::page_model::internal_page::InternalPage;
    use crate::page_model::leaf_page::LeafPage;
    use crate::page_model::node::Node::{Index, Leaf};
    // use crate::record_model::record::Record;
    use crate::record_model::record_point::RecordPoint;
    use super::*;

    const OLC: bool = false;
    type Key = u64;
    type Payload = f64;
    const NUM_RECORDS: usize = 2;
    const FAN_OUT: usize = NUM_RECORDS + 1;
    type SleepyRecord = RecordPoint<Key, Payload>;
    type PageInternal = InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>;
    type PageLeaf = LeafPage<NUM_RECORDS, Key, Payload>;

    #[test]
    fn leaf_page_test() {
        let leaf_page
            = PageLeaf::new();

        assert_eq!(leaf_page.len(), 0);

        let entry = SleepyRecord::default();

        leaf_page.as_records_mut().push(entry.clone());
        assert_eq!(leaf_page.len(), 1);
        assert_eq!(leaf_page.as_records().len(), 1);

        leaf_page.as_records_mut().pop();
        assert_eq!(leaf_page.len(), 0);
        assert_eq!(leaf_page.is_empty(), true);
        leaf_page.as_records_mut().push(entry.clone());

        if NUM_RECORDS == 1 {
            assert_eq!(leaf_page.is_full(), true);
        }
        else {
            assert_eq!(leaf_page.is_full(), false);
        }

    }

    #[test]
    fn internal_page_test() {
        let internal_page
            = PageInternal::new();

        assert_eq!(internal_page.keys_len(), 0);
        assert_eq!(internal_page.children_len(), 0);

        assert_eq!(internal_page.keys().len(), 0);
        assert_eq!(internal_page.children().len(), 0);

        assert_eq!(internal_page.keys_mut().len(), 0);
        assert_eq!(internal_page.children_mut().len(), 0);

        internal_page.keys_mut().push(1);
        assert_eq!(internal_page.keys_len(), 1);
        internal_page.keys_mut().pop();
        assert_eq!(internal_page.keys_len(), 0);
    }

    #[test]
    fn mem_test() {
        let left = Block::<FAN_OUT, NUM_RECORDS, Key, Payload> {
            block_id: 1,
            node_data: Leaf(PageLeaf::new()),
        }.into_cell(!OLC);

        let right = Block::<FAN_OUT, NUM_RECORDS, Key, Payload> {
            block_id: 2,
            node_data: Leaf(PageLeaf::new()),
        }.into_cell(!OLC);

        right.unsafe_borrow_mut()
            .records_mut()
            .push(SleepyRecord::default());

        left.unsafe_borrow_mut()
            .records_mut()
            .push(SleepyRecord::default());

        if NUM_RECORDS > 1 {
            left.unsafe_borrow_mut()
                .records_mut()
                .push(SleepyRecord::default());
        }

        mem::drop(right);
        mem::drop(left);
    }

    #[test]
    fn internal_page_test1() {
        let left = Block {
            block_id: 1,
            node_data: Leaf(PageLeaf::new()),
        }.into_cell(OLC);

        let right = Block {
            block_id: 2,
            node_data: Leaf(PageLeaf::new()),
        }.into_cell(OLC);

        left.unsafe_borrow()
            .records_mut()
            .push(SleepyRecord::default());

        assert_eq!(left.unsafe_borrow().len(), 1);

        right.unsafe_borrow()
            .records_mut()
            .push(SleepyRecord::default());

        assert_eq!(right.unsafe_borrow().len(), 1);

        let root_internal_page = Block {
            block_id: 0,
            node_data: Index(PageInternal::new())
        }.into_cell(OLC);

        assert_eq!(root_internal_page.unsafe_borrow().len(), 0);
        root_internal_page.unsafe_borrow()
            .children_mut()
            .extend([left, right]);

        root_internal_page.unsafe_borrow()
            .keys_mut()
            .push(1337);

        println!("root: {}", root_internal_page.unsafe_borrow().as_ref());

        let new_root = Block {
            block_id: 4,
            node_data: Index(PageInternal::new())
        }.into_cell(OLC);

        let new_right = Block {
            block_id: 5,
            node_data: Index(PageInternal::new())
        }.into_cell(OLC);

        new_root.unsafe_borrow()
            .children_mut()
            .extend([root_internal_page, new_right]);

        new_root.unsafe_borrow()
            .keys_mut()
            .push(41);

        // mem::drop(root_internal_page);
    }
}