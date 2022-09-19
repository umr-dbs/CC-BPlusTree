use std::fmt::{Display, Formatter};
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use chronicle_db::tools::aliases::Key;
use chronicle_db::tools::arrays::array::FixedArray;
use itertools::Itertools;
use mvcc_bplustree::index::record::Record;
use crate::block::aligned_page::{IndexPage, RecordListsPage, RecordsPage};
use crate::block::block::Block;
use crate::index::record_list::RecordList;
use crate::utils::vcc_cell::{ConcurrentCell, ConcurrentGuard, GuardDerefResult};

pub(crate) type BlockGuardResult<'a> = GuardDerefResult<'a, Block>;
pub(crate) type BlockRef = ConcurrentCell<Block>;
pub(crate) type BlockGuard<'a> = ConcurrentGuard<'a, Block>;

pub(crate) enum Node {
    Index(IndexPage),
    Leaf(RecordsPage),
    MultiVersionLeaf(RecordListsPage),
}

impl Display for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Node::Index(
                IndexPage {
                    keys,
                    children,
                    ..
                }) =>
                write!(f, "Index(key: {:?}, Children: {}", keys.as_slice(), children.len()),
            Node::Leaf(records) => write!(f, "Leaf({})", records
                .iter()
                .join("")),
            Node::MultiVersionLeaf(record_lists) =>
                write!(f, "MultiVersionLeaf({})", record_lists
                    .iter()
                    .flat_map(|record_list| record_list.as_records())
                    .join(","))
        }
    }
}

impl Node {
    pub(crate) fn is_overflow(&self, allocation: usize) -> bool {
        debug_assert!(allocation >= self.len());

        self.len() >= allocation
    }

    pub const fn is_leaf(&self) -> bool {
        match self {
            Node::Index(..) => false,
            _ => true
        }
    }

    pub(crate) fn children_mut(&mut self) -> ShadowVec<BlockRef> {
        match self {
            Node::Index(index_page) => index_page.children_mut(),
            _ => unreachable!("Sleepy Joe hit me -> Not index Page .children_mut")
        }
    }

    pub(crate) fn keys_mut(&mut self) -> ShadowVec<Key> {
        match self {
            Node::Index(index_page) => index_page.keys_mut(),
            _ => unreachable!("Sleepy Joe hit me -> Not index Page .keys_mut")
        }
    }

    pub(crate) fn records_mut(&mut self) -> ShadowVec<Record> {
        match self {
            Node::Leaf(records_page) => records_page.as_records(),
            _ => unreachable!("Sleepy Joe hit me -> Not index Page .records_mut")
        }
    }

    pub(crate) fn record_lists_mut(&mut self) -> ShadowVec<RecordList> {
        match self {
            Node::MultiVersionLeaf(record_lists) => record_lists.as_records(),
            _ => unreachable!("Sleepy Joe hit me -> Not index Page .record_lists_mut")
        }
    }

    pub const fn is_directory(&self) -> bool {
        !self.is_leaf()
    }

    pub(crate) fn push_record(&mut self, record: Record, is_update: bool) -> bool {
        match self {
            Node::Leaf(records_page) => match records_page
                .as_slice()
                .binary_search(&record)
            {
                Ok(pos) if is_update => records_page
                    .get_mut(pos)
                    .delete(record.insertion_version())
                    .then(|| {
                        records_page
                            .as_records()
                            .insert(pos + 1, record);

                        true
                    }).unwrap_or(false),
                Err(pos) if !is_update => {
                    records_page
                        .as_records()
                        .insert(pos, record);

                    true
                }
                _ => false
            }
            Node::MultiVersionLeaf(records_lists, ..) => match records_lists
                .as_slice()
                .binary_search_by_key(&record.key(), |record_list| record_list.key())
            {
                Ok(pos) if is_update => {
                    let record_list = records_lists
                        .get_mut(pos);

                    record_list.delete(record.insertion_version())
                        .then(|| {
                            record_list.push_front(record);

                            true
                        }).unwrap_or(false)
                }
                Err(pos) if !is_update => {
                    records_lists
                        .as_records()
                        .insert(pos, RecordList::from_record(record));

                    true
                }
                _ => false
            }
            _ => false
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Node::Index(index_page) => index_page.keys_len(),
            Node::Leaf(records_page) => records_page.len(),
            Node::MultiVersionLeaf(record_lists_page) => record_lists_page.len()
        }
    }
}

impl AsRef<Node> for Node {
    fn as_ref(&self) -> &Node {
        &self
    }
}

impl Default for Node {
    fn default() -> Self {
        Self::Leaf(RecordsPage::default())
    }
}

pub(crate) struct ShadowVec<'a, E: Default> {
    pub(crate) unreal_vec: ManuallyDrop<Vec<E>>,
    pub(crate) p_array: &'a mut FixedArray<E>
}

impl<'a, E: Default> ShadowVec<'a, E> {
    pub(crate) fn new(cap: usize, p_array: &'a mut FixedArray<E>) -> Self {
        unsafe {
            ShadowVec {
                unreal_vec: ManuallyDrop::new(Vec::from_raw_parts(
                    p_array.as_mut_ptr(),
                    p_array.len(),
                    cap)),
                p_array
            }
        }
    }
}

impl<'a, E: Default> Drop for ShadowVec<'a, E> {
    fn drop(&mut self) {
        unsafe {
            self.p_array.set_len(self.unreal_vec.len())
        }
    }
}

impl<'a, E: Default> Deref for ShadowVec<'a, E> {
    type Target = Vec<E>;

    fn deref(&self) -> &Self::Target {
        self.unreal_vec.as_ref()
    }
}

impl<'a, E: Default> DerefMut for ShadowVec<'a, E> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.unreal_vec.as_mut()
    }
}

impl<'a, E: Default> Into<ShadowVec<'a, E>> for (usize, &'a mut FixedArray<E>) {
    fn into(self) -> ShadowVec<'a, E> {
        ShadowVec::new(self.0, self.1)
    }
}