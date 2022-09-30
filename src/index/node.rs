use std::fmt::{Display, Formatter};
use chronicle_db::backbone::core::event::Event;
use chronicle_db::tools::aliases::Key;
use itertools::Itertools;
use mvcc_bplustree::index::record::Record;
use mvcc_bplustree::index::version_info::Version;
use crate::block::aligned_page::{IndexPage, RecordListsPage, EventsPage};
use crate::block::block::BlockRef;
use crate::index::record_list::RecordList;
use crate::utils::shadow_vec::ShadowVec;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub(crate) enum Node {
    Index(IndexPage),
    Leaf(EventsPage),
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

#[repr(u8)]
pub(crate) enum NodeUnsafeDegree {
    Ok,
    Overflow,
    Underflow,
}

impl NodeUnsafeDegree {
    pub(crate) const fn is_ok(&self) -> bool {
        match self {
            Self::Ok => true,
            _ => false
        }
    }

    pub(crate) const fn is_overflow(&self) -> bool {
        match self {
            Self::Overflow => true,
            _ => false
        }
    }

    pub(crate) const fn is_underflow(&self) -> bool {
        match self {
            Self::Underflow => true,
            _ => false
        }
    }
}

impl Node {
    pub(crate) fn is_overflow(&self, allocation: usize) -> bool {
        debug_assert!(allocation >= self.len());

        self.len() >= allocation
    }

    pub(crate) fn is_underflow(&self, allocation: usize) -> bool {
        debug_assert!(allocation > 0 && allocation >= self.len());

        self.len() < allocation / 2
    }

    pub(crate) fn unsafe_degree(&self, allocation: usize) -> NodeUnsafeDegree {
        let len = self.len();

        if len >= allocation {
            NodeUnsafeDegree::Overflow
        } else if len < allocation / 2 {
            NodeUnsafeDegree::Underflow
        } else {
            NodeUnsafeDegree::Ok
        }
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

    pub(crate) fn records_mut(&mut self) -> ShadowVec<Event> {
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

    pub(crate) fn delete_key(&mut self, key: Key, del_version: Version) -> bool {
        match self {
            Node::Leaf(events_page) => events_page
                .as_slice()
                .binary_search_by_key(&key, |event| event.t1())
                .map(|found| events_page.as_records().remove(found))
                .map(|_| true)
                .unwrap_or_default(),
            Node::MultiVersionLeaf(records_lists) => records_lists
                .iter_mut()
                .rev()
                .find(|record_list| record_list.key() == key)
                .map(|record_list| record_list.delete(del_version))
                .unwrap_or(false),
            _ => false,
        }
    }

    pub(crate) fn update_event(&mut self, event: Event) -> bool {
        match self {
            Node::Leaf(events_page) => events_page
                .as_slice()
                .binary_search_by_key(&event.t1(), |e| e.t1())
                .map(|found| events_page.get_mut(found).payload = event.payload)
                .map(|_| true)
                .unwrap_or_default(),
            _ => false
        }
    }

    pub(crate) fn update_record(&mut self, record: Record) -> bool {
        match self {
            Node::MultiVersionLeaf(records_lists) => records_lists
                .as_slice()
                .binary_search_by_key(&record.key(), |record_list| record_list.key())
                .map(|pos| {
                    let list = records_lists.get_mut(pos);
                    if list.delete(record.insertion_version()) {
                        list.push_front(record);
                        true
                    } else {
                        false
                    }
                }).unwrap_or_default(),
            _ => false
        }
    }

    pub(crate) fn push_event(&mut self, event: Event) -> bool {
        match self {
            Node::Leaf(records_page) => match records_page
                .as_slice()
                .binary_search_by_key(&event.t1(), |event| event.t1())
            {
                Err(pos) => {
                    records_page
                        .as_records()
                        .insert(pos, event);

                    true
                }
                _ => false
            }
            _ => false
        }
    }

    pub(crate) fn push_record(&mut self, record: Record) -> bool {
        match self {
            Node::MultiVersionLeaf(records_lists, ..) => match records_lists
                .as_slice()
                .binary_search_by_key(&record.key(), |record_list| record_list.key())
            {
                Err(pos) => {
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
        Self::Leaf(EventsPage::default())
    }
}