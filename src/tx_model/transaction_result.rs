use std::fmt::{Display, Formatter};
use std::hash::Hash;
use serde_json::Value;
// use crate::record_model::record::Record;
// use crate::record_model::record_like::RecordLike;
use crate::record_model::record_point::RecordPoint;
// use crate::record_model::Version;
use crate::tx_model::transaction_result::TransactionResult::{Deleted, Inserted, MatchedRecord, MatchedRecords, Updated};

/// Defines possible Transaction execution result.
/// *Error*, indicates execution error.
/// *Inserted*, indicates that the Transaction executed was successful and the (key, version) pair
/// of matching record is held.
/// *MatchedRecord*, indicates that the Transaction executed was successful and the result of
/// a potential match is held.
/// *MatchedRecords*, indicates that the Transaction executed was successful and the result of
/// matches is held.
#[derive(Clone, Default)]
pub enum TransactionResult<Key: Ord + Hash + Copy + Default, Payload: Clone + Default> {
    MatchedRecords(Vec<RecordPoint<Key, Payload>>),
    // MatchedRecordsVersioned(Vec<Record<Key, Payload>>),
    MatchedRecord(Option<RecordPoint<Key, Payload>>),
    // MatchedRecordVersioned(Option<Record<Key, Payload>>),
    Inserted(Key),
    Updated(Key, Payload),
    Deleted(Key, Payload),

    #[default]
    Error, // flatten no good
}

/// Implements pretty printers for TransactionResult.
impl<Key: Display + Ord + Hash + Copy + Default, Payload: Display + Clone + Default> Display for TransactionResult<Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionResult::Error =>
                write!(f, "Error"),
            MatchedRecord(record) =>
                write!(f, "MatchedRecord({})", record
                    .as_ref()
                    .map(|found| found.to_string())
                    .unwrap_or("None".to_string())),
            // MatchedRecordVersioned(record) =>
            //     write!(f, "MatchedRecordVersioned({})", record
            //         .as_ref()
            //         .map(|found| found.to_string())
            //         .unwrap_or("None".to_string())),
            MatchedRecords(records) => {
                write!(f, "MatchedRecords[len={}\n", records.len());
                records.iter().for_each(|record| {
                    write!(f, "{}\n", record);
                });
                write!(f, "]")
            }
            // MatchedRecordsVersioned(records) => {
            //     write!(f, "MatchedRecordsVersioned[");
            //     records.iter().for_each(|record| {
            //         write!(f, "{},", record);
            //     });
            //     write!(f, "]")
            // }
            Inserted(key) =>
                write!(f, "Inserted(key: {})",
                       key),
            Updated(key, payload) =>
                write!(f, "Updated(key: {}, payload: {})",
                       key,
                       payload),
            Deleted(key, payload) =>
                write!(f, "Deleted(key: {}, version: {})",
                       key,
                       payload),

        }
    }
}

/// Sugar implementation, wrapping collection of records to a TransactionResult.
impl<Key: Ord + Hash + Copy + Default, Payload: Clone + Default> Into<TransactionResult<Key, Payload>> for Vec<RecordPoint<Key, Payload>> {
    fn into(self) -> TransactionResult<Key, Payload> {
        MatchedRecords(self)
    }
}

// /// Sugar implementation, wrapping collection of records to a TransactionResult.
// impl<Key: Ord + Hash + Copy + Default, Payload: Clone + Default> Into<TransactionResult<Key, Payload>> for Vec<Record<Key, Payload>> {
//     fn into(self) -> TransactionResult<Key, Payload> {
//         MatchedRecordsVersioned(self)
//     }
// }

/// Sugar implementation, wrapping a potential record to a TransactionResult.
impl<Key: Ord + Hash + Copy + Default, Payload: Clone + Default> Into<TransactionResult<Key, Payload>> for Option<RecordPoint<Key, Payload>> {
    fn into(self) -> TransactionResult<Key, Payload> {
        MatchedRecord(self)
    }
}

// /// Sugar implementation, wrapping a potential record to a TransactionResult.
// impl<Key: Ord + Hash + Copy + Default, Payload: Clone + Default> Into<TransactionResult<Key, Payload>> for Option<Record<Key, Payload>> {
//     fn into(self) -> TransactionResult<Key, Payload> {
//         MatchedRecordVersioned(self)
//     }
// }
