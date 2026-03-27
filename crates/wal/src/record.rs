use discodb_types::{Lsn, MessageId, RowId, SegmentId, TableId, TxnId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WalRecord {
    Begin(BeginRecord),
    Prepare(PrepareRecord),
    Insert(InsertRecord),
    Update(UpdateRecord),
    Delete(DeleteRecord),
    IndexInsert(IndexInsertRecord),
    IndexDelete(IndexDeleteRecord),
    Commit(CommitRecord),
    Abort(AbortRecord),
    Checkpoint(CheckpointRecord),
    ReserveSlot(ReserveSlotRecord),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BeginRecord {
    pub txn_id: TxnId,
    pub lsn: Lsn,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareRecord {
    pub txn_id: TxnId,
    pub lsn: Lsn,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InsertRecord {
    pub txn_id: TxnId,
    pub lsn: Lsn,
    pub table_id: TableId,
    pub row_id: RowId,
    pub segment_id: SegmentId,
    pub message_id: MessageId,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpdateRecord {
    pub txn_id: TxnId,
    pub lsn: Lsn,
    pub table_id: TableId,
    pub row_id: RowId,
    pub old_segment_id: SegmentId,
    pub old_message_id: MessageId,
    pub new_segment_id: SegmentId,
    pub new_message_id: MessageId,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeleteRecord {
    pub txn_id: TxnId,
    pub lsn: Lsn,
    pub table_id: TableId,
    pub row_id: RowId,
    pub segment_id: SegmentId,
    pub message_id: MessageId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexInsertRecord {
    pub txn_id: TxnId,
    pub lsn: Lsn,
    pub index_id: TableId,
    pub key: Vec<u8>,
    pub row_pointer: RowPointer,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexDeleteRecord {
    pub txn_id: TxnId,
    pub lsn: Lsn,
    pub index_id: TableId,
    pub key: Vec<u8>,
    pub row_pointer: RowPointer,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitRecord {
    pub txn_id: TxnId,
    pub lsn: Lsn,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AbortRecord {
    pub txn_id: TxnId,
    pub lsn: Lsn,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub lsn: Lsn,
    pub catalog_lsn: Lsn,
    pub table_states: Vec<TableState>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableState {
    pub table_id: TableId,
    pub segment_id: SegmentId,
    pub last_row_id: RowId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReserveSlotRecord {
    pub txn_id: TxnId,
    pub lsn: Lsn,
    pub table_id: TableId,
    pub segment_id: SegmentId,
    pub slot_index: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RowPointer {
    pub row_id: RowId,
    pub segment_id: SegmentId,
    pub message_id: MessageId,
}
