use discodb_types::{Lsn, MessageId, RowId, SegmentId, TableId, TxnId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct RowHeader {
    pub row_id: RowId,
    pub table_id: TableId,
    pub segment_id: SegmentId,
    pub message_id: MessageId,
    pub txn_id: TxnId,
    pub lsn: Lsn,
    pub checksum: u32,
    pub flags: RowFlags,
}

#[derive(Clone, Debug, Default)]
pub struct RowFlags(u8);

impl RowFlags {
    pub const TOMBSTONE: u8 = 0x01;
    pub const BLOB_POINTER: u8 = 0x02;
    pub const OVERFLOW: u8 = 0x04;

    pub fn new(bits: u8) -> Self {
        Self(bits)
    }

    pub fn bits(&self) -> u8 {
        self.0
    }

    pub fn has_tombstone(&self) -> bool {
        self.0 & Self::TOMBSTONE != 0
    }

    pub fn has_blob_pointer(&self) -> bool {
        self.0 & Self::BLOB_POINTER != 0
    }

    pub fn has_overflow(&self) -> bool {
        self.0 & Self::OVERFLOW != 0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RowBody {
    pub columns: Vec<ColumnValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ColumnValue {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Text(String),
    Json(String),
    BlobRef(BlobRef),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobRef {
    pub message_id: MessageId,
    pub offset: u32,
    pub length: u32,
}

#[derive(Clone, Debug)]
pub struct Row {
    pub header: RowHeader,
    pub body: RowBody,
}

impl Row {
    pub fn new(header: RowHeader, body: RowBody) -> Self {
        Self { header, body }
    }
}
