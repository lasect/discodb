use crate::{RowBody, RowFlags, RowHeader};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use discodb_types::{Lsn, MessageId, RowId, SegmentId, TableId, TxnId};

pub fn encode_row_header(header: &RowHeader) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);

    buf.write_u64::<BigEndian>(header.row_id.get()).unwrap();
    buf.write_u64::<BigEndian>(header.table_id.get()).unwrap();
    buf.write_u64::<BigEndian>(header.segment_id.get()).unwrap();
    buf.write_u64::<BigEndian>(header.message_id.get()).unwrap();
    buf.write_u64::<BigEndian>(header.txn_id.get()).unwrap();
    buf.write_u64::<BigEndian>(header.lsn.get()).unwrap();
    buf.write_u8(header.flags.bits()).unwrap();

    buf
}

pub fn decode_row_header(data: &[u8]) -> Option<RowHeader> {
    if data.len() < 49 {
        return None;
    }

    let mut cursor = std::io::Cursor::new(data);

    let row_id = RowId::new_unchecked(cursor.read_u64::<BigEndian>().ok()?);
    let table_id = TableId::new_unchecked(cursor.read_u64::<BigEndian>().ok()?);
    let segment_id = SegmentId::new_unchecked(cursor.read_u64::<BigEndian>().ok()?);
    let message_id = MessageId::new_unchecked(cursor.read_u64::<BigEndian>().ok()?);
    let txn_id = TxnId::new_unchecked(cursor.read_u64::<BigEndian>().ok()?);
    let lsn = Lsn::new_unchecked(cursor.read_u64::<BigEndian>().ok()?);
    let flags = cursor.read_u8().ok()?;

    Some(RowHeader {
        row_id,
        table_id,
        segment_id,
        message_id,
        txn_id,
        lsn,
        checksum: 0,
        flags: RowFlags::new(flags),
    })
}

pub fn encode_row_body(body: &RowBody) -> Vec<u8> {
    serde_json::to_vec(body).unwrap_or_default()
}

pub fn decode_row_body(data: &[u8]) -> Option<RowBody> {
    serde_json::from_slice(data).ok()
}

pub fn compute_checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

pub fn encode_message_content(header: &RowHeader) -> String {
    let encoded = encode_row_header(header);
    base64_encode(&encoded)
}

pub fn decode_message_content(content: &str) -> Option<RowHeader> {
    let decoded = base64_decode(content)?;
    decode_row_header(&decoded)
}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

fn base64_decode(data: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(data).ok()
}
