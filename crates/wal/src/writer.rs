use crate::WalRecord;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use discodb_types::{Lsn, TxnId};

pub struct WalWriter {
    write_id_counter: u64,
}

impl WalWriter {
    pub fn new() -> Self {
        Self {
            write_id_counter: 0,
        }
    }

    pub fn encode_record(&self, record: &WalRecord) -> Vec<u8> {
        let json = serde_json::to_vec(record).unwrap_or_default();
        let mut encoded = Vec::with_capacity(json.len() + 16);

        encoded
            .write_u64::<BigEndian>(self.write_id_counter)
            .unwrap();
        encoded.write_u32::<BigEndian>(json.len() as u32).unwrap();

        let checksum = Self::compute_checksum(&json);
        encoded.write_u32::<BigEndian>(checksum).unwrap();

        encoded.extend(json);
        encoded
    }

    pub fn decode_record(data: &[u8]) -> Option<(WalRecord, u64)> {
        if data.len() < 12 {
            return None;
        }

        let mut cursor = std::io::Cursor::new(data);

        let write_id = cursor.read_u64::<BigEndian>().ok()?;
        let len = cursor.read_u32::<BigEndian>().ok()? as usize;
        let checksum = cursor.read_u32::<BigEndian>().ok()?;

        if data.len() < 12 + len {
            return None;
        }

        let payload = &data[12..12 + len];
        let computed = Self::compute_checksum(payload);

        if checksum != computed {
            return None;
        }

        let record: WalRecord = serde_json::from_slice(payload).ok()?;
        Some((record, write_id))
    }

    pub fn compute_write_id(&mut self, txn_id: TxnId, lsn: Lsn) -> u64 {
        self.write_id_counter += 1;
        let base = (txn_id.get() << 16) | (lsn.get() & 0xFFFF);
        base ^ self.write_id_counter
    }

    fn compute_checksum(data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }
}

impl Default for WalWriter {
    fn default() -> Self {
        Self::new()
    }
}
