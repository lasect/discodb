package wal

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"

	"discodb/types"
)

type RowPointer struct {
	RowID     types.RowID     `json:"row_id"`
	SegmentID types.SegmentID `json:"segment_id"`
	MessageID types.MessageID `json:"message_id"`
}

type TableState struct {
	TableID   types.TableID   `json:"table_id"`
	SegmentID types.SegmentID `json:"segment_id"`
	LastRowID types.RowID     `json:"last_row_id"`
}

type Record struct {
	Kind         string          `json:"kind"`
	TxnID        types.TxnID     `json:"txn_id,omitempty"`
	LSN          types.LSN       `json:"lsn,omitempty"`
	TableID      types.TableID   `json:"table_id,omitempty"`
	RowID        types.RowID     `json:"row_id,omitempty"`
	SegmentID    types.SegmentID `json:"segment_id,omitempty"`
	MessageID    types.MessageID `json:"message_id,omitempty"`
	OldSegmentID types.SegmentID `json:"old_segment_id,omitempty"`
	OldMessageID types.MessageID `json:"old_message_id,omitempty"`
	NewSegmentID types.SegmentID `json:"new_segment_id,omitempty"`
	NewMessageID types.MessageID `json:"new_message_id,omitempty"`
	IndexID      types.TableID   `json:"index_id,omitempty"`
	Key          []byte          `json:"key,omitempty"`
	Data         []byte          `json:"data,omitempty"`
	RowPointer   *RowPointer     `json:"row_pointer,omitempty"`
	CatalogLSN   types.LSN       `json:"catalog_lsn,omitempty"`
	TableStates  []TableState    `json:"table_states,omitempty"`
	SlotIndex    uint32          `json:"slot_index,omitempty"`
}

func Begin(txnID types.TxnID, lsn types.LSN) Record {
	return Record{Kind: "BEGIN", TxnID: txnID, LSN: lsn}
}

func Commit(txnID types.TxnID, lsn types.LSN) Record {
	return Record{Kind: "COMMIT", TxnID: txnID, LSN: lsn}
}

type Writer struct {
	writeIDCounter uint64
}

func NewWriter() *Writer {
	return &Writer{}
}

func (w *Writer) EncodeRecord(record Record) []byte {
	payload, _ := json.Marshal(record)
	out := make([]byte, 16+len(payload))
	binary.BigEndian.PutUint64(out[0:8], w.writeIDCounter)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(payload)))
	binary.BigEndian.PutUint32(out[12:16], crc32.ChecksumIEEE(payload))
	copy(out[16:], payload)
	return out
}

func DecodeRecord(data []byte) (Record, uint64, bool) {
	if len(data) < 16 {
		return Record{}, 0, false
	}
	writeID := binary.BigEndian.Uint64(data[0:8])
	payloadLen := int(binary.BigEndian.Uint32(data[8:12]))
	checksum := binary.BigEndian.Uint32(data[12:16])
	if len(data) < 16+payloadLen {
		return Record{}, 0, false
	}
	payload := data[16 : 16+payloadLen]
	if crc32.ChecksumIEEE(payload) != checksum {
		return Record{}, 0, false
	}
	var record Record
	if err := json.Unmarshal(payload, &record); err != nil {
		return Record{}, 0, false
	}
	return record, writeID, true
}

func (w *Writer) ComputeWriteID(txnID types.TxnID, lsn types.LSN) uint64 {
	w.writeIDCounter++
	base := (txnID.Uint64() << 16) | (lsn.Uint64() & 0xFFFF)
	return base ^ w.writeIDCounter
}
