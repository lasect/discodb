package storage

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"

	"discodb/types"
)

type RowHeader struct {
	RowID     types.RowID     `json:"row_id"`
	TableID   types.TableID   `json:"table_id"`
	SegmentID types.SegmentID `json:"segment_id"`
	MessageID types.MessageID `json:"message_id"`
	TxnID     types.TxnID     `json:"txn_id"`
	LSN       types.LSN       `json:"lsn"`
	Checksum  uint32          `json:"checksum"`
	Flags     RowFlags        `json:"flags"`
}

type RowFlags uint8

const (
	FlagTombstone   RowFlags = 0x01
	FlagBlobPointer RowFlags = 0x02
	FlagOverflow    RowFlags = 0x04
)

func (f RowFlags) HasTombstone() bool   { return f&FlagTombstone != 0 }
func (f RowFlags) HasBlobPointer() bool { return f&FlagBlobPointer != 0 }
func (f RowFlags) HasOverflow() bool    { return f&FlagOverflow != 0 }

type BlobRef struct {
	MessageID types.MessageID `json:"message_id"`
	Offset    uint32          `json:"offset"`
	Length    uint32          `json:"length"`
}

type ColumnValue struct {
	Kind    string          `json:"kind"`
	Bool    *bool           `json:"bool,omitempty"`
	Int16   *int16          `json:"int16,omitempty"`
	Int32   *int32          `json:"int32,omitempty"`
	Int64   *int64          `json:"int64,omitempty"`
	Float32 *float32        `json:"float32,omitempty"`
	Float64 *float64        `json:"float64,omitempty"`
	Text    *string         `json:"text,omitempty"`
	JSON    json.RawMessage `json:"json,omitempty"`
	BlobRef *BlobRef        `json:"blob_ref,omitempty"`
	Raw     []byte          `json:"-"` // Raw byte data for blob/attachment handling
}

type RowBody struct {
	Columns []ColumnValue `json:"columns"`
}

type Row struct {
	Header RowHeader `json:"header"`
	Body   RowBody   `json:"body"`
}

// RowHeaderSize is the fixed size of encoded row header in bytes
const RowHeaderSize = 53 // 6x uint64 (48) + flags (1) + checksum (4)

func EncodeRowHeader(header RowHeader) []byte {
	buf := make([]byte, RowHeaderSize)
	binary.BigEndian.PutUint64(buf[0:8], header.RowID.Uint64())
	binary.BigEndian.PutUint64(buf[8:16], header.TableID.Uint64())
	binary.BigEndian.PutUint64(buf[16:24], header.SegmentID.Uint64())
	binary.BigEndian.PutUint64(buf[24:32], header.MessageID.Uint64())
	binary.BigEndian.PutUint64(buf[32:40], header.TxnID.Uint64())
	binary.BigEndian.PutUint64(buf[40:48], header.LSN.Uint64())
	buf[48] = byte(header.Flags)
	binary.BigEndian.PutUint32(buf[49:53], header.Checksum)
	return buf
}

func DecodeRowHeader(data []byte) (RowHeader, bool) {
	if len(data) < RowHeaderSize {
		return RowHeader{}, false
	}
	return RowHeader{
		RowID:     types.MustRowID(binary.BigEndian.Uint64(data[0:8])),
		TableID:   types.MustTableID(binary.BigEndian.Uint64(data[8:16])),
		SegmentID: types.MustSegmentID(binary.BigEndian.Uint64(data[16:24])),
		MessageID: types.MessageID(binary.BigEndian.Uint64(data[24:32])),
		TxnID:     types.MustTxnID(binary.BigEndian.Uint64(data[32:40])),
		LSN:       types.MustLSN(binary.BigEndian.Uint64(data[40:48])),
		Flags:     RowFlags(data[48]),
		Checksum:  binary.BigEndian.Uint32(data[49:53]),
	}, true
}

func EncodeRowBody(body RowBody) []byte {
	data, _ := json.Marshal(body)
	return data
}

func DecodeRowBody(data []byte) (RowBody, bool) {
	var body RowBody
	if err := json.Unmarshal(data, &body); err != nil {
		return RowBody{}, false
	}
	return body, true
}

func ComputeChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// ComputeRowChecksum computes checksum for a row (over body data)
func ComputeRowChecksum(body RowBody) uint32 {
	bodyData := EncodeRowBody(body)
	return ComputeChecksum(bodyData)
}

// ValidateRowChecksum verifies the checksum in a row header matches the body
func ValidateRowChecksum(header RowHeader, body RowBody) bool {
	expected := ComputeRowChecksum(body)
	return header.Checksum == expected
}

// ChecksumError represents a checksum validation failure
type ChecksumError struct {
	Expected uint32
	Actual   uint32
	RowID    types.RowID
}

func (e ChecksumError) Error() string {
	return fmt.Sprintf("checksum mismatch for row %d: expected %08x, got %08x", e.RowID, e.Expected, e.Actual)
}

func EncodeMessageContent(header RowHeader) string {
	return base64.StdEncoding.EncodeToString(EncodeRowHeader(header))
}

func DecodeMessageContent(content string) (RowHeader, bool) {
	decoded, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		return RowHeader{}, false
	}
	return DecodeRowHeader(decoded)
}
