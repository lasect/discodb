package types

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrZeroID = errors.New("id must be non-zero")

type GuildID uint64
type ChannelID uint64
type MessageID uint64
type TxnID uint64
type RowID uint64
type LSN uint64
type TableID uint64
type SegmentID uint64
type SchemaEpoch uint64
type PageID uint64

func newNonZeroID[T ~uint64](v uint64) (T, error) {
	if v == 0 {
		return 0, ErrZeroID
	}
	return T(v), nil
}

func MustGuildID(v uint64) GuildID             { return mustID[GuildID](v) }
func MustChannelID(v uint64) ChannelID         { return mustID[ChannelID](v) }
func MustMessageID(v uint64) MessageID         { return mustID[MessageID](v) }
func MustTxnID(v uint64) TxnID                 { return mustID[TxnID](v) }
func MustRowID(v uint64) RowID                 { return mustID[RowID](v) }
func MustLSN(v uint64) LSN                     { return mustID[LSN](v) }
func MustTableID(v uint64) TableID             { return mustID[TableID](v) }
func MustSegmentID(v uint64) SegmentID         { return mustID[SegmentID](v) }
func MustSchemaEpoch(v uint64) SchemaEpoch     { return mustID[SchemaEpoch](v) }
func MustPageID(v uint64) PageID               { return mustID[PageID](v) }
func NewGuildID(v uint64) (GuildID, error)     { return newNonZeroID[GuildID](v) }
func NewChannelID(v uint64) (ChannelID, error) { return newNonZeroID[ChannelID](v) }
func NewMessageID(v uint64) (MessageID, error) { return newNonZeroID[MessageID](v) }
func NewTxnID(v uint64) (TxnID, error)         { return newNonZeroID[TxnID](v) }
func NewRowID(v uint64) (RowID, error)         { return newNonZeroID[RowID](v) }
func NewLSN(v uint64) (LSN, error)             { return newNonZeroID[LSN](v) }
func NewTableID(v uint64) (TableID, error)     { return newNonZeroID[TableID](v) }
func NewSegmentID(v uint64) (SegmentID, error) { return newNonZeroID[SegmentID](v) }
func NewSchemaEpoch(v uint64) (SchemaEpoch, error) {
	return newNonZeroID[SchemaEpoch](v)
}
func NewPageID(v uint64) (PageID, error) { return newNonZeroID[PageID](v) }

func mustID[T ~uint64](v uint64) T {
	id, err := newNonZeroID[T](v)
	if err != nil {
		panic(err)
	}
	return id
}

func incrementID[T ~uint64](v T) T { return T(uint64(v) + 1) }

func (v GuildID) Uint64() uint64     { return uint64(v) }
func (v ChannelID) Uint64() uint64   { return uint64(v) }
func (v MessageID) Uint64() uint64   { return uint64(v) }
func (v TxnID) Uint64() uint64       { return uint64(v) }
func (v RowID) Uint64() uint64       { return uint64(v) }
func (v LSN) Uint64() uint64         { return uint64(v) }
func (v TableID) Uint64() uint64     { return uint64(v) }
func (v SegmentID) Uint64() uint64   { return uint64(v) }
func (v SchemaEpoch) Uint64() uint64 { return uint64(v) }
func (v PageID) Uint64() uint64      { return uint64(v) }

func (v GuildID) String() string     { return fmt.Sprintf("%d", v) }
func (v ChannelID) String() string   { return fmt.Sprintf("%d", v) }
func (v MessageID) String() string   { return fmt.Sprintf("%d", v) }
func (v TxnID) String() string       { return fmt.Sprintf("%d", v) }
func (v RowID) String() string       { return fmt.Sprintf("%d", v) }
func (v LSN) String() string         { return fmt.Sprintf("%d", v) }
func (v TableID) String() string     { return fmt.Sprintf("%d", v) }
func (v SegmentID) String() string   { return fmt.Sprintf("%d", v) }
func (v SchemaEpoch) String() string { return fmt.Sprintf("%d", v) }
func (v PageID) String() string      { return fmt.Sprintf("%d", v) }

func (v GuildID) Increment() GuildID         { return incrementID(v) }
func (v ChannelID) Increment() ChannelID     { return incrementID(v) }
func (v MessageID) Increment() MessageID     { return incrementID(v) }
func (v TxnID) Increment() TxnID             { return incrementID(v) }
func (v RowID) Increment() RowID             { return incrementID(v) }
func (v LSN) Increment() LSN                 { return incrementID(v) }
func (v TableID) Increment() TableID         { return incrementID(v) }
func (v SegmentID) Increment() SegmentID     { return incrementID(v) }
func (v SchemaEpoch) Increment() SchemaEpoch { return incrementID(v) }
func (v PageID) Increment() PageID           { return incrementID(v) }

func MinSchemaEpoch() SchemaEpoch { return 1 }
func MinTableID() TableID         { return 1 }

type DataType string

const (
	DataTypeNull      DataType = "null"
	DataTypeBool      DataType = "bool"
	DataTypeInt2      DataType = "int2"
	DataTypeInt4      DataType = "int4"
	DataTypeInt8      DataType = "int8"
	DataTypeFloat4    DataType = "float4"
	DataTypeFloat8    DataType = "float8"
	DataTypeText      DataType = "text"
	DataTypeJSON      DataType = "json"
	DataTypeBlob      DataType = "blob"
	DataTypeTimestamp DataType = "timestamp"
	DataTypeDate      DataType = "date"
)

func (d DataType) IsNull() bool {
	return d == DataTypeNull
}

func (d DataType) IsNumeric() bool {
	switch d {
	case DataTypeInt2, DataTypeInt4, DataTypeInt8, DataTypeFloat4, DataTypeFloat8:
		return true
	default:
		return false
	}
}

func (d DataType) IsTextual() bool {
	return d == DataTypeText || d == DataTypeJSON
}

type Value struct {
	Kind  DataType
	Raw   any
	Valid bool
}

func NullValue() Value                  { return Value{Kind: DataTypeNull} }
func BoolValue(v bool) Value            { return Value{Kind: DataTypeBool, Raw: v, Valid: true} }
func Int2Value(v int16) Value           { return Value{Kind: DataTypeInt2, Raw: v, Valid: true} }
func Int4Value(v int32) Value           { return Value{Kind: DataTypeInt4, Raw: v, Valid: true} }
func Int8Value(v int64) Value           { return Value{Kind: DataTypeInt8, Raw: v, Valid: true} }
func Float4Value(v float32) Value       { return Value{Kind: DataTypeFloat4, Raw: v, Valid: true} }
func Float8Value(v float64) Value       { return Value{Kind: DataTypeFloat8, Raw: v, Valid: true} }
func TextValue(v string) Value          { return Value{Kind: DataTypeText, Raw: v, Valid: true} }
func JSONValue(v json.RawMessage) Value { return Value{Kind: DataTypeJSON, Raw: v, Valid: true} }
func BlobValue(v []byte) Value {
	return Value{Kind: DataTypeBlob, Raw: append([]byte(nil), v...), Valid: true}
}
func TimestampValue(v int64) Value       { return Value{Kind: DataTypeTimestamp, Raw: v, Valid: true} }
func DateValue(v int32) Value            { return Value{Kind: DataTypeDate, Raw: v, Valid: true} }
func (v Value) DataType() DataType       { return v.Kind }
func (v Value) IsNull() bool             { return !v.Valid || v.Kind == DataTypeNull }
func (v Value) AsString() (string, bool) { s, ok := v.Raw.(string); return s, ok }
func (v Value) AsBytes() ([]byte, bool)  { b, ok := v.Raw.([]byte); return b, ok }
func (v Value) AsJSON() (json.RawMessage, bool) {
	b, ok := v.Raw.(json.RawMessage)
	return b, ok
}
func (v Value) AsInt64() (int64, bool) {
	switch n := v.Raw.(type) {
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	default:
		return 0, false
	}
}

func (v Value) AsFloat64() (float64, bool) {
	switch n := v.Raw.(type) {
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		if i, ok := v.AsInt64(); ok {
			return float64(i), true
		}
		return 0, false
	}
}

func (v Value) Equal(other Value) bool {
	if v.IsNull() && other.IsNull() {
		return true
	}
	if v.Kind != other.Kind {
		return false
	}
	switch x := v.Raw.(type) {
	case []byte:
		y, ok := other.Raw.([]byte)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	default:
		return fmt.Sprintf("%v", v.Raw) == fmt.Sprintf("%v", other.Raw)
	}
}

func (v Value) PGText() string {
	if v.IsNull() {
		return ""
	}
	switch x := v.Raw.(type) {
	case bool:
		if x {
			return "t"
		}
		return "f"
	case int16, int32, int64, float32, float64, string:
		return fmt.Sprintf("%v", x)
	case json.RawMessage:
		return string(x)
	case []byte:
		return "\\x" + hex.EncodeToString(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

type Column struct {
	Name     string   `json:"name"`
	DataType DataType `json:"data_type"`
	Nullable bool     `json:"nullable"`
	Default  *Value   `json:"default,omitempty"`
}

func NewColumn(name string, dataType DataType) Column {
	return Column{Name: name, DataType: dataType}
}

func (c Column) WithNullable() Column {
	c.Nullable = true
	return c
}

func (c Column) WithDefault(v Value) Column {
	c.Default = &v
	return c
}

type Row struct {
	Columns []Column `json:"columns"`
	Values  []Value  `json:"values"`
}

func NewRow(columns []Column, values []Value) (Row, error) {
	if len(columns) != len(values) {
		return Row{}, fmt.Errorf("column/value count mismatch: %d != %d", len(columns), len(values))
	}
	return Row{Columns: columns, Values: values}, nil
}

func (r Row) Get(idx int) (Value, bool) {
	if idx < 0 || idx >= len(r.Values) {
		return Value{}, false
	}
	return r.Values[idx], true
}

func (r Row) GetByName(name string) (Value, bool) {
	for i, col := range r.Columns {
		if col.Name == name {
			return r.Values[i], true
		}
	}
	return Value{}, false
}
