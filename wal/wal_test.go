package wal

import (
	"testing"

	"discodb/types"
)

func TestEncodeDecodeRecord(t *testing.T) {
	writer := NewWriter()
	record := Begin(types.MustTxnID(11), types.MustLSN(19))
	encoded := writer.EncodeRecord(record)

	decoded, _, ok := DecodeRecord(encoded)
	if !ok {
		t.Fatal("expected wal record decode to succeed")
	}
	if decoded.Kind != "BEGIN" || decoded.TxnID != record.TxnID {
		t.Fatalf("unexpected decode result: %#v", decoded)
	}
}
