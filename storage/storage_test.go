package storage

import (
	"testing"

	"discodb/types"
)

func TestRowHeaderRoundTrip(t *testing.T) {
	header := RowHeader{
		RowID:     types.MustRowID(1),
		TableID:   types.MustTableID(2),
		SegmentID: types.MustSegmentID(3),
		MessageID: types.MustMessageID(4),
		TxnID:     types.MustTxnID(5),
		LSN:       types.MustLSN(6),
		Flags:     FlagOverflow | FlagTombstone,
	}

	encoded := EncodeMessageContent(header)
	decoded, ok := DecodeMessageContent(encoded)
	if !ok {
		t.Fatal("expected message content decode to succeed")
	}
	if decoded.RowID != header.RowID || decoded.Flags != header.Flags {
		t.Fatalf("unexpected decode result: %#v", decoded)
	}
}
