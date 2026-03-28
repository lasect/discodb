package wire

import (
	"encoding/binary"
	"testing"

	"discodb/types"
)

func TestSerializeDataRow(t *testing.T) {
	value := types.TextValue("hello").PGText()
	buf := serializeDataRow([]*string{&value})
	if buf[0] != 'D' {
		t.Fatalf("unexpected message kind: %q", buf[0])
	}
	if got := binary.BigEndian.Uint32(buf[1:5]); got <= 4 {
		t.Fatalf("unexpected message length: %d", got)
	}
}
