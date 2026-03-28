package types

import "testing"

func TestNewIDsRejectZero(t *testing.T) {
	if _, err := NewGuildID(0); err == nil {
		t.Fatal("expected zero guild id to fail")
	}
}

func TestValueConversions(t *testing.T) {
	v := Int4Value(42)
	if got, ok := v.AsInt64(); !ok || got != 42 {
		t.Fatalf("unexpected int conversion: %v %v", got, ok)
	}

	text := BlobValue([]byte{0xde, 0xad, 0xbe, 0xef}).PGText()
	if text != "\\xdeadbeef" {
		t.Fatalf("unexpected pg text: %s", text)
	}
}
