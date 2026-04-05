package index

import (
	"testing"

	"discodb/types"
)

func TestEncodeDecodeEntry(t *testing.T) {
	key := []byte("test_key")
	rowID := types.RowID(42)
	segmentID := types.SegmentID(7)
	messageID := types.MessageID(12345)

	encoded := EncodeEntry(key, rowID, segmentID, messageID, false)
	entry, err := DecodeEntry(encoded)
	if err != nil {
		t.Fatalf("DecodeEntry failed: %v", err)
	}

	if string(entry.Key) != string(key) {
		t.Errorf("key mismatch: got %q, want %q", entry.Key, key)
	}
	if entry.RowID != rowID {
		t.Errorf("row_id mismatch: got %d, want %d", entry.RowID, rowID)
	}
	if entry.SegmentID != segmentID {
		t.Errorf("segment_id mismatch: got %d, want %d", entry.SegmentID, segmentID)
	}
	if entry.MessageID != messageID {
		t.Errorf("message_id mismatch: got %d, want %d", entry.MessageID, messageID)
	}
	if entry.Deleted {
		t.Errorf("expected not deleted")
	}
}

func TestEncodeDecodeEntryDeleted(t *testing.T) {
	key := []byte("deleted_key")
	rowID := types.RowID(99)
	segmentID := types.SegmentID(3)
	messageID := types.MessageID(67890)

	encoded := EncodeEntry(key, rowID, segmentID, messageID, true)
	entry, err := DecodeEntry(encoded)
	if err != nil {
		t.Fatalf("DecodeEntry failed: %v", err)
	}

	if !entry.Deleted {
		t.Errorf("expected deleted flag to be true")
	}
	if entry.RowID != rowID {
		t.Errorf("row_id mismatch: got %d, want %d", entry.RowID, rowID)
	}
}

func TestDecodeEntryInvalid(t *testing.T) {
	tests := []string{
		"",
		"KEY::foo",
		"KEY::foo\nrow_id=abc",
		"INVALID::foo\nrow_id=1\nseg=1\nmsg_id=1",
	}

	for _, content := range tests {
		_, err := DecodeEntry(content)
		if err == nil {
			t.Errorf("expected error for content %q", content)
		}
	}
}

func TestEncodeDecodeInternalNode(t *testing.T) {
	node := InternalNode{
		Level: 2,
		Keys:  [][]byte{[]byte("a"), []byte("m"), []byte("z")},
		Children: []types.ChannelID{
			types.ChannelID(100),
			types.ChannelID(200),
			types.ChannelID(300),
		},
	}

	encoded := EncodeInternalNode(node)
	decoded, err := DecodeInternalNode(encoded)
	if err != nil {
		t.Fatalf("DecodeInternalNode failed: %v", err)
	}

	if decoded.Level != node.Level {
		t.Errorf("level mismatch: got %d, want %d", decoded.Level, node.Level)
	}
	if len(decoded.Keys) != len(node.Keys) {
		t.Fatalf("keys count mismatch: got %d, want %d", len(decoded.Keys), len(node.Keys))
	}
	for i, k := range decoded.Keys {
		if string(k) != string(node.Keys[i]) {
			t.Errorf("key[%d] mismatch: got %q, want %q", i, k, node.Keys[i])
		}
	}
	if len(decoded.Children) != len(node.Children) {
		t.Fatalf("children count mismatch: got %d, want %d", len(decoded.Children), len(node.Children))
	}
	for i, c := range decoded.Children {
		if c != node.Children[i] {
			t.Errorf("child[%d] mismatch: got %d, want %d", i, c, node.Children[i])
		}
	}
}

func TestDecodeInternalNodeInvalid(t *testing.T) {
	tests := []string{
		"",
		"INVALID|",
		"NODE|abc|keys=[]|children=[]",
	}

	for _, content := range tests {
		_, err := DecodeInternalNode(content)
		if err == nil {
			t.Errorf("expected error for content %q", content)
		}
	}
}

func TestGenerateIndexName(t *testing.T) {
	name := GenerateIndexName(types.TableID(5), []string{"email"})
	if name == "" {
		t.Error("index name is empty")
	}

	expected := GenerateIndexName(types.TableID(5), []string{"email"})
	if name != expected {
		t.Errorf("index name not deterministic: got %q, want %q", name, expected)
	}

	name2 := GenerateIndexName(types.TableID(5), []string{"name"})
	if name == name2 {
		t.Errorf("different columns should produce different names: %q == %q", name, name2)
	}
}

func TestGeneratePostTitle(t *testing.T) {
	key := []byte("hello")
	title := GeneratePostTitle(key)
	if title == "" {
		t.Error("post title is empty")
	}

	title2 := GeneratePostTitle(key)
	if title != title2 {
		t.Errorf("post title not deterministic: got %q, want %q", title, title2)
	}
}

func TestGenerateMetaChannelName(t *testing.T) {
	name := GenerateMetaChannelName("idx::5::abc123")
	if name != "idx_meta_idx::5::abc123" {
		t.Errorf("meta channel name mismatch: got %q, want %q", name, "idx_meta_idx::5::abc123")
	}
}
