package mapping

import (
	"testing"

	"discodb/types"
)

func TestSegmentName(t *testing.T) {
	name := SegmentName(types.TableID(1), types.SegmentID(2))
	if name != "seg-1-2" {
		t.Errorf("SegmentName() = %q, want %q", name, "seg-1-2")
	}
}

func TestParseSegmentName(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantTable types.TableID
		wantSeg   types.SegmentID
		wantErr   bool
	}{
		{"valid", "seg-1-2", 1, 2, false},
		{"invalid prefix", "foo-1-2", 0, 0, true},
		{"too few parts", "seg-1", 0, 0, true},
		{"invalid table", "seg-abc-2", 0, 0, true},
		{"invalid segment", "seg-1-xyz", 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableID, segID, err := ParseSegmentName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSegmentName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if tableID != tt.wantTable {
					t.Errorf("tableID = %v, want %v", tableID, tt.wantTable)
				}
				if segID != tt.wantSeg {
					t.Errorf("segmentID = %v, want %v", segID, tt.wantSeg)
				}
			}
		})
	}
}

func TestPageHeaderEncodeDecode(t *testing.T) {
	header := PageHeader{
		SegmentID:   types.SegmentID(123),
		TableID:     types.TableID(456),
		RowCount:    100,
		FreeSlots:   50,
		LSN:         types.LSN(789),
		SchemaEpoch: types.SchemaEpoch(1),
		Checksum:    0xDEADBEEF,
	}

	encoded := header.EncodeToTopic()
	if encoded == "" {
		t.Fatal("EncodeToTopic() returned empty string")
	}

	decoded, err := ParsePageHeader(encoded)
	if err != nil {
		t.Fatalf("ParsePageHeader() error = %v", err)
	}

	if decoded.SegmentID != header.SegmentID {
		t.Errorf("SegmentID = %v, want %v", decoded.SegmentID, header.SegmentID)
	}
	if decoded.TableID != header.TableID {
		t.Errorf("TableID = %v, want %v", decoded.TableID, header.TableID)
	}
	if decoded.RowCount != header.RowCount {
		t.Errorf("RowCount = %v, want %v", decoded.RowCount, header.RowCount)
	}
	if decoded.FreeSlots != header.FreeSlots {
		t.Errorf("FreeSlots = %v, want %v", decoded.FreeSlots, header.FreeSlots)
	}
	if decoded.LSN != header.LSN {
		t.Errorf("LSN = %v, want %v", decoded.LSN, header.LSN)
	}
}

func TestParsePageHeader_Errors(t *testing.T) {
	tests := []struct {
		name  string
		topic string
	}{
		{"empty", ""},
		{"invalid base64", "!!!invalid!!!"},
		{"too short", "aGVsbG8="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParsePageHeader(tt.topic)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestOverflowChunkEncodeDecode(t *testing.T) {
	chunk := OverflowChunk{
		RowID:    types.RowID(12345),
		ChunkIdx: 2,
		Total:    5,
		Data:     []byte("hello world overflow data"),
	}

	encoded := EncodeChunkToMessage(chunk)
	if encoded == "" {
		t.Fatal("EncodeChunkToMessage() returned empty")
	}

	decoded, err := DecodeChunkFromMessage(encoded)
	if err != nil {
		t.Fatalf("DecodeChunkFromMessage() error = %v", err)
	}

	if decoded.RowID != chunk.RowID {
		t.Errorf("RowID = %v, want %v", decoded.RowID, chunk.RowID)
	}
	if decoded.ChunkIdx != chunk.ChunkIdx {
		t.Errorf("ChunkIdx = %v, want %v", decoded.ChunkIdx, chunk.ChunkIdx)
	}
	if decoded.Total != chunk.Total {
		t.Errorf("Total = %v, want %v", decoded.Total, chunk.Total)
	}
	if string(decoded.Data) != string(chunk.Data) {
		t.Errorf("Data = %q, want %q", decoded.Data, chunk.Data)
	}
}

func TestDecodeChunkFromMessage_Errors(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"not toast", "regular message"},
		{"wrong prefix", "FOO:123:1/2:data"},
		{"missing parts", "TOAST:123"},
		{"invalid chunk info", "TOAST:123:abc:data"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeChunkFromMessage(tt.content)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestRowFlags(t *testing.T) {
	flags := RowFlags{
		IsLive:     true,
		IsDead:     false,
		IsLocked:   true,
		IsOverflow: false,
	}

	reactions := flags.ToReactions()
	if len(reactions) != 2 {
		t.Errorf("ToReactions() returned %d reactions, want 2", len(reactions))
	}

	parsed := ParseRowFlags(reactions)
	if !parsed.IsLive {
		t.Error("IsLive should be true")
	}
	if parsed.IsDead {
		t.Error("IsDead should be false")
	}
	if !parsed.IsLocked {
		t.Error("IsLocked should be true")
	}
}

func TestFSMRoleName(t *testing.T) {
	name := EncodeFSMRoleName(types.TableID(1), types.PageID(2))
	if name != "fsm::1::2" {
		t.Errorf("EncodeFSMRoleName() = %q, want %q", name, "fsm::1::2")
	}

	tableID, pageID, err := ParseFSMRoleName(name)
	if err != nil {
		t.Fatalf("ParseFSMRoleName() error = %v", err)
	}
	if tableID != types.TableID(1) {
		t.Errorf("tableID = %v, want 1", tableID)
	}
	if pageID != types.PageID(2) {
		t.Errorf("pageID = %v, want 2", pageID)
	}
}

func TestIndexKeyEncodeDecode(t *testing.T) {
	tableID := types.TableID(42)
	indexName := "pk_users"
	key := []byte{0x01, 0x02, 0x03, 0xFF}

	encoded := EncodeIndexKey(tableID, indexName, key)
	if encoded == "" {
		t.Fatal("EncodeIndexKey() returned empty")
	}

	gotTable, gotIndex, gotKey, err := ParseIndexKey(encoded)
	if err != nil {
		t.Fatalf("ParseIndexKey() error = %v", err)
	}

	if gotTable != tableID {
		t.Errorf("tableID = %v, want %v", gotTable, tableID)
	}
	if gotIndex != indexName {
		t.Errorf("indexName = %v, want %v", gotIndex, indexName)
	}
	if string(gotKey) != string(key) {
		t.Errorf("key = %v, want %v", gotKey, key)
	}
}

func TestBootRecord(t *testing.T) {
	record := BootRecord{
		Version:         1,
		CatalogCategory: types.ChannelID(123),
		WALChannel:      types.ChannelID(456),
		CurrentEpoch:    types.SchemaEpoch(1),
		Checksum:        0xABCD,
	}

	encoded, err := record.EncodeBootRecord()
	if err != nil {
		t.Fatalf("EncodeBootRecord() error = %v", err)
	}

	decoded, err := ParseBootRecord(encoded)
	if err != nil {
		t.Fatalf("ParseBootRecord() error = %v", err)
	}

	if decoded.Version != record.Version {
		t.Errorf("Version = %v, want %v", decoded.Version, record.Version)
	}
	if decoded.CatalogCategory != record.CatalogCategory {
		t.Errorf("CatalogCategory = %v, want %v", decoded.CatalogCategory, record.CatalogCategory)
	}
	if decoded.WALChannel != record.WALChannel {
		t.Errorf("WALChannel = %v, want %v", decoded.WALChannel, record.WALChannel)
	}
}

func TestOverflowRefEncodeDecode(t *testing.T) {
	ref := OverflowRef{
		ThreadID: types.ChannelID(999),
		Chunks:   5,
	}

	data, err := ref.Encode()
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	var decoded OverflowRef
	if err := decoded.Decode(data); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if decoded.ThreadID != ref.ThreadID {
		t.Errorf("ThreadID = %v, want %v", decoded.ThreadID, ref.ThreadID)
	}
	if decoded.Chunks != ref.Chunks {
		t.Errorf("Chunks = %v, want %v", decoded.Chunks, ref.Chunks)
	}
}
