package storage

import (
	"testing"

	"discodb/mapping"
	"discodb/types"
)

func TestEncodeRowToDiscord(t *testing.T) {
	header := RowHeader{
		RowID:     types.MustRowID(1),
		TableID:   types.MustTableID(1),
		SegmentID: types.MustSegmentID(1),
		MessageID: types.MustMessageID(1),
		TxnID:     types.MustTxnID(1),
		LSN:       types.MustLSN(1),
		Flags:     0,
	}

	int32Val := int32(42)
	textVal := "hello world"
	body := RowBody{
		Columns: []ColumnValue{
			{Kind: "int4", Int32: &int32Val},
			{Kind: "text", Text: &textVal},
		},
	}

	row := Row{Header: header, Body: body}
	result, err := EncodeRowToDiscord(row, types.SchemaEpoch(1))
	if err != nil {
		t.Fatalf("EncodeRowToDiscord() error = %v", err)
	}

	if result.Message.Content == "" {
		t.Error("expected non-empty content")
	}
	if len(result.Message.Embeds) == 0 {
		t.Error("expected at least one embed")
	}
	if result.NeedsOverflow {
		t.Error("small row should not need overflow")
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	header := RowHeader{
		RowID:     types.MustRowID(100),
		TableID:   types.MustTableID(2),
		SegmentID: types.MustSegmentID(3),
		MessageID: types.MustMessageID(400),
		TxnID:     types.MustTxnID(500),
		LSN:       types.MustLSN(600),
		Flags:     0,
	}

	boolVal := true
	int16Val := int16(123)
	int64Val := int64(9999999)
	float64Val := float64(3.14159)
	textVal := "test string"

	body := RowBody{
		Columns: []ColumnValue{
			{Kind: "bool", Bool: &boolVal},
			{Kind: "int2", Int16: &int16Val},
			{Kind: "int8", Int64: &int64Val},
			{Kind: "float8", Float64: &float64Val},
			{Kind: "text", Text: &textVal},
		},
	}

	row := Row{Header: header, Body: body}
	result, err := EncodeRowToDiscord(row, types.SchemaEpoch(1))
	if err != nil {
		t.Fatalf("EncodeRowToDiscord() error = %v", err)
	}

	// Decode back
	decodedRow, epoch, err := DecodeRowFromDiscord(result.Message.Content, result.Message.Embeds, nil)
	if err != nil {
		t.Fatalf("DecodeRowFromDiscord() error = %v", err)
	}

	if epoch != types.SchemaEpoch(1) {
		t.Errorf("epoch = %v, want 1", epoch)
	}

	// Check header
	if decodedRow.Header.RowID != header.RowID {
		t.Errorf("RowID = %v, want %v", decodedRow.Header.RowID, header.RowID)
	}
	if decodedRow.Header.TableID != header.TableID {
		t.Errorf("TableID = %v, want %v", decodedRow.Header.TableID, header.TableID)
	}

	// Check column count
	if len(decodedRow.Body.Columns) != len(body.Columns) {
		t.Errorf("column count = %d, want %d", len(decodedRow.Body.Columns), len(body.Columns))
	}
}

func TestComputeStorageRequirements(t *testing.T) {
	header := RowHeader{
		RowID:     types.MustRowID(1),
		TableID:   types.MustTableID(1),
		SegmentID: types.MustSegmentID(1),
		MessageID: types.MustMessageID(1),
		TxnID:     types.MustTxnID(1),
		LSN:       types.MustLSN(1),
	}

	textVal := "small"
	row := Row{
		Header: header,
		Body: RowBody{
			Columns: []ColumnValue{{Kind: "text", Text: &textVal}},
		},
	}

	req := ComputeStorageRequirements(row)
	if req.HeaderSize == 0 {
		t.Error("expected non-zero header size")
	}
	if req.BodySize == 0 {
		t.Error("expected non-zero body size")
	}
	if req.NeedsOverflow {
		t.Error("small row should not need overflow")
	}
	if req.NeedsBlob {
		t.Error("row without blobs should not need blob")
	}
}

func TestSplitToChunks(t *testing.T) {
	rowID := types.MustRowID(123)
	data := make([]byte, 5000) // About 4 chunks
	for i := range data {
		data[i] = byte(i % 256)
	}

	chunks := splitToChunks(rowID, data)
	if len(chunks) == 0 {
		t.Fatal("expected chunks")
	}

	// Verify all chunks have correct metadata
	for i, chunk := range chunks {
		if chunk.RowID != rowID {
			t.Errorf("chunk %d: RowID = %v, want %v", i, chunk.RowID, rowID)
		}
		if chunk.ChunkIdx != i {
			t.Errorf("chunk %d: ChunkIdx = %v, want %v", i, chunk.ChunkIdx, i)
		}
		if chunk.Total != len(chunks) {
			t.Errorf("chunk %d: Total = %v, want %v", i, chunk.Total, len(chunks))
		}
	}

	// Reassemble and verify
	manager := NewTOASTManager(types.ChannelID(0))
	reassembled, err := manager.ChunkReader(chunks)
	if err != nil {
		t.Fatalf("ChunkReader() error = %v", err)
	}
	if len(reassembled) != len(data) {
		t.Errorf("reassembled length = %d, want %d", len(reassembled), len(data))
	}
	for i := range data {
		if reassembled[i] != data[i] {
			t.Errorf("reassembled[%d] = %v, want %v", i, reassembled[i], data[i])
			break
		}
	}
}

func TestTOASTManagerChunkReader_Errors(t *testing.T) {
	manager := NewTOASTManager(types.ChannelID(0))

	// Empty chunks
	_, err := manager.ChunkReader(nil)
	if err == nil {
		t.Error("expected error for empty chunks")
	}

	// Missing chunk
	chunks := []mapping.OverflowChunk{
		{RowID: 1, ChunkIdx: 0, Total: 3, Data: []byte("a")},
		{RowID: 1, ChunkIdx: 2, Total: 3, Data: []byte("c")}, // Missing 1
	}
	_, err = manager.ChunkReader(chunks)
	if err == nil {
		t.Error("expected error for missing chunk")
	}

	// Wrong total
	chunks = []mapping.OverflowChunk{
		{RowID: 1, ChunkIdx: 0, Total: 5, Data: []byte("a")},
		{RowID: 1, ChunkIdx: 1, Total: 3, Data: []byte("b")}, // Different total
	}
	_, err = manager.ChunkReader(chunks)
	if err == nil {
		t.Error("expected error for mismatched total")
	}
}

func TestValidateChunks(t *testing.T) {
	rowID := types.MustRowID(42)
	chunks := []mapping.OverflowChunk{
		{RowID: rowID, ChunkIdx: 0, Total: 2, Data: []byte("a")},
		{RowID: rowID, ChunkIdx: 1, Total: 2, Data: []byte("b")},
	}

	if err := ValidateChunks(chunks, rowID); err != nil {
		t.Errorf("ValidateChunks() error = %v", err)
	}

	// Wrong row ID
	if err := ValidateChunks(chunks, types.MustRowID(999)); err == nil {
		t.Error("expected error for wrong row ID")
	}

	// Duplicate chunk
	dupChunks := []mapping.OverflowChunk{
		{RowID: rowID, ChunkIdx: 0, Total: 2, Data: []byte("a")},
		{RowID: rowID, ChunkIdx: 0, Total: 2, Data: []byte("b")}, // Duplicate
	}
	if err := ValidateChunks(dupChunks, rowID); err == nil {
		t.Error("expected error for duplicate chunk")
	}
}

func TestIsToastChunk(t *testing.T) {
	if !IsToastChunk("TOAST:123:0/1:data") {
		t.Error("should recognize TOAST chunk")
	}
	if IsToastChunk("regular message") {
		t.Error("should not recognize regular message as TOAST")
	}
}

// --- Integrity Tests ---

func TestChecksumValidation(t *testing.T) {
	header := RowHeader{
		RowID:     types.MustRowID(1),
		TableID:   types.MustTableID(1),
		SegmentID: types.MustSegmentID(1),
		MessageID: types.MustMessageID(1),
		TxnID:     types.MustTxnID(1),
		LSN:       types.MustLSN(1),
		Flags:     0,
	}

	textVal := "test data for checksum"
	body := RowBody{
		Columns: []ColumnValue{
			{Kind: "text", Text: &textVal},
		},
	}

	row := Row{Header: header, Body: body}
	result, err := EncodeRowToDiscord(row, types.SchemaEpoch(1))
	if err != nil {
		t.Fatalf("EncodeRowToDiscord() error = %v", err)
	}

	// Verify checksum was set
	decodedHeader, ok := DecodeMessageContent(result.Message.Content)
	if !ok {
		t.Fatal("failed to decode header")
	}
	if decodedHeader.Checksum == 0 {
		t.Error("expected non-zero checksum in header")
	}

	// Decode should succeed with valid checksum
	_, _, err = DecodeRowFromDiscord(result.Message.Content, result.Message.Embeds, nil)
	if err != nil {
		t.Errorf("DecodeRowFromDiscord() should succeed with valid checksum: %v", err)
	}
}

func TestCorruptedChecksumDetected(t *testing.T) {
	header := RowHeader{
		RowID:     types.MustRowID(1),
		TableID:   types.MustTableID(1),
		SegmentID: types.MustSegmentID(1),
		MessageID: types.MustMessageID(1),
		TxnID:     types.MustTxnID(1),
		LSN:       types.MustLSN(1),
		Flags:     0,
	}

	textVal := "original data"
	body := RowBody{
		Columns: []ColumnValue{
			{Kind: "text", Text: &textVal},
		},
	}

	row := Row{Header: header, Body: body}
	result, err := EncodeRowToDiscord(row, types.SchemaEpoch(1))
	if err != nil {
		t.Fatalf("EncodeRowToDiscord() error = %v", err)
	}

	// Corrupt the embed data (body)
	if len(result.Message.Embeds) > 0 {
		// Modify the description to simulate corruption
		corruptedEmbed := DiscordEmbed{
			Description: `{"epoch":1,"cols":[{"i":0,"t":"text","v":"CORRUPTED DATA"}]}`,
		}
		result.Message.Embeds[0] = corruptedEmbed
	}

	// Decode should fail due to checksum mismatch
	_, _, err = DecodeRowFromDiscord(result.Message.Content, result.Message.Embeds, nil)
	if err == nil {
		t.Error("expected checksum error for corrupted data")
	}

	// Verify it's specifically a ChecksumError
	if _, ok := err.(ChecksumError); !ok {
		t.Errorf("expected ChecksumError, got %T: %v", err, err)
	}
}

func TestNullBitmapRoundTrip(t *testing.T) {
	header := RowHeader{
		RowID:     types.MustRowID(1),
		TableID:   types.MustTableID(1),
		SegmentID: types.MustSegmentID(1),
		MessageID: types.MustMessageID(1),
		TxnID:     types.MustTxnID(1),
		LSN:       types.MustLSN(1),
	}

	int32Val := int32(42)
	textVal := "hello"
	body := RowBody{
		Columns: []ColumnValue{
			{Kind: "int4", Int32: &int32Val}, // col 0: not null
			{Kind: "null"},                   // col 1: null
			{Kind: "text", Text: &textVal},   // col 2: not null
			{Kind: "null"},                   // col 3: null
			{Kind: "null"},                   // col 4: null
		},
	}

	row := Row{Header: header, Body: body}
	result, err := EncodeRowToDiscord(row, types.SchemaEpoch(1))
	if err != nil {
		t.Fatalf("EncodeRowToDiscord() error = %v", err)
	}

	// Decode and verify nulls are preserved
	decoded, _, err := DecodeRowFromDiscord(result.Message.Content, result.Message.Embeds, nil)
	if err != nil {
		t.Fatalf("DecodeRowFromDiscord() error = %v", err)
	}

	if len(decoded.Body.Columns) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(decoded.Body.Columns))
	}

	// col 0 should have value
	if decoded.Body.Columns[0].Int32 == nil || *decoded.Body.Columns[0].Int32 != 42 {
		t.Error("col 0 should be int4 = 42")
	}

	// col 1 should be null
	if decoded.Body.Columns[1].Kind != "null" {
		t.Errorf("col 1 should be null, got %s", decoded.Body.Columns[1].Kind)
	}

	// col 2 should have value
	if decoded.Body.Columns[2].Text == nil || *decoded.Body.Columns[2].Text != "hello" {
		t.Error("col 2 should be text = 'hello'")
	}

	// col 3, 4 should be null
	if decoded.Body.Columns[3].Kind != "null" {
		t.Errorf("col 3 should be null, got %s", decoded.Body.Columns[3].Kind)
	}
	if decoded.Body.Columns[4].Kind != "null" {
		t.Errorf("col 4 should be null, got %s", decoded.Body.Columns[4].Kind)
	}
}

func TestChecksumComputeAndValidate(t *testing.T) {
	int32Val := int32(100)
	body := RowBody{
		Columns: []ColumnValue{
			{Kind: "int4", Int32: &int32Val},
		},
	}

	// Compute checksum
	checksum := ComputeRowChecksum(body)
	if checksum == 0 {
		t.Error("expected non-zero checksum")
	}

	// Same body should produce same checksum
	checksum2 := ComputeRowChecksum(body)
	if checksum != checksum2 {
		t.Errorf("checksum mismatch for same body: %d != %d", checksum, checksum2)
	}

	// Different body should produce different checksum
	differentVal := int32(200)
	differentBody := RowBody{
		Columns: []ColumnValue{
			{Kind: "int4", Int32: &differentVal},
		},
	}
	checksum3 := ComputeRowChecksum(differentBody)
	if checksum == checksum3 {
		t.Error("expected different checksum for different body")
	}

	// Validate checksum
	header := RowHeader{Checksum: checksum}
	if !ValidateRowChecksum(header, body) {
		t.Error("valid checksum should pass validation")
	}

	header.Checksum = 12345 // wrong checksum
	if ValidateRowChecksum(header, body) {
		t.Error("invalid checksum should fail validation")
	}
}

func TestIsNullColumn(t *testing.T) {
	tests := []struct {
		name     string
		col      ColumnValue
		wantNull bool
	}{
		{"explicit null kind", ColumnValue{Kind: "null"}, true},
		{"empty column", ColumnValue{}, true},
		{"bool true", ColumnValue{Kind: "bool", Bool: ptr(true)}, false},
		{"int32 zero", ColumnValue{Kind: "int4", Int32: ptr(int32(0))}, false},
		{"text empty", ColumnValue{Kind: "text", Text: ptr("")}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isNullColumn(tt.col)
			if got != tt.wantNull {
				t.Errorf("isNullColumn() = %v, want %v", got, tt.wantNull)
			}
		})
	}
}

// Helper to create pointers
func ptr[T any](v T) *T {
	return &v
}
