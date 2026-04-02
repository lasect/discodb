package storage

import (
	"strings"
	"testing"

	"discodb/constraints"
	"discodb/types"
)

func TestValidatorRowForStorage(t *testing.T) {
	v := NewValidator(false) // non-strict mode

	header := RowHeader{
		RowID:     types.MustRowID(1),
		TableID:   types.MustTableID(1),
		SegmentID: types.MustSegmentID(1),
		MessageID: types.MustMessageID(1),
		TxnID:     types.MustTxnID(1),
		LSN:       types.MustLSN(1),
	}

	// Small row should pass
	textVal := "small"
	row := Row{
		Header: header,
		Body:   RowBody{Columns: []ColumnValue{{Kind: "text", Text: &textVal}}},
	}
	if err := v.ValidateRowForStorage(row); err != nil {
		t.Errorf("small row should pass: %v", err)
	}
}

func TestValidatorStrictMode(t *testing.T) {
	strictV := NewValidator(true)
	relaxedV := NewValidator(false)

	header := RowHeader{
		RowID:     types.MustRowID(1),
		TableID:   types.MustTableID(1),
		SegmentID: types.MustSegmentID(1),
		MessageID: types.MustMessageID(1),
		TxnID:     types.MustTxnID(1),
		LSN:       types.MustLSN(1),
	}

	// Large row that needs overflow
	largeText := strings.Repeat("x", constraints.MaxRowBodyInline+100)
	row := Row{
		Header: header,
		Body:   RowBody{Columns: []ColumnValue{{Kind: "text", Text: &largeText}}},
	}

	// Strict mode should error
	if err := strictV.ValidateRowForStorage(row); err == nil {
		t.Error("strict mode should reject overflow-requiring row")
	}

	// Relaxed mode should allow (overflow is handled)
	if err := relaxedV.ValidateRowForStorage(row); err != nil {
		t.Errorf("relaxed mode should allow overflow: %v", err)
	}
}

func TestValidatorStorageRequirements(t *testing.T) {
	v := NewValidator(false)

	// Normal requirements
	req := StorageRequirements{
		HeaderSize:    50,
		BodySize:      1000,
		NeedsOverflow: false,
	}
	if err := v.ValidateStorageRequirements(req); err != nil {
		t.Errorf("normal requirements should pass: %v", err)
	}

	// Too many blob fields
	req = StorageRequirements{
		NeedsBlob:  true,
		BlobFields: make([]int, 20), // Over limit
	}
	if err := v.ValidateStorageRequirements(req); err == nil {
		t.Error("should reject too many blob fields")
	}
}

func TestValidatorMessageContent(t *testing.T) {
	v := NewValidator(false)

	if err := v.ValidateMessageContent("hello"); err != nil {
		t.Errorf("short content should pass: %v", err)
	}

	long := strings.Repeat("x", constraints.MaxMessageContent+1)
	if err := v.ValidateMessageContent(long); err == nil {
		t.Error("long content should fail")
	}
}

func TestValidatorEmbeds(t *testing.T) {
	v := NewValidator(false)

	// Single small embed
	embeds := []DiscordEmbed{{Description: "hello"}}
	if err := v.ValidateEmbeds(embeds); err != nil {
		t.Errorf("small embed should pass: %v", err)
	}

	// Too many embeds
	manyEmbeds := make([]DiscordEmbed, constraints.MaxEmbedsPerMessage+1)
	if err := v.ValidateEmbeds(manyEmbeds); err == nil {
		t.Error("too many embeds should fail")
	}

	// Large total
	largeEmbeds := []DiscordEmbed{
		{Description: strings.Repeat("a", 3000)},
		{Description: strings.Repeat("b", 3001)},
	}
	if err := v.ValidateEmbeds(largeEmbeds); err == nil {
		t.Error("large total should fail")
	}
}

func TestValidatorSegmentCapacity(t *testing.T) {
	v := NewValidator(false)

	if err := v.ValidateSegmentCapacity(100); err != nil {
		t.Errorf("low count should pass: %v", err)
	}

	if err := v.ValidateSegmentCapacity(5001); err == nil {
		t.Error("over capacity should fail")
	}
}

func TestValidatorChannelCount(t *testing.T) {
	v := NewValidator(false)

	if err := v.ValidateChannelCount(100); err != nil {
		t.Errorf("low count should pass: %v", err)
	}

	if err := v.ValidateChannelCount(constraints.MaxChannelsPerGuild + 1); err == nil {
		t.Error("over limit should fail")
	}
}

func TestValidatorRoleCount(t *testing.T) {
	v := NewValidator(false)

	if err := v.ValidateRoleCount(100); err != nil {
		t.Errorf("low count should pass: %v", err)
	}

	if err := v.ValidateRoleCount(constraints.MaxRolesPerGuild + 1); err == nil {
		t.Error("over limit should fail")
	}
}

func TestValidatorTableNamespace(t *testing.T) {
	v := NewValidator(false)

	if err := v.ValidateTableNamespace("users", types.TableID(1)); err != nil {
		t.Errorf("valid namespace should pass: %v", err)
	}

	if err := v.ValidateTableNamespace("", types.TableID(1)); err == nil {
		t.Error("empty name should fail")
	}

	if err := v.ValidateTableNamespace("users", types.TableID(0)); err == nil {
		t.Error("zero table ID should fail")
	}

	longName := strings.Repeat("x", 101)
	if err := v.ValidateTableNamespace(longName, types.TableID(1)); err == nil {
		t.Error("long name should fail")
	}
}

func TestValidatorIndexKey(t *testing.T) {
	v := NewValidator(false)

	if err := v.ValidateIndexKey([]byte("short")); err != nil {
		t.Errorf("short key should pass: %v", err)
	}

	longKey := make([]byte, 600)
	if err := v.ValidateIndexKey(longKey); err == nil {
		t.Error("long key should fail")
	}
}

func TestValidatorBlobData(t *testing.T) {
	v := NewValidator(false)

	if err := v.ValidateBlobData([]byte("data"), "file.bin"); err != nil {
		t.Errorf("small blob should pass: %v", err)
	}

	if err := v.ValidateBlobData([]byte("data"), ""); err == nil {
		t.Error("empty filename should fail")
	}

	largeData := make([]byte, 9*1024*1024) // 9MB
	if err := v.ValidateBlobData(largeData, "big.bin"); err == nil {
		t.Error("large blob should fail")
	}
}

func TestValidatorWALRecord(t *testing.T) {
	v := NewValidator(false)

	if err := v.ValidateWALRecord("INSERT", []byte("data")); err != nil {
		t.Errorf("small record should pass: %v", err)
	}

	largeData := make([]byte, constraints.MaxMessageContent+1)
	if err := v.ValidateWALRecord("INSERT", largeData); err == nil {
		t.Error("large record should fail")
	}
}

func TestCheckOverflowNeeded(t *testing.T) {
	header := RowHeader{
		RowID:     types.MustRowID(1),
		TableID:   types.MustTableID(1),
		SegmentID: types.MustSegmentID(1),
		MessageID: types.MustMessageID(1),
		TxnID:     types.MustTxnID(1),
		LSN:       types.MustLSN(1),
	}

	// Small row
	smallText := "small"
	smallRow := Row{
		Header: header,
		Body:   RowBody{Columns: []ColumnValue{{Kind: "text", Text: &smallText}}},
	}
	needs, _ := CheckOverflowNeeded(smallRow)
	if needs {
		t.Error("small row should not need overflow")
	}

	// Large row
	largeText := strings.Repeat("x", constraints.MaxRowBodyInline+100)
	largeRow := Row{
		Header: header,
		Body:   RowBody{Columns: []ColumnValue{{Kind: "text", Text: &largeText}}},
	}
	needs, size := CheckOverflowNeeded(largeRow)
	if !needs {
		t.Error("large row should need overflow")
	}
	if size <= constraints.MaxRowBodyInline {
		t.Errorf("size %d should be > %d", size, constraints.MaxRowBodyInline)
	}
}

func TestEstimateRowSize(t *testing.T) {
	header := RowHeader{
		RowID:     types.MustRowID(1),
		TableID:   types.MustTableID(1),
		SegmentID: types.MustSegmentID(1),
		MessageID: types.MustMessageID(1),
		TxnID:     types.MustTxnID(1),
		LSN:       types.MustLSN(1),
	}

	textVal := "test"
	row := Row{
		Header: header,
		Body:   RowBody{Columns: []ColumnValue{{Kind: "text", Text: &textVal}}},
	}

	estimate := EstimateRowSize(row)
	if estimate.HeaderSize == 0 {
		t.Error("expected non-zero header size")
	}
	if estimate.BodySize == 0 {
		t.Error("expected non-zero body size")
	}
	if estimate.TotalSize() == 0 {
		t.Error("expected non-zero total size")
	}
}

func TestValidationError(t *testing.T) {
	err := ValidationError{
		Field:   "test_field",
		Limit:   100,
		Actual:  150,
		Message: "test message",
	}

	errStr := err.Error()
	if errStr == "" {
		t.Error("expected non-empty error string")
	}
	if !strings.Contains(errStr, "test_field") {
		t.Error("error should contain field name")
	}
	if !strings.Contains(errStr, "100") {
		t.Error("error should contain limit")
	}
	if !strings.Contains(errStr, "150") {
		t.Error("error should contain actual")
	}
}
