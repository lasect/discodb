package storage

import (
	"fmt"

	"discodb/constraints"
	"discodb/types"
)

// Validator performs validation of database operations against Discord constraints
type Validator struct {
	strictMode bool
}

// NewValidator creates a new validator
func NewValidator(strictMode bool) *Validator {
	return &Validator{strictMode: strictMode}
}

// ValidationError represents a validation failure
type ValidationError struct {
	Field   string
	Limit   int
	Actual  int
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("validation failed: %s (limit: %d, actual: %d) - %s", e.Field, e.Limit, e.Actual, e.Message)
}

// ValidateRowForStorage checks if a row can be stored within Discord limits
func (v *Validator) ValidateRowForStorage(row Row) error {
	req := ComputeStorageRequirements(row)

	// Check header size
	if req.HeaderSize > constraints.MaxRowHeaderEncoded {
		return ValidationError{
			Field:   "row_header",
			Limit:   constraints.MaxRowHeaderEncoded,
			Actual:  req.HeaderSize,
			Message: "row header exceeds maximum encoded size",
		}
	}

	// Check if row needs overflow (this is OK, just informational)
	if req.NeedsOverflow {
		if v.strictMode {
			return ValidationError{
				Field:   "row_body",
				Limit:   constraints.MaxRowBodyInline,
				Actual:  req.BodySize,
				Message: "row body requires TOAST overflow - ensure thread is available",
			}
		}
	}

	// Check body size (even with overflow, should have reasonable limits)
	maxTotalSize := constraints.MaxRowBodyInline * 10
	if req.BodySize > maxTotalSize {
		return ValidationError{
			Field:   "row_body_total",
			Limit:   maxTotalSize,
			Actual:  req.BodySize,
			Message: "row body exceeds reasonable total size even with overflow",
		}
	}

	return nil
}

// ValidateStorageRequirements checks storage requirements
func (v *Validator) ValidateStorageRequirements(req StorageRequirements) error {
	if req.NeedsOverflow {
		chunks := constraints.ComputeToastChunks(req.BodySize)
		maxChunks := 100
		if chunks > maxChunks {
			return ValidationError{
				Field:   "overflow_chunks",
				Limit:   maxChunks,
				Actual:  chunks,
				Message: "row requires too many overflow chunks",
			}
		}
	}

	if req.NeedsBlob {
		if len(req.BlobFields) > 10 {
			return ValidationError{
				Field:   "blob_fields",
				Limit:   10,
				Actual:  len(req.BlobFields),
				Message: "too many blob fields in single row",
			}
		}
	}

	return nil
}

// ValidateMessageContent checks content against Discord limits
func (v *Validator) ValidateMessageContent(content string) error {
	if err := constraints.ValidateMessageContent(content); err != nil {
		return ValidationError{
			Field:   "message_content",
			Limit:   constraints.MaxMessageContent,
			Actual:  len(content),
			Message: err.Error(),
		}
	}
	return nil
}

// ValidateEmbeds checks embeds against Discord limits
func (v *Validator) ValidateEmbeds(embeds []DiscordEmbed) error {
	if len(embeds) > constraints.MaxEmbedsPerMessage {
		return ValidationError{
			Field:   "embed_count",
			Limit:   constraints.MaxEmbedsPerMessage,
			Actual:  len(embeds),
			Message: "too many embeds per message",
		}
	}

	totalSize := 0
	for i, e := range embeds {
		size := calcEmbedSize(e)
		totalSize += size

		if size > constraints.MaxEmbedDescription {
			return ValidationError{
				Field:   fmt.Sprintf("embed_%d", i),
				Limit:   constraints.MaxEmbedDescription,
				Actual:  size,
				Message: "embed exceeds maximum description size",
			}
		}
	}

	if totalSize > constraints.MaxEmbedTotalChars {
		return ValidationError{
			Field:   "embed_total",
			Limit:   constraints.MaxEmbedTotalChars,
			Actual:  totalSize,
			Message: "embeds exceed total character limit",
		}
	}

	return nil
}

func calcEmbedSize(e DiscordEmbed) int {
	size := len(e.Title) + len(e.Description)
	for _, f := range e.Fields {
		size += len(f.Name) + len(f.Value)
	}
	return size
}

// ValidateSegmentCapacity checks if segment has room for more rows
func (v *Validator) ValidateSegmentCapacity(currentRowCount int) error {
	maxRows := 5000
	if currentRowCount >= maxRows {
		return ValidationError{
			Field:   "segment_capacity",
			Limit:   maxRows,
			Actual:  currentRowCount,
			Message: "segment at practical capacity, consider creating new segment",
		}
	}
	return nil
}

// ValidateChannelCount checks guild channel limits
func (v *Validator) ValidateChannelCount(currentCount int) error {
	if err := constraints.ValidateChannelsCount(currentCount); err != nil {
		return ValidationError{
			Field:   "channel_count",
			Limit:   constraints.MaxChannelsPerGuild,
			Actual:  currentCount,
			Message: err.Error(),
		}
	}
	return nil
}

// ValidateRoleCount checks guild role limits
func (v *Validator) ValidateRoleCount(currentCount int) error {
	if err := constraints.ValidateRolesCount(currentCount); err != nil {
		return ValidationError{
			Field:   "role_count",
			Limit:   constraints.MaxRolesPerGuild,
			Actual:  currentCount,
			Message: err.Error(),
		}
	}
	return nil
}

// ValidateTableNamespace checks if table namespace (category) is valid
func (v *Validator) ValidateTableNamespace(name string, tableID types.TableID) error {
	if name == "" {
		return ValidationError{
			Field:   "table_name",
			Limit:   1,
			Actual:  0,
			Message: "table name cannot be empty",
		}
	}

	if len(name) > 100 {
		return ValidationError{
			Field:   "table_name",
			Limit:   100,
			Actual:  len(name),
			Message: "table name too long",
		}
	}

	if tableID == 0 {
		return ValidationError{
			Field:   "table_id",
			Limit:   0,
			Actual:  0,
			Message: "table ID cannot be zero",
		}
	}

	return nil
}

// ValidateIndexKey checks if an index key is valid
func (v *Validator) ValidateIndexKey(key []byte) error {
	maxKeySize := 512
	if len(key) > maxKeySize {
		return ValidationError{
			Field:   "index_key",
			Limit:   maxKeySize,
			Actual:  len(key),
			Message: "index key too large",
		}
	}
	return nil
}

// ValidateBlobData checks blob data for attachment storage
func (v *Validator) ValidateBlobData(data []byte, filename string) error {
	if filename == "" {
		return ValidationError{
			Field:   "blob_filename",
			Limit:   1,
			Actual:  0,
			Message: "blob filename cannot be empty",
		}
	}

	maxFileSize := 8 * 1024 * 1024 // 8MB
	if len(data) > maxFileSize {
		return ValidationError{
			Field:   "blob_size",
			Limit:   maxFileSize,
			Actual:  len(data),
			Message: "blob exceeds maximum file size for attachment",
		}
	}

	return nil
}

// ValidateWALRecord checks WAL record can be stored
func (v *Validator) ValidateWALRecord(recordType string, data []byte) error {
	if len(data) > constraints.MaxMessageContent {
		return ValidationError{
			Field:   "wal_record",
			Limit:   constraints.MaxMessageContent,
			Actual:  len(data),
			Message: fmt.Sprintf("WAL record of type '%s' exceeds message content limit", recordType),
		}
	}
	return nil
}

// CheckOverflowNeeded determines if a row needs overflow storage
func CheckOverflowNeeded(row Row) (bool, int) {
	req := ComputeStorageRequirements(row)
	return req.NeedsOverflow, req.BodySize
}

// EstimateRowSize calculates the estimated storage size for a row
func EstimateRowSize(row Row) RowSizeEstimate {
	req := ComputeStorageRequirements(row)

	estimate := RowSizeEstimate{
		HeaderSize:    req.HeaderSize,
		BodySize:      req.BodySize,
		NeedsOverflow: req.NeedsOverflow,
	}

	if req.NeedsOverflow {
		chunks := constraints.ComputeToastChunks(req.BodySize)
		estimate.OverflowSize = req.BodySize + chunks*50
		estimate.TotalChunks = chunks
	}

	if req.NeedsBlob {
		estimate.BlobCount = len(req.BlobFields)
	}

	return estimate
}

// RowSizeEstimate provides size breakdown for a row
type RowSizeEstimate struct {
	HeaderSize    int
	BodySize      int
	NeedsOverflow bool
	OverflowSize  int
	TotalChunks   int
	BlobCount     int
}

// TotalSize returns the total estimated size
func (e RowSizeEstimate) TotalSize() int {
	if e.NeedsOverflow {
		return e.HeaderSize + e.OverflowSize
	}
	return e.HeaderSize + e.BodySize
}
