package storage

import (
	"encoding/json"
	"fmt"

	"discodb/constraints"
	"discodb/mapping"
	"discodb/types"
)

// DiscordMessage represents a row encoded for Discord API
type DiscordMessage struct {
	Content     string
	Embeds      []DiscordEmbed
	Attachments []DiscordAttachment
	ThreadName  string // For overflow thread creation
}

// DiscordEmbed represents a simplified embed structure
type DiscordEmbed struct {
	Title       string         `json:"title,omitempty"`
	Description string         `json:"description,omitempty"`
	Fields      []DiscordField `json:"fields,omitempty"`
}

type DiscordField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline,omitempty"`
}

// DiscordAttachment represents attachment metadata
type DiscordAttachment struct {
	Filename string
	Data     []byte
	Size     int
}

// RowBodyJSON is the JSON structure stored in embed description
type RowBodyJSON struct {
	SchemaEpoch types.SchemaEpoch    `json:"epoch"`
	NullBitmap  []byte               `json:"nulls,omitempty"`
	Columns     []ColumnJSON         `json:"cols"`
	OverflowRef *mapping.OverflowRef `json:"overflow,omitempty"`
}

// ColumnJSON represents a single column value
type ColumnJSON struct {
	Index int             `json:"i"`
	Type  string          `json:"t"`
	Value json.RawMessage `json:"v"`
}

// EncodeResult holds the result of row encoding
type EncodeResult struct {
	Message        DiscordMessage
	NeedsOverflow  bool
	OverflowChunks []mapping.OverflowChunk
	NeedsBlob      bool
	BlobData       []BlobAttachment
}

// BlobAttachment holds blob data needing attachment upload
type BlobAttachment struct {
	ColumnIdx int
	Data      []byte
	Filename  string
}

// EncodeRowToDiscord converts a Row to Discord message format
// It computes the checksum over the body and stores it in the header
func EncodeRowToDiscord(row Row, schemaEpoch types.SchemaEpoch) (EncodeResult, error) {
	result := EncodeResult{}

	// Build column data for JSON
	columns := make([]ColumnJSON, 0, len(row.Body.Columns))
	var blobs []BlobAttachment
	var nullBitmap []byte

	// Build null bitmap - 1 bit per column
	numCols := len(row.Body.Columns)
	if numCols > 0 {
		nullBitmap = make([]byte, (numCols+7)/8)
	}

	for i, col := range row.Body.Columns {
		// Check for null columns
		if isNullColumn(col) {
			// Set bit in null bitmap (bit i = 1 means NULL)
			nullBitmap[i/8] |= 1 << (i % 8)
			continue // Don't encode null columns in data
		}

		// Handle blob data specially - store separately as attachment
		if col.BlobRef != nil {
			// For encoding, we create a temp blob that will become an attachment
			// The actual blob data needs to be passed separately
			blobs = append(blobs, BlobAttachment{
				ColumnIdx: i,
				Filename:  fmt.Sprintf("blob_%d_%d.bin", row.Header.RowID, i),
				Data:      nil, // To be filled by caller
			})
			// Store reference in column
			refData, _ := json.Marshal(map[string]string{
				"filename": fmt.Sprintf("blob_%d_%d.bin", row.Header.RowID, i),
			})
			columns = append(columns, ColumnJSON{
				Index: i,
				Type:  "blob_ref",
				Value: refData,
			})
		} else {
			// Encode regular column
			value, err := encodeColumnValue(col)
			if err != nil {
				return result, fmt.Errorf("encode column %d: %w", i, err)
			}
			columns = append(columns, ColumnJSON{
				Index: i,
				Type:  col.Kind,
				Value: value,
			})
		}
	}

	// Build body JSON
	bodyJSON := RowBodyJSON{
		SchemaEpoch: schemaEpoch,
		NullBitmap:  nullBitmap,
		Columns:     columns,
	}

	bodyData, err := json.Marshal(bodyJSON)
	if err != nil {
		return result, fmt.Errorf("marshal body: %w", err)
	}

	// Compute checksum over body data BEFORE checking overflow
	checksum := ComputeChecksum(bodyData)
	row.Header.Checksum = checksum

	// Check if we need overflow
	bodySize := len(bodyData)
	if constraints.NeedsOverflow(bodySize) {
		result.NeedsOverflow = true
		result.OverflowChunks = splitToChunks(row.Header.RowID, bodyData)

		// Embed only contains overflow reference
		bodyJSON.Columns = nil    // Clear inline columns
		bodyJSON.NullBitmap = nil // Clear bitmap (it's in overflow)
		bodyJSON.OverflowRef = &mapping.OverflowRef{
			ThreadID: 0, // Will be filled in by caller
			Chunks:   len(result.OverflowChunks),
		}

		refData, err := json.Marshal(bodyJSON)
		if err != nil {
			return result, fmt.Errorf("marshal overflow ref: %w", err)
		}
		bodyData = refData
	}

	// Encode header to content (now includes checksum)
	headerStr := EncodeMessageContent(row.Header)
	if err := constraints.ValidateMessageContent(headerStr); err != nil {
		return result, fmt.Errorf("header too large: %w", err)
	}
	result.Message.Content = headerStr

	// Create embed with body data
	embed := DiscordEmbed{
		Description: string(bodyData),
	}
	result.Message.Embeds = []DiscordEmbed{embed}

	// Handle blobs
	if len(blobs) > 0 {
		result.NeedsBlob = true
		result.BlobData = blobs
		for _, blob := range blobs {
			result.Message.Attachments = append(result.Message.Attachments, DiscordAttachment{
				Filename: blob.Filename,
				Data:     blob.Data,
				Size:     len(blob.Data),
			})
		}
	}

	// Validate total size
	totalEmbedSize := len(bodyData) + 50 // JSON overhead
	if err := constraints.ValidateRowFits(len(headerStr), totalEmbedSize); err != nil {
		// This shouldn't happen if overflow logic is correct
		return result, fmt.Errorf("row too large even after overflow: %w", err)
	}

	return result, nil
}

// DecodeRowFromDiscord reconstructs a Row from Discord message
// Returns ChecksumError if the checksum validation fails
func DecodeRowFromDiscord(content string, embeds []DiscordEmbed, attachments []DiscordAttachment) (Row, types.SchemaEpoch, error) {
	var row Row
	var schemaEpoch types.SchemaEpoch

	// Decode header from content
	header, ok := DecodeMessageContent(content)
	if !ok {
		return row, schemaEpoch, fmt.Errorf("decode header from content")
	}
	row.Header = header

	// Need at least one embed for body
	if len(embeds) == 0 {
		return row, schemaEpoch, fmt.Errorf("no embeds found")
	}

	// Parse body JSON from first embed description
	var bodyJSON RowBodyJSON
	if err := json.Unmarshal([]byte(embeds[0].Description), &bodyJSON); err != nil {
		return row, schemaEpoch, fmt.Errorf("unmarshal body: %w", err)
	}
	schemaEpoch = bodyJSON.SchemaEpoch

	// Handle overflow reference (body is in thread, not embed)
	if bodyJSON.OverflowRef != nil {
		// Caller must fetch overflow chunks and reconstruct bodyData
		row.Header.Flags |= FlagOverflow
		// Return partial row, caller handles overflow fetch
		return row, schemaEpoch, fmt.Errorf("overflow data not fetched")
	}

	// Validate checksum over the raw body data
	// Re-serialize to compute checksum (must match what was encoded)
	bodyData, err := json.Marshal(bodyJSON)
	if err != nil {
		return row, schemaEpoch, fmt.Errorf("re-serialize body for checksum: %w", err)
	}
	computedChecksum := ComputeChecksum(bodyData)
	if header.Checksum != 0 && header.Checksum != computedChecksum {
		return row, schemaEpoch, ChecksumError{
			Expected: header.Checksum,
			Actual:   computedChecksum,
			RowID:    header.RowID,
		}
	}

	// Determine number of columns from column data and null bitmap
	// We need to consider both the max column index AND the bitmap size
	// to properly handle trailing null columns
	numCols := 0
	for _, col := range bodyJSON.Columns {
		if col.Index+1 > numCols {
			numCols = col.Index + 1
		}
	}

	// If null bitmap indicates more columns, expand to include them
	// (handles trailing null columns)
	if len(bodyJSON.NullBitmap) > 0 {
		// Find the highest set bit in the bitmap to determine true column count
		for i := len(bodyJSON.NullBitmap)*8 - 1; i >= 0; i-- {
			if bodyJSON.NullBitmap[i/8]&(1<<(i%8)) != 0 {
				if i+1 > numCols {
					numCols = i + 1
				}
				break
			}
		}
	}

	// Build column array with nulls filled in from bitmap
	row.Body.Columns = make([]ColumnValue, numCols)

	// First, mark null columns from bitmap (only up to numCols)
	for i := 0; i < numCols; i++ {
		if len(bodyJSON.NullBitmap) > i/8 {
			if bodyJSON.NullBitmap[i/8]&(1<<(i%8)) != 0 {
				row.Body.Columns[i] = ColumnValue{Kind: "null"}
			}
		}
	}

	// Then decode non-null columns
	for _, col := range bodyJSON.Columns {
		if col.Index >= len(row.Body.Columns) {
			// Extend if needed
			newCols := make([]ColumnValue, col.Index+1)
			copy(newCols, row.Body.Columns)
			row.Body.Columns = newCols
		}
		value, err := decodeColumnValue(col.Type, col.Value)
		if err != nil {
			return row, schemaEpoch, fmt.Errorf("decode column %d: %w", col.Index, err)
		}
		value.Kind = col.Type
		row.Body.Columns[col.Index] = value
	}

	// Handle blob references - attach BlobRef pointing to attachment
	for i, col := range row.Body.Columns {
		if col.Kind == "blob_ref" {
			var ref map[string]string
			if err := json.Unmarshal(col.Raw, &ref); err == nil {
				// Find attachment
				for _, att := range attachments {
					if att.Filename == ref["filename"] {
						row.Body.Columns[i] = ColumnValue{
							Kind: "blob",
							BlobRef: &BlobRef{
								MessageID: 0, // To be resolved
								Offset:    0,
								Length:    uint32(len(att.Data)),
							},
							Raw: att.Data,
						}
						break
					}
				}
			}
		}
	}

	return row, schemaEpoch, nil
}

// encodeColumnValue converts ColumnValue to JSON
func encodeColumnValue(col ColumnValue) (json.RawMessage, error) {
	switch col.Kind {
	case "bool":
		if col.Bool != nil {
			return json.Marshal(*col.Bool)
		}
	case "int2":
		if col.Int16 != nil {
			return json.Marshal(*col.Int16)
		}
	case "int4":
		if col.Int32 != nil {
			return json.Marshal(*col.Int32)
		}
	case "int8":
		if col.Int64 != nil {
			return json.Marshal(*col.Int64)
		}
	case "float4":
		if col.Float32 != nil {
			return json.Marshal(*col.Float32)
		}
	case "float8":
		if col.Float64 != nil {
			return json.Marshal(*col.Float64)
		}
	case "text":
		if col.Text != nil {
			return json.Marshal(*col.Text)
		}
	case "json":
		return col.JSON, nil
	case "null":
		return json.Marshal(nil)
	}
	return json.Marshal(nil)
}

// isNullColumn checks if a column value represents NULL
func isNullColumn(col ColumnValue) bool {
	if col.Kind == "null" {
		return true
	}
	// Check if all value pointers are nil
	return col.Bool == nil &&
		col.Int16 == nil &&
		col.Int32 == nil &&
		col.Int64 == nil &&
		col.Float32 == nil &&
		col.Float64 == nil &&
		col.Text == nil &&
		col.JSON == nil &&
		col.BlobRef == nil &&
		col.Raw == nil
}

// decodeColumnValue parses JSON into ColumnValue
func decodeColumnValue(kind string, data json.RawMessage) (ColumnValue, error) {
	var col ColumnValue
	col.Kind = kind
	col.Raw = data

	switch kind {
	case "bool":
		var v bool
		if err := json.Unmarshal(data, &v); err != nil {
			return col, err
		}
		col.Bool = &v
	case "int2":
		var v int16
		if err := json.Unmarshal(data, &v); err != nil {
			return col, err
		}
		col.Int16 = &v
	case "int4":
		var v int32
		if err := json.Unmarshal(data, &v); err != nil {
			return col, err
		}
		col.Int32 = &v
	case "int8":
		var v int64
		if err := json.Unmarshal(data, &v); err != nil {
			return col, err
		}
		col.Int64 = &v
	case "float4":
		var v float32
		if err := json.Unmarshal(data, &v); err != nil {
			return col, err
		}
		col.Float32 = &v
	case "float8":
		var v float64
		if err := json.Unmarshal(data, &v); err != nil {
			return col, err
		}
		col.Float64 = &v
	case "text":
		var v string
		if err := json.Unmarshal(data, &v); err != nil {
			return col, err
		}
		col.Text = &v
	case "json":
		col.JSON = data
	case "blob_ref":
		// Keep the raw data for reference resolution
	}

	return col, nil
}

// splitToChunks divides data into TOAST chunks
func splitToChunks(rowID types.RowID, data []byte) []mapping.OverflowChunk {
	chunks := constraints.ComputeToastChunks(len(data))
	result := make([]mapping.OverflowChunk, chunks)

	for i := 0; i < chunks; i++ {
		start := i * constraints.ToastChunkSize
		end := start + constraints.ToastChunkSize
		if end > len(data) {
			end = len(data)
		}

		result[i] = mapping.OverflowChunk{
			RowID:    rowID,
			ChunkIdx: i,
			Total:    chunks,
			Data:     data[start:end],
		}
	}

	return result
}

// ComputeStorageRequirements analyzes a row to determine storage needs
func ComputeStorageRequirements(row Row) StorageRequirements {
	req := StorageRequirements{
		HeaderSize: len(EncodeMessageContent(row.Header)),
	}

	// Estimate body size
	bodyJSON := RowBodyJSON{
		Columns: make([]ColumnJSON, len(row.Body.Columns)),
	}
	for i, col := range row.Body.Columns {
		value, _ := encodeColumnValue(col)
		bodyJSON.Columns[i] = ColumnJSON{
			Index: i,
			Type:  col.Kind,
			Value: value,
		}

		// Check for blobs
		if col.BlobRef != nil {
			req.BlobFields = append(req.BlobFields, i)
			req.NeedsBlob = true
		}
	}

	bodyData, _ := json.Marshal(bodyJSON)
	req.BodySize = len(bodyData)
	req.NeedsOverflow = constraints.NeedsOverflow(req.BodySize)

	return req
}

// StorageRequirements describes storage needs for a row
type StorageRequirements struct {
	HeaderSize     int
	BodySize       int
	NeedsOverflow  bool
	OverflowFields []int
	NeedsBlob      bool
	BlobFields     []int
}
