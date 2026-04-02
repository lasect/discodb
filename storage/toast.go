package storage

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"discodb/constraints"
	"discodb/mapping"
	"discodb/types"
)

// TOASTManager handles overflow (TOAST) storage operations
type TOASTManager struct {
	// ThreadID is the channel ID of the overflow thread
	ThreadID types.ChannelID
}

// NewTOASTManager creates a new TOAST manager for a thread
func NewTOASTManager(threadID types.ChannelID) *TOASTManager {
	return &TOASTManager{ThreadID: threadID}
}

// ChunkWriter writes row data as TOAST chunks
func (t *TOASTManager) ChunkWriter(rowID types.RowID, data []byte) []mapping.OverflowChunk {
	return splitToChunks(rowID, data)
}

// ChunkReader reconstructs row data from chunks
func (t *TOASTManager) ChunkReader(chunks []mapping.OverflowChunk) ([]byte, error) {
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no chunks provided")
	}

	// Sort by chunk index
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].ChunkIdx < chunks[j].ChunkIdx
	})

	// Verify sequence
	for i, chunk := range chunks {
		if chunk.ChunkIdx != i {
			return nil, fmt.Errorf("missing chunk %d", i)
		}
		if chunk.Total != len(chunks) {
			return nil, fmt.Errorf("chunk %d has wrong total: %d != %d", i, chunk.Total, len(chunks))
		}
	}

	// Reassemble
	var buf bytes.Buffer
	for _, chunk := range chunks {
		buf.Write(chunk.Data)
	}

	return buf.Bytes(), nil
}

// EncodeChunk formats a chunk for Discord message
func EncodeChunk(chunk mapping.OverflowChunk) string {
	return mapping.EncodeChunkToMessage(chunk)
}

// DecodeChunk parses a chunk from Discord message content
func DecodeChunk(content string) (mapping.OverflowChunk, error) {
	return mapping.DecodeChunkFromMessage(content)
}

// IsToastChunk checks if message content is a TOAST chunk
func IsToastChunk(content string) bool {
	return strings.HasPrefix(content, "TOAST:")
}

// NeedsOverflow checks if data requires TOAST storage
func NeedsOverflow(data []byte) bool {
	return constraints.NeedsOverflow(len(data))
}

// ComputeChunks calculates number of chunks needed for data
func ComputeChunks(dataSize int) int {
	return constraints.ComputeToastChunks(dataSize)
}

// OverflowRef represents a reference to TOAST storage
type OverflowRef struct {
	ThreadID types.ChannelID `json:"thread_id"`
	Chunks   int             `json:"chunks"`
}

// EncodeOverflowRef serializes overflow reference to JSON
func (r OverflowRef) Encode() ([]byte, error) {
	return mapping.OverflowRef{
		ThreadID: r.ThreadID,
		Chunks:   r.Chunks,
	}.Encode()
}

// DecodeOverflowRef deserializes overflow reference from JSON
func DecodeOverflowRef(data []byte) (OverflowRef, error) {
	var ref OverflowRef
	// The mapping package has the type, so we use its structure
	var mappingRef mapping.OverflowRef
	if err := mappingRef.Decode(data); err != nil {
		return ref, err
	}
	ref.ThreadID = mappingRef.ThreadID
	ref.Chunks = mappingRef.Chunks
	return ref, nil
}

// ValidateChunks verifies a complete set of chunks
func ValidateChunks(chunks []mapping.OverflowChunk, expectedRowID types.RowID) error {
	if len(chunks) == 0 {
		return fmt.Errorf("no chunks")
	}

	// Check all chunks belong to same row
	for i, chunk := range chunks {
		if chunk.RowID != expectedRowID {
			return fmt.Errorf("chunk %d has wrong row ID: %d != %d", i, chunk.RowID, expectedRowID)
		}
	}

	// Check for duplicates
	seen := make(map[int]bool)
	for _, chunk := range chunks {
		if seen[chunk.ChunkIdx] {
			return fmt.Errorf("duplicate chunk index: %d", chunk.ChunkIdx)
		}
		seen[chunk.ChunkIdx] = true
	}

	// Verify no gaps
	for i := 0; i < len(chunks); i++ {
		if !seen[i] {
			return fmt.Errorf("missing chunk index: %d", i)
		}
	}

	return nil
}

// ChunkInfo provides metadata about a chunk
type ChunkInfo struct {
	RowID   types.RowID
	Index   int
	Total   int
	Size    int
	IsValid bool
}

// ParseChunkInfo extracts metadata from chunk content
func ParseChunkInfo(content string) (ChunkInfo, error) {
	if !IsToastChunk(content) {
		return ChunkInfo{}, fmt.Errorf("not a TOAST chunk")
	}

	chunk, err := DecodeChunk(content)
	if err != nil {
		return ChunkInfo{}, err
	}

	return ChunkInfo{
		RowID:   chunk.RowID,
		Index:   chunk.ChunkIdx,
		Total:   chunk.Total,
		Size:    len(chunk.Data),
		IsValid: true,
	}, nil
}

// EstimateOverflowSize calculates expected storage size for overflow data
func EstimateOverflowSize(dataSize int) int {
	chunks := ComputeChunks(dataSize)
	// Each chunk has overhead: header + base64 encoding overhead
	headerOverhead := chunks * 50                   // "TOAST:{rowID}:{idx}/{total}:" prefix
	base64Overhead := int(float64(dataSize) * 0.34) // base64 is ~33% larger
	return headerOverhead + dataSize + base64Overhead
}

// RowWithOverflow combines inline and overflow data
type RowWithOverflow struct {
	Row          Row
	HasOverflow  bool
	OverflowRef  OverflowRef
	OverflowData []byte
}

// FetchOverflow retrieves overflow data from thread
// This is a placeholder - actual implementation needs Discord client
func FetchOverflow(threadID types.ChannelID, chunks int) ([]mapping.OverflowChunk, error) {
	// TODO: Implement actual Discord thread message fetching
	return nil, fmt.Errorf("FetchOverflow not implemented: requires Discord client integration")
}

// WriteOverflow stores overflow data to thread
// This is a placeholder - actual implementation needs Discord client
func WriteOverflow(threadID types.ChannelID, chunks []mapping.OverflowChunk) error {
	// TODO: Implement actual Discord thread message sending
	return fmt.Errorf("WriteOverflow not implemented: requires Discord client integration")
}
