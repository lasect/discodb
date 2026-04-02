package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"discodb/types"
)

// BlobManager handles blob (attachment) storage with explicit rewrite semantics.
// Attachments in Discord are immutable - any modification requires uploading a
// new attachment and updating all references. BlobManager tracks blob→message
// relationships and handles the rewrite flow.
type BlobManager struct {
	mu sync.RWMutex

	// blobIndex maps blob hash → BlobMetadata for deduplication
	blobIndex map[string]*BlobMetadata

	// rowBlobs maps rowID → list of blob references in that row
	rowBlobs map[types.RowID][]*BlobMetadata

	// pendingUploads tracks blobs waiting to be uploaded
	pendingUploads map[string]*PendingBlob
}

// BlobMetadata tracks a blob stored as a Discord attachment
type BlobMetadata struct {
	// Hash is SHA256 of blob content for deduplication
	Hash string `json:"hash"`

	// MessageID where the attachment lives
	MessageID types.MessageID `json:"message_id"`

	// AttachmentID from Discord
	AttachmentID string `json:"attachment_id"`

	// Filename used in Discord
	Filename string `json:"filename"`

	// URL is the CDN URL (may expire)
	URL string `json:"url"`

	// Size in bytes
	Size int `json:"size"`

	// RefCount tracks how many rows reference this blob
	RefCount int `json:"ref_count"`

	// RowID that owns this blob (for single-owner blobs)
	OwnerRowID types.RowID `json:"owner_row_id,omitempty"`

	// ColumnIdx in the owning row
	ColumnIdx int `json:"column_idx"`
}

// PendingBlob represents a blob waiting to be uploaded
type PendingBlob struct {
	Data      []byte
	Hash      string
	Filename  string
	RowID     types.RowID
	ColumnIdx int
}

// BlobRewriteOp represents an operation to rewrite a blob
type BlobRewriteOp struct {
	// OldBlob is the existing blob being replaced (nil for new blobs)
	OldBlob *BlobMetadata

	// NewData is the new blob content
	NewData []byte

	// RowID of the row being updated
	RowID types.RowID

	// ColumnIdx in the row
	ColumnIdx int

	// NewFilename for the upload
	NewFilename string
}

// BlobRewriteResult contains the result of a blob rewrite
type BlobRewriteResult struct {
	// NewBlob is the metadata for the newly uploaded blob
	NewBlob *BlobMetadata

	// OldBlobOrphaned indicates the old blob has no more references
	OldBlobOrphaned bool

	// OrphanedMessageID is set if old blob should be cleaned up
	OrphanedMessageID types.MessageID
}

// NewBlobManager creates a new blob manager
func NewBlobManager() *BlobManager {
	return &BlobManager{
		blobIndex:      make(map[string]*BlobMetadata),
		rowBlobs:       make(map[types.RowID][]*BlobMetadata),
		pendingUploads: make(map[string]*PendingBlob),
	}
}

// ComputeBlobHash computes SHA256 hash of blob data
func ComputeBlobHash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// PrepareUpload prepares a blob for upload, returning existing blob if deduplicated
func (bm *BlobManager) PrepareUpload(data []byte, rowID types.RowID, columnIdx int) (*PendingBlob, *BlobMetadata) {
	hash := ComputeBlobHash(data)

	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Check for existing blob with same hash (deduplication)
	if existing, ok := bm.blobIndex[hash]; ok {
		// Increment reference count
		existing.RefCount++
		bm.addRowBlobRef(rowID, existing)
		return nil, existing
	}

	// Create pending upload
	pending := &PendingBlob{
		Data:      data,
		Hash:      hash,
		Filename:  fmt.Sprintf("blob_%d_%d_%s.bin", rowID, columnIdx, hash[:8]),
		RowID:     rowID,
		ColumnIdx: columnIdx,
	}
	bm.pendingUploads[hash] = pending

	return pending, nil
}

// RegisterUpload registers a successful blob upload
func (bm *BlobManager) RegisterUpload(pending *PendingBlob, messageID types.MessageID, attachmentID string, url string) *BlobMetadata {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	meta := &BlobMetadata{
		Hash:         pending.Hash,
		MessageID:    messageID,
		AttachmentID: attachmentID,
		Filename:     pending.Filename,
		URL:          url,
		Size:         len(pending.Data),
		RefCount:     1,
		OwnerRowID:   pending.RowID,
		ColumnIdx:    pending.ColumnIdx,
	}

	bm.blobIndex[pending.Hash] = meta
	bm.addRowBlobRef(pending.RowID, meta)
	delete(bm.pendingUploads, pending.Hash)

	return meta
}

// PrepareRewrite prepares a blob rewrite operation
// This is used when a row's blob column is being updated
func (bm *BlobManager) PrepareRewrite(oldRef *BlobRef, newData []byte, rowID types.RowID, columnIdx int) (*BlobRewriteOp, error) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	var oldMeta *BlobMetadata
	if oldRef != nil {
		// Find old blob metadata
		for _, meta := range bm.rowBlobs[rowID] {
			if meta.ColumnIdx == columnIdx {
				oldMeta = meta
				break
			}
		}
	}

	newHash := ComputeBlobHash(newData)

	return &BlobRewriteOp{
		OldBlob:     oldMeta,
		NewData:     newData,
		RowID:       rowID,
		ColumnIdx:   columnIdx,
		NewFilename: fmt.Sprintf("blob_%d_%d_%s.bin", rowID, columnIdx, newHash[:8]),
	}, nil
}

// ExecuteRewrite executes a blob rewrite after new blob is uploaded
func (bm *BlobManager) ExecuteRewrite(op *BlobRewriteOp, newMessageID types.MessageID, newAttachmentID string, newURL string) *BlobRewriteResult {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	result := &BlobRewriteResult{}

	// Register new blob
	newHash := ComputeBlobHash(op.NewData)
	newMeta := &BlobMetadata{
		Hash:         newHash,
		MessageID:    newMessageID,
		AttachmentID: newAttachmentID,
		Filename:     op.NewFilename,
		URL:          newURL,
		Size:         len(op.NewData),
		RefCount:     1,
		OwnerRowID:   op.RowID,
		ColumnIdx:    op.ColumnIdx,
	}
	bm.blobIndex[newHash] = newMeta
	result.NewBlob = newMeta

	// Update row's blob references
	bm.removeRowBlobRef(op.RowID, op.OldBlob)
	bm.addRowBlobRef(op.RowID, newMeta)

	// Handle old blob
	if op.OldBlob != nil {
		op.OldBlob.RefCount--
		if op.OldBlob.RefCount <= 0 {
			result.OldBlobOrphaned = true
			result.OrphanedMessageID = op.OldBlob.MessageID
			delete(bm.blobIndex, op.OldBlob.Hash)
		}
	}

	return result
}

// ReleaseRowBlobs releases all blob references for a row (e.g., on row delete)
func (bm *BlobManager) ReleaseRowBlobs(rowID types.RowID) []types.MessageID {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	var orphaned []types.MessageID
	blobs := bm.rowBlobs[rowID]

	for _, meta := range blobs {
		meta.RefCount--
		if meta.RefCount <= 0 {
			orphaned = append(orphaned, meta.MessageID)
			delete(bm.blobIndex, meta.Hash)
		}
	}

	delete(bm.rowBlobs, rowID)
	return orphaned
}

// GetBlobMetadata retrieves metadata for a blob by hash
func (bm *BlobManager) GetBlobMetadata(hash string) (*BlobMetadata, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	meta, ok := bm.blobIndex[hash]
	return meta, ok
}

// GetRowBlobs retrieves all blob metadata for a row
func (bm *BlobManager) GetRowBlobs(rowID types.RowID) []*BlobMetadata {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	blobs := bm.rowBlobs[rowID]
	result := make([]*BlobMetadata, len(blobs))
	copy(result, blobs)
	return result
}

// ToBlobRef converts BlobMetadata to a BlobRef for storage in a row
func (meta *BlobMetadata) ToBlobRef() *BlobRef {
	return &BlobRef{
		MessageID: meta.MessageID,
		Offset:    0,
		Length:    uint32(meta.Size),
	}
}

// addRowBlobRef adds a blob reference to a row's tracking (must hold lock)
func (bm *BlobManager) addRowBlobRef(rowID types.RowID, meta *BlobMetadata) {
	bm.rowBlobs[rowID] = append(bm.rowBlobs[rowID], meta)
}

// removeRowBlobRef removes a blob reference from a row's tracking (must hold lock)
func (bm *BlobManager) removeRowBlobRef(rowID types.RowID, meta *BlobMetadata) {
	if meta == nil {
		return
	}
	blobs := bm.rowBlobs[rowID]
	for i, b := range blobs {
		if b.Hash == meta.Hash {
			bm.rowBlobs[rowID] = append(blobs[:i], blobs[i+1:]...)
			return
		}
	}
}

// BlobStats returns statistics about blob storage
func (bm *BlobManager) BlobStats() BlobStatistics {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	stats := BlobStatistics{
		TotalBlobs:     len(bm.blobIndex),
		PendingUploads: len(bm.pendingUploads),
	}

	for _, meta := range bm.blobIndex {
		stats.TotalBytes += int64(meta.Size)
		if meta.RefCount > 1 {
			stats.DeduplicatedBlobs++
		}
	}

	return stats
}

// BlobStatistics contains blob storage statistics
type BlobStatistics struct {
	TotalBlobs        int
	TotalBytes        int64
	DeduplicatedBlobs int
	PendingUploads    int
}

// ValidateBlobIntegrity checks if a blob's data matches its stored hash
func ValidateBlobIntegrity(data []byte, expectedHash string) bool {
	actualHash := ComputeBlobHash(data)
	return actualHash == expectedHash
}

// BlobError represents a blob-related error
type BlobError struct {
	Op      string
	Hash    string
	Message string
}

func (e BlobError) Error() string {
	return fmt.Sprintf("blob %s error for %s: %s", e.Op, e.Hash, e.Message)
}
