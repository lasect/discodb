package storage

import (
	"testing"

	"discodb/types"
)

func TestComputeBlobHash(t *testing.T) {
	data := []byte("hello world")
	hash := ComputeBlobHash(data)

	if hash == "" {
		t.Error("expected non-empty hash")
	}

	// Same data should produce same hash
	hash2 := ComputeBlobHash(data)
	if hash != hash2 {
		t.Errorf("hash mismatch for same data: %s != %s", hash, hash2)
	}

	// Different data should produce different hash
	hash3 := ComputeBlobHash([]byte("different"))
	if hash == hash3 {
		t.Error("expected different hash for different data")
	}
}

func TestBlobManagerPrepareUpload(t *testing.T) {
	bm := NewBlobManager()
	data := []byte("test blob data")
	rowID := types.MustRowID(1)

	// First upload should create pending
	pending, existing := bm.PrepareUpload(data, rowID, 0)
	if pending == nil {
		t.Fatal("expected pending upload")
	}
	if existing != nil {
		t.Error("expected no existing blob")
	}
	if pending.Hash == "" {
		t.Error("expected hash in pending")
	}
	if pending.RowID != rowID {
		t.Errorf("rowID = %v, want %v", pending.RowID, rowID)
	}

	// Register the upload
	msgID := types.MustMessageID(100)
	meta := bm.RegisterUpload(pending, msgID, "att_123", "https://cdn.discord.com/...")

	if meta.MessageID != msgID {
		t.Errorf("MessageID = %v, want %v", meta.MessageID, msgID)
	}
	if meta.RefCount != 1 {
		t.Errorf("RefCount = %d, want 1", meta.RefCount)
	}

	// Second upload with same data should deduplicate
	rowID2 := types.MustRowID(2)
	pending2, existing2 := bm.PrepareUpload(data, rowID2, 0)
	if pending2 != nil {
		t.Error("expected no pending for deduplicated blob")
	}
	if existing2 == nil {
		t.Fatal("expected existing blob for deduplication")
	}
	if existing2.RefCount != 2 {
		t.Errorf("RefCount = %d, want 2", existing2.RefCount)
	}
}

func TestBlobManagerRewrite(t *testing.T) {
	bm := NewBlobManager()
	rowID := types.MustRowID(1)

	// Upload initial blob
	data1 := []byte("original data")
	pending1, _ := bm.PrepareUpload(data1, rowID, 0)
	meta1 := bm.RegisterUpload(pending1, types.MustMessageID(100), "att_1", "url1")

	// Prepare rewrite
	data2 := []byte("new data")
	op, err := bm.PrepareRewrite(meta1.ToBlobRef(), data2, rowID, 0)
	if err != nil {
		t.Fatalf("PrepareRewrite error: %v", err)
	}
	if op.OldBlob == nil {
		t.Error("expected old blob in rewrite op")
	}

	// Execute rewrite
	result := bm.ExecuteRewrite(op, types.MustMessageID(200), "att_2", "url2")
	if result.NewBlob == nil {
		t.Fatal("expected new blob in result")
	}
	if !result.OldBlobOrphaned {
		t.Error("expected old blob to be orphaned (refcount was 1)")
	}
	if result.OrphanedMessageID != types.MustMessageID(100) {
		t.Errorf("OrphanedMessageID = %v, want 100", result.OrphanedMessageID)
	}

	// Old blob should be removed from index
	_, found := bm.GetBlobMetadata(meta1.Hash)
	if found {
		t.Error("old blob should be removed from index")
	}

	// New blob should be in index
	_, found = bm.GetBlobMetadata(result.NewBlob.Hash)
	if !found {
		t.Error("new blob should be in index")
	}
}

func TestBlobManagerReleaseRowBlobs(t *testing.T) {
	bm := NewBlobManager()
	rowID := types.MustRowID(1)

	// Upload multiple blobs for same row
	for i := 0; i < 3; i++ {
		data := []byte{byte(i)}
		pending, _ := bm.PrepareUpload(data, rowID, i)
		bm.RegisterUpload(pending, types.MustMessageID(uint64(100+i)), "att", "url")
	}

	stats := bm.BlobStats()
	if stats.TotalBlobs != 3 {
		t.Errorf("TotalBlobs = %d, want 3", stats.TotalBlobs)
	}

	// Release all blobs for the row
	orphaned := bm.ReleaseRowBlobs(rowID)
	if len(orphaned) != 3 {
		t.Errorf("orphaned count = %d, want 3", len(orphaned))
	}

	stats = bm.BlobStats()
	if stats.TotalBlobs != 0 {
		t.Errorf("TotalBlobs after release = %d, want 0", stats.TotalBlobs)
	}
}

func TestBlobManagerDeduplication(t *testing.T) {
	bm := NewBlobManager()
	data := []byte("shared blob")

	// Upload from multiple rows
	rowIDs := []types.RowID{
		types.MustRowID(1),
		types.MustRowID(2),
		types.MustRowID(3),
	}

	var meta *BlobMetadata
	for i, rowID := range rowIDs {
		pending, existing := bm.PrepareUpload(data, rowID, 0)
		if i == 0 {
			// First upload
			meta = bm.RegisterUpload(pending, types.MustMessageID(100), "att", "url")
		} else {
			// Should deduplicate
			if pending != nil {
				t.Errorf("row %d: expected deduplication", i)
			}
			meta = existing
		}
	}

	if meta.RefCount != 3 {
		t.Errorf("RefCount = %d, want 3", meta.RefCount)
	}

	stats := bm.BlobStats()
	if stats.DeduplicatedBlobs != 1 {
		t.Errorf("DeduplicatedBlobs = %d, want 1", stats.DeduplicatedBlobs)
	}

	// Release one row's reference
	bm.ReleaseRowBlobs(rowIDs[0])

	// Blob should still exist
	updated, found := bm.GetBlobMetadata(meta.Hash)
	if !found {
		t.Error("blob should still exist after partial release")
	}
	if updated.RefCount != 2 {
		t.Errorf("RefCount after release = %d, want 2", updated.RefCount)
	}
}

func TestValidateBlobIntegrity(t *testing.T) {
	data := []byte("test data")
	hash := ComputeBlobHash(data)

	if !ValidateBlobIntegrity(data, hash) {
		t.Error("valid data should pass integrity check")
	}

	if ValidateBlobIntegrity([]byte("corrupted"), hash) {
		t.Error("corrupted data should fail integrity check")
	}
}

func TestBlobMetadataToBlobRef(t *testing.T) {
	meta := &BlobMetadata{
		MessageID: types.MustMessageID(123),
		Size:      4096,
	}

	ref := meta.ToBlobRef()
	if ref.MessageID != meta.MessageID {
		t.Errorf("MessageID = %v, want %v", ref.MessageID, meta.MessageID)
	}
	if ref.Length != 4096 {
		t.Errorf("Length = %d, want 4096", ref.Length)
	}
}

func TestBlobError(t *testing.T) {
	err := BlobError{
		Op:      "upload",
		Hash:    "abc123",
		Message: "failed",
	}

	errStr := err.Error()
	if errStr == "" {
		t.Error("expected non-empty error string")
	}
}
