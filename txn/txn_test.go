package txn

import (
	"testing"

	"discodb/mvcc"
	"discodb/storage"
	"discodb/types"
)

func TestTransactionLifecycle(t *testing.T) {
	mgr := NewManager()

	txn := mgr.Begin()
	if txn.ID != 1 {
		t.Fatalf("expected txn ID 1, got %d", txn.ID)
	}
	if txn.State != StateActive {
		t.Fatalf("expected state active, got %s", txn.State)
	}
	if mgr.ActiveCount() != 1 {
		t.Fatalf("expected 1 active txn, got %d", mgr.ActiveCount())
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if txn.State != StateCommitted {
		t.Fatalf("expected state committed, got %s", txn.State)
	}
	if mgr.ActiveCount() != 0 {
		t.Fatalf("expected 0 active txns after commit, got %d", mgr.ActiveCount())
	}
	if !mgr.IsCommitted(1) {
		t.Fatal("expected txn 1 to be marked committed")
	}
}

func TestTransactionAbort(t *testing.T) {
	mgr := NewManager()

	txn := mgr.Begin()
	txn.BufferInsert(types.TableID(1), storage.Row{})

	if err := txn.Abort(); err != nil {
		t.Fatalf("abort failed: %v", err)
	}
	if txn.State != StateAborted {
		t.Fatalf("expected state aborted, got %s", txn.State)
	}

	writes := txn.DrainWrites()
	if len(writes) != 0 {
		t.Fatal("expected writes to be cleared on abort")
	}
}

func TestCannotCommitAfterAbort(t *testing.T) {
	mgr := NewManager()
	txn := mgr.Begin()

	if err := txn.Abort(); err != nil {
		t.Fatalf("abort failed: %v", err)
	}

	if err := txn.Commit(); err == nil {
		t.Fatal("expected error committing after abort")
	}
}

func TestCannotAbortAfterCommit(t *testing.T) {
	mgr := NewManager()
	txn := mgr.Begin()

	if err := txn.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	if err := txn.Abort(); err == nil {
		t.Fatal("expected error aborting after commit")
	}
}

func TestWriteBuffering(t *testing.T) {
	mgr := NewManager()
	txn := mgr.Begin()

	row1 := storage.Row{Header: storage.RowHeader{RowID: 1, TableID: 1}}
	row2 := storage.Row{Header: storage.RowHeader{RowID: 2, TableID: 1}}

	txn.BufferInsert(1, row1)
	txn.BufferInsert(1, row2)

	writes := txn.DrainWrites()
	if len(writes) != 2 {
		t.Fatalf("expected 2 buffered writes, got %d", len(writes))
	}
	if writes[0].Op != WriteOpInsert || writes[0].Row.Header.RowID != 1 {
		t.Fatalf("unexpected first write: %+v", writes[0])
	}
	if writes[1].Op != WriteOpInsert || writes[1].Row.Header.RowID != 2 {
		t.Fatalf("unexpected second write: %+v", writes[1])
	}

	after := txn.DrainWrites()
	if len(after) != 0 {
		t.Fatal("expected no writes after drain")
	}
}

func TestUpdateBuffering(t *testing.T) {
	mgr := NewManager()
	txn := mgr.Begin()

	oldRowID := types.RowID(100)
	oldSegID := types.SegmentID(5)
	oldMsgID := types.MessageID(999)
	newRow := storage.Row{Header: storage.RowHeader{RowID: 200, TableID: 1}}

	txn.BufferUpdate(1, newRow, oldRowID, oldSegID, oldMsgID)

	writes := txn.DrainWrites()
	if len(writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(writes))
	}
	w := writes[0]
	if w.Op != WriteOpUpdate {
		t.Fatalf("expected update op, got %s", w.Op)
	}
	if *w.OldRowID != oldRowID {
		t.Fatalf("expected old row ID %d, got %d", oldRowID, *w.OldRowID)
	}
	if *w.OldSegID != oldSegID {
		t.Fatalf("expected old seg ID %d, got %d", oldSegID, *w.OldSegID)
	}
	if *w.OldMsgID != oldMsgID {
		t.Fatalf("expected old msg ID %d, got %d", oldMsgID, *w.OldMsgID)
	}
}

func TestDeleteBuffering(t *testing.T) {
	mgr := NewManager()
	txn := mgr.Begin()

	row := storage.Row{Header: storage.RowHeader{RowID: 50, TableID: 1, Flags: storage.FlagTombstone}}
	txn.BufferDelete(1, row)

	writes := txn.DrainWrites()
	if len(writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(writes))
	}
	if writes[0].Op != WriteOpDelete {
		t.Fatalf("expected delete op, got %s", writes[0].Op)
	}
	if !writes[0].Row.Header.Flags.HasTombstone() {
		t.Fatal("expected tombstone flag on delete")
	}
}

func TestCatalogOpBuffering(t *testing.T) {
	mgr := NewManager()
	txn := mgr.Begin()

	txn.BufferCatalogOp("CATALOG_CREATE_TABLE", []byte(`{"name":"test"}`))

	ops := txn.DrainCatalogOps()
	if len(ops) != 1 {
		t.Fatalf("expected 1 catalog op, got %d", len(ops))
	}
	if ops[0].Kind != "CATALOG_CREATE_TABLE" {
		t.Fatalf("expected CATALOG_CREATE_TABLE, got %s", ops[0].Kind)
	}
}

func TestSnapshotCreation(t *testing.T) {
	mgr := NewManager()

	txn1 := mgr.Begin()
	_ = mgr.Begin()

	snap := mgr.CreateSnapshot()

	if len(snap.ActiveTxns) != 2 {
		t.Fatalf("expected 2 active txns in snapshot, got %d", len(snap.ActiveTxns))
	}

	txn1.Commit()

	snap2 := mgr.CreateSnapshot()
	if len(snap2.ActiveTxns) != 1 {
		t.Fatalf("expected 1 active txn after commit, got %d", len(snap2.ActiveTxns))
	}
}

func TestSnapshotVisibility(t *testing.T) {
	mgr := NewManager()

	txn1 := mgr.Begin()

	snap := txn1.Snapshot
	if !snap.IsVisible(txn1.ID, nil) {
		t.Fatal("own writes should be visible")
	}

	txn2 := mgr.Begin()

	if snap.IsVisible(txn2.ID, nil) {
		t.Fatal("txn2 ID >= TxnMax should not be visible in txn1's snapshot")
	}

	visible := snap.IsVisible(txn2.ID, &txn2.ID)
	if visible {
		t.Fatal("row with txn_max == snapshot txn should not be visible")
	}
}

func TestMVCCVisibility(t *testing.T) {
	snap := mvcc.TransactionSnapshot{
		TxnID:      10,
		TxnMin:     1,
		TxnMax:     20,
		ActiveTxns: []types.TxnID{12, 15},
	}

	if !snap.IsVisible(5, nil) {
		t.Fatal("txn 5 < snapshot.TxnID should be visible")
	}
	if !snap.IsVisible(8, nil) {
		t.Fatal("txn 8 < snapshot.TxnID should be visible")
	}
	if snap.IsVisible(12, nil) {
		t.Fatal("txn 12 is active, should not be visible")
	}
	if snap.IsVisible(15, nil) {
		t.Fatal("txn 15 is active, should not be visible")
	}
	if snap.IsVisible(18, nil) {
		t.Fatal("txn 18 > snapshot.TxnID, should not be visible")
	}
	if snap.IsVisible(25, nil) {
		t.Fatal("txn 25 > snapshot.TxnID, should not be visible")
	}

	txnMax12 := types.TxnID(12)
	if !snap.IsVisible(5, &txnMax12) {
		t.Fatal("row created at txn 5 but deleted at txn 12 SHOULD be visible to snapshot at txn 10 (deletion is in the future)")
	}

	txnMax8 := types.TxnID(8)
	if snap.IsVisible(5, &txnMax8) {
		t.Fatal("row created at txn 5 but deleted at txn 8 should not be visible (deletion before snapshot)")
	}
}

func TestVersionChain(t *testing.T) {
	v1 := mvcc.NewRowVersion(1, 1, 1, 100, 5, 1, []byte("v1"))
	v2 := mvcc.NewRowVersion(1, 1, 1, 200, 10, 2, []byte("v2"))
	v3 := mvcc.NewRowVersion(1, 1, 1, 300, 15, 3, []byte("v3"))
	v3.IsTombstone = true

	chain := mvcc.VersionChain{}
	chain.Push(v1)
	chain.Push(v2)
	chain.Push(v3)

	snap := mvcc.TransactionSnapshot{
		TxnID:      12,
		TxnMin:     1,
		TxnMax:     20,
		ActiveTxns: nil,
	}

	latest, ok := chain.LatestVisible(snap)
	if !ok {
		t.Fatal("expected a visible version")
	}
	if latest.TxnID != 10 {
		t.Fatalf("expected txn ID 10, got %d", latest.TxnID)
	}

	snap2 := mvcc.TransactionSnapshot{
		TxnID:      20,
		TxnMin:     1,
		TxnMax:     30,
		ActiveTxns: nil,
	}

	latest2, ok := chain.LatestVisible(snap2)
	if !ok {
		t.Fatal("expected a visible version for snapshot 20")
	}
	if !latest2.IsTombstone {
		t.Fatal("expected tombstone for snapshot 20")
	}
	if latest2.TxnID != 15 {
		t.Fatalf("expected txn ID 15 for tombstone, got %d", latest2.TxnID)
	}
}

func TestConcurrentTransactions(t *testing.T) {
	mgr := NewManager()

	txn1 := mgr.Begin()
	txn2 := mgr.Begin()
	txn3 := mgr.Begin()

	if mgr.ActiveCount() != 3 {
		t.Fatalf("expected 3 active, got %d", mgr.ActiveCount())
	}

	snap3 := txn3.Snapshot
	if len(snap3.ActiveTxns) != 2 {
		t.Fatalf("snapshot should see 2 active txns, got %d", len(snap3.ActiveTxns))
	}

	txn1.Commit()
	txn2.Abort()

	if mgr.ActiveCount() != 1 {
		t.Fatalf("expected 1 active after 2 complete, got %d", mgr.ActiveCount())
	}

	if !mgr.IsCommitted(1) {
		t.Fatal("txn 1 should be committed")
	}
}

func TestTxnMinAdvancement(t *testing.T) {
	mgr := NewManager()

	txn1 := mgr.Begin()
	txn2 := mgr.Begin()

	mgr.AdvanceTxnMin()
	if mgr.txnMin != 1 {
		t.Fatalf("txn_min should still be 1, got %d", mgr.txnMin)
	}

	txn1.Commit()
	mgr.AdvanceTxnMin()
	if mgr.txnMin != 2 {
		t.Fatalf("txn_min should be 2 after txn1 commit, got %d", mgr.txnMin)
	}

	txn2.Commit()
	mgr.AdvanceTxnMin()
	if mgr.txnMin != mgr.txnMax {
		t.Fatalf("txn_min should equal txn_max when no active txns")
	}
}

func TestPrepareState(t *testing.T) {
	mgr := NewManager()
	txn := mgr.Begin()

	if err := txn.Prepare(); err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	if txn.State != StatePrepared {
		t.Fatalf("expected prepared, got %s", txn.State)
	}
	if txn.PreparedAt == nil {
		t.Fatal("expected PreparedAt to be set")
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("commit after prepare failed: %v", err)
	}
	if txn.State != StateCommitted {
		t.Fatalf("expected committed after prepare+commit, got %s", txn.State)
	}
}

func TestCannotPrepareFromCommitted(t *testing.T) {
	mgr := NewManager()
	txn := mgr.Begin()
	txn.Commit()

	if err := txn.Prepare(); err == nil {
		t.Fatal("expected error preparing committed txn")
	}
}

func TestReadOnlyTransaction(t *testing.T) {
	mgr := NewManager()

	readTxn := mgr.Begin()
	if !readTxn.IsReadOnly() {
		t.Fatal("expected read-only txn with no writes")
	}

	writeTxn := mgr.Begin()
	writeTxn.BufferInsert(1, storage.Row{})
	if writeTxn.IsReadOnly() {
		t.Fatal("expected non-read-only txn with writes")
	}
}

func TestRegisterCommittedFromReplay(t *testing.T) {
	mgr := NewManager()

	mgr.RegisterCommittedFromReplay(5)
	if !mgr.IsCommitted(5) {
		t.Fatal("expected txn 5 to be registered as committed")
	}
	if mgr.txnMax != 5 {
		t.Fatalf("expected txn_max 5, got %d", mgr.txnMax)
	}
}

func TestGetTransaction(t *testing.T) {
	mgr := NewManager()
	txn := mgr.Begin()

	found, ok := mgr.GetTransaction(txn.ID)
	if !ok {
		t.Fatal("expected to find transaction")
	}
	if found.ID != txn.ID {
		t.Fatalf("expected txn ID %d, got %d", txn.ID, found.ID)
	}

	txn.Commit()

	_, ok = mgr.GetTransaction(txn.ID)
	if ok {
		t.Fatal("should not find committed transaction in active map")
	}
}

func TestCompleteTransaction(t *testing.T) {
	mgr := NewManager()
	txn := mgr.Begin()
	txn.BufferInsert(1, storage.Row{})

	if err := mgr.CompleteTransaction(txn.ID, true); err != nil {
		t.Fatalf("complete committed: %v", err)
	}
	if txn.State != StateCommitted {
		t.Fatalf("expected committed state, got %s", txn.State)
	}
	if !mgr.IsCommitted(txn.ID) {
		t.Fatal("expected txn to be marked committed")
	}

	txn2 := mgr.Begin()
	if err := mgr.CompleteTransaction(txn2.ID, false); err != nil {
		t.Fatalf("complete aborted: %v", err)
	}
	if txn2.State != StateAborted {
		t.Fatalf("expected aborted state, got %s", txn2.State)
	}
}

func TestCompleteTransactionNotFound(t *testing.T) {
	mgr := NewManager()

	err := mgr.CompleteTransaction(999, true)
	if err == nil {
		t.Fatal("expected error for nonexistent transaction")
	}
}

func TestSetChannel(t *testing.T) {
	mgr := NewManager()
	txn := mgr.Begin()

	txn.SetChannel(1, 5, 100)
	if txn.TableID != 1 {
		t.Fatalf("expected table ID 1, got %d", txn.TableID)
	}
	if txn.SegmentID != 5 {
		t.Fatalf("expected segment ID 5, got %d", txn.SegmentID)
	}
	if txn.ChannelID != 100 {
		t.Fatalf("expected channel ID 100, got %d", txn.ChannelID)
	}
}
