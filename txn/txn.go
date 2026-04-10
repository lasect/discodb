package txn

import (
	"fmt"
	"sync"
	"time"

	"discodb/catalog"
	"discodb/mvcc"
	"discodb/storage"
	"discodb/types"
)

type State string

const (
	StateActive    State = "active"
	StatePrepared  State = "prepared"
	StateCommitted State = "committed"
	StateAborted   State = "aborted"
)

type WriteOp string

const (
	WriteOpInsert WriteOp = "insert"
	WriteOpUpdate WriteOp = "update"
	WriteOpDelete WriteOp = "delete"
)

type BufferedWrite struct {
	Op       WriteOp
	TableID  types.TableID
	Row      storage.Row
	OldRowID *types.RowID
	OldSegID *types.SegmentID
	OldMsgID *types.MessageID
}

type CatalogOp struct {
	Kind    string
	Payload []byte
}

type IndexWrite struct {
	Op        WriteOp
	IndexID   types.TableID
	Key       []byte
	RowID     types.RowID
	SegmentID types.SegmentID
	MessageID types.MessageID
}

type Transaction struct {
	mu          sync.Mutex
	mgr         *Manager
	ID          types.TxnID
	State       State
	Snapshot    mvcc.TransactionSnapshot
	StartedAt   time.Time
	PreparedAt  *time.Time
	CommittedAt *time.Time
	Writes      []BufferedWrite
	CatalogOps  []CatalogOp
	IndexWrites []IndexWrite
	TableID     types.TableID
	SegmentID   types.SegmentID
	ChannelID   types.ChannelID
}

func (t *Transaction) BufferInsert(tableID types.TableID, row storage.Row) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Writes = append(t.Writes, BufferedWrite{
		Op:      WriteOpInsert,
		TableID: tableID,
		Row:     row,
	})
}

func (t *Transaction) BufferUpdate(tableID types.TableID, newRow storage.Row, oldRowID types.RowID, oldSegID types.SegmentID, oldMsgID types.MessageID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Writes = append(t.Writes, BufferedWrite{
		Op:       WriteOpUpdate,
		TableID:  tableID,
		Row:      newRow,
		OldRowID: &oldRowID,
		OldSegID: &oldSegID,
		OldMsgID: &oldMsgID,
	})
}

func (t *Transaction) BufferDelete(tableID types.TableID, row storage.Row) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Writes = append(t.Writes, BufferedWrite{
		Op:      WriteOpDelete,
		TableID: tableID,
		Row:     row,
	})
}

func (t *Transaction) BufferCatalogOp(kind string, payload []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.CatalogOps = append(t.CatalogOps, CatalogOp{Kind: kind, Payload: payload})
}

func (t *Transaction) BufferIndexInsert(indexID types.TableID, key []byte, rowID types.RowID, segmentID types.SegmentID, messageID types.MessageID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.IndexWrites = append(t.IndexWrites, IndexWrite{
		Op:        WriteOpInsert,
		IndexID:   indexID,
		Key:       key,
		RowID:     rowID,
		SegmentID: segmentID,
		MessageID: messageID,
	})
}

func (t *Transaction) BufferIndexDelete(indexID types.TableID, key []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.IndexWrites = append(t.IndexWrites, IndexWrite{
		Op:      WriteOpDelete,
		IndexID: indexID,
		Key:     key,
	})
}

func (t *Transaction) DrainIndexWrites() []IndexWrite {
	t.mu.Lock()
	defer t.mu.Unlock()
	iw := t.IndexWrites
	t.IndexWrites = nil
	return iw
}

func (t *Transaction) SetChannel(tableID types.TableID, segmentID types.SegmentID, channelID types.ChannelID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.TableID = tableID
	t.SegmentID = segmentID
	t.ChannelID = channelID
}

func (t *Transaction) Prepare() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.State != StateActive {
		return fmt.Errorf("cannot prepare transaction in state %q", t.State)
	}
	t.State = StatePrepared
	now := time.Now().UTC()
	t.PreparedAt = &now
	return nil
}

func (t *Transaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.State != StateActive && t.State != StatePrepared {
		return fmt.Errorf("cannot commit transaction in state %q", t.State)
	}
	t.State = StateCommitted
	now := time.Now().UTC()
	t.CommittedAt = &now

	if t.mgr != nil {
		t.mgr.mu.Lock()
		t.mgr.committed[t.ID] = true
		delete(t.mgr.active, t.ID)
		if len(t.mgr.active) == 0 {
			t.mgr.txnMin = t.mgr.txnMax
		}
		t.mgr.mu.Unlock()
	}
	return nil
}

func (t *Transaction) Abort() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.State == StateCommitted {
		return fmt.Errorf("cannot abort committed transaction")
	}
	t.State = StateAborted
	t.Writes = nil
	t.CatalogOps = nil

	if t.mgr != nil {
		t.mgr.mu.Lock()
		delete(t.mgr.active, t.ID)
		if len(t.mgr.active) == 0 {
			t.mgr.txnMin = t.mgr.txnMax
		}
		t.mgr.mu.Unlock()
	}
	return nil
}

func (t *Transaction) IsReadOnly() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.Writes) == 0 && len(t.CatalogOps) == 0
}

func (t *Transaction) DrainWrites() []BufferedWrite {
	t.mu.Lock()
	defer t.mu.Unlock()
	ws := t.Writes
	t.Writes = nil
	return ws
}

func (t *Transaction) DrainCatalogOps() []CatalogOp {
	t.mu.Lock()
	defer t.mu.Unlock()
	ops := t.CatalogOps
	t.CatalogOps = nil
	return ops
}

type Manager struct {
	mu         sync.RWMutex
	txnCounter uint64
	txnMin     types.TxnID
	txnMax     types.TxnID
	active     map[types.TxnID]*Transaction
	committed  map[types.TxnID]bool
}

func NewManager() *Manager {
	return &Manager{
		txnCounter: 0,
		txnMin:     types.TxnID(1),
		txnMax:     types.TxnID(1),
		active:     make(map[types.TxnID]*Transaction),
		committed:  make(map[types.TxnID]bool),
	}
}

func (m *Manager) SetTxnMax(max types.TxnID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if max > m.txnMax {
		m.txnMax = max
		m.txnCounter = uint64(max)
	}
}

func (m *Manager) Begin() *Transaction {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.txnCounter++
	txnID := types.TxnID(m.txnCounter)
	if txnID > m.txnMax {
		m.txnMax = txnID
	}

	var activeList []types.TxnID
	for id := range m.active {
		activeList = append(activeList, id)
	}

	snapshot := mvcc.TransactionSnapshot{
		TxnID:      txnID,
		TxnMin:     m.txnMin,
		TxnMax:     m.txnMax,
		ActiveTxns: activeList,
	}

	txn := &Transaction{
		mgr:       m,
		ID:        txnID,
		State:     StateActive,
		Snapshot:  snapshot,
		StartedAt: time.Now().UTC(),
	}

	m.active[txnID] = txn
	return txn
}

func (m *Manager) GetTransaction(id types.TxnID) (*Transaction, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	txn, ok := m.active[id]
	return txn, ok
}

func (m *Manager) CompleteTransaction(id types.TxnID, committed bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	txn, ok := m.active[id]
	if !ok {
		return fmt.Errorf("transaction %d not found", id)
	}

	if committed {
		txn.State = StateCommitted
		now := time.Now().UTC()
		txn.CommittedAt = &now
		m.committed[id] = true
	} else {
		txn.State = StateAborted
		txn.Writes = nil
		txn.CatalogOps = nil
	}

	delete(m.active, id)

	if len(m.active) == 0 {
		m.txnMin = m.txnMax
	}

	return nil
}

func (m *Manager) CreateSnapshot() mvcc.TransactionSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var activeList []types.TxnID
	for id := range m.active {
		activeList = append(activeList, id)
	}

	return mvcc.TransactionSnapshot{
		TxnID:      m.txnMax,
		TxnMin:     m.txnMin,
		TxnMax:     m.txnMax,
		ActiveTxns: activeList,
	}
}

func (m *Manager) ActiveTransactions() []types.TxnID {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var ids []types.TxnID
	for id := range m.active {
		ids = append(ids, id)
	}
	return ids
}

func (m *Manager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.active)
}

func (m *Manager) IsCommitted(id types.TxnID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.committed[id]
}

func (m *Manager) AdvanceTxnMin() {
	m.mu.Lock()
	defer m.mu.Unlock()
	oldMin := m.txnMin
	if len(m.active) == 0 {
		m.txnMin = m.txnMax
	} else {
		minID := m.txnMax
		for id := range m.active {
			if id < minID {
				minID = id
			}
		}
		m.txnMin = minID
	}
	// Prune committed transactions that are no longer needed for visibility checks
	if m.txnMin > oldMin {
		m.pruneCommittedLocked()
	}
}

// pruneCommittedLocked removes entries from the committed map that are older than txnMin.
// These transactions are no longer needed for visibility checks since all active
// transactions have a higher ID. Must be called with m.mu held.
func (m *Manager) pruneCommittedLocked() {
	for id := range m.committed {
		if id < m.txnMin {
			delete(m.committed, id)
		}
	}
}

func (m *Manager) RegisterCommittedFromReplay(id types.TxnID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.committed[id] = true
	if id > m.txnMax {
		m.txnMax = id
	}
	if id < m.txnMin {
		m.txnMin = id
	}
}

func (m *Manager) ReplayApplyCatalog(opKind string, payload []byte, cat *catalog.Catalog) error {
	_ = opKind
	_ = payload
	_ = cat
	return nil
}
