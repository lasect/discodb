package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"discodb/boot"
	"discodb/catalog"
	"discodb/config"
	"discodb/discord"
	"discodb/executor"
	"discodb/mvcc"
	discodbsql "discodb/sql"
	"discodb/storage"
	"discodb/txn"
	"discodb/types"
	"discodb/wal"
)

type ConnTxnState string

const (
	ConnTxnIdle   ConnTxnState = "idle"
	ConnTxnActive ConnTxnState = "active"
	ConnTxnFailed ConnTxnState = "failed"
)

type Engine struct {
	cfg     config.Config
	logger  *slog.Logger
	boot    *boot.BootInfo
	catalog *catalog.Catalog

	catalogClient  *discord.Client
	heapClient     *discord.Client
	walClient      *discord.Client
	indexClient    *discord.Client
	overflowClient *discord.Client

	walWriter    *WALWriter
	walReader    *WALReader
	segMgr       *SegmentManager
	txnManager   *txn.Manager
	txnMu        sync.Mutex
	connTxnState map[string]ConnTxnState

	lsnCounter     atomic.Uint64
	rowCounter     atomic.Uint64
	tableCounter   atomic.Uint64
	segmentCounter atomic.Uint64
}

func NewEngine(cfg config.Config, logger *slog.Logger) (*Engine, error) {
	bootstrapper, err := boot.NewBootstrapper(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("create bootstrapper: %w", err)
	}

	ctx := context.Background()
	bootInfo, err := bootstrapper.Bootstrap(ctx)
	if err != nil {
		bootstrapper.Close()
		return nil, fmt.Errorf("bootstrap: %w", err)
	}

	cat := catalog.New()

	walWriter := NewWALWriter(bootstrapper.WALClient(), bootInfo.WALChannel, logger)
	walReader := NewWALReader(bootstrapper.WALClient(), bootInfo.WALChannel, logger)

	if err := walReader.Replay(ctx, cat); err != nil {
		logger.Warn("WAL replay failed (may be first run)", slog.String("error", err.Error()))
	}

	if err := loadCatalogFromDiscord(ctx, bootstrapper.CatalogClient(), bootInfo.GuildID, bootInfo.CatalogCategory, cat); err != nil {
		logger.Warn("catalog load failed (may be first run)", slog.String("error", err.Error()))
	}

	segMgr := NewSegmentManager(
		bootstrapper.HeapClient(),
		bootstrapper.CatalogClient(),
		bootInfo.GuildID,
		bootInfo.CatalogCategory,
		cfg.Storage.HeapChannelPrefix,
		logger,
	)

	eng := &Engine{
		cfg:            cfg,
		logger:         logger,
		boot:           bootInfo,
		catalog:        cat,
		catalogClient:  bootstrapper.CatalogClient(),
		heapClient:     bootstrapper.HeapClient(),
		walClient:      bootstrapper.WALClient(),
		indexClient:    bootstrapper.IndexClient(),
		overflowClient: bootstrapper.OverflowClient(),
		walWriter:      walWriter,
		walReader:      walReader,
		segMgr:         segMgr,
		txnManager:     txn.NewManager(),
		connTxnState:   make(map[string]ConnTxnState),
	}

	eng.lsnCounter.Store(1)
	eng.rowCounter.Store(1)
	eng.tableCounter.Store(1)
	eng.segmentCounter.Store(1)

	return eng, nil
}

func (e *Engine) Close() error {
	return nil
}

func (e *Engine) TxnManager() *txn.Manager {
	return e.txnManager
}

func (e *Engine) GetConnState(connID string) ConnTxnState {
	e.txnMu.Lock()
	defer e.txnMu.Unlock()
	return e.connTxnState[connID]
}

func (e *Engine) SetConnState(connID string, state ConnTxnState) {
	e.txnMu.Lock()
	defer e.txnMu.Unlock()
	e.connTxnState[connID] = state
}

func (e *Engine) ReadRows(ctx context.Context, tableID types.TableID) ([]storage.Row, error) {
	segments, err := e.segMgr.ListSegments(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("list segments: %w", err)
	}

	var allRows []storage.Row
	for _, seg := range segments {
		rows, _, err := e.segMgr.ReadRows(ctx, seg.ID)
		if err != nil {
			e.logger.Warn("failed to read rows from segment",
				slog.String("segment", seg.Name),
				slog.String("error", err.Error()),
			)
			continue
		}
		allRows = append(allRows, rows...)
	}

	return allRows, nil
}

func (e *Engine) ReadRowsWithSnapshot(ctx context.Context, tableID types.TableID, snap mvcc.TransactionSnapshot) ([]storage.Row, error) {
	segments, err := e.segMgr.ListSegments(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("list segments: %w", err)
	}

	var allRows []storage.Row
	for _, seg := range segments {
		rows, _, err := e.segMgr.ReadRows(ctx, seg.ID)
		if err != nil {
			e.logger.Warn("failed to read rows from segment",
				slog.String("segment", seg.Name),
				slog.String("error", err.Error()),
			)
			continue
		}

		for _, row := range rows {
			if row.Header.Flags.HasTombstone() {
				continue
			}
			var txnMax *types.TxnID
			if row.Header.TxnMax > 0 {
				txnMax = &row.Header.TxnMax
			}
			if !snap.IsVisible(row.Header.TxnID, txnMax) {
				continue
			}
			allRows = append(allRows, row)
		}
	}

	return allRows, nil
}

func (e *Engine) ExecuteQuery(query string) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	return e.ExecuteQueryWithTxn("", query)
}

func (e *Engine) ExecuteQueryWithTxn(connID string, query string) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	stmt, err := discodbsql.Parse(query)
	if err != nil {
		return nil, nil, 0, err
	}

	switch s := stmt.(type) {
	case discodbsql.BeginStmt:
		return e.handleBegin(connID)
	case discodbsql.CommitStmt:
		return e.handleCommit(connID)
	case discodbsql.RollbackStmt:
		return e.handleRollback(connID)
	case discodbsql.CreateTableStmt:
		return e.handleCreateTable(s, connID)
	case discodbsql.InsertStmt:
		return e.handleInsert(s, connID)
	case discodbsql.SelectStmt:
		return e.handleSelect(s, connID)
	case discodbsql.DeleteStmt:
		return e.handleDelete(s, connID)
	case discodbsql.UpdateStmt:
		return e.handleUpdate(s, connID)
	case discodbsql.DropTableStmt:
		return e.handleDropTable(s, connID)
	case discodbsql.CreateIndexStmt:
		return nil, nil, 0, fmt.Errorf("unsupported: CREATE INDEX")
	default:
		return nil, nil, 0, fmt.Errorf("unsupported statement type")
	}
}

func (e *Engine) handleBegin(connID string) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	if connID == "" {
		return nil, nil, 0, fmt.Errorf("BEGIN requires a connection ID")
	}

	e.txnMu.Lock()
	currentState := e.connTxnState[connID]
	e.txnMu.Unlock()

	if currentState == ConnTxnActive {
		return nil, nil, 0, fmt.Errorf("already in a transaction")
	}

	t := e.txnManager.Begin()

	e.txnMu.Lock()
	e.connTxnState[connID] = ConnTxnActive
	e.txnMu.Unlock()

	e.logger.Info("transaction began", slog.String("txn_id", t.ID.String()))
	return nil, nil, 0, nil
}

func (e *Engine) handleCommit(connID string) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	if connID == "" {
		return nil, nil, 0, fmt.Errorf("COMMIT requires a connection ID")
	}

	e.txnMu.Lock()
	currentState := e.connTxnState[connID]
	e.txnMu.Unlock()

	if currentState != ConnTxnActive {
		return nil, nil, 0, fmt.Errorf("no active transaction to commit")
	}

	var activeTxn *txn.Transaction
	for _, txnID := range e.txnManager.ActiveTransactions() {
		t, ok := e.txnManager.GetTransaction(txnID)
		if ok {
			activeTxn = t
			break
		}
	}

	if activeTxn == nil {
		e.txnMu.Lock()
		e.connTxnState[connID] = ConnTxnIdle
		e.txnMu.Unlock()
		return nil, nil, 0, nil
	}

	ctx := context.Background()
	if err := e.flushTransaction(ctx, activeTxn); err != nil {
		_ = activeTxn.Abort()
		e.txnMu.Lock()
		e.connTxnState[connID] = ConnTxnFailed
		e.txnMu.Unlock()
		return nil, nil, 0, fmt.Errorf("commit failed: %w", err)
	}

	if err := activeTxn.Commit(); err != nil {
		e.txnMu.Lock()
		e.connTxnState[connID] = ConnTxnFailed
		e.txnMu.Unlock()
		return nil, nil, 0, fmt.Errorf("commit failed: %w", err)
	}

	e.txnManager.AdvanceTxnMin()

	e.txnMu.Lock()
	e.connTxnState[connID] = ConnTxnIdle
	e.txnMu.Unlock()

	e.logger.Info("transaction committed", slog.String("txn_id", activeTxn.ID.String()))
	return nil, nil, 0, nil
}

func (e *Engine) handleRollback(connID string) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	if connID == "" {
		return nil, nil, 0, fmt.Errorf("ROLLBACK requires a connection ID")
	}

	e.txnMu.Lock()
	currentState := e.connTxnState[connID]
	e.txnMu.Unlock()

	if currentState != ConnTxnActive && currentState != ConnTxnFailed {
		return nil, nil, 0, fmt.Errorf("no active transaction to rollback")
	}

	var activeTxn *txn.Transaction
	for _, txnID := range e.txnManager.ActiveTransactions() {
		t, ok := e.txnManager.GetTransaction(txnID)
		if ok {
			activeTxn = t
			break
		}
	}

	if activeTxn != nil {
		_ = activeTxn.Abort()
		e.logger.Info("transaction rolled back", slog.String("txn_id", activeTxn.ID.String()))
	}

	e.txnManager.AdvanceTxnMin()

	e.txnMu.Lock()
	e.connTxnState[connID] = ConnTxnIdle
	e.txnMu.Unlock()

	return nil, nil, 0, nil
}

func (e *Engine) flushTransaction(ctx context.Context, t *txn.Transaction) error {
	writes := t.DrainWrites()
	if len(writes) == 0 && len(t.CatalogOps) == 0 {
		return nil
	}

	txnID := t.ID
	beginRec := wal.Begin(txnID, e.nextLSN())
	if err := e.walWriter.Append(ctx, beginRec); err != nil {
		return fmt.Errorf("WAL begin: %w", err)
	}

	for _, catOp := range t.DrainCatalogOps() {
		catRec := wal.Record{
			Kind:  catOp.Kind,
			TxnID: txnID,
			LSN:   e.nextLSN(),
			Data:  catOp.Payload,
		}
		if err := e.walWriter.Append(ctx, catRec); err != nil {
			return fmt.Errorf("WAL catalog op: %w", err)
		}
	}

	for _, w := range writes {
		switch w.Op {
		case txn.WriteOpInsert:
			msg, err := e.segMgr.WriteRow(ctx, t.ChannelID, w.Row, e.catalog.Epoch())
			if err != nil {
				return fmt.Errorf("write row: %w", err)
			}
			w.Row.Header.MessageID = msg.ID

			insertRec := wal.Insert(txnID, e.nextLSN(), w.TableID, w.Row.Header.RowID, w.Row.Header.SegmentID, msg.ID, nil)
			if err := e.walWriter.Append(ctx, insertRec); err != nil {
				return fmt.Errorf("WAL insert: %w", err)
			}

		case txn.WriteOpUpdate:
			tombstoneRow := w.Row
			tombstoneRow.Header.Flags |= storage.FlagTombstone

			_, err := e.segMgr.WriteRow(ctx, t.ChannelID, tombstoneRow, e.catalog.Epoch())
			if err != nil {
				return fmt.Errorf("write tombstone for update: %w", err)
			}

			newRow := w.Row
			newMsg, err := e.segMgr.WriteRow(ctx, t.ChannelID, newRow, e.catalog.Epoch())
			if err != nil {
				return fmt.Errorf("write updated row: %w", err)
			}
			newRow.Header.MessageID = newMsg.ID

			updateRec := wal.Update(txnID, e.nextLSN(), w.TableID, newRow.Header.RowID, newRow.Header.SegmentID, newMsg.ID, *w.OldSegID, *w.OldMsgID, nil)
			if err := e.walWriter.Append(ctx, updateRec); err != nil {
				return fmt.Errorf("WAL update: %w", err)
			}

		case txn.WriteOpDelete:
			_, err := e.segMgr.WriteRow(ctx, t.ChannelID, w.Row, e.catalog.Epoch())
			if err != nil {
				return fmt.Errorf("write tombstone for delete: %w", err)
			}

			deleteRec := wal.Delete(txnID, e.nextLSN(), w.TableID, w.Row.Header.RowID, w.Row.Header.SegmentID, w.Row.Header.MessageID)
			if err := e.walWriter.Append(ctx, deleteRec); err != nil {
				return fmt.Errorf("WAL delete: %w", err)
			}
		}
	}

	commitRec := wal.Commit(txnID, e.nextLSN())
	if err := e.walWriter.Append(ctx, commitRec); err != nil {
		return fmt.Errorf("WAL commit: %w", err)
	}

	return nil
}

func (e *Engine) getOrCreateTxn(connID string) (*txn.Transaction, bool) {
	e.txnMu.Lock()
	state := e.connTxnState[connID]
	e.txnMu.Unlock()

	if state == ConnTxnActive {
		for _, txnID := range e.txnManager.ActiveTransactions() {
			t, ok := e.txnManager.GetTransaction(txnID)
			if ok {
				return t, true
			}
		}
	}

	t := e.txnManager.Begin()
	e.txnMu.Lock()
	e.connTxnState[connID] = ConnTxnActive
	e.txnMu.Unlock()
	return t, false
}

func (e *Engine) autoCommitTxn(connID string, t *txn.Transaction, wasExisting bool) error {
	if wasExisting {
		return nil
	}

	ctx := context.Background()
	if err := e.flushTransaction(ctx, t); err != nil {
		_ = t.Abort()
		e.txnMu.Lock()
		e.connTxnState[connID] = ConnTxnIdle
		e.txnMu.Unlock()
		return err
	}

	if err := t.Commit(); err != nil {
		e.txnMu.Lock()
		e.connTxnState[connID] = ConnTxnIdle
		e.txnMu.Unlock()
		return err
	}

	e.txnManager.AdvanceTxnMin()

	e.txnMu.Lock()
	e.connTxnState[connID] = ConnTxnIdle
	e.txnMu.Unlock()

	return nil
}

func (e *Engine) executePlanWithSnapshot(plan executor.PhysicalPlan, snap mvcc.TransactionSnapshot) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	ctx := context.Background()

	e.injectSnapshot(plan.Root, snap)

	var allRows []executor.Row
	var schema []executor.ColumnInfo

	for {
		batch, done, err := plan.Root.Execute(ctx)
		if err != nil {
			return nil, nil, 0, err
		}

		if schema == nil {
			schema = batch.Schema
		}

		for _, row := range batch.Rows {
			allRows = append(allRows, row)
		}

		if done {
			break
		}
	}

	return schema, allRows, uint64(len(allRows)), nil
}

func (e *Engine) injectSnapshot(ex executor.Executor, snap mvcc.TransactionSnapshot) {
	switch n := ex.(type) {
	case *executor.SeqScan:
		if n.Snapshot == nil {
			n.Snapshot = &snap
		}
	case *executor.Filter:
		e.injectSnapshot(n.Input, snap)
	case *executor.Projection:
		e.injectSnapshot(n.Input, snap)
	case *executor.Limit:
		e.injectSnapshot(n.Input, snap)
	case *executor.DeleteExec:
		e.injectSnapshot(n.Input, snap)
	case *executor.UpdateExec:
		e.injectSnapshot(n.Input, snap)
	case *executor.AggregateExec:
		e.injectSnapshot(n.Input, snap)
	case *executor.IndexScan:
	}
}

func (e *Engine) executePlan(plan executor.PhysicalPlan) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	return e.executePlanWithSnapshot(plan, e.txnManager.CreateSnapshot())
}

func (e *Engine) nextLSN() types.LSN {
	return types.LSN(e.lsnCounter.Add(1))
}

func (e *Engine) nextRowID() types.RowID {
	return types.RowID(e.rowCounter.Add(1))
}

func (e *Engine) nextTableID() types.TableID {
	return types.TableID(e.tableCounter.Add(1))
}

func (e *Engine) nextSegmentID() types.SegmentID {
	return types.SegmentID(e.segmentCounter.Add(1))
}

func sqlDataTypeToDiscodb(dt discodbsql.SQLDataType) types.DataType {
	switch dt {
	case discodbsql.SQLBool:
		return types.DataTypeBool
	case discodbsql.SQLInt2:
		return types.DataTypeInt2
	case discodbsql.SQLInt4:
		return types.DataTypeInt4
	case discodbsql.SQLInt8:
		return types.DataTypeInt8
	case discodbsql.SQLFloat4:
		return types.DataTypeFloat4
	case discodbsql.SQLFloat8:
		return types.DataTypeFloat8
	case discodbsql.SQLText:
		return types.DataTypeText
	case discodbsql.SQLJSON:
		return types.DataTypeJSON
	case discodbsql.SQLBlob:
		return types.DataTypeBlob
	case discodbsql.SQLTimestamp:
		return types.DataTypeTimestamp
	default:
		return types.DataTypeText
	}
}
