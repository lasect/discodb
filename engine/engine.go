package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"discodb/boot"
	"discodb/catalog"
	"discodb/config"
	"discodb/discord"
	"discodb/executor"
	discodbsql "discodb/sql"
	"discodb/types"
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

	walWriter      *WALWriter
	walReader      *WALReader
	segMgr         *SegmentManager
	txnCounter     atomic.Uint64
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
	}

	eng.txnCounter.Store(1)
	eng.lsnCounter.Store(1)
	eng.rowCounter.Store(1)
	eng.tableCounter.Store(1)
	eng.segmentCounter.Store(1)

	return eng, nil
}

func (e *Engine) Close() error {
	return nil
}

func (e *Engine) ExecuteQuery(query string) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	stmt, err := discodbsql.Parse(query)
	if err != nil {
		return nil, nil, 0, err
	}

	switch s := stmt.(type) {
	case discodbsql.CreateTableStmt:
		return e.handleCreateTable(s)
	case discodbsql.InsertStmt:
		return e.handleInsert(s)
	case discodbsql.SelectStmt:
		return e.handleSelect(s)
	case discodbsql.DeleteStmt:
		return nil, nil, 0, fmt.Errorf("unsupported: DELETE")
	case discodbsql.UpdateStmt:
		return nil, nil, 0, fmt.Errorf("unsupported: UPDATE")
	case discodbsql.DropTableStmt:
		return nil, nil, 0, fmt.Errorf("unsupported: DROP TABLE")
	case discodbsql.CreateIndexStmt:
		return nil, nil, 0, fmt.Errorf("unsupported: CREATE INDEX")
	default:
		return nil, nil, 0, fmt.Errorf("unsupported statement type")
	}
}

func (e *Engine) nextTxnID() types.TxnID {
	return types.TxnID(e.txnCounter.Add(1))
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
