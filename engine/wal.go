package engine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"discodb/catalog"
	"discodb/discord"
	"discodb/storage"
	"discodb/types"
	"discodb/wal"
)

type WALWriter struct {
	client     *discord.Client
	webhook    *discord.WebhookClient
	walChannel types.ChannelID
	logger     *slog.Logger
	walEncoder *wal.Writer
}

func NewWALWriter(client *discord.Client, webhook *discord.WebhookClient, walChannel types.ChannelID, logger *slog.Logger) *WALWriter {
	return &WALWriter{
		client:     client,
		webhook:    webhook,
		walChannel: walChannel,
		logger:     logger,
		walEncoder: wal.NewWriter(),
	}
}

func (ww *WALWriter) Append(ctx context.Context, record wal.Record) error {
	encoded := ww.walEncoder.EncodeRecord(record)

	writeID := ww.walEncoder.ComputeWriteID(record.TxnID, record.LSN)

	content := fmt.Sprintf("WAL:%d:%s", writeID, base64.StdEncoding.EncodeToString(encoded))

	params := discord.MessageSendParams{
		Content: content,
		AllowedMentions: &discord.AllowedMentions{
			Parse: []string{},
		},
	}

	var msg *discord.Message
	var err error
	if ww.webhook != nil {
		msg, err = ww.webhook.SendWebhookMessage(ctx, params)
	} else {
		msg, err = ww.client.SendMessage(ctx, ww.walChannel, params)
	}
	if err != nil {
		return fmt.Errorf("send WAL message: %w", err)
	}

	ww.logger.Debug("WAL append",
		slog.String("kind", record.Kind),
		slog.String("lsn", record.LSN.String()),
		slog.String("message_id", msg.ID.String()),
	)

	return nil
}

type WALReader struct {
	client     *discord.Client
	walChannel types.ChannelID
	logger     *slog.Logger
	segMgr     *SegmentManager
	indexMgr   *IndexManager
	cat        *catalog.Catalog
}

func NewWALReader(client *discord.Client, walChannel types.ChannelID, logger *slog.Logger) *WALReader {
	return &WALReader{
		client:     client,
		walChannel: walChannel,
		logger:     logger,
	}
}

func (wr *WALReader) SetSegmentManager(segMgr *SegmentManager) {
	wr.segMgr = segMgr
}

func (wr *WALReader) SetIndexManager(idxMgr *IndexManager) {
	wr.indexMgr = idxMgr
}

func (wr *WALReader) SetCatalog(cat *catalog.Catalog) {
	wr.cat = cat
}

func (wr *WALReader) Replay(ctx context.Context) (types.TxnID, error) {
	var walRecords []wal.Record
	var maxTxnID types.TxnID

	err := wr.client.ListAllMessages(ctx, wr.walChannel, func(messages []*discord.Message) error {
		for _, msg := range messages {
			if !strings.HasPrefix(msg.Content, "WAL:") {
				continue
			}

			parts := strings.SplitN(msg.Content, ":", 3)
			if len(parts) < 3 {
				continue
			}

			encoded, err := base64.StdEncoding.DecodeString(parts[2])
			if err != nil {
				wr.logger.Debug("failed to decode WAL base64",
					slog.String("message_id", msg.ID.String()),
				)
				continue
			}

			record, _, ok := wal.DecodeRecord(encoded)
			if !ok {
				wr.logger.Debug("failed to decode WAL record",
					slog.String("message_id", msg.ID.String()),
				)
				continue
			}

			if record.TxnID > maxTxnID {
				maxTxnID = record.TxnID
			}

			walRecords = append(walRecords, record)
		}
		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("list WAL messages: %w", err)
	}

	if len(walRecords) == 0 {
		return 0, nil
	}

	wr.logger.Info("replaying WAL", slog.Int("records", len(walRecords)))

	var pendingTxns = make(map[types.TxnID][]wal.Record)
	var committedTxns = make(map[types.TxnID]bool)

	for _, rec := range walRecords {
		switch rec.Kind {
		case "BEGIN":
			pendingTxns[rec.TxnID] = append(pendingTxns[rec.TxnID], rec)
		case "COMMIT":
			committedTxns[rec.TxnID] = true
			pendingTxns[rec.TxnID] = append(pendingTxns[rec.TxnID], rec)
		case "ABORT":
			delete(pendingTxns, rec.TxnID)
		default:
			pendingTxns[rec.TxnID] = append(pendingTxns[rec.TxnID], rec)
		}
	}

	for txnID, records := range pendingTxns {
		if !committedTxns[txnID] {
			wr.logger.Debug("skipping uncommitted transaction", slog.String("txn_id", txnID.String()))
			continue
		}

		for _, rec := range records {
			if err := wr.applyRecord(rec); err != nil {
				wr.logger.Warn("failed to apply WAL record",
					slog.String("kind", rec.Kind),
					slog.String("error", err.Error()),
				)
			}
		}
	}

	return maxTxnID, nil
}

func (wr *WALReader) applyRecord(record wal.Record) error {
	wr.logger.Debug("applying WAL record",
		slog.String("kind", record.Kind),
		slog.String("lsn", record.LSN.String()),
		slog.String("txn_id", record.TxnID.String()),
	)

	switch record.Kind {
	case "BEGIN", "COMMIT", "ABORT":
		return nil

	case "CATALOG_CREATE_TABLE":
		var payload struct {
			Name    string `json:"name"`
			TableID uint64 `json:"table_id"`
		}
		if err := json.Unmarshal(record.Data, &payload); err != nil {
			return fmt.Errorf("unmarshal catalog payload: %w", err)
		}
		wr.logger.Debug("replayed CATALOG_CREATE_TABLE", slog.String("name", payload.Name))
		return nil

	case "CATALOG_ADD_COLUMN":
		wr.logger.Debug("replayed CATALOG_ADD_COLUMN")
		return nil

	case "INSERT":
		if wr.segMgr == nil || wr.cat == nil {
			wr.logger.Debug("skipping INSERT - no segment manager or catalog")
			return nil
		}
		return wr.applyInsert(record)

	case "UPDATE":
		if wr.segMgr == nil || wr.cat == nil {
			wr.logger.Debug("skipping UPDATE - no segment manager or catalog")
			return nil
		}
		return wr.applyUpdate(record)

	case "DELETE":
		if wr.segMgr == nil || wr.cat == nil {
			wr.logger.Debug("skipping DELETE - no segment manager or catalog")
			return nil
		}
		return wr.applyDelete(record)

	case "INDEX_INSERT":
		if wr.indexMgr == nil {
			wr.logger.Debug("skipping INDEX_INSERT - no index manager")
			return nil
		}
		return wr.applyIndexInsert(record)

	case "INDEX_DELETE":
		if wr.indexMgr == nil {
			wr.logger.Debug("skipping INDEX_DELETE - no index manager")
			return nil
		}
		return wr.applyIndexDelete(record)

	default:
		wr.logger.Debug("unknown WAL record kind", slog.String("kind", record.Kind))
		return nil
	}
}

func (wr *WALReader) applyInsert(record wal.Record) error {
	row := storage.Row{
		Header: storage.RowHeader{
			RowID:     record.RowID,
			TableID:   record.TableID,
			SegmentID: record.SegmentID,
			MessageID: record.MessageID,
			TxnID:     record.TxnID,
			LSN:       record.LSN,
		},
		Body: storage.RowBody{
			Columns: []storage.ColumnValue{},
		},
	}

	segmentChannelID, err := wr.segMgr.GetOrCreateSegment(context.Background(), record.TableID, record.SegmentID)
	if err != nil {
		return fmt.Errorf("get segment for replay: %w", err)
	}

	schema, ok := wr.cat.GetTable(record.TableID)
	if !ok {
		wr.logger.Debug("table not found in catalog during replay, using default epoch",
			slog.String("table_id", record.TableID.String()))
	}

	epoch := schema.Epoch
	if epoch == 0 {
		epoch = types.MinSchemaEpoch()
	}

	msg, err := wr.segMgr.WriteRow(context.Background(), segmentChannelID, row, epoch)
	if err != nil {
		return fmt.Errorf("write row during replay: %w", err)
	}

	wr.logger.Debug("replayed INSERT",
		slog.String("row_id", record.RowID.String()),
		slog.String("segment_id", record.SegmentID.String()),
		slog.String("message_id", msg.ID.String()),
	)

	return nil
}

func (wr *WALReader) applyUpdate(record wal.Record) error {
	tombstoneRow := storage.Row{
		Header: storage.RowHeader{
			RowID:     types.RowID(record.OldMessageID.Uint64()),
			TableID:   record.TableID,
			SegmentID: record.OldSegmentID,
			MessageID: record.OldMessageID,
			TxnID:     record.TxnID,
			LSN:       record.LSN,
			Flags:     storage.FlagTombstone,
		},
	}

	oldSegmentChannelID, err := wr.segMgr.GetOrCreateSegment(context.Background(), record.TableID, record.OldSegmentID)
	if err != nil {
		return fmt.Errorf("get old segment for replay: %w", err)
	}

	schema, _ := wr.cat.GetTable(record.TableID)
	epoch := schema.Epoch
	if epoch == 0 {
		epoch = types.MinSchemaEpoch()
	}

	_, err = wr.segMgr.WriteRow(context.Background(), oldSegmentChannelID, tombstoneRow, epoch)
	if err != nil {
		wr.logger.Warn("write tombstone during replay (continuing)", slog.String("error", err.Error()))
	}

	newRow := storage.Row{
		Header: storage.RowHeader{
			RowID:     record.RowID,
			TableID:   record.TableID,
			SegmentID: record.SegmentID,
			MessageID: record.MessageID,
			TxnID:     record.TxnID,
			LSN:       record.LSN,
		},
		Body: storage.RowBody{
			Columns: []storage.ColumnValue{},
		},
	}

	newSegmentChannelID, err := wr.segMgr.GetOrCreateSegment(context.Background(), record.TableID, record.SegmentID)
	if err != nil {
		return fmt.Errorf("get new segment for replay: %w", err)
	}

	msg, err := wr.segMgr.WriteRow(context.Background(), newSegmentChannelID, newRow, epoch)
	if err != nil {
		return fmt.Errorf("write new row during replay: %w", err)
	}

	wr.logger.Debug("replayed UPDATE",
		slog.String("row_id", record.RowID.String()),
		slog.String("segment_id", record.SegmentID.String()),
		slog.String("message_id", msg.ID.String()),
	)

	return nil
}

func (wr *WALReader) applyDelete(record wal.Record) error {
	tombstoneRow := storage.Row{
		Header: storage.RowHeader{
			RowID:     record.RowID,
			TableID:   record.TableID,
			SegmentID: record.SegmentID,
			MessageID: record.MessageID,
			TxnID:     record.TxnID,
			LSN:       record.LSN,
			Flags:     storage.FlagTombstone,
		},
	}

	segmentChannelID, err := wr.segMgr.GetOrCreateSegment(context.Background(), record.TableID, record.SegmentID)
	if err != nil {
		return fmt.Errorf("get segment for replay: %w", err)
	}

	schema, _ := wr.cat.GetTable(record.TableID)
	epoch := schema.Epoch
	if epoch == 0 {
		epoch = types.MinSchemaEpoch()
	}

	_, err = wr.segMgr.WriteRow(context.Background(), segmentChannelID, tombstoneRow, epoch)
	if err != nil {
		wr.logger.Warn("write tombstone during replay (continuing)", slog.String("error", err.Error()))
	}

	wr.logger.Debug("replayed DELETE",
		slog.String("row_id", record.RowID.String()),
		slog.String("segment_id", record.SegmentID.String()),
	)

	return nil
}

func (wr *WALReader) applyIndexInsert(record wal.Record) error {
	if wr.indexMgr == nil {
		return nil
	}

	err := wr.indexMgr.Insert(context.Background(), record.IndexID, record.Key, record.RowID, record.SegmentID, record.MessageID)
	if err != nil {
		wr.logger.Warn("index insert during replay (continuing)", slog.String("error", err.Error()))
	}

	wr.logger.Debug("replayed INDEX_INSERT",
		slog.String("index_id", record.IndexID.String()),
		slog.String("key", string(record.Key)),
	)

	return nil
}

func (wr *WALReader) applyIndexDelete(record wal.Record) error {
	if wr.indexMgr == nil {
		return nil
	}

	err := wr.indexMgr.Delete(context.Background(), record.IndexID, record.Key)
	if err != nil {
		wr.logger.Warn("index delete during replay (continuing)", slog.String("error", err.Error()))
	}

	wr.logger.Debug("replayed INDEX_DELETE",
		slog.String("index_id", record.IndexID.String()),
		slog.String("key", string(record.Key)),
	)

	return nil
}

type ReplayCatalog interface {
	AddTable(schema catalog.TableSchema) types.TableID
}
