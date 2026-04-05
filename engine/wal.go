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
}

func NewWALReader(client *discord.Client, walChannel types.ChannelID, logger *slog.Logger) *WALReader {
	return &WALReader{
		client:     client,
		walChannel: walChannel,
		logger:     logger,
	}
}

func (wr *WALReader) Replay(ctx context.Context, cat ReplayCatalog) error {
	var walRecords []wal.Record

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

			walRecords = append(walRecords, record)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("list WAL messages: %w", err)
	}

	if len(walRecords) == 0 {
		return nil
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
			if err := wr.applyRecord(rec, cat); err != nil {
				wr.logger.Warn("failed to apply WAL record",
					slog.String("kind", rec.Kind),
					slog.String("error", err.Error()),
				)
			}
		}
	}

	return nil
}

func (wr *WALReader) applyRecord(record wal.Record, cat ReplayCatalog) error {
	switch record.Kind {
	case "CATALOG_CREATE_TABLE":
		var payload struct {
			Name    string `json:"name"`
			TableID uint64 `json:"table_id"`
		}
		if err := json.Unmarshal(record.Data, &payload); err != nil {
			return fmt.Errorf("unmarshal catalog payload: %w", err)
		}
		_ = payload
	case "CATALOG_ADD_COLUMN":
	}
	return nil
}

type ReplayCatalog interface {
	AddTable(schema catalog.TableSchema) types.TableID
}
