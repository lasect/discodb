package observability

import (
	"io"
	"log/slog"
	"os"

	"discodb/config"
)

type Span struct {
	Name   string
	Fields map[string]any
}

func NewLogger(cfg config.LoggingConfig) *slog.Logger {
	writer := io.Writer(os.Stdout)
	if cfg.Output.Mode == "file" || cfg.Output.Mode == "both" {
		file, err := os.OpenFile(cfg.Output.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err == nil {
			writer = file
		}
	}
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: parseLevel(cfg.Level)}
	if cfg.Format == config.LogFormatJSON {
		handler = slog.NewJSONHandler(writer, opts)
	} else {
		handler = slog.NewTextHandler(writer, opts)
	}
	return slog.New(handler)
}

func parseLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func SpanDiscordRequest(endpoint, method string) Span {
	return Span{Name: "discord.request", Fields: map[string]any{"endpoint": endpoint, "method": method}}
}

func SpanWALAppend(lsn, txnID uint64) Span {
	return Span{Name: "wal.append", Fields: map[string]any{"lsn": lsn, "txn_id": txnID}}
}

func SpanTxnBegin(txnID uint64) Span {
	return Span{Name: "txn.begin", Fields: map[string]any{"txn_id": txnID}}
}

func SpanTxnCommit(txnID uint64) Span {
	return Span{Name: "txn.commit", Fields: map[string]any{"txn_id": txnID}}
}

func SpanStorageRead(tableID, rowID uint64) Span {
	return Span{Name: "storage.read", Fields: map[string]any{"table_id": tableID, "row_id": rowID}}
}

func SpanStorageWrite(tableID, rowID uint64) Span {
	return Span{Name: "storage.write", Fields: map[string]any{"table_id": tableID, "row_id": rowID}}
}

func SpanExecutorScan(tableID uint64) Span {
	return Span{Name: "executor.scan", Fields: map[string]any{"table_id": tableID}}
}

func SpanRecovery(replayLSN uint64) Span {
	return Span{Name: "recovery.replay", Fields: map[string]any{"replay_lsn": replayLSN}}
}
