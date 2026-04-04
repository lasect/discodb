package engine

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"discodb/catalog"
	"discodb/discord"
	"discodb/types"
	"discodb/wal"
)

func TestWALReplayWithFakeTransportPagination(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	client, err := discord.NewClientWithTransport(discord.NewFakeTransport(), discord.WithLogger(logger))
	if err != nil {
		t.Fatalf("NewClientWithTransport() error = %v", err)
	}

	ctx := context.Background()
	guildID := types.MustGuildID(7)
	walChannel, err := client.CreateTextChannel(ctx, guildID, "wal", nil)
	if err != nil {
		t.Fatalf("CreateTextChannel() error = %v", err)
	}

	writer := NewWALWriter(client, walChannel.ID, logger)
	// Write >100 messages to force ListAllMessages pagination during replay.
	for i := 1; i <= 130; i++ {
		txn := types.TxnID(i)
		if err := writer.Append(ctx, wal.Begin(txn, types.LSN(i*2))); err != nil {
			t.Fatalf("append BEGIN #%d: %v", i, err)
		}
		if err := writer.Append(ctx, wal.Commit(txn, types.LSN(i*2+1))); err != nil {
			t.Fatalf("append COMMIT #%d: %v", i, err)
		}
	}

	reader := NewWALReader(client, walChannel.ID, logger)
	cat := catalog.New()
	if err := reader.Replay(ctx, cat); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
}
