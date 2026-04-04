package discord

import (
	"context"
	"log/slog"
	"testing"

	"discodb/types"
)

func newFakeClient(t *testing.T) *Client {
	t.Helper()
	client, err := NewClientWithTransport(NewFakeTransport(), WithLogger(slog.Default()))
	if err != nil {
		t.Fatalf("new fake client: %v", err)
	}
	return client
}

func TestFakeTransportChannelsAndParents(t *testing.T) {
	client := newFakeClient(t)
	ctx := context.Background()
	guildID := types.MustGuildID(1)

	category, err := client.CreateCategory(ctx, guildID, "cat")
	if err != nil {
		t.Fatalf("CreateCategory() error = %v", err)
	}

	child, err := client.CreateTextChannel(ctx, guildID, "seg-1-1", &category.ID)
	if err != nil {
		t.Fatalf("CreateTextChannel() error = %v", err)
	}

	channels, err := client.ListGuildChannels(ctx, guildID)
	if err != nil {
		t.Fatalf("ListGuildChannels() error = %v", err)
	}
	if len(channels) != 2 {
		t.Fatalf("ListGuildChannels() len = %d, want 2", len(channels))
	}
	if child.ParentID == nil || *child.ParentID != category.ID {
		t.Fatalf("child.ParentID = %v, want %v", child.ParentID, category.ID)
	}
}

func TestFakeTransportMessagePagination(t *testing.T) {
	client := newFakeClient(t)
	ctx := context.Background()
	guildID := types.MustGuildID(2)

	ch, err := client.CreateTextChannel(ctx, guildID, "wal", nil)
	if err != nil {
		t.Fatalf("CreateTextChannel() error = %v", err)
	}

	for i := 0; i < 205; i++ {
		if _, err := client.SendMessageContent(ctx, ch.ID, "msg"); err != nil {
			t.Fatalf("SendMessageContent() #%d error = %v", i, err)
		}
	}

	var total int
	err = client.ListAllMessages(ctx, ch.ID, func(messages []*Message) error {
		total += len(messages)
		return nil
	})
	if err != nil {
		t.Fatalf("ListAllMessages() error = %v", err)
	}
	if total != 205 {
		t.Fatalf("ListAllMessages() total = %d, want 205", total)
	}
}

func TestFakeTransportPinRoundTrip(t *testing.T) {
	client := newFakeClient(t)
	ctx := context.Background()
	guildID := types.MustGuildID(3)

	ch, err := client.CreateTextChannel(ctx, guildID, "boot", nil)
	if err != nil {
		t.Fatalf("CreateTextChannel() error = %v", err)
	}

	msg, err := client.SendMessageContent(ctx, ch.ID, "boot record")
	if err != nil {
		t.Fatalf("SendMessageContent() error = %v", err)
	}

	if err := client.PinMessage(ctx, ch.ID, msg.ID); err != nil {
		t.Fatalf("PinMessage() error = %v", err)
	}

	pinned, err := client.ListPinnedMessages(ctx, ch.ID)
	if err != nil {
		t.Fatalf("ListPinnedMessages() error = %v", err)
	}
	if len(pinned) != 1 || pinned[0].ID != msg.ID {
		t.Fatalf("ListPinnedMessages() = %+v, want only %v", pinned, msg.ID)
	}
}
