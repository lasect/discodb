package index

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"discodb/discord"
	"discodb/types"
)

type BTreeIndex struct {
	mu             sync.RWMutex
	ID             types.TableID
	Name           string
	TableID        types.TableID
	ColumnNames    []string
	ForumChannelID types.ChannelID
	MetaChannelID  types.ChannelID
	Unique         bool

	client *discord.Client
	guild  types.GuildID
	logger *slog.Logger

	entries map[string]*IndexEntry
	loaded  bool
}

func NewBTreeIndex(client *discord.Client, guild types.GuildID, id types.TableID, name string, tableID types.TableID, columns []string, unique bool, logger *slog.Logger) *BTreeIndex {
	return &BTreeIndex{
		ID:          id,
		Name:        name,
		TableID:     tableID,
		ColumnNames: columns,
		Unique:      unique,
		client:      client,
		guild:       guild,
		logger:      logger,
		entries:     make(map[string]*IndexEntry),
	}
}

func (b *BTreeIndex) CreateForumChannel(ctx context.Context) error {
	ch, err := b.client.CreateForumChannel(ctx, b.guild, b.Name, nil)
	if err != nil {
		return fmt.Errorf("create index forum channel: %w", err)
	}
	b.ForumChannelID = ch.ID

	metaCh, err := b.client.CreateTextChannel(ctx, b.guild, GenerateMetaChannelName(b.Name), nil)
	if err != nil {
		b.logger.Warn("failed to create index meta channel (non-fatal)", slog.String("error", err.Error()))
	} else {
		b.MetaChannelID = metaCh.ID
	}

	b.logger.Info("index forum channel created",
		slog.String("index", b.Name),
		slog.String("forum_id", b.ForumChannelID.String()),
	)
	return nil
}

func (b *BTreeIndex) Load(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	posts, err := b.client.ListForumPosts(ctx, b.ForumChannelID)
	if err != nil {
		return fmt.Errorf("list forum posts: %w", err)
	}

	for _, post := range posts {
		msgs, err := b.client.ListMessages(ctx, post.ID, discord.MessagesListParams{Limit: 1})
		if err != nil {
			b.logger.Warn("failed to read index post",
				slog.String("post_id", post.ID.String()),
				slog.String("error", err.Error()),
			)
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		entry, err := DecodeEntry(msgs[0].Content)
		if err != nil {
			b.logger.Warn("failed to decode index entry",
				slog.String("post_id", post.ID.String()),
				slog.String("error", err.Error()),
			)
			continue
		}
		entry.PostID = post.ID
		b.entries[string(entry.Key)] = entry
	}

	b.loaded = true
	b.logger.Info("index loaded from forum",
		slog.String("index", b.Name),
		slog.Int("entries", len(b.entries)),
	)
	return nil
}

func (b *BTreeIndex) Insert(ctx context.Context, key []byte, rowID types.RowID, segmentID types.SegmentID, messageID types.MessageID) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	keyStr := string(key)
	if existing, ok := b.entries[keyStr]; ok && !existing.Deleted && b.Unique {
		return fmt.Errorf("unique constraint violation on index %q for key %q", b.Name, key)
	}

	if existing, ok := b.entries[keyStr]; ok && existing.Deleted {
		content := EncodeEntry(key, rowID, segmentID, messageID, false)
		_, err := b.client.EditMessage(ctx, existing.PostID, types.MessageID(existing.PostID.Uint64()), discord.MessageEditParams{
			Content: &content,
		})
		if err != nil {
			return fmt.Errorf("edit index post: %w", err)
		}
		existing.RowID = rowID
		existing.SegmentID = segmentID
		existing.MessageID = messageID
		existing.Deleted = false
		return nil
	}

	content := EncodeEntry(key, rowID, segmentID, messageID, false)
	postTitle := GeneratePostTitle(key)

	_, _, err := b.client.CreateForumPost(ctx, b.ForumChannelID, discord.ForumPostCreateParams{
		Name:    postTitle,
		Content: content,
	})
	if err != nil {
		return fmt.Errorf("create forum post: %w", err)
	}

	b.entries[keyStr] = &IndexEntry{
		RowID:     rowID,
		SegmentID: segmentID,
		MessageID: messageID,
		Key:       key,
		Deleted:   false,
	}

	return nil
}

func (b *BTreeIndex) Delete(ctx context.Context, key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	keyStr := string(key)
	existing, ok := b.entries[keyStr]
	if !ok || existing.Deleted {
		return nil
	}

	content := EncodeEntry(key, existing.RowID, existing.SegmentID, existing.MessageID, true)
	_, err := b.client.EditMessage(ctx, existing.PostID, types.MessageID(existing.PostID.Uint64()), discord.MessageEditParams{
		Content: &content,
	})
	if err != nil {
		return fmt.Errorf("edit index post for soft delete: %w", err)
	}

	existing.Deleted = true
	return nil
}

func (b *BTreeIndex) Lookup(ctx context.Context, key []byte) ([]IndexEntry, error) {
	b.mu.RLock()
	if b.loaded {
		if entry, ok := b.entries[string(key)]; ok && !entry.Deleted {
			result := *entry
			b.mu.RUnlock()
			return []IndexEntry{result}, nil
		}
		b.mu.RUnlock()
		return nil, nil
	}
	b.mu.RUnlock()

	posts, err := b.client.ListForumPosts(ctx, b.ForumChannelID)
	if err != nil {
		return nil, fmt.Errorf("list forum posts: %w", err)
	}

	keyStr := string(key)
	var results []IndexEntry
	for _, post := range posts {
		if post.Name != GeneratePostTitle(key) {
			continue
		}

		msgs, err := b.client.ListMessages(ctx, post.ID, discord.MessagesListParams{Limit: 1})
		if err != nil {
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		entry, err := DecodeEntry(msgs[0].Content)
		if err != nil {
			continue
		}

		if !entry.Deleted && string(entry.Key) == keyStr {
			entry.PostID = post.ID
			results = append(results, *entry)
		}
	}

	return results, nil
}

func (b *BTreeIndex) Range(ctx context.Context, startKey []byte, endKey []byte) ([]IndexEntry, error) {
	b.mu.RLock()
	if b.loaded {
		var results []IndexEntry
		for _, entry := range b.entries {
			if entry.Deleted {
				continue
			}
			if compareBytes(entry.Key, startKey) >= 0 && compareBytes(entry.Key, endKey) < 0 {
				results = append(results, *entry)
			}
		}
		b.mu.RUnlock()
		return results, nil
	}
	b.mu.RUnlock()

	posts, err := b.client.ListForumPosts(ctx, b.ForumChannelID)
	if err != nil {
		return nil, fmt.Errorf("list forum posts: %w", err)
	}

	var results []IndexEntry
	for _, post := range posts {
		msgs, err := b.client.ListMessages(ctx, post.ID, discord.MessagesListParams{Limit: 1})
		if err != nil {
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		entry, err := DecodeEntry(msgs[0].Content)
		if err != nil {
			continue
		}

		if entry.Deleted {
			continue
		}

		if compareBytes(entry.Key, startKey) >= 0 && compareBytes(entry.Key, endKey) < 0 {
			entry.PostID = post.ID
			results = append(results, *entry)
		}
	}

	return results, nil
}

func (b *BTreeIndex) Rebuild(ctx context.Context, rows []RebuildRow) error {
	b.mu.Lock()
	b.entries = make(map[string]*IndexEntry)
	b.mu.Unlock()

	for _, row := range rows {
		if err := b.Insert(ctx, row.Key, row.RowID, row.SegmentID, row.MessageID); err != nil {
			return fmt.Errorf("rebuild index: %w", err)
		}
	}

	return nil
}

func (b *BTreeIndex) Drop(ctx context.Context) error {
	posts, err := b.client.ListForumPosts(ctx, b.ForumChannelID)
	if err != nil {
		b.logger.Warn("failed to list posts during index drop", slog.String("error", err.Error()))
	}

	for _, post := range posts {
		if err := b.client.DeleteForumPost(ctx, post.ID); err != nil {
			b.logger.Warn("failed to delete forum post during index drop",
				slog.String("post_id", post.ID.String()),
				slog.String("error", err.Error()),
			)
		}
	}

	if b.ForumChannelID != 0 {
		if err := b.client.DeleteChannel(ctx, b.ForumChannelID); err != nil {
			b.logger.Warn("failed to delete index forum channel",
				slog.String("channel_id", b.ForumChannelID.String()),
				slog.String("error", err.Error()),
			)
		}
	}

	if b.MetaChannelID != 0 {
		if err := b.client.DeleteChannel(ctx, b.MetaChannelID); err != nil {
			b.logger.Warn("failed to delete index meta channel",
				slog.String("channel_id", b.MetaChannelID.String()),
				slog.String("error", err.Error()),
			)
		}
	}

	return nil
}

func (b *BTreeIndex) EntryCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	count := 0
	for _, e := range b.entries {
		if !e.Deleted {
			count++
		}
	}
	return count
}

type RebuildRow struct {
	Key       []byte
	RowID     types.RowID
	SegmentID types.SegmentID
	MessageID types.MessageID
}

func compareBytes(a, b []byte) int {
	return strings.Compare(string(a), string(b))
}
