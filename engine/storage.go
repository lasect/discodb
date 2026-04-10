package engine

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"discodb/discord"
	"discodb/mapping"
	"discodb/storage"
	"discodb/types"

	"github.com/bwmarrin/discordgo"
)

type SegmentManager struct {
	heapClient    *discord.Client
	catalogClient *discord.Client
	guildID       types.GuildID
	catalogCatID  types.ChannelID
	heapPrefix    string
	logger        *slog.Logger
	webhooks      map[types.ChannelID]*discord.WebhookClient
	webhooksMu    sync.RWMutex
}

func NewSegmentManager(heapClient, catalogClient *discord.Client, guildID types.GuildID, catalogCatID types.ChannelID, heapPrefix string, logger *slog.Logger) *SegmentManager {
	return &SegmentManager{
		heapClient:    heapClient,
		catalogClient: catalogClient,
		guildID:       guildID,
		catalogCatID:  catalogCatID,
		heapPrefix:    heapPrefix,
		logger:        logger,
		webhooks:      make(map[types.ChannelID]*discord.WebhookClient),
	}
}

func (sm *SegmentManager) CreateSegment(ctx context.Context, tableID types.TableID, segmentID types.SegmentID) (types.ChannelID, error) {
	name := mapping.SegmentName(tableID, segmentID)

	ch, err := sm.heapClient.CreateTextChannel(ctx, sm.guildID, name, &sm.catalogCatID)
	if err != nil {
		return 0, fmt.Errorf("create text channel for segment: %w", err)
	}

	webhook, err := sm.createWebhook(ctx, ch.ID)
	if err != nil {
		sm.logger.Warn("failed to create webhook for segment (non-fatal)", slog.String("error", err.Error()))
	}

	header := mapping.PageHeader{
		SegmentID: segmentID,
		TableID:   tableID,
		RowCount:  0,
		FreeSlots: 0,
		LSN:       types.LSN(0),
	}
	if webhook != nil {
		header.WebhookID = parseSnowflakeSafe(webhook.ID)
		copy(header.WebhookToken[:], webhook.Token)
	}

	topic := header.EncodeToTopic()
	if _, err := sm.heapClient.EditChannel(ctx, ch.ID, discord.ChannelEditParams{
		Topic: &topic,
	}); err != nil {
		sm.logger.Warn("failed to set segment topic (non-fatal)", slog.String("error", err.Error()))
	}

	if webhook != nil {
		whClient := discord.NewWebhookClient(
			webhook.ID,
			webhook.Token,
			ch.ID,
		)
		sm.webhooksMu.Lock()
		sm.webhooks[ch.ID] = whClient
		sm.webhooksMu.Unlock()
	}

	sm.logger.Info("created segment",
		slog.String("table_id", tableID.String()),
		slog.String("segment_id", segmentID.String()),
		slog.String("channel_id", ch.ID.String()),
	)

	return ch.ID, nil
}

func (sm *SegmentManager) GetOrCreateSegment(ctx context.Context, tableID types.TableID, segmentID types.SegmentID) (types.ChannelID, error) {
	name := mapping.SegmentName(tableID, segmentID)

	channels, err := sm.catalogClient.ListGuildChannels(ctx, sm.guildID)
	if err != nil {
		return 0, fmt.Errorf("list channels: %w", err)
	}

	for _, ch := range channels {
		if ch.Name == name && ch.ParentID != nil && *ch.ParentID == sm.catalogCatID {
			return ch.ID, nil
		}
	}

	return sm.CreateSegment(ctx, tableID, segmentID)
}

func (sm *SegmentManager) GetOrCreateSegmentForReplay(ctx context.Context, tableID types.TableID, segmentID types.SegmentID) (types.ChannelID, error) {
	name := mapping.SegmentName(tableID, segmentID)

	channels, err := sm.catalogClient.ListGuildChannels(ctx, sm.guildID)
	if err != nil {
		return 0, fmt.Errorf("list channels: %w", err)
	}

	for _, ch := range channels {
		if ch.Name == name && ch.ParentID != nil && *ch.ParentID == sm.catalogCatID {
			return ch.ID, nil
		}
	}

	return 0, fmt.Errorf("segment not found during replay (may need manual recovery): seg-%s-%s", tableID.String(), segmentID.String())
}

func (sm *SegmentManager) WriteRow(ctx context.Context, segmentChannelID types.ChannelID, row storage.Row, schemaEpoch types.SchemaEpoch) (*discord.Message, error) {
	encoded, err := storage.EncodeRowToDiscord(row, schemaEpoch)
	if err != nil {
		return nil, fmt.Errorf("encode row: %w", err)
	}

	params := discord.MessageSendParams{
		Content: encoded.Message.Content,
		AllowedMentions: &discord.AllowedMentions{
			Parse: []string{},
		},
	}

	for _, emb := range encoded.Message.Embeds {
		discordEmb := discord.Embed{
			Title:       emb.Title,
			Description: emb.Description,
		}
		for _, f := range emb.Fields {
			discordEmb.Fields = append(discordEmb.Fields, discord.EmbedField{
				Name:   f.Name,
				Value:  f.Value,
				Inline: f.Inline,
			})
		}
		params.Embeds = append(params.Embeds, discordEmb)
	}

	msg, err := sm.heapClient.SendMessage(ctx, segmentChannelID, params)
	if err != nil {
		return nil, fmt.Errorf("send row message: %w", err)
	}

	return msg, nil
}

func (sm *SegmentManager) ReadRows(ctx context.Context, segmentChannelID types.ChannelID) ([]storage.Row, []types.SchemaEpoch, error) {
	type decodedRow struct {
		row   storage.Row
		epoch types.SchemaEpoch
	}

	var allDecoded []decodedRow
	var tombstones []struct {
		rowID     types.RowID
		segmentID types.SegmentID
		messageID types.MessageID
	}

	err := sm.heapClient.ListAllMessages(ctx, segmentChannelID, func(messages []*discord.Message) error {
		sm.logger.Info("ReadRows batch",
			slog.Int("message_count", len(messages)),
		)
		for _, msg := range messages {
			sm.logger.Debug("processing message",
				slog.String("message_id", msg.ID.String()),
				slog.String("content_preview", msg.Content[:min(50, len(msg.Content))]),
				slog.Int("embed_count", len(msg.Embeds)),
			)

			var embeds []storage.DiscordEmbed
			for _, emb := range msg.Embeds {
				sm.logger.Debug("embed description",
					slog.String("preview", emb.Description[:min(100, len(emb.Description))]),
				)
				var fields []storage.DiscordField
				for _, f := range emb.Fields {
					fields = append(fields, storage.DiscordField{
						Name:   f.Name,
						Value:  f.Value,
						Inline: f.Inline,
					})
				}
				embeds = append(embeds, storage.DiscordEmbed{
					Title:       emb.Title,
					Description: emb.Description,
					Fields:      fields,
				})
			}

			var attachments []storage.DiscordAttachment
			for _, att := range msg.Attachments {
				attachments = append(attachments, storage.DiscordAttachment{
					Filename: att.Filename,
					Size:     att.Size,
				})
			}

			row, epoch, err := storage.DecodeRowFromDiscord(msg.Content, embeds, attachments)
			if err != nil {
				sm.logger.Warn("failed to decode row message (skipping)",
					slog.String("message_id", msg.ID.String()),
					slog.String("content", msg.Content),
					slog.Int("embed_count", len(msg.Embeds)),
					slog.String("error", err.Error()),
				)
				continue
			}

			if row.Header.Flags.HasTombstone() {
				sm.logger.Debug("skipping tombstone",
					slog.String("message_id", msg.ID.String()),
					slog.String("txn_id", row.Header.TxnID.String()),
					slog.String("row_id", row.Header.RowID.String()),
				)
				tombstones = append(tombstones, struct {
					rowID     types.RowID
					segmentID types.SegmentID
					messageID types.MessageID
				}{
					rowID:     row.Header.RowID,
					segmentID: row.Header.SegmentID,
					messageID: row.Header.MessageID,
				})
				continue
			}

			sm.logger.Debug("decoded row",
				slog.String("message_id", msg.ID.String()),
				slog.String("txn_id", row.Header.TxnID.String()),
				slog.String("txn_max", fmt.Sprintf("%d", row.Header.TxnMax)),
			)

			allDecoded = append(allDecoded, decodedRow{row: row, epoch: epoch})
		}
		return nil
	})

	if err != nil {
		return nil, nil, fmt.Errorf("list messages: %w", err)
	}

	var allRows []storage.Row
	var allEpochs []types.SchemaEpoch
	for _, dr := range allDecoded {
		tombstoned := false
		for _, ts := range tombstones {
			if dr.row.Header.RowID == ts.rowID && dr.row.Header.SegmentID == ts.segmentID && dr.row.Header.MessageID == ts.messageID {
				tombstoned = true
				sm.logger.Debug("row tombstoned, skipping",
					slog.String("row_id", dr.row.Header.RowID.String()),
					slog.String("segment_id", dr.row.Header.SegmentID.String()),
					slog.String("message_id", dr.row.Header.MessageID.String()),
				)
				break
			}
		}
		if !tombstoned {
			allRows = append(allRows, dr.row)
			allEpochs = append(allEpochs, dr.epoch)
		}
	}

	return allRows, allEpochs, nil
}

func (sm *SegmentManager) FindSegmentByName(ctx context.Context, name string) (*discord.Channel, error) {
	channels, err := sm.catalogClient.ListGuildChannels(ctx, sm.guildID)
	if err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}

	for _, ch := range channels {
		if ch.Name == name && ch.ParentID != nil && *ch.ParentID == sm.catalogCatID {
			return ch, nil
		}
	}

	return nil, nil
}

func (sm *SegmentManager) ListSegments(ctx context.Context, tableID types.TableID) ([]*discord.Channel, error) {
	channels, err := sm.catalogClient.ListGuildChannels(ctx, sm.guildID)
	if err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}

	var segments []*discord.Channel
	prefix := fmt.Sprintf("seg-%d-", tableID.Uint64())
	for _, ch := range channels {
		if ch.ParentID != nil && *ch.ParentID == sm.catalogCatID && strings.HasPrefix(ch.Name, prefix) {
			parts := strings.SplitN(ch.Name, "-", 3)
			if len(parts) == 3 {
				segID, err := strconv.ParseUint(parts[2], 10, 64)
				if err == nil {
					_ = segID
					segments = append(segments, ch)
				}
			}
		}
	}

	return segments, nil
}

func (sm *SegmentManager) createWebhook(ctx context.Context, channelID types.ChannelID) (*discordgo.Webhook, error) {
	session := sm.heapClient.Session()
	if session == nil {
		return nil, fmt.Errorf("no session available")
	}
	return session.WebhookCreate(
		channelID.String(),
		fmt.Sprintf("discodb-heap-%d", channelID.Uint64()),
		"",
		discordgo.WithContext(ctx),
	)
}

func (sm *SegmentManager) getWebhook(channelID types.ChannelID) *discord.WebhookClient {
	sm.webhooksMu.RLock()
	defer sm.webhooksMu.RUnlock()
	return sm.webhooks[channelID]
}

func (sm *SegmentManager) PopulateWebhookCache(ctx context.Context) error {
	channels, err := sm.catalogClient.ListGuildChannels(ctx, sm.guildID)
	if err != nil {
		return fmt.Errorf("list channels for webhook cache: %w", err)
	}

	prefix := sm.heapPrefix
	sm.webhooksMu.Lock()
	defer sm.webhooksMu.Unlock()

	for _, ch := range channels {
		if ch.ParentID != nil && *ch.ParentID == sm.catalogCatID && strings.HasPrefix(ch.Name, prefix) {
			if ch.Topic == "" {
				continue
			}
			header, err := mapping.ParsePageHeader(ch.Topic)
			if err != nil {
				sm.logger.Debug("failed to parse page header for webhook cache",
					slog.String("channel", ch.Name),
					slog.String("error", err.Error()),
				)
				continue
			}
			if header.WebhookID == 0 || header.WebhookToken == [34]byte{} {
				continue
			}
			token := string(header.WebhookToken[:])
			whClient := discord.NewWebhookClient(
				fmt.Sprintf("%d", header.WebhookID),
				token,
				ch.ID,
			)
			sm.webhooks[ch.ID] = whClient
		}
	}

	sm.logger.Info("populated webhook cache", slog.Int("webhooks", len(sm.webhooks)))
	return nil
}

func parseSnowflakeSafe(s string) uint64 {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}
