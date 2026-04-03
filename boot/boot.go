package boot

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"discodb/config"
	"discodb/discord"
	"discodb/mapping"
	"discodb/types"
)

type BootInfo struct {
	GuildID         types.GuildID
	CatalogCategory types.ChannelID
	WALChannel      types.ChannelID
}

type Bootstrapper struct {
	catalogClient  *discord.Client
	heapClient     *discord.Client
	walClient      *discord.Client
	indexClient    *discord.Client
	overflowClient *discord.Client
	cfg            config.Config
	logger         *slog.Logger
}

func NewBootstrapper(cfg config.Config, logger *slog.Logger) (*Bootstrapper, error) {
	catalogClient, err := discord.NewClient(cfg.Discord.Tokens.Catalog,
		discord.WithLogger(logger),
		discord.WithTimeout(timeout(cfg)),
		discord.WithMaxRetries(int(cfg.Discord.MaxRetries)),
	)
	if err != nil {
		return nil, fmt.Errorf("create catalog client: %w", err)
	}

	heapClient, err := discord.NewClient(cfg.Discord.Tokens.Heap,
		discord.WithLogger(logger),
		discord.WithTimeout(timeout(cfg)),
		discord.WithMaxRetries(int(cfg.Discord.MaxRetries)),
	)
	if err != nil {
		return nil, fmt.Errorf("create heap client: %w", err)
	}

	walClient, err := discord.NewClient(cfg.Discord.Tokens.WAL,
		discord.WithLogger(logger),
		discord.WithTimeout(timeout(cfg)),
		discord.WithMaxRetries(int(cfg.Discord.MaxRetries)),
	)
	if err != nil {
		return nil, fmt.Errorf("create wal client: %w", err)
	}

	indexClient, err := discord.NewClient(cfg.Discord.Tokens.Index,
		discord.WithLogger(logger),
		discord.WithTimeout(timeout(cfg)),
		discord.WithMaxRetries(int(cfg.Discord.MaxRetries)),
	)
	if err != nil {
		return nil, fmt.Errorf("create index client: %w", err)
	}

	overflowClient, err := discord.NewClient(cfg.Discord.Tokens.Overflow,
		discord.WithLogger(logger),
		discord.WithTimeout(timeout(cfg)),
		discord.WithMaxRetries(int(cfg.Discord.MaxRetries)),
	)
	if err != nil {
		return nil, fmt.Errorf("create overflow client: %w", err)
	}

	return &Bootstrapper{
		catalogClient:  catalogClient,
		heapClient:     heapClient,
		walClient:      walClient,
		indexClient:    indexClient,
		overflowClient: overflowClient,
		cfg:            cfg,
		logger:         logger,
	}, nil
}

func timeout(cfg config.Config) time.Duration {
	if cfg.Discord.RequestTimeoutSecs == 0 {
		return 30 * time.Second
	}
	return time.Duration(cfg.Discord.RequestTimeoutSecs) * time.Second
}

func (b *Bootstrapper) Close() error {
	var firstErr error
	for _, c := range []*discord.Client{b.catalogClient, b.heapClient, b.walClient, b.indexClient, b.overflowClient} {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (b *Bootstrapper) Bootstrap(ctx context.Context) (*BootInfo, error) {
	guildID := b.cfg.Discord.GuildIDs[0]

	bootInfo, err := b.Discover(ctx, guildID)
	if err == nil {
		b.logger.Info("discovered existing boot record",
			slog.String("guild_id", guildID.String()),
			slog.String("catalog_category", bootInfo.CatalogCategory.String()),
			slog.String("wal_channel", bootInfo.WALChannel.String()),
		)
		return bootInfo, nil
	}

	b.logger.Info("no boot record found, creating infrastructure",
		slog.String("guild_id", guildID.String()),
	)

	catalogCategory, err := b.catalogClient.CreateCategory(ctx, guildID, b.cfg.Storage.CatalogCategoryPrefix)
	if err != nil {
		return nil, fmt.Errorf("create catalog category: %w", err)
	}

	walChannel, err := b.catalogClient.CreateTextChannel(ctx, guildID, b.cfg.Storage.WALChannelName, &catalogCategory.ID)
	if err != nil {
		return nil, fmt.Errorf("create WAL channel: %w", err)
	}

	bootRecord := mapping.BootRecord{
		Version:         1,
		CatalogCategory: catalogCategory.ID,
		WALChannel:      walChannel.ID,
		CurrentEpoch:    types.MinSchemaEpoch(),
		Checksum:        0,
	}

	bootContent, err := bootRecord.EncodeBootRecord()
	if err != nil {
		return nil, fmt.Errorf("encode boot record: %w", err)
	}

	bootMsg, err := b.catalogClient.SendMessage(ctx, walChannel.ID, discord.MessageSendParams{
		Content: bootContent,
		AllowedMentions: &discord.AllowedMentions{
			Parse: []string{},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("send boot message: %w", err)
	}

	if err := b.catalogClient.PinMessage(ctx, walChannel.ID, bootMsg.ID); err != nil {
		b.logger.Warn("failed to pin boot message (non-fatal)", slog.String("error", err.Error()))
	}

	bootInfo = &BootInfo{
		GuildID:         guildID,
		CatalogCategory: catalogCategory.ID,
		WALChannel:      walChannel.ID,
	}

	b.logger.Info("bootstrap complete",
		slog.String("guild_id", guildID.String()),
		slog.String("catalog_category", catalogCategory.ID.String()),
		slog.String("wal_channel", walChannel.ID.String()),
	)

	return bootInfo, nil
}

func (b *Bootstrapper) Discover(ctx context.Context, guildID types.GuildID) (*BootInfo, error) {
	channels, err := b.catalogClient.ListGuildChannels(ctx, guildID)
	if err != nil {
		return nil, fmt.Errorf("list guild channels: %w", err)
	}

	var bootChannel *discord.Channel
	for _, ch := range channels {
		if ch.Name == strings.TrimPrefix(b.cfg.Discord.BootChannelName, "#") || ch.Name == b.cfg.Discord.BootChannelName {
			bootChannel = ch
			break
		}
	}

	if bootChannel == nil {
		for _, ch := range channels {
			if strings.Contains(ch.Name, "discodb-boot") || strings.Contains(ch.Name, "discorddb-boot") {
				bootChannel = ch
				break
			}
		}
	}

	if bootChannel != nil {
		pinned, err := b.catalogClient.ListPinnedMessages(ctx, bootChannel.ID)
		if err == nil && len(pinned) > 0 {
			content := pinned[0].Content
			if content != "" {
				record, err := mapping.ParseBootRecord(content)
				if err == nil && record.CatalogCategory.Uint64() != 0 && record.WALChannel.Uint64() != 0 {
					return &BootInfo{
						GuildID:         guildID,
						CatalogCategory: record.CatalogCategory,
						WALChannel:      record.WALChannel,
					}, nil
				}
			}
		}

		messages, err := b.catalogClient.ListMessages(ctx, bootChannel.ID, discord.MessagesListParams{Limit: 10})
		if err == nil {
			for _, msg := range messages {
				if strings.HasPrefix(msg.Content, "{\"version\":") {
					record, err := mapping.ParseBootRecord(msg.Content)
					if err == nil && record.CatalogCategory.Uint64() != 0 && record.WALChannel.Uint64() != 0 {
						return &BootInfo{
							GuildID:         guildID,
							CatalogCategory: record.CatalogCategory,
							WALChannel:      record.WALChannel,
						}, nil
					}
				}
			}
		}
	}

	categories, err := b.catalogClient.ListCategories(ctx, guildID)
	if err != nil {
		return nil, fmt.Errorf("list categories: %w", err)
	}

	var catalogCategory *discord.Channel
	for _, cat := range categories {
		if strings.HasPrefix(cat.Name, b.cfg.Storage.CatalogCategoryPrefix) {
			catalogCategory = cat
			break
		}
	}

	if catalogCategory == nil {
		return nil, fmt.Errorf("no boot record or catalog found")
	}

	children, err := b.catalogClient.ListGuildChannels(ctx, guildID)
	if err != nil {
		return nil, fmt.Errorf("list channels for WAL discovery: %w", err)
	}

	var walChannel *discord.Channel
	for _, ch := range children {
		if ch.ParentID != nil && *ch.ParentID == catalogCategory.ID && ch.Name == b.cfg.Storage.WALChannelName {
			walChannel = ch
			break
		}
	}

	if walChannel == nil {
		return nil, fmt.Errorf("catalog category found but WAL channel missing")
	}

	return &BootInfo{
		GuildID:         guildID,
		CatalogCategory: catalogCategory.ID,
		WALChannel:      walChannel.ID,
	}, nil
}

func (b *Bootstrapper) CatalogClient() *discord.Client {
	return b.catalogClient
}

func (b *Bootstrapper) HeapClient() *discord.Client {
	return b.heapClient
}

func (b *Bootstrapper) WALClient() *discord.Client {
	return b.walClient
}

func (b *Bootstrapper) IndexClient() *discord.Client {
	return b.indexClient
}

func (b *Bootstrapper) OverflowClient() *discord.Client {
	return b.overflowClient
}
