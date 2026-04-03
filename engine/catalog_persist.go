package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"discodb/catalog"
	"discodb/discord"
	"discodb/types"
)

const catalogMsgPrefix = "CATALOG:"

func persistCatalogToDiscord(ctx context.Context, client *discord.Client, guildID types.GuildID, catalogCatID types.ChannelID, cat *catalog.Catalog) error {
	snapshot := catalogSnapshot{
		Tables:  make(map[uint64]tableSnapshot),
		Indexes: make(map[uint64]indexSnapshot),
		Epoch:   cat.Epoch().Uint64(),
	}

	for id, table := range cat.Tables() {
		ts := tableSnapshot{
			ID:      id.Uint64(),
			Name:    table.Name,
			Epoch:   table.Epoch.Uint64(),
			Columns: make([]columnSnapshot, len(table.Columns)),
		}
		for i, col := range table.Columns {
			ts.Columns[i] = columnSnapshot{
				Name:     col.Name,
				DataType: string(col.DataType),
				Nullable: col.Nullable,
				Ordinal:  col.Ordinal,
			}
		}
		snapshot.Tables[id.Uint64()] = ts
	}

	for id, idx := range cat.Indexes() {
		snapshot.Indexes[id.Uint64()] = indexSnapshot{
			ID:        id.Uint64(),
			Name:      idx.Name,
			TableID:   idx.TableID.Uint64(),
			Columns:   idx.Columns,
			Unique:    idx.Unique,
			IndexType: string(idx.IndexType),
		}
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal catalog: %w", err)
	}

	content := catalogMsgPrefix + string(data)

	channels, err := client.ListGuildChannels(ctx, guildID)
	if err != nil {
		return fmt.Errorf("list channels for catalog persist: %w", err)
	}

	var catalogChannel *discord.Channel
	var textChannel *discord.Channel
	for _, ch := range channels {
		if ch.ID == catalogCatID {
			catalogChannel = ch
		}
		if ch.ParentID != nil && *ch.ParentID == catalogCatID && ch.Type == discord.ChannelTypeGuildText {
			textChannel = ch
			break
		}
	}

	if catalogChannel == nil {
		return fmt.Errorf("catalog category not found")
	}

	if textChannel == nil {
		return fmt.Errorf("no text channel found in catalog category")
	}

	messages, err := client.ListMessages(ctx, textChannel.ID, discord.MessagesListParams{Limit: 50})
	if err != nil {
		return fmt.Errorf("list catalog messages: %w", err)
	}

	for _, msg := range messages {
		if strings.HasPrefix(msg.Content, catalogMsgPrefix) {
			_, err := client.EditMessage(ctx, textChannel.ID, msg.ID, discord.MessageEditParams{
				Content: &content,
			})
			if err != nil {
				return fmt.Errorf("edit catalog message: %w", err)
			}
			return nil
		}
	}

	_, err = client.SendMessage(ctx, textChannel.ID, discord.MessageSendParams{
		Content: content,
		AllowedMentions: &discord.AllowedMentions{
			Parse: []string{},
		},
	})
	if err != nil {
		return fmt.Errorf("send catalog message: %w", err)
	}

	return nil
}

func loadCatalogFromDiscord(ctx context.Context, client *discord.Client, guildID types.GuildID, catalogCatID types.ChannelID, cat *catalog.Catalog) error {
	channels, err := client.ListGuildChannels(ctx, guildID)
	if err != nil {
		return fmt.Errorf("list channels for catalog load: %w", err)
	}

	var textChannel *discord.Channel
	for _, ch := range channels {
		if ch.ParentID != nil && *ch.ParentID == catalogCatID && ch.Type == discord.ChannelTypeGuildText {
			textChannel = ch
			break
		}
	}

	if textChannel == nil {
		return fmt.Errorf("no text channel found in catalog category")
	}

	messages, err := client.ListMessages(ctx, textChannel.ID, discord.MessagesListParams{Limit: 50})
	if err != nil {
		return fmt.Errorf("list catalog messages: %w", err)
	}

	for _, msg := range messages {
		if strings.HasPrefix(msg.Content, catalogMsgPrefix) {
			var snapshot catalogSnapshot
			if err := json.Unmarshal([]byte(msg.Content[len(catalogMsgPrefix):]), &snapshot); err != nil {
				return fmt.Errorf("unmarshal catalog snapshot: %w", err)
			}

			for _, ts := range snapshot.Tables {
				var cols []catalog.ColumnSchema
				for _, cs := range ts.Columns {
					cols = append(cols, catalog.ColumnSchema{
						Name:     cs.Name,
						DataType: types.DataType(cs.DataType),
						Nullable: cs.Nullable,
						Ordinal:  cs.Ordinal,
					})
				}
				schema := catalog.NewTableSchema(
					types.TableID(ts.ID),
					ts.Name,
					cols,
				)
				schema.Epoch = types.SchemaEpoch(ts.Epoch)
				cat.AddTable(schema)
			}

			for _, is := range snapshot.Indexes {
				cat.AddIndex(catalog.IndexSchema{
					ID:        types.TableID(is.ID),
					Name:      is.Name,
					TableID:   types.TableID(is.TableID),
					Columns:   is.Columns,
					Unique:    is.Unique,
					IndexType: catalog.IndexType(is.IndexType),
				})
			}

			return nil
		}
	}

	return fmt.Errorf("no catalog snapshot found")
}

type catalogSnapshot struct {
	Tables  map[uint64]tableSnapshot `json:"tables"`
	Indexes map[uint64]indexSnapshot `json:"indexes"`
	Epoch   uint64                   `json:"epoch"`
}

type tableSnapshot struct {
	ID      uint64           `json:"id"`
	Name    string           `json:"name"`
	Columns []columnSnapshot `json:"columns"`
	Epoch   uint64           `json:"epoch"`
}

type columnSnapshot struct {
	Name     string `json:"name"`
	DataType string `json:"data_type"`
	Nullable bool   `json:"nullable"`
	Ordinal  uint32 `json:"ordinal"`
}

type indexSnapshot struct {
	ID        uint64   `json:"id"`
	Name      string   `json:"name"`
	TableID   uint64   `json:"table_id"`
	Columns   []string `json:"columns"`
	Unique    bool     `json:"unique"`
	IndexType string   `json:"index_type"`
}
