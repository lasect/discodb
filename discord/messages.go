package discord

import (
	"context"
	"fmt"
	"io"

	"discodb/types"

	"github.com/bwmarrin/discordgo"
)

// MessageSendParams contains parameters for sending a message.
type MessageSendParams struct {
	Content string
	Embeds  []Embed

	// File attachment for blob storage
	File *MessageFile

	// AllowedMentions controls mention parsing - discodb sets this to none
	// to ensure deterministic index behavior
	AllowedMentions *AllowedMentions
}

// MessageFile represents a file to attach to a message.
type MessageFile struct {
	Name   string
	Reader io.Reader
}

// AllowedMentions controls what mentions are parsed in a message.
type AllowedMentions struct {
	Parse       []string // "roles", "users", "everyone"
	Users       []string // User IDs to mention
	Roles       []string // Role IDs to mention
	RepliedUser bool
}

// MessageEditParams contains parameters for editing a message.
type MessageEditParams struct {
	Content *string
	Embeds  []Embed
}

// MessagesListParams contains parameters for listing messages.
type MessagesListParams struct {
	Limit  int             // Max 100
	Before types.MessageID // Get messages before this ID
	After  types.MessageID // Get messages after this ID
	Around types.MessageID // Get messages around this ID
}

// SendMessage sends a message to a channel.
func (c *Client) SendMessage(ctx context.Context, channelID types.ChannelID, params MessageSendParams) (*Message, error) {
	const op = "SendMessage"

	data := &discordgo.MessageSend{
		Content: params.Content,
	}

	// Convert embeds
	for _, e := range params.Embeds {
		data.Embeds = append(data.Embeds, embedToDiscordgo(e))
	}

	// Set allowed mentions - default to none for discodb
	if params.AllowedMentions != nil {
		parsedTypes := make([]discordgo.AllowedMentionType, len(params.AllowedMentions.Parse))
		for i, p := range params.AllowedMentions.Parse {
			parsedTypes[i] = discordgo.AllowedMentionType(p)
		}
		data.AllowedMentions = &discordgo.MessageAllowedMentions{
			Parse:       parsedTypes,
			Users:       params.AllowedMentions.Users,
			Roles:       params.AllowedMentions.Roles,
			RepliedUser: params.AllowedMentions.RepliedUser,
		}
	} else {
		// Default: no mentions parsed (important for discodb index consistency)
		data.AllowedMentions = &discordgo.MessageAllowedMentions{
			Parse: []discordgo.AllowedMentionType{},
		}
	}

	// Add file if present
	if params.File != nil {
		data.Files = []*discordgo.File{
			{
				Name:   params.File.Name,
				Reader: params.File.Reader,
			},
		}
	}

	var result *Message
	err := c.withRetry(ctx, op, func() error {
		msg, err := c.session.ChannelMessageSendComplex(
			channelIDToString(channelID),
			data,
			c.requestOption(ctx)...,
		)
		if err != nil {
			return wrapError(op, err)
		}

		result, err = messageFromDiscordgo(msg)
		return err
	})

	if err != nil {
		return nil, err
	}

	c.logger.Debug("sent message",
		"channel_id", channelID,
		"message_id", result.ID,
		"content_len", len(params.Content),
	)

	return result, nil
}

// SendMessageContent is a convenience method for sending a simple text message.
func (c *Client) SendMessageContent(ctx context.Context, channelID types.ChannelID, content string) (*Message, error) {
	return c.SendMessage(ctx, channelID, MessageSendParams{
		Content: content,
	})
}

// SendMessageWithEmbeds sends a message with embeds.
func (c *Client) SendMessageWithEmbeds(ctx context.Context, channelID types.ChannelID, content string, embeds []Embed) (*Message, error) {
	return c.SendMessage(ctx, channelID, MessageSendParams{
		Content: content,
		Embeds:  embeds,
	})
}

// GetMessage retrieves a message by ID.
func (c *Client) GetMessage(ctx context.Context, channelID types.ChannelID, messageID types.MessageID) (*Message, error) {
	const op = "GetMessage"

	var result *Message
	err := c.withRetry(ctx, op, func() error {
		msg, err := c.session.ChannelMessage(
			channelIDToString(channelID),
			messageIDToString(messageID),
			c.requestOption(ctx)...,
		)
		if err != nil {
			return wrapError(op, err)
		}

		result, err = messageFromDiscordgo(msg)
		return err
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

// ListMessages retrieves messages from a channel.
// Discord limits this to 100 messages per request.
func (c *Client) ListMessages(ctx context.Context, channelID types.ChannelID, params MessagesListParams) ([]*Message, error) {
	const op = "ListMessages"

	limit := params.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}

	var beforeID, afterID, aroundID string
	if params.Before != 0 {
		beforeID = messageIDToString(params.Before)
	}
	if params.After != 0 {
		afterID = messageIDToString(params.After)
	}
	if params.Around != 0 {
		aroundID = messageIDToString(params.Around)
	}

	var results []*Message
	err := c.withRetry(ctx, op, func() error {
		messages, err := c.session.ChannelMessages(
			channelIDToString(channelID),
			limit,
			beforeID,
			afterID,
			aroundID,
			c.requestOption(ctx)...,
		)
		if err != nil {
			return wrapError(op, err)
		}

		results = make([]*Message, 0, len(messages))
		for _, msg := range messages {
			m, err := messageFromDiscordgo(msg)
			if err != nil {
				c.logger.Warn("skipping invalid message",
					"message_id", msg.ID,
					"error", err,
				)
				continue
			}
			results = append(results, m)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return results, nil
}

// ListAllMessages iterates through all messages in a channel, calling fn for each batch.
// This handles pagination automatically. Returns early if fn returns an error.
func (c *Client) ListAllMessages(ctx context.Context, channelID types.ChannelID, fn func([]*Message) error) error {
	var lastID types.MessageID

	for {
		params := MessagesListParams{
			Limit: 100,
		}
		if lastID != 0 {
			params.Before = lastID
		}

		messages, err := c.ListMessages(ctx, channelID, params)
		if err != nil {
			return err
		}

		if len(messages) == 0 {
			break
		}

		if err := fn(messages); err != nil {
			return err
		}

		// Messages are returned newest first, so the last one has the smallest ID
		lastID = messages[len(messages)-1].ID
	}

	return nil
}

// EditMessage modifies an existing message.
func (c *Client) EditMessage(ctx context.Context, channelID types.ChannelID, messageID types.MessageID, params MessageEditParams) (*Message, error) {
	const op = "EditMessage"

	edit := &discordgo.MessageEdit{
		Channel: channelIDToString(channelID),
		ID:      messageIDToString(messageID),
	}

	if params.Content != nil {
		edit.Content = params.Content
	}

	if len(params.Embeds) > 0 {
		embeds := make([]*discordgo.MessageEmbed, len(params.Embeds))
		for i, e := range params.Embeds {
			embeds[i] = embedToDiscordgo(e)
		}
		edit.Embeds = &embeds
	}

	// Disable mentions on edit
	edit.AllowedMentions = &discordgo.MessageAllowedMentions{
		Parse: []discordgo.AllowedMentionType{},
	}

	var result *Message
	err := c.withRetry(ctx, op, func() error {
		msg, err := c.session.ChannelMessageEditComplex(edit, c.requestOption(ctx)...)
		if err != nil {
			return wrapError(op, err)
		}

		result, err = messageFromDiscordgo(msg)
		return err
	})

	if err != nil {
		return nil, err
	}

	c.logger.Debug("edited message",
		"channel_id", channelID,
		"message_id", messageID,
	)

	return result, nil
}

// EditMessageContent is a convenience method for editing just the content.
func (c *Client) EditMessageContent(ctx context.Context, channelID types.ChannelID, messageID types.MessageID, content string) (*Message, error) {
	return c.EditMessage(ctx, channelID, messageID, MessageEditParams{
		Content: &content,
	})
}

// DeleteMessage deletes a message.
func (c *Client) DeleteMessage(ctx context.Context, channelID types.ChannelID, messageID types.MessageID) error {
	const op = "DeleteMessage"

	err := c.withRetry(ctx, op, func() error {
		return wrapError(op, c.session.ChannelMessageDelete(
			channelIDToString(channelID),
			messageIDToString(messageID),
			c.requestOption(ctx)...,
		))
	})

	if err != nil {
		return err
	}

	c.logger.Debug("deleted message",
		"channel_id", channelID,
		"message_id", messageID,
	)

	return nil
}

// BulkDeleteMessages deletes multiple messages at once.
// Messages must be less than 14 days old.
// Maximum 100 messages per call.
func (c *Client) BulkDeleteMessages(ctx context.Context, channelID types.ChannelID, messageIDs []types.MessageID) error {
	const op = "BulkDeleteMessages"

	if len(messageIDs) == 0 {
		return nil
	}
	if len(messageIDs) > 100 {
		return fmt.Errorf("%s: cannot delete more than 100 messages at once", op)
	}

	ids := make([]string, len(messageIDs))
	for i, id := range messageIDs {
		ids[i] = messageIDToString(id)
	}

	err := c.withRetry(ctx, op, func() error {
		return wrapError(op, c.session.ChannelMessagesBulkDelete(
			channelIDToString(channelID),
			ids,
			c.requestOption(ctx)...,
		))
	})

	if err != nil {
		return err
	}

	c.logger.Debug("bulk deleted messages",
		"channel_id", channelID,
		"count", len(messageIDs),
	)

	return nil
}

// PinMessage pins a message to a channel.
func (c *Client) PinMessage(ctx context.Context, channelID types.ChannelID, messageID types.MessageID) error {
	const op = "PinMessage"

	err := c.withRetry(ctx, op, func() error {
		return wrapError(op, c.session.ChannelMessagePin(
			channelIDToString(channelID),
			messageIDToString(messageID),
			c.requestOption(ctx)...,
		))
	})

	if err != nil {
		return err
	}

	c.logger.Debug("pinned message",
		"channel_id", channelID,
		"message_id", messageID,
	)

	return nil
}

// UnpinMessage unpins a message from a channel.
func (c *Client) UnpinMessage(ctx context.Context, channelID types.ChannelID, messageID types.MessageID) error {
	const op = "UnpinMessage"

	err := c.withRetry(ctx, op, func() error {
		return wrapError(op, c.session.ChannelMessageUnpin(
			channelIDToString(channelID),
			messageIDToString(messageID),
			c.requestOption(ctx)...,
		))
	})

	if err != nil {
		return err
	}

	c.logger.Debug("unpinned message",
		"channel_id", channelID,
		"message_id", messageID,
	)

	return nil
}

// ListPinnedMessages retrieves all pinned messages in a channel.
func (c *Client) ListPinnedMessages(ctx context.Context, channelID types.ChannelID) ([]*Message, error) {
	const op = "ListPinnedMessages"

	var results []*Message
	err := c.withRetry(ctx, op, func() error {
		messages, err := c.session.ChannelMessagesPinned(
			channelIDToString(channelID),
			c.requestOption(ctx)...,
		)
		if err != nil {
			return wrapError(op, err)
		}

		results = make([]*Message, 0, len(messages))
		for _, msg := range messages {
			m, err := messageFromDiscordgo(msg)
			if err != nil {
				c.logger.Warn("skipping invalid pinned message",
					"message_id", msg.ID,
					"error", err,
				)
				continue
			}
			results = append(results, m)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return results, nil
}
