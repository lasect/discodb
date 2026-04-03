package discord

import (
	"context"
	"fmt"

	"discodb/types"
)

// ChannelCreateParams contains parameters for creating a channel.
type ChannelCreateParams struct {
	Name     string
	Type     ChannelType
	Topic    string
	ParentID *types.ChannelID // Category parent for nested channels
	Position int
}

// ChannelEditParams contains parameters for editing a channel.
type ChannelEditParams struct {
	Name     *string
	Topic    *string
	Position *int
	ParentID *types.ChannelID
}

// GetChannel retrieves a channel by ID.
func (c *Client) GetChannel(ctx context.Context, channelID types.ChannelID) (*Channel, error) {
	const op = "GetChannel"

	var result *Channel
	err := c.withRetry(ctx, op, func() error {
		var err error
		result, err = c.backend.GetChannel(ctx, channelIDToString(channelID))
		return c.normalizeError(op, err)
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

// ListGuildChannels retrieves all channels in a guild.
func (c *Client) ListGuildChannels(ctx context.Context, guildID types.GuildID) ([]*Channel, error) {
	const op = "ListGuildChannels"

	var results []*Channel
	err := c.withRetry(ctx, op, func() error {
		var err error
		results, err = c.backend.ListGuildChannels(ctx, guildIDToString(guildID))
		return c.normalizeError(op, err)
	})

	if err != nil {
		return nil, err
	}
	return results, nil
}

// CreateChannel creates a new channel in a guild.
func (c *Client) CreateChannel(ctx context.Context, guildID types.GuildID, params ChannelCreateParams) (*Channel, error) {
	const op = "CreateChannel"

	var result *Channel
	err := c.withRetry(ctx, op, func() error {
		var err error
		result, err = c.backend.CreateChannel(ctx, guildIDToString(guildID), params)
		return c.normalizeError(op, err)
	})

	if err != nil {
		return nil, err
	}

	c.logger.Debug("created channel",
		"guild_id", guildID,
		"channel_id", result.ID,
		"name", result.Name,
		"type", result.Type.String(),
	)

	return result, nil
}

// CreateTextChannel is a convenience method for creating a text channel.
func (c *Client) CreateTextChannel(ctx context.Context, guildID types.GuildID, name string, parentID *types.ChannelID) (*Channel, error) {
	return c.CreateChannel(ctx, guildID, ChannelCreateParams{
		Name:     name,
		Type:     ChannelTypeGuildText,
		ParentID: parentID,
	})
}

// CreateCategory creates a new category channel.
func (c *Client) CreateCategory(ctx context.Context, guildID types.GuildID, name string) (*Channel, error) {
	return c.CreateChannel(ctx, guildID, ChannelCreateParams{
		Name: name,
		Type: ChannelTypeGuildCategory,
	})
}

// CreateForumChannel creates a new forum channel.
func (c *Client) CreateForumChannel(ctx context.Context, guildID types.GuildID, name string, parentID *types.ChannelID) (*Channel, error) {
	return c.CreateChannel(ctx, guildID, ChannelCreateParams{
		Name:     name,
		Type:     ChannelTypeGuildForum,
		ParentID: parentID,
	})
}

// EditChannel modifies an existing channel.
func (c *Client) EditChannel(ctx context.Context, channelID types.ChannelID, params ChannelEditParams) (*Channel, error) {
	const op = "EditChannel"

	var result *Channel
	err := c.withRetry(ctx, op, func() error {
		var err error
		result, err = c.backend.EditChannel(ctx, channelIDToString(channelID), params)
		return c.normalizeError(op, err)
	})

	if err != nil {
		return nil, err
	}

	c.logger.Debug("edited channel",
		"channel_id", channelID,
		"name", result.Name,
	)

	return result, nil
}

// SetChannelTopic updates only the topic of a channel.
// This is used for storing page headers in discodb.
func (c *Client) SetChannelTopic(ctx context.Context, channelID types.ChannelID, topic string) error {
	_, err := c.EditChannel(ctx, channelID, ChannelEditParams{
		Topic: &topic,
	})
	return err
}

// DeleteChannel deletes a channel.
func (c *Client) DeleteChannel(ctx context.Context, channelID types.ChannelID) error {
	const op = "DeleteChannel"

	err := c.withRetry(ctx, op, func() error {
		_, err := c.session.ChannelDelete(channelIDToString(channelID), c.requestOption(ctx)...)
		return wrapError(op, err)
	})

	if err != nil {
		return err
	}

	c.logger.Debug("deleted channel", "channel_id", channelID)
	return nil
}

// ListChannelsByType filters channels by type within a guild.
func (c *Client) ListChannelsByType(ctx context.Context, guildID types.GuildID, channelType ChannelType) ([]*Channel, error) {
	channels, err := c.ListGuildChannels(ctx, guildID)
	if err != nil {
		return nil, err
	}

	var filtered []*Channel
	for _, ch := range channels {
		if ch.Type == channelType {
			filtered = append(filtered, ch)
		}
	}
	return filtered, nil
}

// ListTextChannels returns all text channels in a guild.
func (c *Client) ListTextChannels(ctx context.Context, guildID types.GuildID) ([]*Channel, error) {
	return c.ListChannelsByType(ctx, guildID, ChannelTypeGuildText)
}

// ListCategories returns all category channels in a guild.
func (c *Client) ListCategories(ctx context.Context, guildID types.GuildID) ([]*Channel, error) {
	return c.ListChannelsByType(ctx, guildID, ChannelTypeGuildCategory)
}

// ListForumChannels returns all forum channels in a guild.
func (c *Client) ListForumChannels(ctx context.Context, guildID types.GuildID) ([]*Channel, error) {
	return c.ListChannelsByType(ctx, guildID, ChannelTypeGuildForum)
}

// FindChannelByName finds a channel by name within a guild.
func (c *Client) FindChannelByName(ctx context.Context, guildID types.GuildID, name string) (*Channel, error) {
	channels, err := c.ListGuildChannels(ctx, guildID)
	if err != nil {
		return nil, err
	}

	for _, ch := range channels {
		if ch.Name == name {
			return ch, nil
		}
	}

	return nil, fmt.Errorf("%w: channel %q in guild %d", ErrChannelNotFound, name, guildID)
}

// FindChannelByNameInCategory finds a channel by name within a specific category.
func (c *Client) FindChannelByNameInCategory(ctx context.Context, guildID types.GuildID, categoryID types.ChannelID, name string) (*Channel, error) {
	channels, err := c.ListGuildChannels(ctx, guildID)
	if err != nil {
		return nil, err
	}

	for _, ch := range channels {
		if ch.Name == name && ch.ParentID != nil && *ch.ParentID == categoryID {
			return ch, nil
		}
	}

	return nil, fmt.Errorf("%w: channel %q in category %d", ErrChannelNotFound, name, categoryID)
}

// GetOrCreateChannel gets a channel by name or creates it if it doesn't exist.
func (c *Client) GetOrCreateChannel(ctx context.Context, guildID types.GuildID, params ChannelCreateParams) (*Channel, error) {
	// Try to find existing channel
	ch, err := c.FindChannelByName(ctx, guildID, params.Name)
	if err == nil {
		return ch, nil
	}

	if !IsNotFound(err) {
		// Check if it's a "channel not found" error vs other errors
		if apiErr, ok := err.(*APIError); ok && apiErr.Err != ErrChannelNotFound {
			return nil, err
		}
	}

	// Create new channel
	return c.CreateChannel(ctx, guildID, params)
}
