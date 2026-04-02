package discord

import (
	"context"

	"discodb/types"
)

// Common emoji constants used as row flags in discodb.
const (
	// Row state flags
	EmojiLive   = "\U0001F7E2" // Green circle - row is live
	EmojiDead   = "\U0001F534" // Red circle - row is dead/deleted
	EmojiLocked = "\U0001F512" // Lock - row is locked

	// Transaction state flags
	EmojiPrepared  = "\U0001F7E1" // Yellow circle - transaction prepared
	EmojiCommitted = "\u2705"     // Check mark - transaction committed
	EmojiAborted   = "\u274C"     // X mark - transaction aborted
)

// AddReaction adds a reaction to a message.
// The emoji can be a Unicode emoji or a custom emoji in the format "name:id".
func (c *Client) AddReaction(ctx context.Context, channelID types.ChannelID, messageID types.MessageID, emoji string) error {
	const op = "AddReaction"

	err := c.withRetry(ctx, op, func() error {
		return wrapError(op, c.session.MessageReactionAdd(
			channelIDToString(channelID),
			messageIDToString(messageID),
			emoji,
			c.requestOption(ctx)...,
		))
	})

	if err != nil {
		return err
	}

	c.logger.Debug("added reaction",
		"channel_id", channelID,
		"message_id", messageID,
		"emoji", emoji,
	)

	return nil
}

// RemoveReaction removes the bot's reaction from a message.
func (c *Client) RemoveReaction(ctx context.Context, channelID types.ChannelID, messageID types.MessageID, emoji string) error {
	const op = "RemoveReaction"

	err := c.withRetry(ctx, op, func() error {
		return wrapError(op, c.session.MessageReactionRemove(
			channelIDToString(channelID),
			messageIDToString(messageID),
			emoji,
			"@me",
			c.requestOption(ctx)...,
		))
	})

	if err != nil {
		return err
	}

	c.logger.Debug("removed reaction",
		"channel_id", channelID,
		"message_id", messageID,
		"emoji", emoji,
	)

	return nil
}

// RemoveAllReactions removes all reactions from a message.
func (c *Client) RemoveAllReactions(ctx context.Context, channelID types.ChannelID, messageID types.MessageID) error {
	const op = "RemoveAllReactions"

	err := c.withRetry(ctx, op, func() error {
		return wrapError(op, c.session.MessageReactionsRemoveAll(
			channelIDToString(channelID),
			messageIDToString(messageID),
			c.requestOption(ctx)...,
		))
	})

	if err != nil {
		return err
	}

	c.logger.Debug("removed all reactions",
		"channel_id", channelID,
		"message_id", messageID,
	)

	return nil
}

// RemoveAllReactionsForEmoji removes all reactions for a specific emoji from a message.
func (c *Client) RemoveAllReactionsForEmoji(ctx context.Context, channelID types.ChannelID, messageID types.MessageID, emoji string) error {
	const op = "RemoveAllReactionsForEmoji"

	err := c.withRetry(ctx, op, func() error {
		return wrapError(op, c.session.MessageReactionsRemoveEmoji(
			channelIDToString(channelID),
			messageIDToString(messageID),
			emoji,
			c.requestOption(ctx)...,
		))
	})

	if err != nil {
		return err
	}

	c.logger.Debug("removed all reactions for emoji",
		"channel_id", channelID,
		"message_id", messageID,
		"emoji", emoji,
	)

	return nil
}

// RowFlags provides convenience methods for managing row state flags via reactions.
type RowFlags struct {
	client *Client
}

// Flags returns a RowFlags helper for managing row state.
func (c *Client) Flags() *RowFlags {
	return &RowFlags{client: c}
}

// MarkLive marks a row as live (visible).
func (rf *RowFlags) MarkLive(ctx context.Context, channelID types.ChannelID, messageID types.MessageID) error {
	// Remove dead flag if present, add live flag
	_ = rf.client.RemoveReaction(ctx, channelID, messageID, EmojiDead)
	return rf.client.AddReaction(ctx, channelID, messageID, EmojiLive)
}

// MarkDead marks a row as dead (deleted).
func (rf *RowFlags) MarkDead(ctx context.Context, channelID types.ChannelID, messageID types.MessageID) error {
	// Remove live flag if present, add dead flag
	_ = rf.client.RemoveReaction(ctx, channelID, messageID, EmojiLive)
	return rf.client.AddReaction(ctx, channelID, messageID, EmojiDead)
}

// MarkLocked marks a row as locked.
func (rf *RowFlags) MarkLocked(ctx context.Context, channelID types.ChannelID, messageID types.MessageID) error {
	return rf.client.AddReaction(ctx, channelID, messageID, EmojiLocked)
}

// MarkUnlocked removes the locked flag from a row.
func (rf *RowFlags) MarkUnlocked(ctx context.Context, channelID types.ChannelID, messageID types.MessageID) error {
	return rf.client.RemoveReaction(ctx, channelID, messageID, EmojiLocked)
}

// MarkPrepared marks a transaction as prepared.
func (rf *RowFlags) MarkPrepared(ctx context.Context, channelID types.ChannelID, messageID types.MessageID) error {
	return rf.client.AddReaction(ctx, channelID, messageID, EmojiPrepared)
}

// MarkCommitted marks a transaction as committed.
func (rf *RowFlags) MarkCommitted(ctx context.Context, channelID types.ChannelID, messageID types.MessageID) error {
	// Remove prepared flag, add committed flag
	_ = rf.client.RemoveReaction(ctx, channelID, messageID, EmojiPrepared)
	return rf.client.AddReaction(ctx, channelID, messageID, EmojiCommitted)
}

// MarkAborted marks a transaction as aborted.
func (rf *RowFlags) MarkAborted(ctx context.Context, channelID types.ChannelID, messageID types.MessageID) error {
	// Remove prepared flag, add aborted flag
	_ = rf.client.RemoveReaction(ctx, channelID, messageID, EmojiPrepared)
	return rf.client.AddReaction(ctx, channelID, messageID, EmojiAborted)
}

// HasFlag checks if a message has a specific reaction flag.
func (rf *RowFlags) HasFlag(msg *Message, emoji string) bool {
	for _, r := range msg.Reactions {
		if r.Emoji == emoji && r.Me {
			return true
		}
	}
	return false
}

// IsLive returns true if the message has the live flag.
func (rf *RowFlags) IsLive(msg *Message) bool {
	return rf.HasFlag(msg, EmojiLive)
}

// IsDead returns true if the message has the dead flag.
func (rf *RowFlags) IsDead(msg *Message) bool {
	return rf.HasFlag(msg, EmojiDead)
}

// IsLocked returns true if the message has the locked flag.
func (rf *RowFlags) IsLocked(msg *Message) bool {
	return rf.HasFlag(msg, EmojiLocked)
}
