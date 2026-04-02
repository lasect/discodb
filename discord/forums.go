package discord

import (
	"context"

	"discodb/types"

	"github.com/bwmarrin/discordgo"
)

// ForumPostCreateParams contains parameters for creating a forum post.
type ForumPostCreateParams struct {
	Name            string   // Thread name (required)
	Content         string   // Initial message content
	Embeds          []Embed  // Initial message embeds
	AppliedTags     []string // Tag IDs to apply
	AutoArchiveDays int      // Auto-archive duration in minutes (60, 1440, 4320, 10080)
}

// CreateForumPost creates a new post (thread) in a forum channel.
// This is used for B-tree index entries in discodb.
func (c *Client) CreateForumPost(ctx context.Context, forumID types.ChannelID, params ForumPostCreateParams) (*ForumPost, *Message, error) {
	const op = "CreateForumPost"

	// Default auto-archive to 1 day
	archiveDuration := params.AutoArchiveDays
	if archiveDuration == 0 {
		archiveDuration = 1440 // 1 day in minutes
	}

	threadData := &discordgo.ThreadStart{
		Name:                params.Name,
		AutoArchiveDuration: archiveDuration,
		AppliedTags:         params.AppliedTags,
	}

	messageData := &discordgo.MessageSend{
		Content: params.Content,
		AllowedMentions: &discordgo.MessageAllowedMentions{
			Parse: []discordgo.AllowedMentionType{},
		},
	}

	for _, e := range params.Embeds {
		messageData.Embeds = append(messageData.Embeds, embedToDiscordgo(e))
	}

	var post *ForumPost
	var initialMsg *Message

	err := c.withRetry(ctx, op, func() error {
		thread, err := c.session.ForumThreadStartComplex(
			channelIDToString(forumID),
			threadData,
			messageData,
			c.requestOption(ctx)...,
		)
		if err != nil {
			return wrapError(op, err)
		}

		post, err = forumPostFromDiscordgo(thread)
		if err != nil {
			return err
		}

		// The thread's first message ID equals the thread ID
		if thread.ID != "" {
			msgID, err := stringToMessageID(thread.ID)
			if err == nil {
				msg, err := c.GetMessage(ctx, post.ID, msgID)
				if err == nil {
					initialMsg = msg
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	c.logger.Debug("created forum post",
		"forum_id", forumID,
		"post_id", post.ID,
		"name", post.Name,
	)

	return post, initialMsg, nil
}

// GetForumPost retrieves a forum post (thread) by ID.
func (c *Client) GetForumPost(ctx context.Context, postID types.ChannelID) (*ForumPost, error) {
	const op = "GetForumPost"

	var result *ForumPost
	err := c.withRetry(ctx, op, func() error {
		ch, err := c.session.Channel(channelIDToString(postID), c.requestOption(ctx)...)
		if err != nil {
			return wrapError(op, err)
		}

		result, err = forumPostFromDiscordgo(ch)
		return err
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

// ListForumPosts retrieves active threads in a forum channel.
func (c *Client) ListForumPosts(ctx context.Context, forumID types.ChannelID) ([]*ForumPost, error) {
	const op = "ListForumPosts"

	var results []*ForumPost
	err := c.withRetry(ctx, op, func() error {
		// Get active threads in the guild first
		ch, err := c.session.Channel(channelIDToString(forumID), c.requestOption(ctx)...)
		if err != nil {
			return wrapError(op, err)
		}

		threads, err := c.session.GuildThreadsActive(ch.GuildID, c.requestOption(ctx)...)
		if err != nil {
			return wrapError(op, err)
		}

		results = make([]*ForumPost, 0)
		for _, thread := range threads.Threads {
			// Filter to only threads in this forum
			if thread.ParentID != channelIDToString(forumID) {
				continue
			}

			post, err := forumPostFromDiscordgo(thread)
			if err != nil {
				c.logger.Warn("skipping invalid forum post",
					"post_id", thread.ID,
					"error", err,
				)
				continue
			}
			results = append(results, post)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return results, nil
}

// ListArchivedForumPosts retrieves archived threads in a forum channel.
func (c *Client) ListArchivedForumPosts(ctx context.Context, forumID types.ChannelID, limit int) ([]*ForumPost, error) {
	const op = "ListArchivedForumPosts"

	if limit <= 0 || limit > 100 {
		limit = 100
	}

	var results []*ForumPost
	err := c.withRetry(ctx, op, func() error {
		threads, err := c.session.ThreadsArchived(
			channelIDToString(forumID),
			nil, // before timestamp
			limit,
			c.requestOption(ctx)...,
		)
		if err != nil {
			return wrapError(op, err)
		}

		results = make([]*ForumPost, 0, len(threads.Threads))
		for _, thread := range threads.Threads {
			post, err := forumPostFromDiscordgo(thread)
			if err != nil {
				c.logger.Warn("skipping invalid archived forum post",
					"post_id", thread.ID,
					"error", err,
				)
				continue
			}
			results = append(results, post)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return results, nil
}

// ArchiveForumPost archives a forum post.
func (c *Client) ArchiveForumPost(ctx context.Context, postID types.ChannelID) error {
	const op = "ArchiveForumPost"

	archived := true
	err := c.withRetry(ctx, op, func() error {
		_, err := c.session.ChannelEditComplex(
			channelIDToString(postID),
			&discordgo.ChannelEdit{
				Archived: &archived,
			},
			c.requestOption(ctx)...,
		)
		return wrapError(op, err)
	})

	if err != nil {
		return err
	}

	c.logger.Debug("archived forum post", "post_id", postID)
	return nil
}

// UnarchiveForumPost unarchives a forum post.
func (c *Client) UnarchiveForumPost(ctx context.Context, postID types.ChannelID) error {
	const op = "UnarchiveForumPost"

	archived := false
	err := c.withRetry(ctx, op, func() error {
		_, err := c.session.ChannelEditComplex(
			channelIDToString(postID),
			&discordgo.ChannelEdit{
				Archived: &archived,
			},
			c.requestOption(ctx)...,
		)
		return wrapError(op, err)
	})

	if err != nil {
		return err
	}

	c.logger.Debug("unarchived forum post", "post_id", postID)
	return nil
}

// DeleteForumPost deletes a forum post (thread).
func (c *Client) DeleteForumPost(ctx context.Context, postID types.ChannelID) error {
	return c.DeleteChannel(ctx, postID)
}

// AddMessageToPost sends a message to a forum post (thread).
// This is used to add more content to an index entry, e.g., for overflow or B-tree internal nodes.
func (c *Client) AddMessageToPost(ctx context.Context, postID types.ChannelID, params MessageSendParams) (*Message, error) {
	return c.SendMessage(ctx, postID, params)
}

// FindForumPostByName finds a forum post by name within a forum channel.
func (c *Client) FindForumPostByName(ctx context.Context, forumID types.ChannelID, name string) (*ForumPost, error) {
	posts, err := c.ListForumPosts(ctx, forumID)
	if err != nil {
		return nil, err
	}

	for _, post := range posts {
		if post.Name == name {
			return post, nil
		}
	}

	// Also check archived posts
	archivedPosts, err := c.ListArchivedForumPosts(ctx, forumID, 100)
	if err != nil {
		return nil, err
	}

	for _, post := range archivedPosts {
		if post.Name == name {
			return post, nil
		}
	}

	return nil, ErrNotFound
}
