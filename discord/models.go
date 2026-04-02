package discord

import (
	"time"

	"discodb/types"

	"github.com/bwmarrin/discordgo"
)

// ChannelType represents Discord channel types.
type ChannelType int

const (
	ChannelTypeGuildText          ChannelType = ChannelType(discordgo.ChannelTypeGuildText)
	ChannelTypeGuildVoice         ChannelType = ChannelType(discordgo.ChannelTypeGuildVoice)
	ChannelTypeGuildCategory      ChannelType = ChannelType(discordgo.ChannelTypeGuildCategory)
	ChannelTypeGuildNews          ChannelType = ChannelType(discordgo.ChannelTypeGuildNews)
	ChannelTypeGuildStore         ChannelType = ChannelType(discordgo.ChannelTypeGuildStore)
	ChannelTypeGuildNewsThread    ChannelType = ChannelType(discordgo.ChannelTypeGuildNewsThread)
	ChannelTypeGuildPublicThread  ChannelType = ChannelType(discordgo.ChannelTypeGuildPublicThread)
	ChannelTypeGuildPrivateThread ChannelType = ChannelType(discordgo.ChannelTypeGuildPrivateThread)
	ChannelTypeGuildStageVoice    ChannelType = ChannelType(discordgo.ChannelTypeGuildStageVoice)
	ChannelTypeGuildForum         ChannelType = ChannelType(discordgo.ChannelTypeGuildForum)
)

// String returns the string representation of the channel type.
func (ct ChannelType) String() string {
	switch ct {
	case ChannelTypeGuildText:
		return "text"
	case ChannelTypeGuildVoice:
		return "voice"
	case ChannelTypeGuildCategory:
		return "category"
	case ChannelTypeGuildStore:
		return "store"
	case ChannelTypeGuildNewsThread:
		return "news_thread"
	case ChannelTypeGuildPublicThread:
		return "public_thread"
	case ChannelTypeGuildPrivateThread:
		return "private_thread"
	case ChannelTypeGuildStageVoice:
		return "stage_voice"
	case ChannelTypeGuildForum:
		return "forum"
	default:
		return "unknown"
	}
}

// Channel represents a Discord channel with typed IDs.
type Channel struct {
	ID       types.ChannelID  `json:"id"`
	GuildID  types.GuildID    `json:"guild_id,omitempty"`
	Name     string           `json:"name"`
	Type     ChannelType      `json:"type"`
	ParentID *types.ChannelID `json:"parent_id,omitempty"`
	Topic    string           `json:"topic,omitempty"`
	Position int              `json:"position"`

	// Forum-specific fields
	AvailableTags []ForumTag `json:"available_tags,omitempty"`
}

// ForumTag represents a tag that can be applied to forum posts.
type ForumTag struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Moderated bool   `json:"moderated"`
	EmojiID   string `json:"emoji_id,omitempty"`
	EmojiName string `json:"emoji_name,omitempty"`
}

// Message represents a Discord message with typed IDs.
type Message struct {
	ID        types.MessageID `json:"id"`
	ChannelID types.ChannelID `json:"channel_id"`
	GuildID   types.GuildID   `json:"guild_id,omitempty"`
	Content   string          `json:"content"`
	Timestamp time.Time       `json:"timestamp"`
	EditedAt  *time.Time      `json:"edited_timestamp,omitempty"`
	Pinned    bool            `json:"pinned"`

	// Author info (simplified)
	AuthorID   string `json:"author_id"`
	AuthorName string `json:"author_name"`

	// Embeds for structured data storage
	Embeds []Embed `json:"embeds,omitempty"`

	// Attachments for blob storage
	Attachments []Attachment `json:"attachments,omitempty"`

	// Reactions for row flags
	Reactions []Reaction `json:"reactions,omitempty"`

	// Thread info if this message started a thread
	ThreadID *types.ChannelID `json:"thread_id,omitempty"`
}

// Embed represents a Discord embed for structured data.
type Embed struct {
	Title       string       `json:"title,omitempty"`
	Description string       `json:"description,omitempty"`
	URL         string       `json:"url,omitempty"`
	Color       int          `json:"color,omitempty"`
	Fields      []EmbedField `json:"fields,omitempty"`
	Footer      *EmbedFooter `json:"footer,omitempty"`
	Timestamp   string       `json:"timestamp,omitempty"`
}

// EmbedField represents a field in an embed.
type EmbedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline,omitempty"`
}

// EmbedFooter represents the footer of an embed.
type EmbedFooter struct {
	Text    string `json:"text"`
	IconURL string `json:"icon_url,omitempty"`
}

// Attachment represents a Discord attachment.
type Attachment struct {
	ID          string `json:"id"`
	Filename    string `json:"filename"`
	URL         string `json:"url"`
	ProxyURL    string `json:"proxy_url"`
	Size        int    `json:"size"`
	ContentType string `json:"content_type,omitempty"`
}

// Reaction represents a reaction on a message.
type Reaction struct {
	Emoji string `json:"emoji"`
	Count int    `json:"count"`
	Me    bool   `json:"me"`
}

// Role represents a Discord role with typed IDs.
type Role struct {
	ID          string        `json:"id"`
	GuildID     types.GuildID `json:"guild_id"`
	Name        string        `json:"name"`
	Color       int           `json:"color"`
	Position    int           `json:"position"`
	Permissions int64         `json:"permissions"`
	Managed     bool          `json:"managed"`
	Mentionable bool          `json:"mentionable"`
}

// Guild represents a Discord guild (server).
type Guild struct {
	ID   types.GuildID `json:"id"`
	Name string        `json:"name"`
}

// ThreadMetadata contains thread-specific information.
type ThreadMetadata struct {
	Archived            bool      `json:"archived"`
	AutoArchiveDuration int       `json:"auto_archive_duration"`
	ArchiveTimestamp    time.Time `json:"archive_timestamp"`
	Locked              bool      `json:"locked"`
}

// ForumPost represents a forum channel post (thread).
type ForumPost struct {
	ID             types.ChannelID `json:"id"`
	GuildID        types.GuildID   `json:"guild_id"`
	ParentID       types.ChannelID `json:"parent_id"`
	Name           string          `json:"name"`
	OwnerID        string          `json:"owner_id"`
	MessageCount   int             `json:"message_count"`
	MemberCount    int             `json:"member_count"`
	ThreadMetadata *ThreadMetadata `json:"thread_metadata,omitempty"`
	AppliedTags    []string        `json:"applied_tags,omitempty"`
}

// Conversion functions from discordgo types to discodb types

// channelFromDiscordgo converts a discordgo.Channel to our Channel type.
func channelFromDiscordgo(ch *discordgo.Channel) (*Channel, error) {
	if ch == nil {
		return nil, nil
	}

	channelID, err := stringToChannelID(ch.ID)
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		ID:       channelID,
		Name:     ch.Name,
		Type:     ChannelType(ch.Type),
		Topic:    ch.Topic,
		Position: ch.Position,
	}

	if ch.GuildID != "" {
		guildID, err := stringToGuildID(ch.GuildID)
		if err != nil {
			return nil, err
		}
		channel.GuildID = guildID
	}

	if ch.ParentID != "" {
		parentID, err := stringToChannelID(ch.ParentID)
		if err != nil {
			return nil, err
		}
		channel.ParentID = &parentID
	}

	// Convert forum tags
	for _, tag := range ch.AvailableTags {
		channel.AvailableTags = append(channel.AvailableTags, ForumTag{
			ID:        tag.ID,
			Name:      tag.Name,
			Moderated: tag.Moderated,
			EmojiID:   tag.EmojiID,
			EmojiName: tag.EmojiName,
		})
	}

	return channel, nil
}

// messageFromDiscordgo converts a discordgo.Message to our Message type.
func messageFromDiscordgo(msg *discordgo.Message) (*Message, error) {
	if msg == nil {
		return nil, nil
	}

	messageID, err := stringToMessageID(msg.ID)
	if err != nil {
		return nil, err
	}

	channelID, err := stringToChannelID(msg.ChannelID)
	if err != nil {
		return nil, err
	}

	message := &Message{
		ID:        messageID,
		ChannelID: channelID,
		Content:   msg.Content,
		Timestamp: msg.Timestamp,
		Pinned:    msg.Pinned,
	}

	if msg.GuildID != "" {
		guildID, err := stringToGuildID(msg.GuildID)
		if err != nil {
			return nil, err
		}
		message.GuildID = guildID
	}

	if msg.EditedTimestamp != nil {
		message.EditedAt = msg.EditedTimestamp
	}

	if msg.Author != nil {
		message.AuthorID = msg.Author.ID
		message.AuthorName = msg.Author.Username
	}

	// Convert embeds
	for _, embed := range msg.Embeds {
		e := Embed{
			Title:       embed.Title,
			Description: embed.Description,
			URL:         embed.URL,
			Color:       embed.Color,
			Timestamp:   embed.Timestamp,
		}

		for _, field := range embed.Fields {
			e.Fields = append(e.Fields, EmbedField{
				Name:   field.Name,
				Value:  field.Value,
				Inline: field.Inline,
			})
		}

		if embed.Footer != nil {
			e.Footer = &EmbedFooter{
				Text:    embed.Footer.Text,
				IconURL: embed.Footer.IconURL,
			}
		}

		message.Embeds = append(message.Embeds, e)
	}

	// Convert attachments
	for _, att := range msg.Attachments {
		message.Attachments = append(message.Attachments, Attachment{
			ID:          att.ID,
			Filename:    att.Filename,
			URL:         att.URL,
			ProxyURL:    att.ProxyURL,
			Size:        att.Size,
			ContentType: att.ContentType,
		})
	}

	// Convert reactions
	for _, r := range msg.Reactions {
		emoji := r.Emoji.Name
		if r.Emoji.ID != "" {
			emoji = r.Emoji.ID
		}
		message.Reactions = append(message.Reactions, Reaction{
			Emoji: emoji,
			Count: r.Count,
			Me:    r.Me,
		})
	}

	// Thread info
	if msg.Thread != nil {
		threadID, err := stringToChannelID(msg.Thread.ID)
		if err == nil {
			message.ThreadID = &threadID
		}
	}

	return message, nil
}

// roleFromDiscordgo converts a discordgo.Role to our Role type.
func roleFromDiscordgo(r *discordgo.Role, guildID types.GuildID) *Role {
	if r == nil {
		return nil
	}

	return &Role{
		ID:          r.ID,
		GuildID:     guildID,
		Name:        r.Name,
		Color:       r.Color,
		Position:    r.Position,
		Permissions: r.Permissions,
		Managed:     r.Managed,
		Mentionable: r.Mentionable,
	}
}

// forumPostFromDiscordgo converts a discordgo.Channel (thread) to ForumPost.
func forumPostFromDiscordgo(ch *discordgo.Channel) (*ForumPost, error) {
	if ch == nil {
		return nil, nil
	}

	id, err := stringToChannelID(ch.ID)
	if err != nil {
		return nil, err
	}

	var guildID types.GuildID
	if ch.GuildID != "" {
		guildID, err = stringToGuildID(ch.GuildID)
		if err != nil {
			return nil, err
		}
	}

	var parentID types.ChannelID
	if ch.ParentID != "" {
		parentID, err = stringToChannelID(ch.ParentID)
		if err != nil {
			return nil, err
		}
	}

	post := &ForumPost{
		ID:           id,
		GuildID:      guildID,
		ParentID:     parentID,
		Name:         ch.Name,
		OwnerID:      ch.OwnerID,
		MessageCount: ch.MessageCount,
		MemberCount:  ch.MemberCount,
		AppliedTags:  ch.AppliedTags,
	}

	if ch.ThreadMetadata != nil {
		post.ThreadMetadata = &ThreadMetadata{
			Archived:            ch.ThreadMetadata.Archived,
			AutoArchiveDuration: ch.ThreadMetadata.AutoArchiveDuration,
			ArchiveTimestamp:    ch.ThreadMetadata.ArchiveTimestamp,
			Locked:              ch.ThreadMetadata.Locked,
		}
	}

	return post, nil
}

// embedToDiscordgo converts our Embed to discordgo.MessageEmbed.
func embedToDiscordgo(e Embed) *discordgo.MessageEmbed {
	embed := &discordgo.MessageEmbed{
		Title:       e.Title,
		Description: e.Description,
		URL:         e.URL,
		Color:       e.Color,
		Timestamp:   e.Timestamp,
	}

	for _, field := range e.Fields {
		embed.Fields = append(embed.Fields, &discordgo.MessageEmbedField{
			Name:   field.Name,
			Value:  field.Value,
			Inline: field.Inline,
		})
	}

	if e.Footer != nil {
		embed.Footer = &discordgo.MessageEmbedFooter{
			Text:    e.Footer.Text,
			IconURL: e.Footer.IconURL,
		}
	}

	return embed
}
