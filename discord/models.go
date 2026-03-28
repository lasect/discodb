package discord

import "discodb/types"

type ChannelType string

const (
	ChannelTypeGuildText          ChannelType = "guild_text"
	ChannelTypeGuildVoice         ChannelType = "guild_voice"
	ChannelTypeGuildCategory      ChannelType = "guild_category"
	ChannelTypeGuildNews          ChannelType = "guild_news"
	ChannelTypeGuildStore         ChannelType = "guild_store"
	ChannelTypeGuildThread        ChannelType = "guild_thread"
	ChannelTypeGuildNewsThread    ChannelType = "guild_news_thread"
	ChannelTypeGuildPrivateThread ChannelType = "guild_private_thread"
	ChannelTypeGuildPublicThread  ChannelType = "guild_public_thread"
	ChannelTypeGuildStageVoice    ChannelType = "guild_stage_voice"
	ChannelTypeGuildForum         ChannelType = "guild_forum"
)

type Channel struct {
	ID       types.ChannelID  `json:"id"`
	GuildID  *types.GuildID   `json:"guild_id,omitempty"`
	Name     string           `json:"name"`
	Kind     ChannelType      `json:"kind"`
	ParentID *types.ChannelID `json:"parent_id,omitempty"`
	Topic    *string          `json:"topic,omitempty"`
	Position *int32           `json:"position,omitempty"`
}
