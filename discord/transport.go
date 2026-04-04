package discord

import (
	"context"

	"github.com/bwmarrin/discordgo"
)

// Transport is the minimal Discord API surface required by core discodb paths.
type Transport interface {
	GetChannel(ctx context.Context, channelID string) (*Channel, error)
	ListGuildChannels(ctx context.Context, guildID string) ([]*Channel, error)
	CreateChannel(ctx context.Context, guildID string, params ChannelCreateParams) (*Channel, error)
	EditChannel(ctx context.Context, channelID string, params ChannelEditParams) (*Channel, error)

	SendMessage(ctx context.Context, channelID string, params MessageSendParams) (*Message, error)
	GetMessage(ctx context.Context, channelID, messageID string) (*Message, error)
	ListMessages(ctx context.Context, channelID string, limit int, beforeID, afterID, aroundID string) ([]*Message, error)
	EditMessage(ctx context.Context, channelID, messageID string, params MessageEditParams) (*Message, error)

	PinMessage(ctx context.Context, channelID, messageID string) error
	UnpinMessage(ctx context.Context, channelID, messageID string) error
	ListPinnedMessages(ctx context.Context, channelID string) ([]*Message, error)
}

type discordgoTransport struct {
	session *discordgo.Session
}

func newDiscordgoTransport(session *discordgo.Session) Transport {
	return &discordgoTransport{session: session}
}

func (t *discordgoTransport) GetChannel(ctx context.Context, channelID string) (*Channel, error) {
	ch, err := t.session.Channel(channelID, discordgo.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	return channelFromDiscordgo(ch)
}

func (t *discordgoTransport) ListGuildChannels(ctx context.Context, guildID string) ([]*Channel, error) {
	channels, err := t.session.GuildChannels(guildID, discordgo.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	results := make([]*Channel, 0, len(channels))
	for _, ch := range channels {
		channel, convErr := channelFromDiscordgo(ch)
		if convErr != nil {
			continue
		}
		results = append(results, channel)
	}
	return results, nil
}

func (t *discordgoTransport) CreateChannel(ctx context.Context, guildID string, params ChannelCreateParams) (*Channel, error) {
	data := discordgo.GuildChannelCreateData{
		Name:     params.Name,
		Type:     discordgo.ChannelType(params.Type),
		Topic:    params.Topic,
		Position: params.Position,
	}
	if params.ParentID != nil {
		data.ParentID = channelIDToString(*params.ParentID)
	}

	ch, err := t.session.GuildChannelCreateComplex(guildID, data, discordgo.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	return channelFromDiscordgo(ch)
}

func (t *discordgoTransport) EditChannel(ctx context.Context, channelID string, params ChannelEditParams) (*Channel, error) {
	edit := &discordgo.ChannelEdit{}
	if params.Name != nil {
		edit.Name = *params.Name
	}
	if params.Topic != nil {
		edit.Topic = *params.Topic
	}
	if params.Position != nil {
		edit.Position = params.Position
	}
	if params.ParentID != nil {
		parentStr := channelIDToString(*params.ParentID)
		edit.ParentID = parentStr
	}

	ch, err := t.session.ChannelEditComplex(channelID, edit, discordgo.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	return channelFromDiscordgo(ch)
}

func (t *discordgoTransport) SendMessage(ctx context.Context, channelID string, params MessageSendParams) (*Message, error) {
	data := &discordgo.MessageSend{Content: params.Content}

	for _, e := range params.Embeds {
		data.Embeds = append(data.Embeds, embedToDiscordgo(e))
	}

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
		data.AllowedMentions = &discordgo.MessageAllowedMentions{Parse: []discordgo.AllowedMentionType{}}
	}

	if params.File != nil {
		data.Files = []*discordgo.File{{Name: params.File.Name, Reader: params.File.Reader}}
	}

	msg, err := t.session.ChannelMessageSendComplex(channelID, data, discordgo.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	return messageFromDiscordgo(msg)
}

func (t *discordgoTransport) GetMessage(ctx context.Context, channelID, messageID string) (*Message, error) {
	msg, err := t.session.ChannelMessage(channelID, messageID, discordgo.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	return messageFromDiscordgo(msg)
}

func (t *discordgoTransport) ListMessages(ctx context.Context, channelID string, limit int, beforeID, afterID, aroundID string) ([]*Message, error) {
	messages, err := t.session.ChannelMessages(channelID, limit, beforeID, afterID, aroundID, discordgo.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	results := make([]*Message, 0, len(messages))
	for _, msg := range messages {
		m, convErr := messageFromDiscordgo(msg)
		if convErr != nil {
			continue
		}
		results = append(results, m)
	}
	return results, nil
}

func (t *discordgoTransport) EditMessage(ctx context.Context, channelID, messageID string, params MessageEditParams) (*Message, error) {
	edit := &discordgo.MessageEdit{
		Channel: channelID,
		ID:      messageID,
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
	edit.AllowedMentions = &discordgo.MessageAllowedMentions{Parse: []discordgo.AllowedMentionType{}}

	msg, err := t.session.ChannelMessageEditComplex(edit, discordgo.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	return messageFromDiscordgo(msg)
}

func (t *discordgoTransport) PinMessage(ctx context.Context, channelID, messageID string) error {
	return t.session.ChannelMessagePin(channelID, messageID, discordgo.WithContext(ctx))
}

func (t *discordgoTransport) UnpinMessage(ctx context.Context, channelID, messageID string) error {
	return t.session.ChannelMessageUnpin(channelID, messageID, discordgo.WithContext(ctx))
}

func (t *discordgoTransport) ListPinnedMessages(ctx context.Context, channelID string) ([]*Message, error) {
	messages, err := t.session.ChannelMessagesPinned(channelID, discordgo.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	results := make([]*Message, 0, len(messages))
	for _, msg := range messages {
		m, convErr := messageFromDiscordgo(msg)
		if convErr != nil {
			continue
		}
		results = append(results, m)
	}
	return results, nil
}
