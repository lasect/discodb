package discord

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"discodb/types"
)

// FakeTransport is an in-memory Discord transport used for tests.
type FakeTransport struct {
	mu sync.Mutex

	nextChannelID types.ChannelID
	nextMessageID types.MessageID

	channelsByGuild map[types.GuildID][]*Channel
	channelByID     map[types.ChannelID]*Channel
	messagesByCh    map[types.ChannelID][]*Message
}

func NewFakeTransport() *FakeTransport {
	return &FakeTransport{
		nextChannelID:   1000,
		nextMessageID:   100000,
		channelsByGuild: make(map[types.GuildID][]*Channel),
		channelByID:     make(map[types.ChannelID]*Channel),
		messagesByCh:    make(map[types.ChannelID][]*Message),
	}
}

func (f *FakeTransport) nextChannel() types.ChannelID {
	f.nextChannelID++
	return f.nextChannelID
}

func (f *FakeTransport) nextMessage() types.MessageID {
	f.nextMessageID++
	return f.nextMessageID
}

func (f *FakeTransport) GetChannel(ctx context.Context, channelID string) (*Channel, error) {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()

	id, err := parseChannelID(channelID)
	if err != nil {
		return nil, err
	}

	ch, ok := f.channelByID[id]
	if !ok {
		return nil, fmt.Errorf("%w: channel %q", ErrChannelNotFound, channelID)
	}
	return cloneChannel(ch), nil
}

func (f *FakeTransport) ListGuildChannels(ctx context.Context, guildID string) ([]*Channel, error) {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()

	id, err := parseGuildID(guildID)
	if err != nil {
		return nil, err
	}

	src := f.channelsByGuild[id]
	out := make([]*Channel, 0, len(src))
	for _, ch := range src {
		out = append(out, cloneChannel(ch))
	}
	return out, nil
}

func (f *FakeTransport) CreateChannel(ctx context.Context, guildID string, params ChannelCreateParams) (*Channel, error) {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()

	id, err := parseGuildID(guildID)
	if err != nil {
		return nil, err
	}

	ch := &Channel{
		ID:       f.nextChannel(),
		GuildID:  id,
		Name:     params.Name,
		Type:     params.Type,
		ParentID: cloneChannelID(params.ParentID),
		Topic:    params.Topic,
		Position: params.Position,
	}

	f.channelByID[ch.ID] = ch
	f.channelsByGuild[id] = append(f.channelsByGuild[id], ch)
	return cloneChannel(ch), nil
}

func (f *FakeTransport) EditChannel(ctx context.Context, channelID string, params ChannelEditParams) (*Channel, error) {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()

	id, err := parseChannelID(channelID)
	if err != nil {
		return nil, err
	}

	ch, ok := f.channelByID[id]
	if !ok {
		return nil, fmt.Errorf("%w: channel %q", ErrChannelNotFound, channelID)
	}

	if params.Name != nil {
		ch.Name = *params.Name
	}
	if params.Topic != nil {
		ch.Topic = *params.Topic
	}
	if params.Position != nil {
		ch.Position = *params.Position
	}
	if params.ParentID != nil {
		ch.ParentID = cloneChannelID(params.ParentID)
	}

	return cloneChannel(ch), nil
}

func (f *FakeTransport) SendMessage(ctx context.Context, channelID string, params MessageSendParams) (*Message, error) {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()

	chID, err := parseChannelID(channelID)
	if err != nil {
		return nil, err
	}

	ch, ok := f.channelByID[chID]
	if !ok {
		return nil, fmt.Errorf("%w: channel %q", ErrChannelNotFound, channelID)
	}

	msg := &Message{
		ID:          f.nextMessage(),
		ChannelID:   chID,
		GuildID:     ch.GuildID,
		Content:     params.Content,
		Timestamp:   time.Now().UTC(),
		Embeds:      cloneEmbeds(params.Embeds),
		Attachments: nil,
	}

	f.messagesByCh[chID] = append(f.messagesByCh[chID], msg)
	return cloneMessage(msg), nil
}

func (f *FakeTransport) GetMessage(ctx context.Context, channelID, messageID string) (*Message, error) {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()

	chID, err := parseChannelID(channelID)
	if err != nil {
		return nil, err
	}
	msgID, err := parseMessageID(messageID)
	if err != nil {
		return nil, err
	}

	for _, msg := range f.messagesByCh[chID] {
		if msg.ID == msgID {
			return cloneMessage(msg), nil
		}
	}
	return nil, fmt.Errorf("%w: message %q", ErrMessageNotFound, messageID)
}

func (f *FakeTransport) ListMessages(ctx context.Context, channelID string, limit int, beforeID, afterID, aroundID string) ([]*Message, error) {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()

	chID, err := parseChannelID(channelID)
	if err != nil {
		return nil, err
	}

	if limit <= 0 || limit > 100 {
		limit = 100
	}

	var before, after, around types.MessageID
	if beforeID != "" {
		before, err = parseMessageID(beforeID)
		if err != nil {
			return nil, err
		}
	}
	if afterID != "" {
		after, err = parseMessageID(afterID)
		if err != nil {
			return nil, err
		}
	}
	if aroundID != "" {
		around, err = parseMessageID(aroundID)
		if err != nil {
			return nil, err
		}
		_ = around
	}

	src := f.messagesByCh[chID]
	out := make([]*Message, 0, len(src))
	for i := len(src) - 1; i >= 0; i-- {
		msg := src[i]
		if before != 0 && msg.ID >= before {
			continue
		}
		if after != 0 && msg.ID <= after {
			continue
		}
		out = append(out, cloneMessage(msg))
		if len(out) >= limit {
			break
		}
	}

	return out, nil
}

func (f *FakeTransport) EditMessage(ctx context.Context, channelID, messageID string, params MessageEditParams) (*Message, error) {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()

	chID, err := parseChannelID(channelID)
	if err != nil {
		return nil, err
	}
	msgID, err := parseMessageID(messageID)
	if err != nil {
		return nil, err
	}

	for _, msg := range f.messagesByCh[chID] {
		if msg.ID != msgID {
			continue
		}
		if params.Content != nil {
			msg.Content = *params.Content
		}
		if len(params.Embeds) > 0 {
			msg.Embeds = cloneEmbeds(params.Embeds)
		}
		now := time.Now().UTC()
		msg.EditedAt = &now
		return cloneMessage(msg), nil
	}

	return nil, fmt.Errorf("%w: message %q", ErrMessageNotFound, messageID)
}

func (f *FakeTransport) PinMessage(ctx context.Context, channelID, messageID string) error {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()

	chID, err := parseChannelID(channelID)
	if err != nil {
		return err
	}
	msgID, err := parseMessageID(messageID)
	if err != nil {
		return err
	}

	for _, msg := range f.messagesByCh[chID] {
		if msg.ID == msgID {
			msg.Pinned = true
			return nil
		}
	}
	return fmt.Errorf("%w: message %q", ErrMessageNotFound, messageID)
}

func (f *FakeTransport) UnpinMessage(ctx context.Context, channelID, messageID string) error {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()

	chID, err := parseChannelID(channelID)
	if err != nil {
		return err
	}
	msgID, err := parseMessageID(messageID)
	if err != nil {
		return err
	}

	for _, msg := range f.messagesByCh[chID] {
		if msg.ID == msgID {
			msg.Pinned = false
			return nil
		}
	}
	return fmt.Errorf("%w: message %q", ErrMessageNotFound, messageID)
}

func (f *FakeTransport) ListPinnedMessages(ctx context.Context, channelID string) ([]*Message, error) {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()

	chID, err := parseChannelID(channelID)
	if err != nil {
		return nil, err
	}

	var out []*Message
	for _, msg := range f.messagesByCh[chID] {
		if msg.Pinned {
			out = append(out, cloneMessage(msg))
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ID > out[j].ID
	})
	return out, nil
}

func parseGuildID(v string) (types.GuildID, error) {
	raw, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse guild id %q: %w", v, err)
	}
	return types.NewGuildID(raw)
}

func parseChannelID(v string) (types.ChannelID, error) {
	raw, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse channel id %q: %w", v, err)
	}
	return types.NewChannelID(raw)
}

func parseMessageID(v string) (types.MessageID, error) {
	raw, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse message id %q: %w", v, err)
	}
	return types.NewMessageID(raw)
}

func cloneChannelID(v *types.ChannelID) *types.ChannelID {
	if v == nil {
		return nil
	}
	id := *v
	return &id
}

func cloneChannel(ch *Channel) *Channel {
	if ch == nil {
		return nil
	}
	out := *ch
	out.ParentID = cloneChannelID(ch.ParentID)
	out.AvailableTags = append([]ForumTag(nil), ch.AvailableTags...)
	return &out
}

func cloneEmbeds(in []Embed) []Embed {
	out := make([]Embed, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].Fields = append([]EmbedField(nil), in[i].Fields...)
		if in[i].Footer != nil {
			footer := *in[i].Footer
			out[i].Footer = &footer
		}
	}
	return out
}

func cloneAttachments(in []Attachment) []Attachment {
	return append([]Attachment(nil), in...)
}

func cloneReactions(in []Reaction) []Reaction {
	return append([]Reaction(nil), in...)
}

func cloneMessage(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	out := *msg
	if msg.EditedAt != nil {
		edited := *msg.EditedAt
		out.EditedAt = &edited
	}
	out.Embeds = cloneEmbeds(msg.Embeds)
	out.Attachments = cloneAttachments(msg.Attachments)
	out.Reactions = cloneReactions(msg.Reactions)
	out.ThreadID = cloneChannelID(msg.ThreadID)
	return &out
}
