package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"discodb/types"
)

const webhookAPIBase = "https://discord.com/api/v10"

// WebhookClient executes Discord webhooks for low-latency message operations.
// Webhooks do not require a bot token and have separate rate limit buckets.
type WebhookClient struct {
	id        string
	token     string
	channelID types.ChannelID

	httpClient *http.Client
	logger     *slog.Logger
	timeout    time.Duration
	maxRetries int
}

// WebhookClientOption configures a WebhookClient.
type WebhookClientOption func(*WebhookClient)

// WithWebhookLogger sets the logger.
func WithWebhookLogger(logger *slog.Logger) WebhookClientOption {
	return func(c *WebhookClient) {
		c.logger = logger
	}
}

// WithWebhookTimeout sets the request timeout.
func WithWebhookTimeout(timeout time.Duration) WebhookClientOption {
	return func(c *WebhookClient) {
		c.timeout = timeout
	}
}

// WithWebhookMaxRetries sets the max retries.
func WithWebhookMaxRetries(maxRetries int) WebhookClientOption {
	return func(c *WebhookClient) {
		c.maxRetries = maxRetries
	}
}

// NewWebhookClient creates a client for executing a specific webhook.
func NewWebhookClient(id, token string, channelID types.ChannelID, opts ...WebhookClientOption) *WebhookClient {
	c := &WebhookClient{
		id:         id,
		token:      token,
		channelID:  channelID,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		logger:     slog.Default(),
		timeout:    30 * time.Second,
		maxRetries: 3,
	}
	for _, opt := range opts {
		opt(c)
	}
	// Apply timeout to http client
	c.httpClient.Timeout = c.timeout
	return c
}

// ID returns the webhook ID.
func (wc *WebhookClient) ID() string { return wc.id }

// Token returns the webhook token.
func (wc *WebhookClient) Token() string { return wc.token }

// ChannelID returns the channel this webhook posts to.
func (wc *WebhookClient) ChannelID() types.ChannelID { return wc.channelID }

// webhookSendBody is the JSON body for executing a webhook.
type webhookSendBody struct {
	Content         string               `json:"content,omitempty"`
	Username        string               `json:"username,omitempty"`
	AvatarURL       string               `json:"avatar_url,omitempty"`
	TTS             bool                 `json:"tts,omitempty"`
	Embeds          []webhookEmbed       `json:"embeds,omitempty"`
	AllowedMentions *webhookAllowedMents `json:"allowed_mentions,omitempty"`
	Components      []json.RawMessage    `json:"components,omitempty"`
	Attachments     []webhookAttachment  `json:"attachments,omitempty"`
	Flags           int                  `json:"flags,omitempty"`
}

type webhookEmbed struct {
	Title       string              `json:"title,omitempty"`
	Description string              `json:"description,omitempty"`
	URL         string              `json:"url,omitempty"`
	Color       int                 `json:"color,omitempty"`
	Footer      *webhookEmbedFooter `json:"footer,omitempty"`
	Image       *webhookEmbedImage  `json:"image,omitempty"`
	Thumbnail   *webhookEmbedImage  `json:"thumbnail,omitempty"`
	Fields      []webhookEmbedField `json:"fields,omitempty"`
	Timestamp   string              `json:"timestamp,omitempty"`
}

type webhookEmbedFooter struct {
	Text    string `json:"text"`
	IconURL string `json:"icon_url,omitempty"`
}

type webhookEmbedImage struct {
	URL string `json:"url"`
}

type webhookEmbedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline,omitempty"`
}

type webhookAllowedMents struct {
	Parse       []string `json:"parse,omitempty"`
	Users       []string `json:"users,omitempty"`
	Roles       []string `json:"roles,omitempty"`
	RepliedUser bool     `json:"replied_user,omitempty"`
}

type webhookAttachment struct {
	ID          string `json:"id"`
	Filename    string `json:"filename"`
	Description string `json:"description,omitempty"`
}

// webhookEditBody is the JSON body for editing a webhook message.
type webhookEditBody struct {
	Content         *string              `json:"content"`
	Embeds          []webhookEmbed       `json:"embeds,omitempty"`
	AllowedMentions *webhookAllowedMents `json:"allowed_mentions,omitempty"`
	Components      []json.RawMessage    `json:"components,omitempty"`
	Attachments     []webhookAttachment  `json:"attachments,omitempty"`
	Flags           *int                 `json:"flags,omitempty"`
}

// webhookMessage is the JSON response from Discord for a webhook message.
type webhookMessage struct {
	ID              string            `json:"id"`
	ChannelID       string            `json:"channel_id"`
	GuildID         string            `json:"guild_id,omitempty"`
	Content         string            `json:"content"`
	Timestamp       string            `json:"timestamp"`
	EditedTimestamp *string           `json:"edited_timestamp,omitempty"`
	Pinned          bool              `json:"pinned"`
	Author          *webhookAuthor    `json:"author,omitempty"`
	Embeds          []webhookEmbed    `json:"embeds,omitempty"`
	Attachments     []webhookAttach   `json:"attachments,omitempty"`
	Reactions       []webhookReaction `json:"reactions,omitempty"`
}

type webhookAuthor struct {
	ID       string `json:"id"`
	Username string `json:"username"`
	Avatar   string `json:"avatar,omitempty"`
}

type webhookAttach struct {
	ID          string `json:"id"`
	Filename    string `json:"filename"`
	URL         string `json:"url"`
	ProxyURL    string `json:"proxy_url"`
	Size        int    `json:"size"`
	ContentType string `json:"content_type,omitempty"`
}

type webhookReaction struct {
	Count int  `json:"count"`
	Me    bool `json:"me"`
	Emoji struct {
		ID   string `json:"id,omitempty"`
		Name string `json:"name,omitempty"`
	} `json:"emoji"`
}

// SendWebhookMessage sends a message via the webhook.
// Returns the created message when wait=true (the default).
func (wc *WebhookClient) SendWebhookMessage(ctx context.Context, params MessageSendParams) (*Message, error) {
	const op = "SendWebhookMessage"

	body := webhookSendBody{
		Content: params.Content,
	}

	if params.AllowedMentions != nil {
		body.AllowedMentions = &webhookAllowedMents{
			Parse:       params.AllowedMentions.Parse,
			Users:       params.AllowedMentions.Users,
			Roles:       params.AllowedMentions.Roles,
			RepliedUser: params.AllowedMentions.RepliedUser,
		}
	}

	for _, e := range params.Embeds {
		body.Embeds = append(body.Embeds, embedToWebhookEmbed(e))
	}

	// Note: file uploads via webhooks require multipart/form-data.
	// For now, files are not supported through the webhook client.
	// The bot client should be used for blob/attachment operations.

	var result *Message
	err := wc.withRetry(ctx, op, func() error {
		msg, err := wc.doJSON(ctx, http.MethodPost, wc.executeURL(), body)
		if err != nil {
			return err
		}
		result = msg
		return nil
	})

	if err != nil {
		return nil, err
	}

	wc.logger.Debug("sent webhook message",
		"channel_id", wc.channelID,
		"message_id", result.ID,
		"content_len", len(params.Content),
	)

	return result, nil
}

// EditWebhookMessage edits a previously-sent webhook message.
func (wc *WebhookClient) EditWebhookMessage(ctx context.Context, messageID types.MessageID, params MessageEditParams) (*Message, error) {
	const op = "EditWebhookMessage"

	body := webhookEditBody{
		Content: params.Content,
	}

	body.AllowedMentions = &webhookAllowedMents{Parse: []string{}}

	for _, e := range params.Embeds {
		body.Embeds = append(body.Embeds, embedToWebhookEmbed(e))
	}

	var result *Message
	err := wc.withRetry(ctx, op, func() error {
		msg, err := wc.doJSON(ctx, http.MethodPatch, wc.editMessageURL(messageID), body)
		if err != nil {
			return err
		}
		result = msg
		return nil
	})

	if err != nil {
		return nil, err
	}

	wc.logger.Debug("edited webhook message",
		"channel_id", wc.channelID,
		"message_id", messageID,
	)

	return result, nil
}

// GetWebhookMessage retrieves a previously-sent webhook message.
func (wc *WebhookClient) GetWebhookMessage(ctx context.Context, messageID types.MessageID) (*Message, error) {
	const op = "GetWebhookMessage"

	var result *Message
	err := wc.withRetry(ctx, op, func() error {
		msg, err := wc.doJSON(ctx, http.MethodGet, wc.getMessageURL(messageID), nil)
		if err != nil {
			return err
		}
		result = msg
		return nil
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

// DeleteWebhookMessage deletes a message that was created by this webhook.
func (wc *WebhookClient) DeleteWebhookMessage(ctx context.Context, messageID types.MessageID) error {
	const op = "DeleteWebhookMessage"

	err := wc.withRetry(ctx, op, func() error {
		return wc.doNoContent(ctx, http.MethodDelete, wc.deleteMessageURL(messageID))
	})

	if err != nil {
		return err
	}

	wc.logger.Debug("deleted webhook message",
		"channel_id", wc.channelID,
		"message_id", messageID,
	)

	return nil
}

// executeURL returns the URL for executing the webhook.
func (wc *WebhookClient) executeURL() string {
	return fmt.Sprintf("%s/webhooks/%s/%s?wait=true", webhookAPIBase, wc.id, wc.token)
}

// editMessageURL returns the URL for editing a webhook message.
func (wc *WebhookClient) editMessageURL(messageID types.MessageID) string {
	return fmt.Sprintf("%s/webhooks/%s/%s/messages/%s", webhookAPIBase, wc.id, wc.token, messageIDToString(messageID))
}

// getMessageURL returns the URL for getting a webhook message.
func (wc *WebhookClient) getMessageURL(messageID types.MessageID) string {
	return fmt.Sprintf("%s/webhooks/%s/%s/messages/%s", webhookAPIBase, wc.id, wc.token, messageIDToString(messageID))
}

// deleteMessageURL returns the URL for deleting a webhook message.
func (wc *WebhookClient) deleteMessageURL(messageID types.MessageID) string {
	return fmt.Sprintf("%s/webhooks/%s/%s/messages/%s", webhookAPIBase, wc.id, wc.token, messageIDToString(messageID))
}

// doJSON performs an HTTP request and decodes the JSON response into a Message.
func (wc *WebhookClient) doJSON(ctx context.Context, method, url string, body any) (*Message, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal webhook body: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("create webhook request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := wc.httpClient.Do(req)
	if err != nil {
		return nil, &APIError{
			Op:      "webhook_http",
			Message: err.Error(),
			Err:     ErrTransport,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, wc.httpError(method, url, resp)
	}

	if method == http.MethodDelete {
		return nil, nil
	}

	var wm webhookMessage
	if err := json.NewDecoder(resp.Body).Decode(&wm); err != nil {
		return nil, fmt.Errorf("decode webhook response: %w", err)
	}

	return wc.toMessage(&wm), nil
}

// doNoContent performs an HTTP request expecting a 204 response.
func (wc *WebhookClient) doNoContent(ctx context.Context, method, url string) error {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return fmt.Errorf("create webhook request: %w", err)
	}

	resp, err := wc.httpClient.Do(req)
	if err != nil {
		return &APIError{
			Op:      "webhook_http",
			Message: err.Error(),
			Err:     ErrTransport,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return wc.httpError(method, url, resp)
	}

	return nil
}

// httpError constructs an APIError from an HTTP response.
func (wc *WebhookClient) httpError(method, url string, resp *http.Response) error {
	apiErr := &APIError{
		Op:         fmt.Sprintf("webhook_%s", method),
		StatusCode: resp.StatusCode,
	}

	// Try to read error body
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if len(body) > 0 {
		var errResp struct {
			Message string `json:"message"`
			Code    int    `json:"code"`
		}
		if json.Unmarshal(body, &errResp) == nil {
			apiErr.Message = errResp.Message
			apiErr.Code = errResp.Code
		} else {
			apiErr.Message = string(body)
		}
	}

	// Handle rate limits
	if resp.StatusCode == http.StatusTooManyRequests {
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			if secs, err := strconv.ParseFloat(retryAfter, 64); err == nil {
				apiErr.RetryAfter = time.Duration(secs * float64(time.Second))
			}
		}
		apiErr.Err = ErrRateLimited
	} else {
		switch resp.StatusCode {
		case http.StatusNotFound:
			apiErr.Err = ErrNotFound
		case http.StatusForbidden:
			apiErr.Err = ErrForbidden
		case http.StatusUnauthorized:
			apiErr.Err = ErrUnauthorized
		case http.StatusBadRequest:
			apiErr.Err = ErrBadRequest
		default:
			if resp.StatusCode >= 500 {
				apiErr.Err = ErrServerError
			} else {
				apiErr.Err = ErrTransport
			}
		}
	}

	return apiErr
}

// toMessage converts a webhookMessage to the internal Message type.
func (wc *WebhookClient) toMessage(wm *webhookMessage) *Message {
	msg := &Message{
		Content: wm.Content,
		Pinned:  wm.Pinned,
	}

	if wm.ID != "" {
		if id, err := stringToMessageID(wm.ID); err == nil {
			msg.ID = id
		}
	}
	if wm.ChannelID != "" {
		if id, err := stringToChannelID(wm.ChannelID); err == nil {
			msg.ChannelID = id
		}
	}
	if wm.GuildID != "" {
		if id, err := stringToGuildID(wm.GuildID); err == nil {
			msg.GuildID = id
		}
	}
	if wm.Timestamp != "" {
		if t, err := time.Parse(time.RFC3339, wm.Timestamp); err == nil {
			msg.Timestamp = t
		}
	}
	if wm.EditedTimestamp != nil && *wm.EditedTimestamp != "" {
		if t, err := time.Parse(time.RFC3339, *wm.EditedTimestamp); err == nil {
			msg.EditedAt = &t
		}
	}
	if wm.Author != nil {
		msg.AuthorID = wm.Author.ID
		msg.AuthorName = wm.Author.Username
	}

	for _, e := range wm.Embeds {
		msg.Embeds = append(msg.Embeds, webhookEmbedToEmbed(e))
	}
	for _, a := range wm.Attachments {
		msg.Attachments = append(msg.Attachments, Attachment{
			ID:          a.ID,
			Filename:    a.Filename,
			URL:         a.URL,
			ProxyURL:    a.ProxyURL,
			Size:        a.Size,
			ContentType: a.ContentType,
		})
	}
	for _, r := range wm.Reactions {
		emoji := r.Emoji.Name
		if r.Emoji.ID != "" {
			emoji = r.Emoji.ID
		}
		msg.Reactions = append(msg.Reactions, Reaction{
			Emoji: emoji,
			Count: r.Count,
			Me:    r.Me,
		})
	}

	return msg
}

// embedToWebhookEmbed converts an Embed to a webhookEmbed.
func embedToWebhookEmbed(e Embed) webhookEmbed {
	we := webhookEmbed{
		Title:       e.Title,
		Description: e.Description,
		URL:         e.URL,
		Color:       e.Color,
		Timestamp:   e.Timestamp,
	}
	for _, f := range e.Fields {
		we.Fields = append(we.Fields, webhookEmbedField{
			Name:   f.Name,
			Value:  f.Value,
			Inline: f.Inline,
		})
	}
	if e.Footer != nil {
		we.Footer = &webhookEmbedFooter{
			Text:    e.Footer.Text,
			IconURL: e.Footer.IconURL,
		}
	}
	return we
}

// webhookEmbedToEmbed converts a webhookEmbed to an Embed.
func webhookEmbedToEmbed(we webhookEmbed) Embed {
	e := Embed{
		Title:       we.Title,
		Description: we.Description,
		URL:         we.URL,
		Color:       we.Color,
		Timestamp:   we.Timestamp,
	}
	for _, f := range we.Fields {
		e.Fields = append(e.Fields, EmbedField{
			Name:   f.Name,
			Value:  f.Value,
			Inline: f.Inline,
		})
	}
	if we.Footer != nil {
		e.Footer = &EmbedFooter{
			Text:    we.Footer.Text,
			IconURL: we.Footer.IconURL,
		}
	}
	return e
}

// withRetry executes fn with retries for transient failures.
func (wc *WebhookClient) withRetry(ctx context.Context, op string, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= wc.maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(1<<uint(attempt-1)) * 100 * time.Millisecond
			if retryAfter := RetryAfter(lastErr); retryAfter > 0 {
				delay = retryAfter
			}
			wc.logger.Debug("retrying webhook after error",
				"op", op,
				"attempt", attempt,
				"delay", delay,
				"error", lastErr,
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !IsRetryable(lastErr) {
			return lastErr
		}
	}
	return fmt.Errorf("%s: max retries exceeded: %w", op, lastErr)
}
