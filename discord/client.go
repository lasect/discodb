package discord

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"discodb/types"

	"github.com/bwmarrin/discordgo"
)

// Client wraps discordgo.Session and provides typed operations for discodb.
// It handles type conversion between discordgo types and discodb internal types,
// normalizes errors, and provides context-aware operations.
type Client struct {
	session *discordgo.Session
	backend Transport
	logger  *slog.Logger

	// Default request timeout
	timeout time.Duration

	// Maximum retries for transient failures
	maxRetries int
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithLogger sets the logger for the client.
func WithLogger(logger *slog.Logger) ClientOption {
	return func(c *Client) {
		c.logger = logger
	}
}

// WithTimeout sets the default request timeout.
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// WithMaxRetries sets the maximum number of retries for transient failures.
func WithMaxRetries(maxRetries int) ClientOption {
	return func(c *Client) {
		c.maxRetries = maxRetries
	}
}

// NewClient creates a new Discord client with the given bot token.
// The token should NOT include the "Bot " prefix - it will be added automatically.
func NewClient(token string, opts ...ClientOption) (*Client, error) {
	if token == "" {
		return nil, fmt.Errorf("discord: token is required")
	}

	// Create discordgo session with Bot prefix
	session, err := discordgo.New("Bot " + token)
	if err != nil {
		return nil, fmt.Errorf("discord: create session: %w", err)
	}

	// Configure session for REST-only usage (no gateway/websocket)
	session.ShouldReconnectOnError = false
	session.StateEnabled = false

	client := &Client{
		session:    session,
		backend:    newDiscordgoTransport(session),
		logger:     slog.Default(),
		timeout:    30 * time.Second,
		maxRetries: 3,
	}

	for _, opt := range opts {
		opt(client)
	}

	return client, nil
}

// NewClientWithTransport creates a client backed by a custom transport.
// This is primarily used for deterministic tests without live Discord I/O.
func NewClientWithTransport(backend Transport, opts ...ClientOption) (*Client, error) {
	if backend == nil {
		return nil, fmt.Errorf("discord: backend is required")
	}

	client := &Client{
		backend:    backend,
		logger:     slog.Default(),
		timeout:    30 * time.Second,
		maxRetries: 3,
	}

	for _, opt := range opts {
		opt(client)
	}

	return client, nil
}

// Session returns the underlying discordgo session for advanced operations.
// Use sparingly - prefer the typed methods on Client.
func (c *Client) Session() *discordgo.Session {
	return c.session
}

// Close closes the Discord client connection.
func (c *Client) Close() error {
	if c.session == nil {
		return nil
	}
	return c.session.Close()
}

// parseSnowflake converts a Discord snowflake string to uint64.
func parseSnowflake(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}

// formatSnowflake converts a uint64 to a Discord snowflake string.
func formatSnowflake(id uint64) string {
	return strconv.FormatUint(id, 10)
}

// guildIDToString converts a types.GuildID to a discordgo guild ID string.
func guildIDToString(id types.GuildID) string {
	return formatSnowflake(id.Uint64())
}

// channelIDToString converts a types.ChannelID to a discordgo channel ID string.
func channelIDToString(id types.ChannelID) string {
	return formatSnowflake(id.Uint64())
}

// messageIDToString converts a types.MessageID to a discordgo message ID string.
func messageIDToString(id types.MessageID) string {
	return formatSnowflake(id.Uint64())
}

// stringToGuildID converts a discordgo guild ID string to types.GuildID.
func stringToGuildID(s string) (types.GuildID, error) {
	id, err := parseSnowflake(s)
	if err != nil {
		return 0, fmt.Errorf("parse guild id %q: %w", s, err)
	}
	return types.NewGuildID(id)
}

// stringToChannelID converts a discordgo channel ID string to types.ChannelID.
func stringToChannelID(s string) (types.ChannelID, error) {
	id, err := parseSnowflake(s)
	if err != nil {
		return 0, fmt.Errorf("parse channel id %q: %w", s, err)
	}
	return types.NewChannelID(id)
}

// stringToMessageID converts a discordgo message ID string to types.MessageID.
func stringToMessageID(s string) (types.MessageID, error) {
	id, err := parseSnowflake(s)
	if err != nil {
		return 0, fmt.Errorf("parse message id %q: %w", s, err)
	}
	return types.NewMessageID(id)
}

// requestOption wraps discordgo.RequestOption with context support.
func (c *Client) requestOption(ctx context.Context) []discordgo.RequestOption {
	return []discordgo.RequestOption{
		discordgo.WithContext(ctx),
	}
}

// withRetry executes fn with retries for transient failures.
func (c *Client) withRetry(ctx context.Context, op string, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			// Calculate backoff with exponential delay
			delay := time.Duration(1<<uint(attempt-1)) * 100 * time.Millisecond

			// For rate limits, use the retry-after duration
			if retryAfter := RetryAfter(lastErr); retryAfter > 0 {
				delay = retryAfter
			}

			c.logger.Debug("retrying after error",
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

func (c *Client) normalizeError(op string, err error) error {
	if err == nil {
		return nil
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return err
	}
	if _, ok := c.backend.(*discordgoTransport); ok {
		return wrapError(op, err)
	}
	return err
}
