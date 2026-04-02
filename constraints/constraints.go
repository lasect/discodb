package constraints

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrContentTooLarge    = errors.New("content exceeds 2000 chars")
	ErrEmbedsTooLarge     = errors.New("embeds exceed 6000 chars total")
	ErrTooManyEmbeds      = errors.New("exceeds 10 embeds per message")
	ErrEmbedFieldTooLarge = errors.New("embed field exceeds size limit")
	ErrOverflowRequired   = errors.New("value requires TOAST overflow")
	ErrTooManyRoles       = errors.New("exceeds 250 roles per guild")
	ErrTooManyChannels    = errors.New("exceeds 500 channels per guild")
	ErrTooManyPins        = errors.New("exceeds 50 pins per channel")
	ErrTooManyReactions   = errors.New("exceeds 20 reactions per message")
)

const (
	MaxMessageContent   = 2000
	MaxEmbedsPerMessage = 10
	MaxEmbedTotalChars  = 6000
	MaxEmbedDescription = 4096
	MaxEmbedFields      = 25
	MaxEmbedFieldName   = 256
	MaxEmbedFieldValue  = 1024
	MaxMessagesPerFetch = 100
	MaxRolesPerGuild    = 250
	MaxChannelsPerGuild = 500
	MaxPinsPerChannel   = 50
	MaxReactionsPerMsg  = 20
)

const (
	MaxRowHeaderEncoded    = 100
	MaxRowBodyInline       = 5800
	ToastChunkSize         = 1500
	MaxChannelsPerCategory = 50
)

// DiscordAPIConstraint is the interface for Discord API constraints
type DiscordAPIConstraint struct {
	Name        string
	MaxValue    int
	Current     int
	Description string
}

// ValidateMessageContent checks if content fits Discord's limit
func ValidateMessageContent(content string) error {
	if len(content) > MaxMessageContent {
		return fmt.Errorf("%w: %d > %d", ErrContentTooLarge, len(content), MaxMessageContent)
	}
	return nil
}

// ValidateEmbedDescription checks if description fits limit
func ValidateEmbedDescription(desc string) error {
	if len(desc) > MaxEmbedDescription {
		return fmt.Errorf("description too large: %d > %d", len(desc), MaxEmbedDescription)
	}
	return nil
}

// ValidateEmbedField checks field name and value limits
func ValidateEmbedField(name, value string) error {
	if len(name) > MaxEmbedFieldName {
		return fmt.Errorf("%w: field name %d > %d", ErrEmbedFieldTooLarge, len(name), MaxEmbedFieldName)
	}
	if len(value) > MaxEmbedFieldValue {
		return fmt.Errorf("%w: field value %d > %d", ErrEmbedFieldTooLarge, len(value), MaxEmbedFieldValue)
	}
	return nil
}

// ValidateEmbeds checks total embed size
func ValidateEmbeds(embeds []Embed) error {
	if len(embeds) > MaxEmbedsPerMessage {
		return fmt.Errorf("%w: %d > %d", ErrTooManyEmbeds, len(embeds), MaxEmbedsPerMessage)
	}

	total := 0
	for i, e := range embeds {
		size := EmbedSize(e)
		total += size
		if size > MaxEmbedDescription {
			return fmt.Errorf("embed %d exceeds max description: %d > %d", i, size, MaxEmbedDescription)
		}
	}

	if total > MaxEmbedTotalChars {
		return fmt.Errorf("%w: %d > %d", ErrEmbedsTooLarge, total, MaxEmbedTotalChars)
	}
	return nil
}

// Embed represents a simplified Discord embed for validation
type Embed struct {
	Title       string  `json:"title,omitempty"`
	Description string  `json:"description,omitempty"`
	URL         string  `json:"url,omitempty"`
	Fields      []Field `json:"fields,omitempty"`
	Footer      *Footer `json:"footer,omitempty"`
}

type Field struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Footer struct {
	Text string `json:"text"`
}

// EmbedSize calculates total character count
func EmbedSize(e Embed) int {
	size := len(e.Title) + len(e.Description) + len(e.URL)
	if e.Footer != nil {
		size += len(e.Footer.Text)
	}
	for _, f := range e.Fields {
		size += len(f.Name) + len(f.Value)
	}
	return size
}

// ValidateRowFits checks if encoded row fits message limits
func ValidateRowFits(headerSize, bodySize int) error {
	if headerSize > MaxRowHeaderEncoded {
		return fmt.Errorf("header too large: %d > %d", headerSize, MaxRowHeaderEncoded)
	}
	if bodySize > MaxRowBodyInline {
		return fmt.Errorf("%w: body %d > %d", ErrOverflowRequired, bodySize, MaxRowBodyInline)
	}
	return nil
}

// NeedsOverflow determines if data requires TOAST storage
func NeedsOverflow(encodedSize int) bool {
	return encodedSize > MaxRowBodyInline
}

// ComputeToastChunks calculates how many chunks needed
func ComputeToastChunks(dataSize int) int {
	if dataSize <= 0 {
		return 0
	}
	chunks := dataSize / ToastChunkSize
	if dataSize%ToastChunkSize > 0 {
		chunks++
	}
	return chunks
}

// ValidateRolesCount checks guild role limit
func ValidateRolesCount(count int) error {
	if count > MaxRolesPerGuild {
		return fmt.Errorf("%w: %d > %d", ErrTooManyRoles, count, MaxRolesPerGuild)
	}
	return nil
}

// ValidateChannelsCount checks guild channel limit
func ValidateChannelsCount(count int) error {
	if count > MaxChannelsPerGuild {
		return fmt.Errorf("%w: %d > %d", ErrTooManyChannels, count, MaxChannelsPerGuild)
	}
	return nil
}

// ValidatePinsCount checks channel pin limit
func ValidatePinsCount(count int) error {
	if count > MaxPinsPerChannel {
		return fmt.Errorf("%w: %d > %d", ErrTooManyPins, count, MaxPinsPerChannel)
	}
	return nil
}

// ValidateReactionsCount checks message reaction limit
func ValidateReactionsCount(count int) error {
	if count > MaxReactionsPerMsg {
		return fmt.Errorf("%w: %d > %d", ErrTooManyReactions, count, MaxReactionsPerMsg)
	}
	return nil
}

// ValidateAttachmentImmutable checks attachment replacement constraint
// Attachments must be re-specified on edit - they cannot be incrementally modified
func ValidateAttachmentImmutable(isEdit bool, newAttachments []string) error {
	if isEdit && len(newAttachments) == 0 {
		return errors.New("attachments must be re-specified on message edit")
	}
	return nil
}

// AllLimits returns a summary of all Discord limits for documentation
func AllLimits() map[string]int {
	return map[string]int{
		"message_content":       MaxMessageContent,
		"embeds_per_message":    MaxEmbedsPerMessage,
		"embed_total_chars":     MaxEmbedTotalChars,
		"embed_description":     MaxEmbedDescription,
		"embed_fields":          MaxEmbedFields,
		"embed_field_name":      MaxEmbedFieldName,
		"embed_field_value":     MaxEmbedFieldValue,
		"messages_per_fetch":    MaxMessagesPerFetch,
		"roles_per_guild":       MaxRolesPerGuild,
		"channels_per_guild":    MaxChannelsPerGuild,
		"pins_per_channel":      MaxPinsPerChannel,
		"reactions_per_message": MaxReactionsPerMsg,
		"toast_chunk_size":      ToastChunkSize,
		"channels_per_category": MaxChannelsPerCategory,
	}
}

// JSONSize estimates the byte size of a value when JSON encoded
func JSONSize(v interface{}) (int, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

// SafeJSONSize returns JSON size or 0 on error
func SafeJSONSize(v interface{}) int {
	size, _ := JSONSize(v)
	return size
}
