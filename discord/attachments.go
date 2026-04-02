package discord

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"discodb/types"

	"github.com/bwmarrin/discordgo"
)

// BlobUploadParams contains parameters for uploading a blob.
type BlobUploadParams struct {
	Filename    string    // Filename for the attachment
	Content     []byte    // Blob content
	Reader      io.Reader // Alternative to Content - reader for streaming
	ContentType string    // MIME type (optional)
}

// BlobRef contains a reference to an uploaded blob.
type BlobRef struct {
	MessageID    types.MessageID
	ChannelID    types.ChannelID
	AttachmentID string
	URL          string
	ProxyURL     string
	Filename     string
	Size         int
	ContentType  string
}

// UploadBlob uploads a blob as a message attachment.
// This is used for BLOB storage in discodb.
func (c *Client) UploadBlob(ctx context.Context, channelID types.ChannelID, params BlobUploadParams) (*BlobRef, error) {
	const op = "UploadBlob"

	var reader io.Reader
	if params.Reader != nil {
		reader = params.Reader
	} else if params.Content != nil {
		reader = bytes.NewReader(params.Content)
	} else {
		return nil, fmt.Errorf("%s: either Content or Reader must be provided", op)
	}

	data := &discordgo.MessageSend{
		Content: "", // Empty content, just the file
		Files: []*discordgo.File{
			{
				Name:        params.Filename,
				ContentType: params.ContentType,
				Reader:      reader,
			},
		},
		AllowedMentions: &discordgo.MessageAllowedMentions{
			Parse: []discordgo.AllowedMentionType{},
		},
	}

	var result *BlobRef
	err := c.withRetry(ctx, op, func() error {
		msg, err := c.session.ChannelMessageSendComplex(
			channelIDToString(channelID),
			data,
			c.requestOption(ctx)...,
		)
		if err != nil {
			return wrapError(op, err)
		}

		if len(msg.Attachments) == 0 {
			return fmt.Errorf("%s: no attachment returned", op)
		}

		messageID, err := stringToMessageID(msg.ID)
		if err != nil {
			return err
		}

		att := msg.Attachments[0]
		result = &BlobRef{
			MessageID:    messageID,
			ChannelID:    channelID,
			AttachmentID: att.ID,
			URL:          att.URL,
			ProxyURL:     att.ProxyURL,
			Filename:     att.Filename,
			Size:         att.Size,
			ContentType:  att.ContentType,
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	c.logger.Debug("uploaded blob",
		"channel_id", channelID,
		"message_id", result.MessageID,
		"filename", result.Filename,
		"size", result.Size,
	)

	return result, nil
}

// DownloadBlob downloads a blob from its URL.
// Note: This uses HTTP directly, not the Discord API.
func (c *Client) DownloadBlob(ctx context.Context, url string) ([]byte, error) {
	const op = "DownloadBlob"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: create request: %w", op, err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: download: %w", op, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: unexpected status %d", op, resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%s: read body: %w", op, err)
	}

	return data, nil
}

// DownloadBlobFromRef downloads a blob using its reference.
func (c *Client) DownloadBlobFromRef(ctx context.Context, ref *BlobRef) ([]byte, error) {
	// Prefer proxy URL as it's more reliable
	url := ref.ProxyURL
	if url == "" {
		url = ref.URL
	}
	return c.DownloadBlob(ctx, url)
}

// DeleteBlob deletes a blob by deleting its containing message.
func (c *Client) DeleteBlob(ctx context.Context, ref *BlobRef) error {
	return c.DeleteMessage(ctx, ref.ChannelID, ref.MessageID)
}

// GetBlobFromMessage extracts blob reference from a message.
func (c *Client) GetBlobFromMessage(msg *Message) (*BlobRef, error) {
	if len(msg.Attachments) == 0 {
		return nil, fmt.Errorf("message has no attachments")
	}

	att := msg.Attachments[0]
	return &BlobRef{
		MessageID:    msg.ID,
		ChannelID:    msg.ChannelID,
		AttachmentID: att.ID,
		URL:          att.URL,
		ProxyURL:     att.ProxyURL,
		Filename:     att.Filename,
		Size:         att.Size,
		ContentType:  att.ContentType,
	}, nil
}

// BlobStore provides a higher-level interface for blob storage.
type BlobStore struct {
	client    *Client
	channelID types.ChannelID
}

// NewBlobStore creates a BlobStore for a specific channel.
func (c *Client) NewBlobStore(channelID types.ChannelID) *BlobStore {
	return &BlobStore{
		client:    c,
		channelID: channelID,
	}
}

// Put stores a blob and returns its reference.
func (bs *BlobStore) Put(ctx context.Context, filename string, data []byte) (*BlobRef, error) {
	return bs.client.UploadBlob(ctx, bs.channelID, BlobUploadParams{
		Filename: filename,
		Content:  data,
	})
}

// PutReader stores a blob from a reader.
func (bs *BlobStore) PutReader(ctx context.Context, filename string, reader io.Reader) (*BlobRef, error) {
	return bs.client.UploadBlob(ctx, bs.channelID, BlobUploadParams{
		Filename: filename,
		Reader:   reader,
	})
}

// Get retrieves a blob by its reference.
func (bs *BlobStore) Get(ctx context.Context, ref *BlobRef) ([]byte, error) {
	return bs.client.DownloadBlobFromRef(ctx, ref)
}

// Delete removes a blob.
func (bs *BlobStore) Delete(ctx context.Context, ref *BlobRef) error {
	return bs.client.DeleteBlob(ctx, ref)
}

// List returns all blob references in the store channel.
func (bs *BlobStore) List(ctx context.Context) ([]*BlobRef, error) {
	var refs []*BlobRef

	err := bs.client.ListAllMessages(ctx, bs.channelID, func(messages []*Message) error {
		for _, msg := range messages {
			if len(msg.Attachments) > 0 {
				ref, err := bs.client.GetBlobFromMessage(msg)
				if err == nil {
					refs = append(refs, ref)
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return refs, nil
}
