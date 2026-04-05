package mapping

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"discodb/types"
)

// Database represents a Discord guild mapped as a database
type Database struct {
	GuildID types.GuildID
	Name    string
}

// TableNamespace represents a category mapped as a table namespace
type TableNamespace struct {
	CategoryID types.ChannelID
	Name       string
	TableID    types.TableID
}

// Segment represents a text channel mapped as a storage segment
type Segment struct {
	ChannelID types.ChannelID
	TableID   types.TableID
	SegmentID types.SegmentID
	Name      string
}

// SegmentName generates the canonical channel name for a segment
func SegmentName(tableID types.TableID, segmentID types.SegmentID) string {
	return fmt.Sprintf("seg-%d-%d", tableID, segmentID)
}

// ParseSegmentName extracts table and segment IDs from channel name
func ParseSegmentName(name string) (types.TableID, types.SegmentID, error) {
	parts := strings.Split(name, "-")
	if len(parts) != 3 || parts[0] != "seg" {
		return 0, 0, fmt.Errorf("invalid segment name format: %s", name)
	}

	tableID, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse table ID: %w", err)
	}

	segmentID, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse segment ID: %w", err)
	}

	return types.TableID(tableID), types.SegmentID(segmentID), nil
}

// PageHeader represents metadata stored in channel topic
type PageHeader struct {
	SegmentID    types.SegmentID   `json:"segment_id"`
	TableID      types.TableID     `json:"table_id"`
	RowCount     uint32            `json:"row_count"`
	FreeSlots    uint32            `json:"free_slots"`
	LSN          types.LSN         `json:"lsn"`
	SchemaEpoch  types.SchemaEpoch `json:"schema_epoch"`
	Checksum     uint32            `json:"checksum"`
	WebhookID    uint64            `json:"webhook_id"`
	WebhookToken [34]byte          `json:"webhook_token"`
}

// EncodeToTopic serializes PageHeader to channel topic string
func (p PageHeader) EncodeToTopic() string {
	buf := make([]byte, 84)
	binary.BigEndian.PutUint64(buf[0:8], p.SegmentID.Uint64())
	binary.BigEndian.PutUint64(buf[8:16], p.TableID.Uint64())
	binary.BigEndian.PutUint32(buf[16:20], p.RowCount)
	binary.BigEndian.PutUint32(buf[20:24], p.FreeSlots)
	binary.BigEndian.PutUint64(buf[24:32], p.LSN.Uint64())
	binary.BigEndian.PutUint32(buf[32:36], uint32(p.SchemaEpoch))
	binary.BigEndian.PutUint32(buf[36:40], p.Checksum)
	binary.BigEndian.PutUint64(buf[40:48], p.WebhookID)
	copy(buf[48:82], p.WebhookToken[:])
	buf[82] = 2
	buf[83] = 0
	return base64.StdEncoding.EncodeToString(buf)
}

// ParsePageHeader extracts PageHeader from channel topic
func ParsePageHeader(topic string) (PageHeader, error) {
	if topic == "" {
		return PageHeader{}, fmt.Errorf("empty topic")
	}

	data, err := base64.StdEncoding.DecodeString(topic)
	if err != nil {
		return PageHeader{}, fmt.Errorf("decode topic: %w", err)
	}

	if len(data) < 42 {
		return PageHeader{}, fmt.Errorf("topic too short: %d < 42", len(data))
	}

	version := data[len(data)-2]
	if version != 2 {
		return PageHeader{}, fmt.Errorf("unsupported topic version: %d", version)
	}

	if len(data) < 84 {
		return PageHeader{}, fmt.Errorf("topic too short for v2: %d < 84", len(data))
	}

	var token [34]byte
	copy(token[:], data[48:82])

	return PageHeader{
		SegmentID:    types.SegmentID(binary.BigEndian.Uint64(data[0:8])),
		TableID:      types.TableID(binary.BigEndian.Uint64(data[8:16])),
		RowCount:     binary.BigEndian.Uint32(data[16:20]),
		FreeSlots:    binary.BigEndian.Uint32(data[20:24]),
		LSN:          types.LSN(binary.BigEndian.Uint64(data[24:32])),
		SchemaEpoch:  types.SchemaEpoch(binary.BigEndian.Uint32(data[32:36])),
		Checksum:     binary.BigEndian.Uint32(data[36:40]),
		WebhookID:    binary.BigEndian.Uint64(data[40:48]),
		WebhookToken: token,
	}, nil
}

// RowStorage represents a message mapped as a database row
type RowStorage struct {
	MessageID types.MessageID
	ChannelID types.ChannelID
	Content   string  // RowHeader encoded
	Embeds    []Embed // RowBody encoded
}

// Embed represents a simplified Discord embed for mapping
type Embed struct {
	Description string `json:"description,omitempty"`
}

// OverflowContainer represents a thread mapped as TOAST storage
type OverflowContainer struct {
	ThreadID  types.ChannelID
	ParentMsg types.MessageID
	RowID     types.RowID
	Chunks    int
}

// OverflowChunk represents one chunk of overflow data
type OverflowChunk struct {
	RowID    types.RowID
	ChunkIdx int
	Total    int
	Data     []byte
}

// EncodeChunkToMessage formats a chunk for Discord message content
func EncodeChunkToMessage(chunk OverflowChunk) string {
	header := fmt.Sprintf("TOAST:%d:%d/%d:", chunk.RowID, chunk.ChunkIdx, chunk.Total)
	return header + base64.StdEncoding.EncodeToString(chunk.Data)
}

// DecodeChunkFromMessage parses a chunk from message content
func DecodeChunkFromMessage(content string) (OverflowChunk, error) {
	if !strings.HasPrefix(content, "TOAST:") {
		return OverflowChunk{}, fmt.Errorf("not a toast chunk: %s", content[:min(20, len(content))])
	}

	parts := strings.SplitN(content[6:], ":", 3)
	if len(parts) != 3 {
		return OverflowChunk{}, fmt.Errorf("invalid toast format")
	}

	rowID, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return OverflowChunk{}, fmt.Errorf("parse row ID: %w", err)
	}

	chunkInfo := strings.Split(parts[1], "/")
	if len(chunkInfo) != 2 {
		return OverflowChunk{}, fmt.Errorf("invalid chunk info format")
	}

	chunkIdx, err := strconv.Atoi(chunkInfo[0])
	if err != nil {
		return OverflowChunk{}, fmt.Errorf("parse chunk index: %w", err)
	}

	total, err := strconv.Atoi(chunkInfo[1])
	if err != nil {
		return OverflowChunk{}, fmt.Errorf("parse total chunks: %w", err)
	}

	data, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return OverflowChunk{}, fmt.Errorf("decode chunk data: %w", err)
	}

	return OverflowChunk{
		RowID:    types.RowID(rowID),
		ChunkIdx: chunkIdx,
		Total:    total,
		Data:     data,
	}, nil
}

// BlobObject represents an attachment mapped as immutable blob storage
type BlobObject struct {
	AttachmentID string
	URL          string
	Filename     string
	Size         int
	ContentType  string
}

// RowFlags represents reactions mapped as row flags
type RowFlags struct {
	IsLive     bool // Row is active
	IsDead     bool // Row is tombstoned
	IsLocked   bool // Row is locked for update
	IsOverflow bool // Row has TOAST data
}

// ToReactions converts RowFlags to Discord reaction emojis
func (f RowFlags) ToReactions() []string {
	var reactions []string
	if f.IsLive {
		reactions = append(reactions, "🟢")
	}
	if f.IsDead {
		reactions = append(reactions, "🔴")
	}
	if f.IsLocked {
		reactions = append(reactions, "🔒")
	}
	if f.IsOverflow {
		reactions = append(reactions, "📦")
	}
	return reactions
}

// ParseRowFlags extracts RowFlags from Discord reactions
func ParseRowFlags(reactions []string) RowFlags {
	var flags RowFlags
	for _, r := range reactions {
		switch r {
		case "🟢":
			flags.IsLive = true
		case "🔴":
			flags.IsDead = true
		case "🔒":
			flags.IsLocked = true
		case "📦":
			flags.IsOverflow = true
		}
	}
	return flags
}

// FSMPage represents a role mapped as Free Space Map page
type FSMPage struct {
	RoleID      string
	Permissions int64  // 40 trits packed into 64 bits
	Color       int    // Metadata storage
	Name        string // Address encoding: fsm::{table_id}::{page_id}
}

// EncodeFSMRoleName generates the canonical role name for FSM
func EncodeFSMRoleName(tableID types.TableID, pageID types.PageID) string {
	return fmt.Sprintf("fsm::%d::%d", tableID, pageID)
}

// ParseFSMRoleName extracts table and page IDs from role name
func ParseFSMRoleName(name string) (types.TableID, types.PageID, error) {
	parts := strings.Split(name, "::")
	if len(parts) != 3 || parts[0] != "fsm" {
		return 0, 0, fmt.Errorf("invalid FSM role name: %s", name)
	}

	tableID, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse table ID: %w", err)
	}

	pageID, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse page ID: %w", err)
	}

	return types.TableID(tableID), types.PageID(pageID), nil
}

// BTreeIndex represents a forum channel mapped as B-tree index
type BTreeIndex struct {
	ForumChannelID types.ChannelID
	TableID        types.TableID
	IndexName      string
	Columns        []string
}

// IndexEntry represents a forum post mapped as index entry
type IndexEntry struct {
	ForumPostID types.ChannelID
	Key         []byte
	RowPointer  RowPointer
}

// RowPointer addresses a specific row in storage
type RowPointer struct {
	RowID     types.RowID     `json:"row_id"`
	SegmentID types.SegmentID `json:"segment_id"`
	MessageID types.MessageID `json:"message_id"`
}

// EncodeIndexKey generates a forum post name from index key
func EncodeIndexKey(tableID types.TableID, indexName string, key []byte) string {
	// Use base64 for key to handle binary data safely
	encoded := base64.URLEncoding.EncodeToString(key)
	return fmt.Sprintf("idx::%d::%s::%s", tableID, indexName, encoded)
}

// ParseIndexKey extracts components from forum post name
func ParseIndexKey(name string) (types.TableID, string, []byte, error) {
	parts := strings.SplitN(name, "::", 4)
	if len(parts) != 4 || parts[0] != "idx" {
		return 0, "", nil, fmt.Errorf("invalid index key format: %s", name)
	}

	tableID, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, "", nil, fmt.Errorf("parse table ID: %w", err)
	}

	key, err := base64.URLEncoding.DecodeString(parts[3])
	if err != nil {
		return 0, "", nil, fmt.Errorf("decode key: %w", err)
	}

	return types.TableID(tableID), parts[2], key, nil
}

// BootRecord represents the pinned bootstrap message
type BootRecord struct {
	Version             uint32            `json:"version"`
	CatalogCategory     types.ChannelID   `json:"catalog_category"`
	WALChannel          types.ChannelID   `json:"wal_channel"`
	WALWebhookID        string            `json:"wal_webhook_id"`
	WALWebhookToken     string            `json:"wal_webhook_token"`
	CatalogWebhookID    string            `json:"catalog_webhook_id"`
	CatalogWebhookToken string            `json:"catalog_webhook_token"`
	CurrentEpoch        types.SchemaEpoch `json:"current_epoch"`
	Checksum            uint32            `json:"checksum"`
}

// EncodeBootRecord serializes boot record to JSON
func (b BootRecord) EncodeBootRecord() (string, error) {
	data, err := json.Marshal(b)
	if err != nil {
		return "", fmt.Errorf("marshal boot record: %w", err)
	}
	return string(data), nil
}

// ParseBootRecord deserializes boot record from JSON
func ParseBootRecord(content string) (BootRecord, error) {
	var record BootRecord
	if err := json.Unmarshal([]byte(content), &record); err != nil {
		return BootRecord{}, fmt.Errorf("unmarshal boot record: %w", err)
	}
	return record, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// WALMessage represents a WAL record encoded as a Discord message
type WALMessage struct {
	LSN  types.LSN
	Type string
	Data []byte
}

// RowBodyJSON is the JSON structure stored in embed description
type RowBodyJSON struct {
	SchemaEpoch types.SchemaEpoch `json:"epoch"`
	NullBitmap  []byte            `json:"nulls,omitempty"`
	Columns     []ColumnJSON      `json:"cols"`
	OverflowRef *OverflowRef      `json:"overflow,omitempty"`
}

// ColumnJSON represents a single column value
type ColumnJSON struct {
	Index int             `json:"i"`
	Type  string          `json:"t"`
	Value json.RawMessage `json:"v"`
}

// OverflowRef points to TOAST storage
type OverflowRef struct {
	ThreadID types.ChannelID `json:"thread_id"`
	Chunks   int             `json:"chunks"`
}

// Encode serializes OverflowRef to JSON
func (r OverflowRef) Encode() ([]byte, error) {
	return json.Marshal(r)
}

// Decode deserializes JSON into OverflowRef
func (r *OverflowRef) Decode(data []byte) error {
	return json.Unmarshal(data, r)
}
