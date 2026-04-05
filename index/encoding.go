package index

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"discodb/types"
)

const (
	entryPrefix   = "KEY::"
	rowIDPrefix   = "row_id="
	segPrefix     = "seg="
	msgIDPrefix   = "msg_id="
	deletedMarker = "deleted=true"
	nodePrefix    = "NODE|"
)

type IndexEntry struct {
	RowID     types.RowID
	SegmentID types.SegmentID
	MessageID types.MessageID
	PostID    types.ChannelID
	Key       []byte
	Deleted   bool
}

type InternalNode struct {
	Level    uint32
	Keys     [][]byte
	Children []types.ChannelID
}

func EncodeEntry(key []byte, rowID types.RowID, segmentID types.SegmentID, messageID types.MessageID, deleted bool) string {
	var b strings.Builder
	b.WriteString(entryPrefix)
	b.WriteString(string(key))
	b.WriteString("\n")
	b.WriteString(rowIDPrefix)
	b.WriteString(rowID.String())
	b.WriteString("\n")
	b.WriteString(segPrefix)
	b.WriteString(segmentID.String())
	b.WriteString("\n")
	b.WriteString(msgIDPrefix)
	b.WriteString(messageID.String())
	if deleted {
		b.WriteString("\n")
		b.WriteString(deletedMarker)
	}
	return b.String()
}

func DecodeEntry(content string) (*IndexEntry, error) {
	lines := strings.Split(content, "\n")
	if len(lines) < 4 {
		return nil, fmt.Errorf("index entry: too few lines")
	}

	if !strings.HasPrefix(lines[0], entryPrefix) {
		return nil, fmt.Errorf("index entry: missing KEY:: prefix")
	}
	key := []byte(strings.TrimPrefix(lines[0], entryPrefix))

	var entry IndexEntry
	entry.Key = key

	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		switch {
		case strings.HasPrefix(line, rowIDPrefix):
			v := strings.TrimPrefix(line, rowIDPrefix)
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("index entry: invalid row_id %q: %w", v, err)
			}
			entry.RowID = types.RowID(n)
		case strings.HasPrefix(line, segPrefix):
			v := strings.TrimPrefix(line, segPrefix)
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("index entry: invalid seg %q: %w", v, err)
			}
			entry.SegmentID = types.SegmentID(n)
		case strings.HasPrefix(line, msgIDPrefix):
			v := strings.TrimPrefix(line, msgIDPrefix)
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("index entry: invalid msg_id %q: %w", v, err)
			}
			entry.MessageID = types.MessageID(n)
		case line == deletedMarker:
			entry.Deleted = true
		}
	}

	if entry.RowID == 0 {
		return nil, fmt.Errorf("index entry: missing row_id")
	}

	return &entry, nil
}

func EncodeInternalNode(node InternalNode) string {
	var b strings.Builder
	b.WriteString(nodePrefix)
	b.WriteString(strconv.FormatUint(uint64(node.Level), 10))
	b.WriteString("|")

	var keys []string
	for _, k := range node.Keys {
		keys = append(keys, hex.EncodeToString(k))
	}
	b.WriteString("keys=[")
	b.WriteString(strings.Join(keys, ","))
	b.WriteString("]|")

	var children []string
	for _, c := range node.Children {
		children = append(children, c.String())
	}
	b.WriteString("children=[")
	b.WriteString(strings.Join(children, ","))
	b.WriteString("]")

	return b.String()
}

func DecodeInternalNode(content string) (*InternalNode, error) {
	if !strings.HasPrefix(content, nodePrefix) {
		return nil, fmt.Errorf("internal node: missing NODE| prefix")
	}

	content = strings.TrimPrefix(content, nodePrefix)
	parts := strings.SplitN(content, "|", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("internal node: invalid format")
	}

	level, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("internal node: invalid level: %w", err)
	}

	node := &InternalNode{Level: uint32(level)}

	keysStr := strings.TrimPrefix(parts[1], "keys=[")
	keysStr = strings.TrimSuffix(keysStr, "]")
	if keysStr != "" {
		for _, ks := range strings.Split(keysStr, ",") {
			b, err := hex.DecodeString(ks)
			if err != nil {
				return nil, fmt.Errorf("internal node: invalid key hex: %w", err)
			}
			node.Keys = append(node.Keys, b)
		}
	}

	childrenStr := strings.TrimPrefix(parts[2], "children=[")
	childrenStr = strings.TrimSuffix(childrenStr, "]")
	if childrenStr != "" {
		for _, cs := range strings.Split(childrenStr, ",") {
			n, err := strconv.ParseUint(cs, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("internal node: invalid child id: %w", err)
			}
			node.Children = append(node.Children, types.ChannelID(n))
		}
	}

	return node, nil
}

func GenerateIndexName(tableID types.TableID, columnNames []string) string {
	h := sha256.Sum256([]byte(strings.Join(columnNames, ",")))
	colHash := hex.EncodeToString(h[:4])
	return fmt.Sprintf("idx::%d::%s", tableID.Uint64(), colHash)
}

func GeneratePostTitle(key []byte) string {
	return fmt.Sprintf("idx:%s", hex.EncodeToString(key))
}

func GenerateMetaChannelName(indexName string) string {
	return fmt.Sprintf("idx_meta_%s", indexName)
}
