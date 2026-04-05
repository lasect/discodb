package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"discodb/catalog"
	"discodb/discord"
	"discodb/index"
	"discodb/storage"
	"discodb/types"
)

type IndexManager struct {
	mu      sync.RWMutex
	indexes map[types.TableID]*index.BTreeIndex
	client  *discord.Client
	guild   types.GuildID
	logger  *slog.Logger
	segMgr  *SegmentManager
}

func NewIndexManager(client *discord.Client, guild types.GuildID, logger *slog.Logger, segMgr *SegmentManager) *IndexManager {
	return &IndexManager{
		indexes: make(map[types.TableID]*index.BTreeIndex),
		client:  client,
		guild:   guild,
		logger:  logger,
		segMgr:  segMgr,
	}
}

func (m *IndexManager) CreateIndex(ctx context.Context, schema catalog.IndexSchema) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx := index.NewBTreeIndex(m.client, m.guild, schema.ID, schema.Name, schema.TableID, schema.Columns, schema.Unique, m.logger)

	if err := idx.CreateForumChannel(ctx); err != nil {
		return err
	}

	m.indexes[schema.ID] = idx
	return nil
}

func (m *IndexManager) DropIndex(ctx context.Context, indexID types.TableID) error {
	m.mu.Lock()
	idx, ok := m.indexes[indexID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("index %d not found", indexID)
	}
	delete(m.indexes, indexID)
	m.mu.Unlock()

	return idx.Drop(ctx)
}

func (m *IndexManager) GetIndex(indexID types.TableID) (*index.BTreeIndex, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	idx, ok := m.indexes[indexID]
	return idx, ok
}

func (m *IndexManager) GetIndexesForTable(tableID types.TableID) []*index.BTreeIndex {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*index.BTreeIndex
	for _, idx := range m.indexes {
		if idx.TableID == tableID {
			result = append(result, idx)
		}
	}
	return result
}

func (m *IndexManager) LoadIndexes(ctx context.Context, cat *catalog.Catalog) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, schema := range cat.Indexes() {
		idx := index.NewBTreeIndex(m.client, m.guild, schema.ID, schema.Name, schema.TableID, schema.Columns, schema.Unique, m.logger)
		m.indexes[schema.ID] = idx

		if err := idx.Load(ctx); err != nil {
			m.logger.Warn("failed to load index from forum",
				slog.String("index", schema.Name),
				slog.String("error", err.Error()),
			)
		}
	}

	return nil
}

func (m *IndexManager) Lookup(ctx context.Context, indexID types.TableID, key []byte) ([]index.IndexEntry, error) {
	m.mu.RLock()
	idx, ok := m.indexes[indexID]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("index %d not found", indexID)
	}
	return idx.Lookup(ctx, key)
}

func (m *IndexManager) Range(ctx context.Context, indexID types.TableID, startKey []byte, endKey []byte) ([]index.IndexEntry, error) {
	m.mu.RLock()
	idx, ok := m.indexes[indexID]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("index %d not found", indexID)
	}
	return idx.Range(ctx, startKey, endKey)
}

func (m *IndexManager) FetchRow(ctx context.Context, tableID types.TableID, segmentID types.SegmentID, messageID types.MessageID) (*storage.Row, error) {
	segments, err := m.segMgr.ListSegments(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("list segments: %w", err)
	}

	for _, seg := range segments {
		rows, _, err := m.segMgr.ReadRows(ctx, seg.ID)
		if err != nil {
			continue
		}

		for _, row := range rows {
			if row.Header.MessageID == messageID && row.Header.SegmentID == segmentID {
				return &row, nil
			}
		}
	}

	return nil, fmt.Errorf("row not found: seg=%d msg=%d", segmentID, messageID)
}

func (m *IndexManager) Insert(ctx context.Context, indexID types.TableID, key []byte, rowID types.RowID, segmentID types.SegmentID, messageID types.MessageID) error {
	m.mu.RLock()
	idx, ok := m.indexes[indexID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("index %d not found", indexID)
	}
	return idx.Insert(ctx, key, rowID, segmentID, messageID)
}

func (m *IndexManager) Delete(ctx context.Context, indexID types.TableID, key []byte) error {
	m.mu.RLock()
	idx, ok := m.indexes[indexID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("index %d not found", indexID)
	}
	return idx.Delete(ctx, key)
}

func (m *IndexManager) RebuildIndex(ctx context.Context, indexID types.TableID, rows []index.RebuildRow) error {
	m.mu.RLock()
	idx, ok := m.indexes[indexID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("index %d not found", indexID)
	}
	return idx.Rebuild(ctx, rows)
}
