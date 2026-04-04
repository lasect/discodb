package mvcc

import (
	"discodb/types"
	"slices"
)

type TransactionSnapshot struct {
	TxnID      types.TxnID   `json:"txn_id"`
	TxnMin     types.TxnID   `json:"txn_min"`
	TxnMax     types.TxnID   `json:"txn_max"`
	ActiveTxns []types.TxnID `json:"active_txns"`
}

func NewSnapshot(txnID, txnMin, txnMax types.TxnID) TransactionSnapshot {
	return TransactionSnapshot{
		TxnID:      txnID,
		TxnMin:     txnMin,
		TxnMax:     txnMax,
		ActiveTxns: make([]types.TxnID, 0),
	}
}

func (s *TransactionSnapshot) IsVisible(rowTxnID types.TxnID, rowTxnMax *types.TxnID) bool {
	if rowTxnID == s.TxnID {
		return true
	}
	if rowTxnID > s.TxnID {
		return false
	}
	if slices.Contains(s.ActiveTxns, rowTxnID) {
		return false
	}
	if rowTxnMax != nil && *rowTxnMax <= s.TxnID {
		return false
	}
	return true
}

func (s *TransactionSnapshot) AddActive(txnID types.TxnID) {
	if slices.Contains(s.ActiveTxns, txnID) {
		return
	}
	s.ActiveTxns = append(s.ActiveTxns, txnID)
}

func (s *TransactionSnapshot) RemoveActive(txnID types.TxnID) {
	filtered := s.ActiveTxns[:0]
	for _, active := range s.ActiveTxns {
		if active != txnID {
			filtered = append(filtered, active)
		}
	}
	s.ActiveTxns = filtered
}

type RowVersion struct {
	RowID       types.RowID     `json:"row_id"`
	TableID     types.TableID   `json:"table_id"`
	SegmentID   types.SegmentID `json:"segment_id"`
	MessageID   types.MessageID `json:"message_id"`
	TxnID       types.TxnID     `json:"txn_id"`
	TxnMax      *types.TxnID    `json:"txn_max,omitempty"`
	LSN         types.LSN       `json:"lsn"`
	Data        []byte          `json:"data"`
	IsTombstone bool            `json:"is_tombstone"`
}

func NewRowVersion(rowID types.RowID, tableID types.TableID, segmentID types.SegmentID, messageID types.MessageID, txnID types.TxnID, lsn types.LSN, data []byte) RowVersion {
	return RowVersion{
		RowID:     rowID,
		TableID:   tableID,
		SegmentID: segmentID,
		MessageID: messageID,
		TxnID:     txnID,
		LSN:       lsn,
		Data:      append([]byte(nil), data...),
	}
}

func (v RowVersion) WithTombstone(txnMax types.TxnID) RowVersion {
	v.TxnMax = &txnMax
	v.IsTombstone = true
	return v
}

func (v RowVersion) IsVisible(snapshot TransactionSnapshot) bool {
	return snapshot.IsVisible(v.TxnID, v.TxnMax)
}

type VersionChain struct {
	Versions []RowVersion `json:"versions"`
}

func (c *VersionChain) Push(version RowVersion) {
	c.Versions = append(c.Versions, version)
}

func (c VersionChain) LatestVisible(snapshot TransactionSnapshot) (RowVersion, bool) {
	for i := len(c.Versions) - 1; i >= 0; i-- {
		if c.Versions[i].IsVisible(snapshot) {
			return c.Versions[i], true
		}
	}
	return RowVersion{}, false
}
