package executor

import (
	"context"
	"testing"

	"discodb/mvcc"
	"discodb/storage"
	"discodb/types"
)

type mockMVCCReader struct {
	rows []storage.Row
}

func (m *mockMVCCReader) ReadRows(ctx context.Context, tableID types.TableID) ([]storage.Row, error) {
	return m.rows, nil
}

func TestSeqScanMVCCVisibility(t *testing.T) {
	schema := []ColumnInfo{
		{Name: "id", Ordinal: 0},
		{Name: "name", Ordinal: 1},
	}

	rows := []storage.Row{
		{
			Header: storage.RowHeader{
				RowID:   1,
				TableID: 1,
				TxnID:   1,
				TxnMax:  0,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(1)},
					{Kind: "text", Text: ptrStr("alice")},
				},
			},
		},
		{
			Header: storage.RowHeader{
				RowID:   2,
				TableID: 1,
				TxnID:   5,
				TxnMax:  0,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(2)},
					{Kind: "text", Text: ptrStr("bob")},
				},
			},
		},
		{
			Header: storage.RowHeader{
				RowID:   3,
				TableID: 1,
				TxnID:   10,
				TxnMax:  0,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(3)},
					{Kind: "text", Text: ptrStr("charlie")},
				},
			},
		},
	}

	snap := mvcc.TransactionSnapshot{
		TxnID:      7,
		TxnMin:     1,
		TxnMax:     10,
		ActiveTxns: nil,
	}

	reader := &mockMVCCReader{rows: rows}
	scan := NewSeqScanWithSnapshot(reader, 1, nil, schema, snap)

	batch, done, err := scan.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if !done {
		t.Fatal("expected done=true")
	}

	if len(batch.Rows) != 2 {
		t.Fatalf("expected 2 visible rows (txn 1 and 5), got %d", len(batch.Rows))
	}

	name0, _ := batch.Rows[0].Get(1)
	name1, _ := batch.Rows[1].Get(1)
	if s, _ := name0.AsString(); s != "alice" {
		t.Fatalf("expected first row alice, got %s", s)
	}
	if s, _ := name1.AsString(); s != "bob" {
		t.Fatalf("expected second row bob, got %s", s)
	}
}

func TestSeqScanMVCCActiveTxnInvisible(t *testing.T) {
	schema := []ColumnInfo{
		{Name: "id", Ordinal: 0},
	}

	rows := []storage.Row{
		{
			Header: storage.RowHeader{
				RowID:   1,
				TableID: 1,
				TxnID:   1,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(1)},
				},
			},
		},
		{
			Header: storage.RowHeader{
				RowID:   2,
				TableID: 1,
				TxnID:   3,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(2)},
				},
			},
		},
		{
			Header: storage.RowHeader{
				RowID:   3,
				TableID: 1,
				TxnID:   5,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(3)},
				},
			},
		},
	}

	snap := mvcc.TransactionSnapshot{
		TxnID:      7,
		TxnMin:     1,
		TxnMax:     10,
		ActiveTxns: []types.TxnID{3, 5},
	}

	reader := &mockMVCCReader{rows: rows}
	scan := NewSeqScanWithSnapshot(reader, 1, nil, schema, snap)

	batch, _, err := scan.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if len(batch.Rows) != 1 {
		t.Fatalf("expected 1 visible row (only txn 1), got %d", len(batch.Rows))
	}
}

func TestSeqScanMVCCTombstone(t *testing.T) {
	schema := []ColumnInfo{
		{Name: "id", Ordinal: 0},
	}

	rows := []storage.Row{
		{
			Header: storage.RowHeader{
				RowID:   1,
				TableID: 1,
				TxnID:   1,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(1)},
				},
			},
		},
		{
			Header: storage.RowHeader{
				RowID:   2,
				TableID: 1,
				TxnID:   2,
				Flags:   storage.FlagTombstone,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(2)},
				},
			},
		},
		{
			Header: storage.RowHeader{
				RowID:   3,
				TableID: 1,
				TxnID:   3,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(3)},
				},
			},
		},
	}

	snap := mvcc.TransactionSnapshot{
		TxnID:      10,
		TxnMin:     1,
		TxnMax:     20,
		ActiveTxns: nil,
	}

	reader := &mockMVCCReader{rows: rows}
	scan := NewSeqScanWithSnapshot(reader, 1, nil, schema, snap)

	batch, _, err := scan.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if len(batch.Rows) != 2 {
		t.Fatalf("expected 2 rows (tombstone skipped), got %d", len(batch.Rows))
	}
}

func TestSeqScanMVCCTxnMaxDeleted(t *testing.T) {
	schema := []ColumnInfo{
		{Name: "id", Ordinal: 0},
	}

	rows := []storage.Row{
		{
			Header: storage.RowHeader{
				RowID:   1,
				TableID: 1,
				TxnID:   1,
				TxnMax:  5,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(1)},
				},
			},
		},
		{
			Header: storage.RowHeader{
				RowID:   2,
				TableID: 1,
				TxnID:   2,
				TxnMax:  0,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(2)},
				},
			},
		},
	}

	snap := mvcc.TransactionSnapshot{
		TxnID:      10,
		TxnMin:     1,
		TxnMax:     20,
		ActiveTxns: nil,
	}

	reader := &mockMVCCReader{rows: rows}
	scan := NewSeqScanWithSnapshot(reader, 1, nil, schema, snap)

	batch, _, err := scan.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if len(batch.Rows) != 1 {
		t.Fatalf("expected 1 row (row 1 deleted at txn 5), got %d", len(batch.Rows))
	}

	val, _ := batch.Rows[0].Get(0)
	n, _ := val.AsInt64()
	if n != 2 {
		t.Fatalf("expected row with id=2, got %d", n)
	}
}

func TestSeqScanMVCCReadYourOwnWrites(t *testing.T) {
	schema := []ColumnInfo{
		{Name: "id", Ordinal: 0},
	}

	rows := []storage.Row{
		{
			Header: storage.RowHeader{
				RowID:   1,
				TableID: 1,
				TxnID:   1,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(1)},
				},
			},
		},
		{
			Header: storage.RowHeader{
				RowID:   2,
				TableID: 1,
				TxnID:   10,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(2)},
				},
			},
		},
	}

	snap := mvcc.TransactionSnapshot{
		TxnID:      10,
		TxnMin:     1,
		TxnMax:     20,
		ActiveTxns: nil,
	}

	reader := &mockMVCCReader{rows: rows}
	scan := NewSeqScanWithSnapshot(reader, 1, nil, schema, snap)

	batch, _, err := scan.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if len(batch.Rows) != 2 {
		t.Fatalf("expected 2 rows (own writes visible), got %d", len(batch.Rows))
	}
}

func TestSeqScanMVCCNoSnapshot(t *testing.T) {
	schema := []ColumnInfo{
		{Name: "id", Ordinal: 0},
	}

	rows := []storage.Row{
		{
			Header: storage.RowHeader{
				RowID:   1,
				TableID: 1,
				TxnID:   1,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(1)},
				},
			},
		},
		{
			Header: storage.RowHeader{
				RowID:   2,
				TableID: 1,
				TxnID:   99,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(2)},
				},
			},
		},
	}

	reader := &mockMVCCReader{rows: rows}
	scan := NewSeqScan(reader, 1, nil, schema)

	batch, _, err := scan.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if len(batch.Rows) != 2 {
		t.Fatalf("expected 2 rows (no snapshot = all visible), got %d", len(batch.Rows))
	}
}

func TestSeqScanMVCCWithFilter(t *testing.T) {
	schema := []ColumnInfo{
		{Name: "id", Ordinal: 0},
		{Name: "name", Ordinal: 1},
	}

	rows := []storage.Row{
		{
			Header: storage.RowHeader{
				RowID:   1,
				TableID: 1,
				TxnID:   1,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(1)},
					{Kind: "text", Text: ptrStr("alice")},
				},
			},
		},
		{
			Header: storage.RowHeader{
				RowID:   2,
				TableID: 1,
				TxnID:   2,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(2)},
					{Kind: "text", Text: ptrStr("bob")},
				},
			},
		},
		{
			Header: storage.RowHeader{
				RowID:   3,
				TableID: 1,
				TxnID:   3,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(3)},
					{Kind: "text", Text: ptrStr("charlie")},
				},
			},
		},
	}

	snap := mvcc.TransactionSnapshot{
		TxnID:      10,
		TxnMin:     1,
		TxnMax:     20,
		ActiveTxns: nil,
	}

	filter := Predicate{
		Left:  Expression{ColumnIndex: ptrInt(0)},
		Op:    ComparisonGt,
		Right: Expression{Constant: ptrValue(types.Int8Value(1))},
	}

	reader := &mockMVCCReader{rows: rows}
	scan := NewSeqScanWithSnapshot(reader, 1, &filter, schema, snap)

	batch, _, err := scan.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if len(batch.Rows) != 2 {
		t.Fatalf("expected 2 rows (id > 1), got %d", len(batch.Rows))
	}
}

func TestSeqScanMVCCFutureTxnID(t *testing.T) {
	schema := []ColumnInfo{
		{Name: "id", Ordinal: 0},
	}

	rows := []storage.Row{
		{
			Header: storage.RowHeader{
				RowID:   1,
				TableID: 1,
				TxnID:   1,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(1)},
				},
			},
		},
		{
			Header: storage.RowHeader{
				RowID:   2,
				TableID: 1,
				TxnID:   100,
				Flags:   0,
			},
			Body: storage.RowBody{
				Columns: []storage.ColumnValue{
					{Kind: "int8", Int64: ptrInt64(2)},
				},
			},
		},
	}

	snap := mvcc.TransactionSnapshot{
		TxnID:      10,
		TxnMin:     1,
		TxnMax:     50,
		ActiveTxns: nil,
	}

	reader := &mockMVCCReader{rows: rows}
	scan := NewSeqScanWithSnapshot(reader, 1, nil, schema, snap)

	batch, _, err := scan.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if len(batch.Rows) != 1 {
		t.Fatalf("expected 1 row (txn 100 >= TxnMax 50), got %d", len(batch.Rows))
	}
}

func ptrInt64(v int64) *int64 { return &v }
func ptrStr(v string) *string { return &v }
func ptrInt(v int) *int       { return &v }
func ptrValue(v types.Value) *types.Value {
	return &v
}
