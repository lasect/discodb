package executor

import (
	"context"
	"testing"

	"discodb/storage"
	"discodb/types"
)

type mockStorageReader struct {
	rows map[types.TableID][]storage.Row
}

func (m *mockStorageReader) ReadRows(ctx context.Context, tableID types.TableID) ([]storage.Row, error) {
	return m.rows[tableID], nil
}

func newTestRow(id types.RowID, tableID types.TableID, cols []storage.ColumnValue) storage.Row {
	return storage.Row{
		Header: storage.RowHeader{
			RowID:   id,
			TableID: tableID,
		},
		Body: storage.RowBody{Columns: cols},
	}
}

func TestSeqScan(t *testing.T) {
	tableID := types.TableID(1)
	schema := []ColumnInfo{
		{Name: "id", TableID: &tableID, Ordinal: 0},
		{Name: "name", TableID: &tableID, Ordinal: 1},
		{Name: "age", TableID: &tableID, Ordinal: 2},
	}

	name := "alice"
	age := int64(30)
	name2 := "bob"
	age2 := int64(25)

	reader := &mockStorageReader{
		rows: map[types.TableID][]storage.Row{
			tableID: {
				newTestRow(1, tableID, []storage.ColumnValue{
					{Kind: "int8", Int64: ptr(int64(1))},
					{Kind: "text", Text: &name},
					{Kind: "int8", Int64: &age},
				}),
				newTestRow(2, tableID, []storage.ColumnValue{
					{Kind: "int8", Int64: ptr(int64(2))},
					{Kind: "text", Text: &name2},
					{Kind: "int8", Int64: &age2},
				}),
			},
		},
	}

	t.Run("full scan no filter", func(t *testing.T) {
		scan := NewSeqScan(reader, tableID, nil, schema)
		batch, done, err := scan.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !done {
			t.Fatal("expected done=true (all rows returned)")
		}
		if len(batch.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(batch.Rows))
		}
		if len(batch.Schema) != 3 {
			t.Fatalf("expected 3 columns in schema, got %d", len(batch.Schema))
		}
	})

	t.Run("scan with equality filter", func(t *testing.T) {
		colIdx := 1
		filter := &Predicate{
			Left:  Expression{ColumnIndex: &colIdx},
			Op:    ComparisonEq,
			Right: Expression{Constant: ptr(types.TextValue("alice"))},
		}

		scan := NewSeqScan(reader, tableID, filter, schema)
		batch, _, err := scan.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(batch.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(batch.Rows))
		}
		got, _ := batch.Rows[0].Get(1)
		if s, ok := got.AsString(); !ok || s != "alice" {
			t.Fatalf("expected name=alice, got %v", got)
		}
	})
}

func TestFilter(t *testing.T) {
	tableID := types.TableID(1)
	schema := []ColumnInfo{
		{Name: "id", TableID: &tableID, Ordinal: 0},
		{Name: "value", TableID: &tableID, Ordinal: 1},
	}

	v1 := int64(10)
	v2 := int64(20)
	v3 := int64(5)

	reader := &mockStorageReader{
		rows: map[types.TableID][]storage.Row{
			tableID: {
				newTestRow(1, tableID, []storage.ColumnValue{
					{Kind: "int8", Int64: ptr(int64(1))},
					{Kind: "int8", Int64: &v1},
				}),
				newTestRow(2, tableID, []storage.ColumnValue{
					{Kind: "int8", Int64: ptr(int64(2))},
					{Kind: "int8", Int64: &v2},
				}),
				newTestRow(3, tableID, []storage.ColumnValue{
					{Kind: "int8", Int64: ptr(int64(3))},
					{Kind: "int8", Int64: &v3},
				}),
			},
		},
	}

	scan := NewSeqScan(reader, tableID, nil, schema)

	t.Run("filter gt", func(t *testing.T) {
		colIdx := 1
		pred := Predicate{
			Left:  Expression{ColumnIndex: &colIdx},
			Op:    ComparisonGt,
			Right: Expression{Constant: ptr(types.Int8Value(8))},
		}

		filter := &Filter{Input: scan, Predicate: pred}
		batch, _, err := filter.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(batch.Rows) != 2 {
			t.Fatalf("expected 2 rows (value > 8), got %d", len(batch.Rows))
		}
	})

	t.Run("filter le", func(t *testing.T) {
		colIdx := 1
		pred := Predicate{
			Left:  Expression{ColumnIndex: &colIdx},
			Op:    ComparisonLe,
			Right: Expression{Constant: ptr(types.Int8Value(10))},
		}

		filter := &Filter{Input: NewSeqScan(reader, tableID, nil, schema), Predicate: pred}
		batch, _, err := filter.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(batch.Rows) != 2 {
			t.Fatalf("expected 2 rows (value <= 10), got %d", len(batch.Rows))
		}
	})

	t.Run("filter ne", func(t *testing.T) {
		colIdx := 1
		pred := Predicate{
			Left:  Expression{ColumnIndex: &colIdx},
			Op:    ComparisonNe,
			Right: Expression{Constant: ptr(types.Int8Value(20))},
		}

		filter := &Filter{Input: NewSeqScan(reader, tableID, nil, schema), Predicate: pred}
		batch, _, err := filter.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(batch.Rows) != 2 {
			t.Fatalf("expected 2 rows (value != 20), got %d", len(batch.Rows))
		}
	})
}

func TestProjection(t *testing.T) {
	tableID := types.TableID(1)
	schema := []ColumnInfo{
		{Name: "id", TableID: &tableID, Ordinal: 0},
		{Name: "name", TableID: &tableID, Ordinal: 1},
		{Name: "age", TableID: &tableID, Ordinal: 2},
	}

	name := "alice"
	age := int64(30)

	reader := &mockStorageReader{
		rows: map[types.TableID][]storage.Row{
			tableID: {
				newTestRow(1, tableID, []storage.ColumnValue{
					{Kind: "int8", Int64: ptr(int64(1))},
					{Kind: "text", Text: &name},
					{Kind: "int8", Int64: &age},
				}),
			},
		},
	}

	projSchema := []ColumnInfo{{Name: "name", Ordinal: 0}, {Name: "age", Ordinal: 1}}
	proj := &Projection{
		Input:   NewSeqScan(reader, tableID, nil, schema),
		Columns: []int{1, 2},
		Schema:  projSchema,
	}

	batch, _, err := proj.Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(batch.Rows))
	}
	if len(batch.Rows[0].Values) != 2 {
		t.Fatalf("expected 2 columns after projection, got %d", len(batch.Rows[0].Values))
	}

	n, _ := batch.Rows[0].Get(0)
	if s, ok := n.AsString(); !ok || s != "alice" {
		t.Fatalf("expected name=alice, got %v", n)
	}
}

func TestLimit(t *testing.T) {
	tableID := types.TableID(1)
	schema := []ColumnInfo{{Name: "id", Ordinal: 0}}

	var rows []storage.Row
	for i := 0; i < 10; i++ {
		v := int64(i)
		rows = append(rows, newTestRow(types.RowID(i+1), tableID, []storage.ColumnValue{
			{Kind: "int8", Int64: &v},
		}))
	}

	reader := &mockStorageReader{rows: map[types.TableID][]storage.Row{tableID: rows}}

	t.Run("limit 3", func(t *testing.T) {
		lim := &Limit{Input: NewSeqScan(reader, tableID, nil, schema), Remaining: 3, Offset: 0}
		batch, done, err := lim.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(batch.Rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(batch.Rows))
		}
		if done {
			t.Fatal("expected done=false (more available)")
		}
	})

	t.Run("limit with offset", func(t *testing.T) {
		lim := &Limit{Input: NewSeqScan(reader, tableID, nil, schema), Remaining: 2, Offset: 3}
		batch, _, err := lim.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(batch.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(batch.Rows))
		}
		first, _ := batch.Rows[0].Get(0)
		v, _ := first.AsInt64()
		if v != 3 {
			t.Fatalf("expected first value=3 (after offset), got %d", v)
		}
	})

	t.Run("limit exceeds rows", func(t *testing.T) {
		lim := &Limit{Input: NewSeqScan(reader, tableID, nil, schema), Remaining: 100, Offset: 0}
		batch, done, err := lim.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(batch.Rows) != 10 {
			t.Fatalf("expected 10 rows, got %d", len(batch.Rows))
		}
		if !done {
			t.Fatal("expected done=true")
		}
	})
}

func TestValues(t *testing.T) {
	schema := []ColumnInfo{{Name: "x", Ordinal: 0}}
	rows := []Row{
		{Values: []types.Value{types.Int8Value(1)}},
		{Values: []types.Value{types.Int8Value(2)}},
	}

	v := NewValues(rows, schema)
	batch, done, err := v.Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !done {
		t.Fatal("expected done=true")
	}
	if len(batch.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(batch.Rows))
	}

	_, done2, _ := v.Execute(context.Background())
	if !done2 {
		t.Fatal("expected done=true on second call")
	}
}

func TestAggregate(t *testing.T) {
	tableID := types.TableID(1)
	schema := []ColumnInfo{{Name: "val", Ordinal: 0}}

	v1 := int64(10)
	v2 := int64(20)
	v3 := int64(30)

	reader := &mockStorageReader{
		rows: map[types.TableID][]storage.Row{
			tableID: {
				newTestRow(1, tableID, []storage.ColumnValue{{Kind: "int8", Int64: &v1}}),
				newTestRow(2, tableID, []storage.ColumnValue{{Kind: "int8", Int64: &v2}}),
				newTestRow(3, tableID, []storage.ColumnValue{{Kind: "int8", Int64: &v3}}),
			},
		},
	}

	t.Run("count", func(t *testing.T) {
		agg := &AggregateExec{
			Input:   NewSeqScan(reader, tableID, nil, schema),
			Funcs:   []Aggregate{AggregateCount},
			ColIdxs: []int{0},
		}
		batch, done, err := agg.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !done {
			t.Fatal("expected done=true")
		}
		if len(batch.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(batch.Rows))
		}
		c, _ := batch.Rows[0].Get(0)
		v, _ := c.AsInt64()
		if v != 3 {
			t.Fatalf("expected count=3, got %d", v)
		}
	})

	t.Run("sum", func(t *testing.T) {
		agg := &AggregateExec{
			Input:   NewSeqScan(reader, tableID, nil, schema),
			Funcs:   []Aggregate{AggregateSum},
			ColIdxs: []int{0},
		}
		batch, _, err := agg.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		s, _ := batch.Rows[0].Get(0)
		f, _ := s.AsFloat64()
		if f != 60 {
			t.Fatalf("expected sum=60, got %f", f)
		}
	})

	t.Run("avg", func(t *testing.T) {
		agg := &AggregateExec{
			Input:   NewSeqScan(reader, tableID, nil, schema),
			Funcs:   []Aggregate{AggregateAvg},
			ColIdxs: []int{0},
		}
		batch, _, err := agg.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		a, _ := batch.Rows[0].Get(0)
		f, _ := a.AsFloat64()
		if f != 20 {
			t.Fatalf("expected avg=20, got %f", f)
		}
	})

	t.Run("min", func(t *testing.T) {
		agg := &AggregateExec{
			Input:   NewSeqScan(reader, tableID, nil, schema),
			Funcs:   []Aggregate{AggregateMin},
			ColIdxs: []int{0},
		}
		batch, _, err := agg.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		m, _ := batch.Rows[0].Get(0)
		v, _ := m.AsInt64()
		if v != 10 {
			t.Fatalf("expected min=10, got %d", v)
		}
	})

	t.Run("max", func(t *testing.T) {
		agg := &AggregateExec{
			Input:   NewSeqScan(reader, tableID, nil, schema),
			Funcs:   []Aggregate{AggregateMax},
			ColIdxs: []int{0},
		}
		batch, _, err := agg.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		m, _ := batch.Rows[0].Get(0)
		v, _ := m.AsInt64()
		if v != 30 {
			t.Fatalf("expected max=30, got %d", v)
		}
	})
}

func TestEvaluatePredicate(t *testing.T) {
	row := Row{Values: []types.Value{types.Int8Value(42), types.TextValue("hello")}}

	tests := []struct {
		name string
		pred Predicate
		want bool
	}{
		{
			name: "eq match",
			pred: Predicate{
				Left:  Expression{ColumnIndex: ptr(0)},
				Op:    ComparisonEq,
				Right: Expression{Constant: ptr(types.Int8Value(42))},
			},
			want: true,
		},
		{
			name: "eq no match",
			pred: Predicate{
				Left:  Expression{ColumnIndex: ptr(0)},
				Op:    ComparisonEq,
				Right: Expression{Constant: ptr(types.Int8Value(99))},
			},
			want: false,
		},
		{
			name: "lt true",
			pred: Predicate{
				Left:  Expression{ColumnIndex: ptr(0)},
				Op:    ComparisonLt,
				Right: Expression{Constant: ptr(types.Int8Value(50))},
			},
			want: true,
		},
		{
			name: "lt false",
			pred: Predicate{
				Left:  Expression{ColumnIndex: ptr(0)},
				Op:    ComparisonLt,
				Right: Expression{Constant: ptr(types.Int8Value(42))},
			},
			want: false,
		},
		{
			name: "ge true",
			pred: Predicate{
				Left:  Expression{ColumnIndex: ptr(0)},
				Op:    ComparisonGe,
				Right: Expression{Constant: ptr(types.Int8Value(42))},
			},
			want: true,
		},
		{
			name: "text eq",
			pred: Predicate{
				Left:  Expression{ColumnIndex: ptr(1)},
				Op:    ComparisonEq,
				Right: Expression{Constant: ptr(types.TextValue("hello"))},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EvaluatePredicate(row, tt.pred)
			if got != tt.want {
				t.Errorf("EvaluatePredicate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLikeMatch(t *testing.T) {
	tests := []struct {
		s, pattern string
		want       bool
	}{
		{"hello", "%", true},
		{"hello", "hello", true},
		{"hello", "hel%", true},
		{"hello", "%llo", true},
		{"hello", "h%o", true},
		{"hello", "h_llo", true},
		{"hello", "h%l%", true},
		{"hello", "world", false},
		{"hello", "helx%", false},
		{"", "%", true},
		{"", "", true},
		{"abc", "a_c", true},
	}

	for _, tt := range tests {
		t.Run(tt.s+"_"+tt.pattern, func(t *testing.T) {
			got := likeMatch(tt.s, tt.pattern)
			if got != tt.want {
				t.Errorf("likeMatch(%q, %q) = %v, want %v", tt.s, tt.pattern, got, tt.want)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

func TestDeleteExec(t *testing.T) {
	tableID := types.TableID(1)
	schema := []ColumnInfo{{Name: "id", TableID: &tableID, Ordinal: 0}}

	var rows []storage.Row
	for i := 0; i < 5; i++ {
		v := int64(i)
		rows = append(rows, newTestRow(types.RowID(i+1), tableID, []storage.ColumnValue{
			{Kind: "int8", Int64: &v},
		}))
	}

	reader := &mockStorageReader{rows: map[types.TableID][]storage.Row{tableID: rows}}

	t.Run("delete all", func(t *testing.T) {
		del := &DeleteExec{Input: NewSeqScan(reader, tableID, nil, schema), TableID: tableID}
		batch, done, err := del.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !done {
			t.Fatal("expected done=true")
		}
		if len(batch.Rows) != 5 {
			t.Fatalf("expected 5 rows to delete, got %d", len(batch.Rows))
		}
	})

	t.Run("delete with filter", func(t *testing.T) {
		colIdx := 0
		pred := Predicate{
			Left:  Expression{ColumnIndex: &colIdx},
			Op:    ComparisonGt,
			Right: Expression{Constant: ptr(types.Int8Value(2))},
		}

		del := &DeleteExec{
			Input:   &Filter{Input: NewSeqScan(reader, tableID, nil, schema), Predicate: pred},
			TableID: tableID,
		}
		batch, _, err := del.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(batch.Rows) != 2 {
			t.Fatalf("expected 2 rows to delete (id > 2), got %d", len(batch.Rows))
		}
	})
}

func TestUpdateExec(t *testing.T) {
	tableID := types.TableID(1)
	schema := []ColumnInfo{{Name: "id", TableID: &tableID, Ordinal: 0}, {Name: "name", TableID: &tableID, Ordinal: 1}}

	name := "alice"
	v1 := int64(1)

	reader := &mockStorageReader{
		rows: map[types.TableID][]storage.Row{
			tableID: {
				newTestRow(1, tableID, []storage.ColumnValue{
					{Kind: "int8", Int64: &v1},
					{Kind: "text", Text: &name},
				}),
			},
		},
	}

	t.Run("update all", func(t *testing.T) {
		upd := &UpdateExec{
			Input:    NewSeqScan(reader, tableID, nil, schema),
			TableID:  tableID,
			SetCols:  []string{"name"},
			SetExprs: []Expression{{Constant: ptr(types.TextValue("bob"))}},
		}
		batch, done, err := upd.Execute(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !done {
			t.Fatal("expected done=true")
		}
		if len(batch.Rows) != 1 {
			t.Fatalf("expected 1 row to update, got %d", len(batch.Rows))
		}
	})
}
