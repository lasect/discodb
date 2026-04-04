package sql

import (
	"context"
	"testing"

	"discodb/catalog"
	"discodb/executor"
	"discodb/storage"
	"discodb/types"
)

type mockReader struct{}

func (m *mockReader) ReadRows(ctx context.Context, tableID types.TableID) ([]storage.Row, error) {
	return nil, nil
}

func TestPlannerSelectStar(t *testing.T) {
	cat := catalog.New()
	tableID := types.TableID(1)
	cat.AddTable(catalog.TableSchema{
		ID:   tableID,
		Name: "users",
		Columns: []catalog.ColumnSchema{
			{Name: "id", DataType: types.DataTypeInt8, Ordinal: 0},
			{Name: "name", DataType: types.DataTypeText, Ordinal: 1},
		},
	})

	planner := NewPlanner(cat, &mockReader{})
	stmt, err := Parse("SELECT * FROM users")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("plan error: %v", err)
	}

	scan, ok := plan.Root.(*executor.SeqScan)
	if !ok {
		t.Fatalf("expected SeqScan, got %T", plan.Root)
	}
	if scan.TableID != tableID {
		t.Fatalf("expected tableID=%d, got %d", tableID, scan.TableID)
	}
	if scan.Filter != nil {
		t.Fatal("expected no filter for SELECT *")
	}
}

func TestPlannerSelectColumns(t *testing.T) {
	cat := catalog.New()
	tableID := types.TableID(1)
	cat.AddTable(catalog.TableSchema{
		ID:   tableID,
		Name: "users",
		Columns: []catalog.ColumnSchema{
			{Name: "id", DataType: types.DataTypeInt8, Ordinal: 0},
			{Name: "name", DataType: types.DataTypeText, Ordinal: 1},
			{Name: "age", DataType: types.DataTypeInt8, Ordinal: 2},
		},
	})

	planner := NewPlanner(cat, &mockReader{})
	stmt, err := Parse("SELECT name, age FROM users")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("plan error: %v", err)
	}

	proj, ok := plan.Root.(*executor.Projection)
	if !ok {
		t.Fatalf("expected Projection at root, got %T", plan.Root)
	}
	if len(proj.Columns) != 2 {
		t.Fatalf("expected 2 projected columns, got %d", len(proj.Columns))
	}
	if proj.Columns[0] != 1 || proj.Columns[1] != 2 {
		t.Fatalf("expected columns [1, 2], got %v", proj.Columns)
	}
}

func TestPlannerSelectWithWhere(t *testing.T) {
	cat := catalog.New()
	tableID := types.TableID(1)
	cat.AddTable(catalog.TableSchema{
		ID:   tableID,
		Name: "users",
		Columns: []catalog.ColumnSchema{
			{Name: "id", DataType: types.DataTypeInt8, Ordinal: 0},
			{Name: "name", DataType: types.DataTypeText, Ordinal: 1},
		},
	})

	planner := NewPlanner(cat, &mockReader{})
	stmt, err := Parse("SELECT * FROM users WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("plan error: %v", err)
	}

	scan, ok := plan.Root.(*executor.SeqScan)
	if !ok {
		t.Fatalf("expected SeqScan, got %T", plan.Root)
	}
	if scan.Filter == nil {
		t.Fatal("expected filter on SeqScan")
	}
}

func TestPlannerSelectWithLimit(t *testing.T) {
	cat := catalog.New()
	tableID := types.TableID(1)
	cat.AddTable(catalog.TableSchema{
		ID:   tableID,
		Name: "users",
		Columns: []catalog.ColumnSchema{
			{Name: "id", DataType: types.DataTypeInt8, Ordinal: 0},
		},
	})

	planner := NewPlanner(cat, &mockReader{})
	stmt, err := Parse("SELECT * FROM users LIMIT 10")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("plan error: %v", err)
	}

	lim, ok := plan.Root.(*executor.Limit)
	if !ok {
		t.Fatalf("expected Limit at root, got %T", plan.Root)
	}
	if lim.Remaining != 10 {
		t.Fatalf("expected remaining=10, got %d", lim.Remaining)
	}
}

func TestPlannerSelectWithWhereAndLimit(t *testing.T) {
	cat := catalog.New()
	tableID := types.TableID(1)
	cat.AddTable(catalog.TableSchema{
		ID:   tableID,
		Name: "users",
		Columns: []catalog.ColumnSchema{
			{Name: "id", DataType: types.DataTypeInt8, Ordinal: 0},
			{Name: "name", DataType: types.DataTypeText, Ordinal: 1},
		},
	})

	planner := NewPlanner(cat, &mockReader{})
	stmt, err := Parse("SELECT * FROM users WHERE id > 5 LIMIT 3")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("plan error: %v", err)
	}

	lim, ok := plan.Root.(*executor.Limit)
	if !ok {
		t.Fatalf("expected Limit at root, got %T", plan.Root)
	}

	scan, ok := lim.Input.(*executor.SeqScan)
	if !ok {
		t.Fatalf("expected SeqScan under Limit, got %T", lim.Input)
	}
	if scan.Filter == nil {
		t.Fatal("expected filter on SeqScan")
	}
}

func TestPlannerTableNotFound(t *testing.T) {
	cat := catalog.New()
	planner := NewPlanner(cat, &mockReader{})

	stmt, err := Parse("SELECT * FROM nonexistent")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	_, err = planner.Plan(stmt)
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
}

func TestParseDelete(t *testing.T) {
	tests := []struct {
		query string
		want  string
	}{
		{"DELETE FROM users", "users"},
		{"DELETE FROM users WHERE id = 1", "users"},
		{"DELETE FROM users WHERE name = 'alice'", "users"},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			stmt, err := Parse(tt.query)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			del, ok := stmt.(DeleteStmt)
			if !ok {
				t.Fatalf("expected DeleteStmt, got %T", stmt)
			}
			if del.Table.Name != tt.want {
				t.Fatalf("expected table %q, got %q", tt.want, del.Table.Name)
			}
		})
	}
}

func TestParseUpdate(t *testing.T) {
	tests := []struct {
		query  string
		table  string
		setLen int
	}{
		{"UPDATE users SET name = 'bob'", "users", 1},
		{"UPDATE users SET name = 'bob', age = 30", "users", 2},
		{"UPDATE users SET name = 'bob' WHERE id = 1", "users", 1},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			stmt, err := Parse(tt.query)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			upd, ok := stmt.(UpdateStmt)
			if !ok {
				t.Fatalf("expected UpdateStmt, got %T", stmt)
			}
			if upd.Table.Name != tt.table {
				t.Fatalf("expected table %q, got %q", tt.table, upd.Table.Name)
			}
			if len(upd.Set) != tt.setLen {
				t.Fatalf("expected %d set clauses, got %d", tt.setLen, len(upd.Set))
			}
		})
	}
}

func TestParseDropTable(t *testing.T) {
	stmt, err := Parse("DROP TABLE users")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	drop, ok := stmt.(DropTableStmt)
	if !ok {
		t.Fatalf("expected DropTableStmt, got %T", stmt)
	}
	if drop.Name != "users" {
		t.Fatalf("expected table name 'users', got %q", drop.Name)
	}
}

func TestPlannerDelete(t *testing.T) {
	cat := catalog.New()
	tableID := types.TableID(1)
	cat.AddTable(catalog.TableSchema{
		ID:   tableID,
		Name: "users",
		Columns: []catalog.ColumnSchema{
			{Name: "id", DataType: types.DataTypeInt8, Ordinal: 0},
			{Name: "name", DataType: types.DataTypeText, Ordinal: 1},
		},
	})

	planner := NewPlanner(cat, &mockReader{})
	stmt, err := Parse("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("plan error: %v", err)
	}

	del, ok := plan.Root.(*executor.DeleteExec)
	if !ok {
		t.Fatalf("expected DeleteExec at root, got %T", plan.Root)
	}
	if del.TableID != tableID {
		t.Fatalf("expected tableID=%d, got %d", tableID, del.TableID)
	}

	scan, ok := del.Input.(*executor.SeqScan)
	if !ok {
		t.Fatalf("expected SeqScan under DeleteExec (filter pushed down), got %T", del.Input)
	}
	if scan.Filter == nil {
		t.Fatal("expected filter pushed down into SeqScan")
	}
}

func TestPlannerUpdate(t *testing.T) {
	cat := catalog.New()
	tableID := types.TableID(1)
	cat.AddTable(catalog.TableSchema{
		ID:   tableID,
		Name: "users",
		Columns: []catalog.ColumnSchema{
			{Name: "id", DataType: types.DataTypeInt8, Ordinal: 0},
			{Name: "name", DataType: types.DataTypeText, Ordinal: 1},
		},
	})

	planner := NewPlanner(cat, &mockReader{})
	stmt, err := Parse("UPDATE users SET name = 'bob' WHERE id = 1")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("plan error: %v", err)
	}

	upd, ok := plan.Root.(*executor.UpdateExec)
	if !ok {
		t.Fatalf("expected UpdateExec at root, got %T", plan.Root)
	}
	if upd.TableID != tableID {
		t.Fatalf("expected tableID=%d, got %d", tableID, upd.TableID)
	}
	if len(upd.SetCols) != 1 || upd.SetCols[0] != "name" {
		t.Fatalf("expected setCols=['name'], got %v", upd.SetCols)
	}
}

func TestPlannerDeleteNoWhere(t *testing.T) {
	cat := catalog.New()
	tableID := types.TableID(1)
	cat.AddTable(catalog.TableSchema{
		ID:   tableID,
		Name: "users",
		Columns: []catalog.ColumnSchema{
			{Name: "id", DataType: types.DataTypeInt8, Ordinal: 0},
		},
	})

	planner := NewPlanner(cat, &mockReader{})
	stmt, err := Parse("DELETE FROM users")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("plan error: %v", err)
	}

	del, ok := plan.Root.(*executor.DeleteExec)
	if !ok {
		t.Fatalf("expected DeleteExec at root, got %T", plan.Root)
	}

	_, ok = del.Input.(*executor.SeqScan)
	if !ok {
		t.Fatalf("expected SeqScan under DeleteExec (no filter), got %T", del.Input)
	}
}
