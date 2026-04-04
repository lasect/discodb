package catalog

import (
	"testing"

	"discodb/types"
)

func TestCatalogAddAndGet(t *testing.T) {
	cat := New()
	tableID := types.TableID(1)

	schema := TableSchema{
		ID:   tableID,
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "id", DataType: types.DataTypeInt8, Ordinal: 0},
			{Name: "name", DataType: types.DataTypeText, Ordinal: 1},
		},
	}

	cat.AddTable(schema)

	got, ok := cat.GetTable(tableID)
	if !ok {
		t.Fatal("expected table to exist")
	}
	if got.Name != "users" {
		t.Fatalf("expected name 'users', got %q", got.Name)
	}

	got2, ok := cat.GetTableByName("users")
	if !ok {
		t.Fatal("expected table by name to exist")
	}
	if got2.ID != tableID {
		t.Fatalf("expected ID=%d, got %d", tableID, got2.ID)
	}
}

func TestCatalogRemoveTable(t *testing.T) {
	cat := New()
	tableID := types.TableID(1)

	schema := TableSchema{
		ID:   tableID,
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "id", DataType: types.DataTypeInt8, Ordinal: 0},
		},
	}

	cat.AddTable(schema)

	cat.RemoveTable(tableID)

	_, ok := cat.GetTable(tableID)
	if ok {
		t.Fatal("expected table to be removed")
	}

	_, ok = cat.GetTableByName("users")
	if ok {
		t.Fatal("expected table by name to be removed")
	}

	if len(cat.tables) != 0 {
		t.Fatalf("expected empty tables map, got %d entries", len(cat.tables))
	}
	if len(cat.tableNames) != 0 {
		t.Fatalf("expected empty tableNames map, got %d entries", len(cat.tableNames))
	}
}

func TestCatalogRemoveTableNotFound(t *testing.T) {
	cat := New()

	cat.RemoveTable(types.TableID(999))

	if len(cat.tables) != 0 {
		t.Fatal("expected no panic on removing nonexistent table")
	}
}
