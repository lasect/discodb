package catalog

import "discodb/types"

type TableSchema struct {
	ID      types.TableID     `json:"id"`
	Name    string            `json:"name"`
	Columns []ColumnSchema    `json:"columns"`
	Epoch   types.SchemaEpoch `json:"epoch"`
}

type ColumnSchema struct {
	Name     string         `json:"name"`
	DataType types.DataType `json:"data_type"`
	Nullable bool           `json:"nullable"`
	Ordinal  uint32         `json:"ordinal"`
}

func NewTableSchema(id types.TableID, name string, columns []ColumnSchema) TableSchema {
	return TableSchema{ID: id, Name: name, Columns: columns, Epoch: types.MinSchemaEpoch()}
}

func (s TableSchema) Column(name string) (ColumnSchema, bool) {
	for _, col := range s.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return ColumnSchema{}, false
}

func (s TableSchema) ColumnIndex(name string) (int, bool) {
	for i, col := range s.Columns {
		if col.Name == name {
			return i, true
		}
	}
	return 0, false
}

type IndexType string

const (
	IndexTypePrimary IndexType = "primary"
	IndexTypeBTree   IndexType = "btree"
	IndexTypeHash    IndexType = "hash"
)

type IndexSchema struct {
	ID        types.TableID `json:"id"`
	Name      string        `json:"name"`
	TableID   types.TableID `json:"table_id"`
	Columns   []string      `json:"columns"`
	Unique    bool          `json:"unique"`
	IndexType IndexType     `json:"index_type"`
}

type Catalog struct {
	tables     map[types.TableID]TableSchema
	tableNames map[string]types.TableID
	indexes    map[types.TableID]IndexSchema
	indexNames map[string]types.TableID
	epoch      types.SchemaEpoch
}

func New() *Catalog {
	return &Catalog{
		tables:     make(map[types.TableID]TableSchema),
		tableNames: make(map[string]types.TableID),
		indexes:    make(map[types.TableID]IndexSchema),
		indexNames: make(map[string]types.TableID),
		epoch:      types.MinSchemaEpoch(),
	}
}

func (c *Catalog) AddTable(schema TableSchema) types.TableID {
	c.tables[schema.ID] = schema
	c.tableNames[schema.Name] = schema.ID
	c.epoch = c.epoch.Increment()
	return schema.ID
}

func (c *Catalog) GetTable(id types.TableID) (TableSchema, bool) {
	schema, ok := c.tables[id]
	return schema, ok
}

func (c *Catalog) GetTableByName(name string) (TableSchema, bool) {
	id, ok := c.tableNames[name]
	if !ok {
		return TableSchema{}, false
	}
	return c.GetTable(id)
}

func (c *Catalog) AddIndex(schema IndexSchema) types.TableID {
	c.indexes[schema.ID] = schema
	c.indexNames[schema.Name] = schema.ID
	return schema.ID
}

func (c *Catalog) GetIndex(id types.TableID) (IndexSchema, bool) {
	schema, ok := c.indexes[id]
	return schema, ok
}

func (c *Catalog) GetIndexByName(name string) (IndexSchema, bool) {
	id, ok := c.indexNames[name]
	if !ok {
		return IndexSchema{}, false
	}
	return c.GetIndex(id)
}

func (c *Catalog) Epoch() types.SchemaEpoch {
	return c.epoch
}

var SystemTables = []string{"sys_tables", "sys_columns", "sys_indexes"}
