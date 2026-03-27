use crate::schema::{IndexSchema, TableSchema};
use ahash::AHashMap;
use discodb_types::{SchemaEpoch, TableId};

pub struct Catalog {
    tables: AHashMap<TableId, TableSchema>,
    table_names: AHashMap<String, TableId>,
    indexes: AHashMap<TableId, IndexSchema>,
    index_names: AHashMap<String, TableId>,
    epoch: SchemaEpoch,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: AHashMap::new(),
            table_names: AHashMap::new(),
            indexes: AHashMap::new(),
            index_names: AHashMap::new(),
            epoch: SchemaEpoch::min_value(),
        }
    }

    pub fn add_table(&mut self, schema: TableSchema) -> TableId {
        let id = schema.id;
        self.table_names.insert(schema.name.clone(), id);
        self.tables.insert(id, schema);
        self.epoch = self.epoch.increment();
        id
    }

    pub fn get_table(&self, id: TableId) -> Option<&TableSchema> {
        self.tables.get(&id)
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<&TableSchema> {
        self.table_names
            .get(name)
            .and_then(|id| self.tables.get(id))
    }

    pub fn add_index(&mut self, schema: IndexSchema) -> TableId {
        let id = schema.id;
        self.index_names.insert(schema.name.clone(), id);
        self.indexes.insert(id, schema);
        id
    }

    pub fn get_index(&self, id: TableId) -> Option<&IndexSchema> {
        self.indexes.get(&id)
    }

    pub fn get_index_by_name(&self, name: &str) -> Option<&IndexSchema> {
        self.index_names
            .get(name)
            .and_then(|id| self.indexes.get(id))
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableSchema> {
        self.tables.values()
    }

    pub fn indexes(&self) -> impl Iterator<Item = &IndexSchema> {
        self.indexes.values()
    }

    pub fn epoch(&self) -> SchemaEpoch {
        self.epoch
    }

    pub fn increment_epoch(&mut self) {
        self.epoch = self.epoch.increment();
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

pub const SYSTEM_TABLES: &[&str] = &["sys_tables", "sys_columns", "sys_indexes"];
