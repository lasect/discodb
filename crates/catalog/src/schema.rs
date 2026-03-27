use discodb_types::DataType;
use discodb_types::{SchemaEpoch, TableId};

#[derive(Clone, Debug)]
pub struct TableSchema {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub epoch: SchemaEpoch,
}

#[derive(Clone, Debug)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub ordinal: u32,
}

impl TableSchema {
    pub fn new(id: TableId, name: String, columns: Vec<ColumnSchema>) -> Self {
        Self {
            id,
            name,
            columns,
            epoch: SchemaEpoch::min_value(),
        }
    }

    pub fn column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }
}

#[derive(Clone, Debug)]
pub struct IndexSchema {
    pub id: TableId,
    pub name: String,
    pub table_id: TableId,
    pub columns: Vec<String>,
    pub unique: bool,
    pub index_type: IndexType,
}

#[derive(Clone, Debug)]
pub enum IndexType {
    Primary,
    BTree,
    Hash,
}
