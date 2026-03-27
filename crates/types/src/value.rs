use crate::error::TypesError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Null,
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Text,
    Json,
    Blob,
    Timestamp,
    Date,
}

impl DataType {
    pub fn is_null(&self) -> bool {
        matches!(self, DataType::Null)
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int2 | DataType::Int4 | DataType::Int8 | DataType::Float4 | DataType::Float8
        )
    }

    pub fn is_textual(&self) -> bool {
        matches!(self, DataType::Text | DataType::Json)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Text(String),
    Json(serde_json::Value),
    Blob(Vec<u8>),
    Timestamp(i64),
    Date(i32),
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Bool(_) => DataType::Bool,
            Value::Int2(_) => DataType::Int2,
            Value::Int4(_) => DataType::Int4,
            Value::Int8(_) => DataType::Int8,
            Value::Float4(_) => DataType::Float4,
            Value::Float8(_) => DataType::Float8,
            Value::Text(_) => DataType::Text,
            Value::Json(_) => DataType::Json,
            Value::Blob(_) => DataType::Blob,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Date(_) => DataType::Date,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int2(n) => Some(*n as i64),
            Value::Int4(n) => Some(*n as i64),
            Value::Int8(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float4(n) => Some(*n as f64),
            Value::Float8(n) => Some(*n),
            _ => self.as_i64().map(|n| n as f64),
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(b) => Some(b),
            _ => None,
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl TryFrom<serde_json::Value> for Value {
    type Error = crate::error::TypesError;

    fn try_from(v: serde_json::Value) -> Result<Self, Self::Error> {
        match v {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::Int8(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::Float8(f))
                } else {
                    Err(TypesError::InvalidJsonConversion)
                }
            }
            serde_json::Value::String(s) => Ok(Value::Text(s)),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => Ok(Value::Json(v)),
        }
    }
}

impl From<Value> for serde_json::Value {
    fn from(v: Value) -> Self {
        match v {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(b),
            Value::Int2(n) => serde_json::Value::Number(n.into()),
            Value::Int4(n) => serde_json::Value::Number(n.into()),
            Value::Int8(n) => serde_json::Number::from_i128(n as i128)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::Float4(n) => serde_json::Number::from_f64(n as f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::Float8(n) => serde_json::Number::from_f64(n)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::Text(s) => serde_json::Value::String(s),
            Value::Json(j) => j,
            Value::Blob(_) => serde_json::Value::String("<blob>".to_string()),
            Value::Timestamp(n) => serde_json::Value::Number(n.into()),
            Value::Date(n) => serde_json::Value::Number(n.into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: false,
            default: None,
        }
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    pub fn with_default(mut self, default: Value) -> Self {
        self.default = Some(default);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub columns: Vec<Column>,
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(columns: Vec<Column>, values: Vec<Value>) -> Self {
        assert_eq!(columns.len(), values.len());
        Self { columns, values }
    }

    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }

    pub fn get_by_name(&self, name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|c| c.name == name)
            .and_then(|i| self.values.get(i))
    }
}
