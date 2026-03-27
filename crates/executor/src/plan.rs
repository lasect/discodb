use crate::error::ExecutorResult;
use discodb_types::{TableId, Value};

pub struct PhysicalPlan {
    pub root: Box<dyn Executor>,
}

pub trait Executor: Send {
    fn execute(&mut self) -> ExecutorResult<Option<RowBatch>>;
}

pub struct RowBatch {
    pub rows: Vec<Row>,
    pub schema: Vec<ColumnInfo>,
}

#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub table_id: Option<TableId>,
    pub ordinal: usize,
}

#[derive(Clone, Debug)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }
}

#[derive(Clone, Debug)]
pub enum ScanType {
    Sequential,
    Index(IndexScanPlan),
}

#[derive(Clone, Debug)]
pub struct SeqScanPlan {
    pub table_id: TableId,
    pub filter: Option<Predicate>,
}

#[derive(Clone, Debug)]
pub struct IndexScanPlan {
    pub table_id: TableId,
    pub index_id: TableId,
    pub key_range: Option<(Value, Value)>,
}

pub struct FilterPlan {
    pub input: Box<PhysicalPlan>,
    pub predicate: Predicate,
}

pub struct ProjectionPlan {
    pub input: Box<PhysicalPlan>,
    pub columns: Vec<String>,
}

pub struct LimitPlan {
    pub input: Box<PhysicalPlan>,
    pub limit: usize,
    pub offset: usize,
}

pub struct AggregatePlan {
    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<Aggregate>,
}

#[derive(Clone, Debug)]
pub enum Aggregate {
    Count(String),
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

#[derive(Clone, Debug)]
pub struct Predicate {
    pub left: Expression,
    pub op: ComparisonOp,
    pub right: Expression,
}

#[derive(Clone, Debug)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    In,
    Like,
}

#[derive(Clone, Debug)]
pub enum Expression {
    Column(usize),
    Constant(Value),
    Function(String, Vec<Expression>),
}
