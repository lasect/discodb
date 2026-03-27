use crate::{
    ColumnInfo, ComparisonOp, Executor, ExecutorResult, Expression, PhysicalPlan, Predicate, Row,
    RowBatch,
};
use discodb_types::{TableId, Value};

pub struct SeqScan {
    table_id: TableId,
    filter: Option<Predicate>,
    schema: Vec<ColumnInfo>,
    current_batch: Option<RowBatch>,
}

impl SeqScan {
    pub fn new(table_id: TableId, filter: Option<Predicate>, schema: Vec<ColumnInfo>) -> Self {
        Self {
            table_id,
            filter,
            schema,
            current_batch: None,
        }
    }
}

impl Executor for SeqScan {
    fn execute(&mut self) -> ExecutorResult<Option<RowBatch>> {
        Ok(Some(RowBatch {
            rows: Vec::new(),
            schema: self.schema.clone(),
        }))
    }
}

pub struct IndexScan {
    table_id: TableId,
    index_id: TableId,
    key_range: Option<(Value, Value)>,
    schema: Vec<ColumnInfo>,
}

impl IndexScan {
    pub fn new(
        table_id: TableId,
        index_id: TableId,
        key_range: Option<(Value, Value)>,
        schema: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            table_id,
            index_id,
            key_range,
            schema,
        }
    }
}

impl Executor for IndexScan {
    fn execute(&mut self) -> ExecutorResult<Option<RowBatch>> {
        Ok(Some(RowBatch {
            rows: Vec::new(),
            schema: self.schema.clone(),
        }))
    }
}

pub struct Filter {
    input: Box<dyn Executor>,
    predicate: Predicate,
}

impl Filter {
    pub fn new(input: Box<dyn Executor>, predicate: Predicate) -> Self {
        Self { input, predicate }
    }
}

impl Executor for Filter {
    fn execute(&mut self) -> ExecutorResult<Option<RowBatch>> {
        Ok(Some(RowBatch {
            rows: Vec::new(),
            schema: Vec::new(),
        }))
    }
}

pub struct Projection {
    input: Box<dyn Executor>,
    columns: Vec<usize>,
}

impl Projection {
    pub fn new(input: Box<dyn Executor>, columns: Vec<usize>) -> Self {
        Self { input, columns }
    }
}

impl Executor for Projection {
    fn execute(&mut self) -> ExecutorResult<Option<RowBatch>> {
        Ok(Some(RowBatch {
            rows: Vec::new(),
            schema: Vec::new(),
        }))
    }
}

pub struct Limit {
    input: Box<dyn Executor>,
    remaining: usize,
    offset: usize,
}

impl Limit {
    pub fn new(input: Box<dyn Executor>, limit: usize, offset: usize) -> Self {
        Self {
            input,
            remaining: limit,
            offset,
        }
    }
}

impl Executor for Limit {
    fn execute(&mut self) -> ExecutorResult<Option<RowBatch>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        Ok(Some(RowBatch {
            rows: Vec::new(),
            schema: Vec::new(),
        }))
    }
}

pub fn evaluate_predicate(row: &Row, pred: &Predicate) -> bool {
    let left_val = match &pred.left {
        Expression::Column(idx) => row.get(*idx).cloned().unwrap_or(Value::Null),
        Expression::Constant(v) => v.clone(),
        _ => Value::Null,
    };

    let right_val = match &pred.right {
        Expression::Column(idx) => row.get(*idx).cloned().unwrap_or(Value::Null),
        Expression::Constant(v) => v.clone(),
        _ => Value::Null,
    };

    match pred.op {
        ComparisonOp::Eq => left_val == right_val,
        ComparisonOp::Ne => left_val != right_val,
        _ => false,
    }
}
