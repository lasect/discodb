use crate::error::{SqlError, SqlResult};

#[derive(Clone, Debug)]
pub enum Statement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    CreateIndex(CreateIndexStmt),
}

#[derive(Clone, Debug)]
pub struct SelectStmt {
    pub columns: Vec<SelectColumn>,
    pub from: Option<TableRef>,
    pub where_clause: Option<Predicate>,
    pub group_by: Vec<String>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Clone, Debug)]
pub enum SelectColumn {
    All,
    Column { name: String, alias: Option<String> },
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Clone, Debug)]
pub struct InsertStmt {
    pub table: TableRef,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expr>>,
}

#[derive(Clone, Debug)]
pub struct UpdateStmt {
    pub table: TableRef,
    pub set: Vec<SetClause>,
    pub where_clause: Option<Predicate>,
}

#[derive(Clone, Debug)]
pub struct SetClause {
    pub column: String,
    pub value: Expr,
}

#[derive(Clone, Debug)]
pub struct DeleteStmt {
    pub table: TableRef,
    pub where_clause: Option<Predicate>,
}

#[derive(Clone, Debug)]
pub struct CreateTableStmt {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Clone, Debug)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: SqlDataType,
    pub nullable: bool,
    pub default: Option<Expr>,
}

#[derive(Clone, Debug)]
pub enum SqlDataType {
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
}

#[derive(Clone, Debug)]
pub struct DropTableStmt {
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct CreateIndexStmt {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Clone, Debug)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Predicate {
    Comparison(Comparison),
    Logical {
        left: Box<Predicate>,
        op: LogicalOp,
        right: Box<Predicate>,
    },
}

#[derive(Clone, Debug)]
pub struct Comparison {
    pub left: Expr,
    pub op: CompOp,
    pub right: Expr,
}

#[derive(Clone, Debug)]
pub enum CompOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    In,
}

#[derive(Clone, Debug)]
pub enum LogicalOp {
    And,
    Or,
}

#[derive(Clone, Debug)]
pub enum Expr {
    Column(String),
    Constant(Constant),
    Function {
        name: String,
        args: Vec<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
}

#[derive(Clone, Debug)]
pub enum Constant {
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Null,
}

#[derive(Clone, Debug)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Clone, Debug)]
pub struct OrderBy {
    pub expr: Expr,
    pub ascending: bool,
}

pub fn parse(sql: &str) -> SqlResult<Statement> {
    let sql = sql.trim();

    if sql.to_uppercase().starts_with("SELECT") || sql.to_uppercase().starts_with("WITH") {
        return Err(SqlError::Unsupported("complex queries".to_string()));
    }

    if sql.to_uppercase().starts_with("INSERT") {
        return Err(SqlError::Unsupported("INSERT".to_string()));
    }

    if sql.to_uppercase().starts_with("CREATE TABLE") {
        return Err(SqlError::Unsupported("CREATE TABLE".to_string()));
    }

    Err(SqlError::Unsupported(
        "MVP only supports simple SELECT".to_string(),
    ))
}
