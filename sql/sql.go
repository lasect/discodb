package sql

import (
	"fmt"
	"strings"

	"discodb/executor"
	"discodb/types"
)

type Statement interface {
	statement()
}

type SelectStmt struct {
	Columns     []SelectColumn
	From        *TableRef
	WhereClause *Predicate
	GroupBy     []string
	OrderBy     []OrderBy
	Limit       *int
	Offset      *int
}

func (SelectStmt) statement() {}

type InsertStmt struct {
	Table   TableRef
	Columns []string
	Values  [][]Expr
}

func (InsertStmt) statement() {}

type UpdateStmt struct {
	Table       TableRef
	Set         []SetClause
	WhereClause *Predicate
}

func (UpdateStmt) statement() {}

type DeleteStmt struct {
	Table       TableRef
	WhereClause *Predicate
}

func (DeleteStmt) statement() {}

type CreateTableStmt struct {
	Name    string
	Columns []ColumnDef
}

func (CreateTableStmt) statement() {}

type DropTableStmt struct {
	Name string
}

func (DropTableStmt) statement() {}

type CreateIndexStmt struct {
	Name    string
	Table   string
	Columns []string
	Unique  bool
}

func (CreateIndexStmt) statement() {}

type SelectColumn struct {
	All   bool
	Name  string
	Alias string
	Expr  *Expr
}

type SetClause struct {
	Column string
	Value  Expr
}

type ColumnDef struct {
	Name     string
	DataType SQLDataType
	Nullable bool
	Default  *Expr
}

type SQLDataType string

const (
	SQLBool      SQLDataType = "bool"
	SQLInt2      SQLDataType = "int2"
	SQLInt4      SQLDataType = "int4"
	SQLInt8      SQLDataType = "int8"
	SQLFloat4    SQLDataType = "float4"
	SQLFloat8    SQLDataType = "float8"
	SQLText      SQLDataType = "text"
	SQLJSON      SQLDataType = "json"
	SQLBlob      SQLDataType = "blob"
	SQLTimestamp SQLDataType = "timestamp"
)

type TableRef struct {
	Name  string
	Alias string
}

type Predicate struct {
	Comparison *Comparison
	Logical    *LogicalPredicate
}

type Comparison struct {
	Left  Expr
	Op    CompOp
	Right Expr
}

type LogicalPredicate struct {
	Left  *Predicate
	Op    LogicalOp
	Right *Predicate
}

type CompOp string
type LogicalOp string
type BinOp string

const (
	CompEq   CompOp = "="
	CompNe   CompOp = "!="
	CompLt   CompOp = "<"
	CompLe   CompOp = "<="
	CompGt   CompOp = ">"
	CompGe   CompOp = ">="
	CompLike CompOp = "like"
	CompIn   CompOp = "in"
)

const (
	LogicalAnd LogicalOp = "and"
	LogicalOr  LogicalOp = "or"
)

const (
	BinAdd BinOp = "+"
	BinSub BinOp = "-"
	BinMul BinOp = "*"
	BinDiv BinOp = "/"
	BinMod BinOp = "%"
)

type Expr struct {
	Column   string
	Constant *Constant
	Function string
	Args     []Expr
	Left     *Expr
	Op       BinOp
	Right    *Expr
}

type Constant struct {
	Value types.Value
}

type OrderBy struct {
	Expr      Expr
	Ascending bool
}

func Parse(query string) (Statement, error) {
	trimmed := strings.TrimSpace(query)
	upper := strings.ToUpper(trimmed)

	switch {
	case strings.HasPrefix(upper, "SELECT"):
		return SelectStmt{
			Columns: []SelectColumn{{All: true}},
		}, nil
	case strings.HasPrefix(upper, "WITH"):
		return nil, fmt.Errorf("unsupported: complex queries")
	case strings.HasPrefix(upper, "INSERT"):
		return nil, fmt.Errorf("unsupported: INSERT")
	case strings.HasPrefix(upper, "CREATE TABLE"):
		return nil, fmt.Errorf("unsupported: CREATE TABLE")
	default:
		return nil, fmt.Errorf("unsupported: discodb only supports simple SELECT")
	}
}

type Planner struct{}

func NewPlanner() Planner {
	return Planner{}
}

func (Planner) Plan(stmt Statement) (executor.PhysicalPlan, error) {
	switch stmt.(type) {
	case SelectStmt:
		return executor.PhysicalPlan{
			Root: executor.NewSeqScan(types.MinTableID(), nil, nil),
		}, nil
	default:
		return executor.PhysicalPlan{}, fmt.Errorf("unsupported: only SELECT is supported")
	}
}
