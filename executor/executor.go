package executor

import (
	"context"
	"errors"

	"discodb/types"
)

var ErrUnsupportedExpression = errors.New("unsupported expression")

type PhysicalPlan struct {
	Root Executor
}

type Executor interface {
	Execute(context.Context) (RowBatch, bool, error)
}

type RowBatch struct {
	Rows   []Row
	Schema []ColumnInfo
}

type ColumnInfo struct {
	Name    string         `json:"name"`
	TableID *types.TableID `json:"table_id,omitempty"`
	Ordinal int            `json:"ordinal"`
}

type Row struct {
	Values []types.Value `json:"values"`
}

func NewRow(values []types.Value) Row {
	return Row{Values: values}
}

func (r Row) Get(idx int) (types.Value, bool) {
	if idx < 0 || idx >= len(r.Values) {
		return types.Value{}, false
	}
	return r.Values[idx], true
}

type ComparisonOp string

const (
	ComparisonEq   ComparisonOp = "="
	ComparisonNe   ComparisonOp = "!="
	ComparisonLt   ComparisonOp = "<"
	ComparisonLe   ComparisonOp = "<="
	ComparisonGt   ComparisonOp = ">"
	ComparisonGe   ComparisonOp = ">="
	ComparisonAnd  ComparisonOp = "and"
	ComparisonOr   ComparisonOp = "or"
	ComparisonIn   ComparisonOp = "in"
	ComparisonLike ComparisonOp = "like"
)

type Expression struct {
	ColumnIndex *int         `json:"column_index,omitempty"`
	Constant    *types.Value `json:"constant,omitempty"`
	Function    string       `json:"function,omitempty"`
	Args        []Expression `json:"args,omitempty"`
}

type Predicate struct {
	Left  Expression   `json:"left"`
	Op    ComparisonOp `json:"op"`
	Right Expression   `json:"right"`
}

type Aggregate string

const (
	AggregateCount Aggregate = "count"
	AggregateSum   Aggregate = "sum"
	AggregateAvg   Aggregate = "avg"
	AggregateMin   Aggregate = "min"
	AggregateMax   Aggregate = "max"
)

type SeqScan struct {
	TableID types.TableID
	Filter  *Predicate
	Schema  []ColumnInfo
}

func NewSeqScan(tableID types.TableID, filter *Predicate, schema []ColumnInfo) *SeqScan {
	return &SeqScan{TableID: tableID, Filter: filter, Schema: schema}
}

func (s *SeqScan) Execute(context.Context) (RowBatch, bool, error) {
	return RowBatch{Rows: []Row{}, Schema: append([]ColumnInfo(nil), s.Schema...)}, true, nil
}

type IndexScan struct {
	TableID  types.TableID
	IndexID  types.TableID
	KeyRange *[2]types.Value
	Schema   []ColumnInfo
}

func NewIndexScan(tableID, indexID types.TableID, keyRange *[2]types.Value, schema []ColumnInfo) *IndexScan {
	return &IndexScan{TableID: tableID, IndexID: indexID, KeyRange: keyRange, Schema: schema}
}

func (s *IndexScan) Execute(context.Context) (RowBatch, bool, error) {
	return RowBatch{Rows: []Row{}, Schema: append([]ColumnInfo(nil), s.Schema...)}, true, nil
}

type Filter struct {
	Input     Executor
	Predicate Predicate
}

func (f *Filter) Execute(ctx context.Context) (RowBatch, bool, error) {
	return RowBatch{}, true, nil
}

type Projection struct {
	Input   Executor
	Columns []int
}

func (p *Projection) Execute(ctx context.Context) (RowBatch, bool, error) {
	return RowBatch{}, true, nil
}

type Limit struct {
	Input     Executor
	Remaining int
	Offset    int
}

func (l *Limit) Execute(ctx context.Context) (RowBatch, bool, error) {
	if l.Remaining == 0 {
		return RowBatch{}, false, nil
	}
	return RowBatch{}, true, nil
}

func EvaluatePredicate(row Row, pred Predicate) bool {
	left, ok := evaluateExpression(row, pred.Left)
	if !ok {
		return false
	}
	right, ok := evaluateExpression(row, pred.Right)
	if !ok {
		return false
	}
	switch pred.Op {
	case ComparisonEq:
		return left.Equal(right)
	case ComparisonNe:
		return !left.Equal(right)
	default:
		return false
	}
}

func evaluateExpression(row Row, expr Expression) (types.Value, bool) {
	if expr.ColumnIndex != nil {
		return row.Get(*expr.ColumnIndex)
	}
	if expr.Constant != nil {
		return *expr.Constant, true
	}
	return types.NullValue(), false
}
