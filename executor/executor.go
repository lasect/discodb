package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"discodb/index"
	"discodb/mvcc"
	"discodb/storage"
	"discodb/types"
)

var ErrUnsupportedExpression = errors.New("unsupported expression")

type StorageReader interface {
	ReadRows(ctx context.Context, tableID types.TableID) ([]storage.Row, error)
}

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

// RowMeta contains storage-level metadata for a row, used for UPDATE/DELETE operations
type RowMeta struct {
	RowID     types.RowID     `json:"row_id"`
	SegmentID types.SegmentID `json:"segment_id"`
	MessageID types.MessageID `json:"message_id"`
}

type Row struct {
	Values []types.Value `json:"values"`
	Meta   *RowMeta      `json:"meta,omitempty"`
}

func NewRow(values []types.Value) Row {
	return Row{Values: values}
}

func NewRowWithMeta(values []types.Value, meta *RowMeta) Row {
	return Row{Values: values, Meta: meta}
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
	Left  Expression   `json:"left,omitempty"`
	Op    ComparisonOp `json:"op,omitempty"`
	Right Expression   `json:"right,omitempty"`

	LeftPred  *Predicate   `json:"left_pred,omitempty"`
	RightPred *Predicate   `json:"right_pred,omitempty"`
	LogicalOp ComparisonOp `json:"logical_op,omitempty"`
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
	Reader   StorageReader
	TableID  types.TableID
	Filter   *Predicate
	Schema   []ColumnInfo
	Snapshot *mvcc.TransactionSnapshot
}

func NewSeqScan(reader StorageReader, tableID types.TableID, filter *Predicate, schema []ColumnInfo) *SeqScan {
	return &SeqScan{Reader: reader, TableID: tableID, Filter: filter, Schema: append([]ColumnInfo(nil), schema...)}
}

func NewSeqScanWithSnapshot(reader StorageReader, tableID types.TableID, filter *Predicate, schema []ColumnInfo, snap mvcc.TransactionSnapshot) *SeqScan {
	return &SeqScan{Reader: reader, TableID: tableID, Filter: filter, Schema: append([]ColumnInfo(nil), schema...), Snapshot: &snap}
}

func (s *SeqScan) Execute(ctx context.Context) (RowBatch, bool, error) {
	storageRows, err := s.Reader.ReadRows(ctx, s.TableID)
	if err != nil {
		return RowBatch{}, false, fmt.Errorf("seq scan read: %w", err)
	}

	var rows []Row
	for _, sr := range storageRows {
		if sr.Header.Flags.HasTombstone() {
			continue
		}

		if s.Snapshot != nil {
			var txnMax *types.TxnID
			if sr.Header.TxnMax > 0 {
				txnMax = &sr.Header.TxnMax
			}
			if !s.Snapshot.IsVisible(sr.Header.TxnID, txnMax) {
				continue
			}
		}

		values := storageRowToValues(sr)
		for len(values) < len(s.Schema) {
			values = append(values, types.NullValue())
		}

		meta := &RowMeta{
			RowID:     sr.Header.RowID,
			SegmentID: sr.Header.SegmentID,
			MessageID: sr.Header.MessageID,
		}

		if s.Filter != nil {
			row := Row{Values: values, Meta: meta}
			if !EvaluatePredicate(row, *s.Filter) {
				continue
			}
		}

		rows = append(rows, Row{Values: values, Meta: meta})
	}

	return RowBatch{Rows: rows, Schema: append([]ColumnInfo(nil), s.Schema...)}, true, nil
}

type IndexManager interface {
	Lookup(ctx context.Context, indexID types.TableID, key []byte) ([]index.IndexEntry, error)
	Range(ctx context.Context, indexID types.TableID, startKey []byte, endKey []byte) ([]index.IndexEntry, error)
	FetchRow(ctx context.Context, tableID types.TableID, segmentID types.SegmentID, messageID types.MessageID) (*storage.Row, error)
}

type IndexScan struct {
	TableID    types.TableID
	IndexID    types.TableID
	KeyRange   *[2]types.Value
	Schema     []ColumnInfo
	IndexMgr   IndexManager
	Snapshot   *mvcc.TransactionSnapshot
	StorageRdr StorageReader
}

func NewIndexScan(tableID, indexID types.TableID, keyRange *[2]types.Value, schema []ColumnInfo) *IndexScan {
	return &IndexScan{TableID: tableID, IndexID: indexID, KeyRange: keyRange, Schema: append([]ColumnInfo(nil), schema...)}
}

func NewIndexScanWithMgr(tableID, indexID types.TableID, keyRange *[2]types.Value, schema []ColumnInfo, mgr IndexManager, snap mvcc.TransactionSnapshot, rdr StorageReader) *IndexScan {
	return &IndexScan{TableID: tableID, IndexID: indexID, KeyRange: keyRange, Schema: append([]ColumnInfo(nil), schema...), IndexMgr: mgr, Snapshot: &snap, StorageRdr: rdr}
}

func (s *IndexScan) Execute(ctx context.Context) (RowBatch, bool, error) {
	if s.IndexMgr == nil {
		return RowBatch{Rows: []Row{}, Schema: append([]ColumnInfo(nil), s.Schema...)}, true, nil
	}

	var entries []index.IndexEntry
	var err error

	if s.KeyRange != nil {
		startKey := valueToKey(s.KeyRange[0])
		endKey := valueToKey(s.KeyRange[1])
		entries, err = s.IndexMgr.Range(ctx, s.IndexID, startKey, endKey)
	} else {
		entries, err = s.IndexMgr.Lookup(ctx, s.IndexID, nil)
	}
	if err != nil {
		return RowBatch{}, false, fmt.Errorf("index scan: %w", err)
	}

	var rows []Row
	for _, entry := range entries {
		if s.StorageRdr != nil {
			storageRows, err := s.StorageRdr.ReadRows(ctx, s.TableID)
			if err != nil {
				continue
			}

			for _, sr := range storageRows {
				if sr.Header.RowID != entry.RowID || sr.Header.SegmentID != entry.SegmentID || sr.Header.MessageID != entry.MessageID {
					continue
				}

				if sr.Header.Flags.HasTombstone() {
					continue
				}

				if s.Snapshot != nil {
					var txnMax *types.TxnID
					if sr.Header.TxnMax > 0 {
						txnMax = &sr.Header.TxnMax
					}
					if !s.Snapshot.IsVisible(sr.Header.TxnID, txnMax) {
						continue
					}
				}

				values := storageRowToValues(sr)
				for len(values) < len(s.Schema) {
					values = append(values, types.NullValue())
				}

				meta := &RowMeta{
					RowID:     sr.Header.RowID,
					SegmentID: sr.Header.SegmentID,
					MessageID: sr.Header.MessageID,
				}

				rows = append(rows, Row{Values: values, Meta: meta})
				break
			}
		}
	}

	return RowBatch{Rows: rows, Schema: append([]ColumnInfo(nil), s.Schema...)}, true, nil
}

func valueToKey(v types.Value) []byte {
	if !v.Valid {
		return nil
	}
	if s, ok := v.Raw.(string); ok {
		return []byte(s)
	}
	return []byte(v.PGText())
}

type Filter struct {
	Input     Executor
	Predicate Predicate
}

func (f *Filter) Execute(ctx context.Context) (RowBatch, bool, error) {
	batch, done, err := f.Input.Execute(ctx)
	if err != nil {
		return RowBatch{}, false, err
	}

	var filtered []Row
	for _, row := range batch.Rows {
		if EvaluatePredicate(row, f.Predicate) {
			filtered = append(filtered, row)
		}
	}

	return RowBatch{Rows: filtered, Schema: append([]ColumnInfo(nil), batch.Schema...)}, done, nil
}

type Projection struct {
	Input   Executor
	Columns []int
	Schema  []ColumnInfo
}

func (p *Projection) Execute(ctx context.Context) (RowBatch, bool, error) {
	batch, done, err := p.Input.Execute(ctx)
	if err != nil {
		return RowBatch{}, false, err
	}

	var projected []Row
	for _, row := range batch.Rows {
		var vals []types.Value
		for _, idx := range p.Columns {
			if idx >= 0 && idx < len(row.Values) {
				vals = append(vals, row.Values[idx])
			} else {
				vals = append(vals, types.NullValue())
			}
		}
		projected = append(projected, Row{Values: vals})
	}

	schema := append([]ColumnInfo(nil), p.Schema...)
	return RowBatch{Rows: projected, Schema: schema}, done, nil
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

	batch, done, err := l.Input.Execute(ctx)
	if err != nil {
		return RowBatch{}, false, err
	}

	var result []Row
	for _, row := range batch.Rows {
		if l.Offset > 0 {
			l.Offset--
			continue
		}
		if l.Remaining <= 0 {
			return RowBatch{Rows: result, Schema: append([]ColumnInfo(nil), batch.Schema...)}, false, nil
		}
		result = append(result, row)
		l.Remaining--
	}

	if l.Remaining == 0 {
		done = false
	}

	return RowBatch{Rows: result, Schema: append([]ColumnInfo(nil), batch.Schema...)}, done, nil
}

type Values struct {
	Rows   []Row
	Schema []ColumnInfo
	pos    int
}

func NewValues(rows []Row, schema []ColumnInfo) *Values {
	return &Values{Rows: rows, Schema: append([]ColumnInfo(nil), schema...)}
}

func (v *Values) Execute(context.Context) (RowBatch, bool, error) {
	if v.pos >= len(v.Rows) {
		return RowBatch{}, true, nil
	}
	v.pos = len(v.Rows)
	return RowBatch{Rows: v.Rows, Schema: append([]ColumnInfo(nil), v.Schema...)}, true, nil
}

type DeleteExec struct {
	Input    Executor
	TableID  types.TableID
	consumed bool
}

func (d *DeleteExec) Execute(ctx context.Context) (RowBatch, bool, error) {
	if d.consumed {
		return RowBatch{}, true, nil
	}
	d.consumed = true

	var rows []Row
	for {
		batch, done, err := d.Input.Execute(ctx)
		if err != nil {
			return RowBatch{}, false, err
		}
		rows = append(rows, batch.Rows...)
		if done {
			break
		}
	}

	return RowBatch{Rows: rows, Schema: []ColumnInfo{{Name: "deleted", Ordinal: 0}}}, true, nil
}

type UpdateExec struct {
	Input    Executor
	TableID  types.TableID
	SetCols  []string
	SetExprs []Expression
	consumed bool
}

func (u *UpdateExec) Execute(ctx context.Context) (RowBatch, bool, error) {
	if u.consumed {
		return RowBatch{}, true, nil
	}
	u.consumed = true

	var rows []Row
	for {
		batch, done, err := u.Input.Execute(ctx)
		if err != nil {
			return RowBatch{}, false, err
		}
		rows = append(rows, batch.Rows...)
		if done {
			break
		}
	}

	return RowBatch{Rows: rows, Schema: []ColumnInfo{{Name: "updated", Ordinal: 0}}}, true, nil
}

type AggregateExec struct {
	Input    Executor
	Funcs    []Aggregate
	ColIdxs  []int
	Aliases  []string
	consumed bool
}

func (a *AggregateExec) Execute(ctx context.Context) (RowBatch, bool, error) {
	if a.consumed {
		return RowBatch{}, true, nil
	}
	a.consumed = true

	var accumulators []aggAccumulator
	for _, fn := range a.Funcs {
		accumulators = append(accumulators, newAccumulator(fn))
	}

	for {
		batch, done, err := a.Input.Execute(ctx)
		if err != nil {
			return RowBatch{}, false, err
		}

		for _, row := range batch.Rows {
			for i, idx := range a.ColIdxs {
				accumulators[i].accumulate(row, idx)
			}
		}

		if done {
			break
		}
	}

	var vals []types.Value
	var schema []ColumnInfo
	for i, acc := range accumulators {
		vals = append(vals, acc.result())
		name := ""
		if i < len(a.Aliases) {
			name = a.Aliases[i]
		}
		if name == "" {
			name = string(a.Funcs[i])
		}
		schema = append(schema, ColumnInfo{Name: name, Ordinal: i})
	}

	return RowBatch{Rows: []Row{{Values: vals}}, Schema: schema}, true, nil
}

type aggAccumulator struct {
	fn    Aggregate
	count int
	sum   float64
	min   types.Value
	max   types.Value
}

func newAccumulator(fn Aggregate) aggAccumulator {
	a := aggAccumulator{fn: fn}
	if fn == AggregateMin {
		a.min = types.NullValue()
	}
	if fn == AggregateMax {
		a.max = types.NullValue()
	}
	return a
}

func (a *aggAccumulator) accumulate(row Row, idx int) {
	val, ok := row.Get(idx)
	if !ok || val.IsNull() {
		return
	}

	a.count++

	switch a.fn {
	case AggregateSum, AggregateAvg:
		if f, ok := val.AsFloat64(); ok {
			a.sum += f
		}
	case AggregateMin:
		if a.min.IsNull() || compareValues(val, a.min) < 0 {
			a.min = val
		}
	case AggregateMax:
		if a.max.IsNull() || compareValues(val, a.max) > 0 {
			a.max = val
		}
	}
}

func (a *aggAccumulator) result() types.Value {
	switch a.fn {
	case AggregateCount:
		return types.Int8Value(int64(a.count))
	case AggregateSum:
		return types.Float8Value(a.sum)
	case AggregateAvg:
		if a.count == 0 {
			return types.NullValue()
		}
		return types.Float8Value(a.sum / float64(a.count))
	case AggregateMin:
		return a.min
	case AggregateMax:
		return a.max
	default:
		return types.NullValue()
	}
}

func compareValues(a, b types.Value) int {
	af, aok := a.AsFloat64()
	bf, bok := b.AsFloat64()
	if aok && bok {
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}

	as, aok := a.AsString()
	bs, bok := b.AsString()
	if aok && bok {
		return strings.Compare(as, bs)
	}

	return 0
}

func EvaluatePredicate(row Row, pred Predicate) bool {
	switch pred.Op {
	case ComparisonAnd:
		if pred.LeftPred == nil || pred.RightPred == nil {
			return false
		}
		return EvaluatePredicate(row, *pred.LeftPred) && EvaluatePredicate(row, *pred.RightPred)
	case ComparisonOr:
		if pred.LeftPred == nil || pred.RightPred == nil {
			return false
		}
		return EvaluatePredicate(row, *pred.LeftPred) || EvaluatePredicate(row, *pred.RightPred)
	}

	left, ok := evaluateExpression(row, pred.Left)
	if !ok {
		return false
	}
	right, ok := evaluateExpression(row, pred.Right)
	if !ok {
		return false
	}

	return compareValuesOp(left, right, pred.Op)
}

func compareValuesOp(left, right types.Value, op ComparisonOp) bool {
	switch op {
	case ComparisonEq:
		return left.Equal(right)
	case ComparisonNe:
		return !left.Equal(right)
	case ComparisonLt:
		return compareValues(left, right) < 0
	case ComparisonLe:
		return compareValues(left, right) <= 0
	case ComparisonGt:
		return compareValues(left, right) > 0
	case ComparisonGe:
		return compareValues(left, right) >= 0
	case ComparisonLike:
		ls, lok := left.AsString()
		rs, rok := right.AsString()
		if !lok || !rok {
			return false
		}
		return likeMatch(ls, rs)
	case ComparisonIn:
		return left.Equal(right)
	default:
		return false
	}
}

func likeMatch(s, pattern string) bool {
	if pattern == "%" {
		return true
	}
	if pattern == "" {
		return s == ""
	}

	var re []rune
	for _, c := range pattern {
		switch c {
		case '%':
			re = append(re, '*', '*')
		case '_':
			re = append(re, '?')
		default:
			re = append(re, c)
		}
	}

	return globMatch(s, string(re))
}

func globMatch(s, pattern string) bool {
	if pattern == "" {
		return s == ""
	}

	px := 0
	sx := 0
	starPx := -1
	starSx := -1

	for sx < len(s) {
		if px < len(pattern) && (pattern[px] == '?' || pattern[px] == s[sx]) {
			px++
			sx++
		} else if px < len(pattern) && pattern[px] == '*' {
			starPx = px
			starSx = sx
			px++
		} else if starPx >= 0 {
			px = starPx + 1
			starSx++
			sx = starSx
		} else {
			return false
		}
	}

	for px < len(pattern) && pattern[px] == '*' {
		px++
	}

	return px == len(pattern)
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

func storageRowToValues(sr storage.Row) []types.Value {
	var values []types.Value
	for _, colVal := range sr.Body.Columns {
		values = append(values, columnValueToTypesValue(colVal))
	}
	return values
}

func columnValueToTypesValue(cv storage.ColumnValue) types.Value {
	switch cv.Kind {
	case "bool":
		if cv.Bool != nil {
			return types.BoolValue(*cv.Bool)
		}
	case "int2":
		if cv.Int16 != nil {
			return types.Int2Value(*cv.Int16)
		}
	case "int4":
		if cv.Int32 != nil {
			return types.Int4Value(*cv.Int32)
		}
	case "int8":
		if cv.Int64 != nil {
			return types.Int8Value(*cv.Int64)
		}
	case "float4":
		if cv.Float32 != nil {
			return types.Float4Value(*cv.Float32)
		}
	case "float8":
		if cv.Float64 != nil {
			return types.Float8Value(*cv.Float64)
		}
	case "text":
		if cv.Text != nil {
			return types.TextValue(*cv.Text)
		}
	case "json":
		if cv.Text != nil {
			return types.JSONValue(json.RawMessage(*cv.Text))
		}
	}
	return types.NullValue()
}
