package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"discodb/catalog"
	"discodb/executor"
	"discodb/storage"
	"discodb/types"
	"discodb/wal"

	discodbsql "discodb/sql"
)

func ptr[T any](v T) *T { return &v }

func (e *Engine) handleCreateTable(stmt discodbsql.CreateTableStmt) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	tableID := e.nextTableID()
	segmentID := e.nextSegmentID()

	var cols []catalog.ColumnSchema
	for i, col := range stmt.Columns {
		cols = append(cols, catalog.ColumnSchema{
			Name:     col.Name,
			DataType: sqlDataTypeToDiscodb(col.DataType),
			Nullable: col.Nullable,
			Ordinal:  uint32(i),
		})
	}

	schema := catalog.NewTableSchema(tableID, stmt.Name, cols)
	e.catalog.AddTable(schema)

	ctx := context.Background()

	_, err := e.segMgr.CreateSegment(ctx, tableID, segmentID)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("create segment: %w", err)
	}

	txnID := e.nextTxnID()
	lsn := e.nextLSN()

	beginRec := wal.Begin(txnID, lsn)
	if err := e.walWriter.Append(ctx, beginRec); err != nil {
		e.logger.Warn("WAL begin failed (non-fatal for DDL)", slog.String("error", err.Error()))
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"name":     stmt.Name,
		"table_id": tableID.Uint64(),
		"columns":  stmt.Columns,
	})

	catRec := wal.Record{
		Kind:    "CATALOG_CREATE_TABLE",
		TxnID:   txnID,
		LSN:     e.nextLSN(),
		TableID: tableID,
		Data:    payload,
	}
	if err := e.walWriter.Append(ctx, catRec); err != nil {
		e.logger.Warn("WAL catalog record failed", slog.String("error", err.Error()))
	}

	commitRec := wal.Commit(txnID, e.nextLSN())
	if err := e.walWriter.Append(ctx, commitRec); err != nil {
		e.logger.Warn("WAL commit failed (non-fatal for DDL)", slog.String("error", err.Error()))
	}

	if err := persistCatalogToDiscord(ctx, e.catalogClient, e.boot.GuildID, e.boot.CatalogCategory, e.catalog); err != nil {
		e.logger.Warn("catalog persist failed", slog.String("error", err.Error()))
	}

	e.logger.Info("table created",
		slog.String("name", stmt.Name),
		slog.String("table_id", tableID.String()),
		slog.String("segment_id", segmentID.String()),
	)

	return nil, nil, 0, nil
}

func (e *Engine) handleInsert(stmt discodbsql.InsertStmt) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	tableSchema, ok := e.catalog.GetTableByName(stmt.Table.Name)
	if !ok {
		return nil, nil, 0, fmt.Errorf("table %q not found", stmt.Table.Name)
	}

	if len(stmt.Columns) == 0 && len(stmt.Values) > 0 {
		if len(stmt.Values[0]) != len(tableSchema.Columns) {
			return nil, nil, 0, fmt.Errorf("expected %d values, got %d", len(tableSchema.Columns), len(stmt.Values[0]))
		}
	}

	ctx := context.Background()

	segments, err := e.segMgr.ListSegments(ctx, tableSchema.ID)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("list segments: %w", err)
	}

	var segChannelID types.ChannelID
	if len(segments) > 0 {
		segChannelID = segments[0].ID
	} else {
		segmentID := e.nextSegmentID()
		segChannelID, err = e.segMgr.CreateSegment(ctx, tableSchema.ID, segmentID)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("create segment: %w", err)
		}
	}

	txnID := e.nextTxnID()
	beginRec := wal.Begin(txnID, e.nextLSN())
	if err := e.walWriter.Append(ctx, beginRec); err != nil {
		e.logger.Warn("WAL begin failed", slog.String("error", err.Error()))
	}

	var insertedRows int

	for _, valueExprs := range stmt.Values {
		rowID := e.nextRowID()

		var colValues []storage.ColumnValue
		for i, expr := range valueExprs {
			if i >= len(tableSchema.Columns) {
				break
			}
			colType := tableSchema.Columns[i].DataType

			if expr.Constant != nil && expr.Constant.Value.Valid {
				colValues = append(colValues, valueToColumnValue(expr.Constant.Value, colType))
			} else {
				colValues = append(colValues, storage.ColumnValue{Kind: string(colType)})
			}
		}

		row := storage.Row{
			Header: storage.RowHeader{
				RowID:     rowID,
				TableID:   tableSchema.ID,
				SegmentID: types.SegmentID(1),
				MessageID: types.MessageID(0),
				TxnID:     txnID,
				LSN:       e.nextLSN(),
				Flags:     0,
			},
			Body: storage.RowBody{
				Columns: colValues,
			},
		}

		msg, err := e.segMgr.WriteRow(ctx, segChannelID, row, tableSchema.Epoch)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("write row: %w", err)
		}

		row.Header.MessageID = msg.ID

		insertRec := wal.Record{
			Kind:      "INSERT",
			TxnID:     txnID,
			LSN:       e.nextLSN(),
			TableID:   tableSchema.ID,
			RowID:     rowID,
			SegmentID: types.SegmentID(1),
			MessageID: msg.ID,
			Data:      nil,
		}
		if err := e.walWriter.Append(ctx, insertRec); err != nil {
			e.logger.Warn("WAL insert failed", slog.String("error", err.Error()))
		}

		insertedRows++
	}

	commitRec := wal.Commit(txnID, e.nextLSN())
	if err := e.walWriter.Append(ctx, commitRec); err != nil {
		e.logger.Warn("WAL commit failed", slog.String("error", err.Error()))
	}

	e.logger.Info("rows inserted",
		slog.String("table", stmt.Table.Name),
		slog.Int("count", insertedRows),
	)

	return nil, nil, uint64(insertedRows), nil
}

func (e *Engine) handleSelect(stmt discodbsql.SelectStmt) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	if stmt.From == nil {
		return nil, nil, 0, fmt.Errorf("SELECT requires a FROM clause")
	}

	tableSchema, ok := e.catalog.GetTableByName(stmt.From.Name)
	if !ok {
		return nil, nil, 0, fmt.Errorf("table %q not found", stmt.From.Name)
	}

	ctx := context.Background()

	segments, err := e.segMgr.ListSegments(ctx, tableSchema.ID)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("list segments: %w", err)
	}

	if len(segments) == 0 {
		colInfos := make([]executor.ColumnInfo, len(tableSchema.Columns))
		for i, col := range tableSchema.Columns {
			colInfos[i] = executor.ColumnInfo{
				Name:    col.Name,
				Ordinal: int(col.Ordinal),
			}
		}
		return colInfos, nil, 0, nil
	}

	var allResults []executor.Row

	for _, seg := range segments {
		rows, _, err := e.segMgr.ReadRows(ctx, seg.ID)
		if err != nil {
			e.logger.Warn("failed to read rows from segment",
				slog.String("segment", seg.Name),
				slog.String("error", err.Error()),
			)
			continue
		}

		for _, row := range rows {
			var values []types.Value
			for _, colVal := range row.Body.Columns {
				values = append(values, columnValueToTypesValue(colVal))
			}

			for len(values) < len(tableSchema.Columns) {
				values = append(values, types.NullValue())
			}

			if stmt.WhereClause != nil {
				pred := buildPredicate(stmt.WhereClause, tableSchema)
				execRow := executor.Row{Values: values}
				if !executor.EvaluatePredicate(execRow, pred) {
					continue
				}
			}

			if len(stmt.Columns) == 1 && stmt.Columns[0].All {
				allResults = append(allResults, executor.Row{Values: values})
			} else {
				var projected []types.Value
				for _, selCol := range stmt.Columns {
					if selCol.Name != "" {
						idx, ok := tableSchema.ColumnIndex(selCol.Name)
						if ok && idx < len(values) {
							projected = append(projected, values[idx])
						} else {
							projected = append(projected, types.NullValue())
						}
					}
				}
				if len(projected) > 0 {
					allResults = append(allResults, executor.Row{Values: projected})
				} else {
					allResults = append(allResults, executor.Row{Values: values})
				}
			}
		}
	}

	if stmt.Limit != nil && *stmt.Limit < len(allResults) {
		allResults = allResults[:*stmt.Limit]
	}

	colInfos := make([]executor.ColumnInfo, len(tableSchema.Columns))
	for i, col := range tableSchema.Columns {
		colInfos[i] = executor.ColumnInfo{
			Name:    col.Name,
			Ordinal: int(col.Ordinal),
		}
	}

	if len(stmt.Columns) > 0 && !stmt.Columns[0].All {
		colInfos = nil
		for _, selCol := range stmt.Columns {
			if selCol.Name != "" {
				colInfos = append(colInfos, executor.ColumnInfo{
					Name: selCol.Name,
				})
			}
		}
	}

	return colInfos, allResults, uint64(len(allResults)), nil
}

func valueToColumnValue(v types.Value, colType types.DataType) storage.ColumnValue {
	if !v.Valid || v.IsNull() {
		return storage.ColumnValue{Kind: string(colType)}
	}

	switch colType {
	case types.DataTypeBool:
		if b, ok := v.Raw.(bool); ok {
			return storage.ColumnValue{Kind: "bool", Bool: &b}
		}
	case types.DataTypeInt2:
		switch n := v.Raw.(type) {
		case int16:
			return storage.ColumnValue{Kind: "int2", Int16: &n}
		case int64:
			v := int16(n)
			return storage.ColumnValue{Kind: "int2", Int16: &v}
		}
	case types.DataTypeInt4:
		switch n := v.Raw.(type) {
		case int32:
			return storage.ColumnValue{Kind: "int4", Int32: &n}
		case int64:
			v := int32(n)
			return storage.ColumnValue{Kind: "int4", Int32: &v}
		}
	case types.DataTypeInt8:
		switch n := v.Raw.(type) {
		case int64:
			return storage.ColumnValue{Kind: "int8", Int64: &n}
		case int32:
			v := int64(n)
			return storage.ColumnValue{Kind: "int8", Int64: &v}
		}
	case types.DataTypeFloat4:
		switch n := v.Raw.(type) {
		case float32:
			return storage.ColumnValue{Kind: "float4", Float32: &n}
		case float64:
			v := float32(n)
			return storage.ColumnValue{Kind: "float4", Float32: &v}
		}
	case types.DataTypeFloat8:
		switch n := v.Raw.(type) {
		case float64:
			return storage.ColumnValue{Kind: "float8", Float64: &n}
		case float32:
			v := float64(n)
			return storage.ColumnValue{Kind: "float8", Float64: &v}
		}
	case types.DataTypeText:
		if s, ok := v.Raw.(string); ok {
			return storage.ColumnValue{Kind: "text", Text: &s}
		}
	case types.DataTypeJSON:
		if b, ok := v.Raw.(json.RawMessage); ok {
			s := string(b)
			return storage.ColumnValue{Kind: "json", Text: &s}
		}
	}

	if s, ok := v.Raw.(string); ok {
		return storage.ColumnValue{Kind: string(colType), Text: &s}
	}

	return storage.ColumnValue{Kind: string(colType)}
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

func buildPredicate(pred *discodbsql.Predicate, schema catalog.TableSchema) executor.Predicate {
	if pred == nil || pred.Comparison == nil {
		return executor.Predicate{}
	}

	leftIdx := -1
	if pred.Comparison.Left.Column != "" {
		if idx, ok := schema.ColumnIndex(pred.Comparison.Left.Column); ok {
			leftIdx = idx
		}
	}

	rightIdx := -1
	if pred.Comparison.Right.Column != "" {
		if idx, ok := schema.ColumnIndex(pred.Comparison.Right.Column); ok {
			rightIdx = idx
		}
	}

	var leftExpr, rightExpr executor.Expression

	if leftIdx >= 0 {
		leftExpr = executor.Expression{ColumnIndex: &leftIdx}
	} else if pred.Comparison.Left.Constant != nil {
		v := pred.Comparison.Left.Constant.Value
		leftExpr = executor.Expression{Constant: &v}
	}

	if rightIdx >= 0 {
		rightExpr = executor.Expression{ColumnIndex: &rightIdx}
	} else if pred.Comparison.Right.Constant != nil {
		v := pred.Comparison.Right.Constant.Value
		rightExpr = executor.Expression{Constant: &v}
	}

	return executor.Predicate{
		Left:  leftExpr,
		Op:    executor.ComparisonOp(pred.Comparison.Op),
		Right: rightExpr,
	}
}
