package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"discodb/catalog"
	"discodb/executor"
	"discodb/mvcc"
	"discodb/storage"
	"discodb/types"

	discodbsql "discodb/sql"
)

func ptr[T any](v T) *T { return &v }

func (e *Engine) handleCreateTable(stmt discodbsql.CreateTableStmt, connID string) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
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

	t, wasExisting := e.getOrCreateTxn(connID)

	payload, _ := json.Marshal(map[string]interface{}{
		"name":     stmt.Name,
		"table_id": tableID.Uint64(),
		"columns":  stmt.Columns,
	})

	t.BufferCatalogOp("CATALOG_CREATE_TABLE", payload)

	if err := e.autoCommitTxn(connID, t, wasExisting); err != nil {
		return nil, nil, 0, fmt.Errorf("auto-commit: %w", err)
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

func (e *Engine) handleInsert(stmt discodbsql.InsertStmt, connID string) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
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

	t, wasExisting := e.getOrCreateTxn(connID)
	t.SetChannel(tableSchema.ID, types.SegmentID(1), segChannelID)

	txnID := t.ID

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

		t.BufferInsert(tableSchema.ID, row)
	}

	if err := e.autoCommitTxn(connID, t, wasExisting); err != nil {
		return nil, nil, 0, fmt.Errorf("auto-commit: %w", err)
	}

	e.logger.Info("rows inserted",
		slog.String("table", stmt.Table.Name),
		slog.Int("count", len(stmt.Values)),
	)

	return nil, nil, uint64(len(stmt.Values)), nil
}

func (e *Engine) handleSelect(stmt discodbsql.SelectStmt, connID string) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	var snap mvcc.TransactionSnapshot

	if connID != "" {
		e.txnMu.Lock()
		state := e.connTxnState[connID]
		e.txnMu.Unlock()

		if state == ConnTxnActive {
			for _, txnID := range e.txnManager.ActiveTransactions() {
				t, ok := e.txnManager.GetTransaction(txnID)
				if ok {
					snap = t.Snapshot
					break
				}
			}
		}
	}

	if snap.TxnID == 0 {
		snap = e.txnManager.CreateSnapshot()
	}

	planner := discodbsql.NewPlanner(e.catalog, e)
	plan, err := planner.Plan(stmt)
	if err != nil {
		return nil, nil, 0, err
	}

	schema, rows, count, err := e.executePlanWithSnapshot(plan, snap)
	if err != nil {
		return nil, nil, 0, err
	}

	return schema, rows, count, nil
}

func (e *Engine) handleDelete(stmt discodbsql.DeleteStmt, connID string) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	planner := discodbsql.NewPlanner(e.catalog, e)
	plan, err := planner.Plan(stmt)
	if err != nil {
		return nil, nil, 0, err
	}

	ctx := context.Background()
	_, rows, _, err := e.executePlan(plan)
	if err != nil {
		return nil, nil, 0, err
	}

	tableSchema, ok := e.catalog.GetTableByName(stmt.Table.Name)
	if !ok {
		return nil, nil, 0, fmt.Errorf("table %q not found", stmt.Table.Name)
	}

	segments, err := e.segMgr.ListSegments(ctx, tableSchema.ID)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("list segments: %w", err)
	}

	if len(segments) == 0 {
		return nil, nil, 0, nil
	}

	segChannelID := segments[0].ID
	t, wasExisting := e.getOrCreateTxn(connID)
	t.SetChannel(tableSchema.ID, types.SegmentID(1), segChannelID)

	txnID := t.ID

	for _, row := range rows {
		if len(row.Values) < len(tableSchema.Columns) {
			continue
		}

		var colValues []storage.ColumnValue
		for i, val := range row.Values {
			if i >= len(tableSchema.Columns) {
				break
			}
			colValues = append(colValues, valueToColumnValue(val, tableSchema.Columns[i].DataType))
		}

		tombstoneRow := storage.Row{
			Header: storage.RowHeader{
				RowID:     e.nextRowID(),
				TableID:   tableSchema.ID,
				SegmentID: types.SegmentID(1),
				MessageID: types.MessageID(0),
				TxnID:     txnID,
				LSN:       e.nextLSN(),
				Flags:     storage.FlagTombstone,
			},
			Body: storage.RowBody{
				Columns: colValues,
			},
		}

		t.BufferDelete(tableSchema.ID, tombstoneRow)
	}

	var deleted int
	for _, row := range rows {
		if len(row.Values) >= len(tableSchema.Columns) {
			deleted++
		}
	}

	if err := e.autoCommitTxn(connID, t, wasExisting); err != nil {
		return nil, nil, 0, fmt.Errorf("auto-commit: %w", err)
	}

	e.logger.Info("rows deleted",
		slog.String("table", stmt.Table.Name),
		slog.Int("count", deleted),
	)

	return nil, nil, uint64(deleted), nil
}

func (e *Engine) handleUpdate(stmt discodbsql.UpdateStmt, connID string) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	planner := discodbsql.NewPlanner(e.catalog, e)
	plan, err := planner.Plan(stmt)
	if err != nil {
		return nil, nil, 0, err
	}

	ctx := context.Background()
	_, rows, _, err := e.executePlan(plan)
	if err != nil {
		return nil, nil, 0, err
	}

	tableSchema, ok := e.catalog.GetTableByName(stmt.Table.Name)
	if !ok {
		return nil, nil, 0, fmt.Errorf("table %q not found", stmt.Table.Name)
	}

	segments, err := e.segMgr.ListSegments(ctx, tableSchema.ID)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("list segments: %w", err)
	}

	if len(segments) == 0 {
		return nil, nil, 0, nil
	}

	segChannelID := segments[0].ID
	t, wasExisting := e.getOrCreateTxn(connID)
	t.SetChannel(tableSchema.ID, types.SegmentID(1), segChannelID)

	txnID := t.ID

	var updated int
	for _, row := range rows {
		if len(row.Values) < len(tableSchema.Columns) {
			continue
		}

		newValues := make([]types.Value, len(tableSchema.Columns))
		copy(newValues, row.Values)

		for _, setCol := range stmt.Set {
			colIdx, ok := tableSchema.ColumnIndex(setCol.Column)
			if !ok {
				continue
			}
			if setCol.Value.Constant != nil && setCol.Value.Constant.Value.Valid {
				newValues[colIdx] = setCol.Value.Constant.Value
			}
		}

		var newColValues []storage.ColumnValue
		for i, val := range newValues {
			if i >= len(tableSchema.Columns) {
				break
			}
			newColValues = append(newColValues, valueToColumnValue(val, tableSchema.Columns[i].DataType))
		}

		newRow := storage.Row{
			Header: storage.RowHeader{
				RowID:     e.nextRowID(),
				TableID:   tableSchema.ID,
				SegmentID: types.SegmentID(1),
				MessageID: types.MessageID(0),
				TxnID:     txnID,
				LSN:       e.nextLSN(),
				Flags:     0,
			},
			Body: storage.RowBody{
				Columns: newColValues,
			},
		}

		t.BufferInsert(tableSchema.ID, newRow)
		updated++
	}

	if err := e.autoCommitTxn(connID, t, wasExisting); err != nil {
		return nil, nil, 0, fmt.Errorf("auto-commit: %w", err)
	}

	e.logger.Info("rows updated",
		slog.String("table", stmt.Table.Name),
		slog.Int("count", updated),
	)

	return nil, nil, uint64(updated), nil
}

func (e *Engine) handleDropTable(stmt discodbsql.DropTableStmt, connID string) ([]executor.ColumnInfo, []executor.Row, uint64, error) {
	tableSchema, ok := e.catalog.GetTableByName(stmt.Name)
	if !ok {
		return nil, nil, 0, fmt.Errorf("table %q not found", stmt.Name)
	}

	ctx := context.Background()

	segments, err := e.segMgr.ListSegments(ctx, tableSchema.ID)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("list segments: %w", err)
	}

	t, wasExisting := e.getOrCreateTxn(connID)

	if len(segments) > 0 {
		segChannelID := segments[0].ID
		t.SetChannel(tableSchema.ID, types.SegmentID(1), segChannelID)

		allRows, _, err := e.segMgr.ReadRows(ctx, segChannelID)
		if err != nil {
			e.logger.Warn("failed to read rows for drop", slog.String("error", err.Error()))
		}

		txnID := t.ID
		for _, row := range allRows {
			if row.Header.Flags.HasTombstone() {
				continue
			}

			tombstoneRow := storage.Row{
				Header: storage.RowHeader{
					RowID:     e.nextRowID(),
					TableID:   tableSchema.ID,
					SegmentID: types.SegmentID(1),
					MessageID: types.MessageID(0),
					TxnID:     txnID,
					LSN:       e.nextLSN(),
					Flags:     storage.FlagTombstone,
				},
				Body: row.Body,
			}

			t.BufferDelete(tableSchema.ID, tombstoneRow)
		}
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"name":     stmt.Name,
		"table_id": tableSchema.ID.Uint64(),
	})

	t.BufferCatalogOp("CATALOG_DROP_TABLE", payload)

	if err := e.autoCommitTxn(connID, t, wasExisting); err != nil {
		return nil, nil, 0, fmt.Errorf("auto-commit: %w", err)
	}

	e.catalog.RemoveTable(tableSchema.ID)

	if err := persistCatalogToDiscord(ctx, e.catalogClient, e.boot.GuildID, e.boot.CatalogCategory, e.catalog); err != nil {
		e.logger.Warn("catalog persist failed", slog.String("error", err.Error()))
	}

	e.logger.Info("table dropped",
		slog.String("name", stmt.Name),
		slog.String("table_id", tableSchema.ID.String()),
	)

	return nil, nil, 0, nil
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
