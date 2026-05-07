package engine

import (
	"context"
	"fmt"
	"log/slog"

	"codeberg.org/micro-ts/mts/internal/query"
	"codeberg.org/micro-ts/mts/types"
)

// Query 执行范围查询。
func (e *Engine) Query(ctx context.Context, req *types.QueryRangeRequest) (*types.QueryRangeResponse, error) {
	if e.isClosed() {
		return nil, fmt.Errorf("engine is closed")
	}

	e.queryWg.Add(1)
	defer e.queryWg.Done()

	shards := e.shardManager.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)

	if len(shards) == 0 {
		return &types.QueryRangeResponse{
			Database:    req.Database,
			Measurement: req.Measurement,
			StartTime:   req.StartTime,
			EndTime:     req.EndTime,
			TotalCount:  0,
			Rows:        []*types.Row{},
		}, nil
	}

	qit, err := e.QueryIterator(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("create query iterator: %w", err)
	}
	defer func() {
		if err := qit.Close(); err != nil {
			slog.Warn("failed to close query iterator", "error", err)
		}
	}()

	pointRows, skipped, collected, hasMore, err := e.collectQueryResults(ctx, qit, req)
	if err != nil {
		return nil, err
	}

	return e.buildQueryResponse(req, pointRows, skipped, collected, hasMore)
}

// collectQueryResults 流式收集查询结果
func (e *Engine) collectQueryResults(ctx context.Context, qit *query.QueryIterator, req *types.QueryRangeRequest) ([]*types.PointRow, int, int, bool, error) {
	var pointRows []*types.PointRow
	hasLimit := req.Limit > 0

	skipped := 0
	collected := 0

	for qit.Next(ctx) {
		row := qit.Points()
		if row == nil {
			continue
		}
		if int64(skipped) < req.Offset {
			skipped++
			continue
		}
		pointRows = append(pointRows, row)
		collected++
		if hasLimit && collected >= int(req.Limit) {
			break
		}
	}

	hasMore := hasLimit && collected >= int(req.Limit)

	return pointRows, skipped, collected, hasMore, nil
}

// buildQueryResponse 构建查询响应
func (e *Engine) buildQueryResponse(req *types.QueryRangeRequest, pointRows []*types.PointRow, skipped, collected int, hasMore bool) (*types.QueryRangeResponse, error) {
	totalCount := int64(skipped + collected)

	rows := make([]*types.Row, len(pointRows))
	for i, pr := range pointRows {
		fields := make(map[string]*types.FieldValue, len(pr.Fields))
		for name, v := range pr.Fields {
			fv, err := anyToProtoFieldValue(v)
			if err != nil {
				return nil, fmt.Errorf("convert field %s: %w", name, err)
			}
			fields[name] = fv
		}
		rows[i] = &types.Row{
			Timestamp: pr.Timestamp,
			Tags:      pr.Tags,
			Fields:    fields,
		}
	}

	return &types.QueryRangeResponse{
		Database:    req.Database,
		Measurement: req.Measurement,
		StartTime:   req.StartTime,
		EndTime:     req.EndTime,
		TotalCount:  totalCount,
		HasMore:     hasMore,
		Rows:        rows,
	}, nil
}

// anyToProtoFieldValue 将 any 转换为 protobuf FieldValue。
func anyToProtoFieldValue(v any) (*types.FieldValue, error) {
	if fv, ok := v.(*types.FieldValue); ok {
		return fv, nil
	}

	switch val := v.(type) {
	case int64:
		return &types.FieldValue{Value: &types.FieldValue_IntValue{IntValue: val}}, nil
	case float64:
		return &types.FieldValue{Value: &types.FieldValue_FloatValue{FloatValue: val}}, nil
	case string:
		return &types.FieldValue{Value: &types.FieldValue_StringValue{StringValue: val}}, nil
	case bool:
		return &types.FieldValue{Value: &types.FieldValue_BoolValue{BoolValue: val}}, nil
	default:
		return nil, fmt.Errorf("unsupported field type: %T", v)
	}
}

// QueryIterator 返回流式查询迭代器。
func (e *Engine) QueryIterator(ctx context.Context, req *types.QueryRangeRequest) (*query.QueryIterator, error) {
	if e.isClosed() {
		return nil, fmt.Errorf("engine is closed")
	}

	shards := e.shardManager.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)
	if len(shards) == 0 {
		return nil, fmt.Errorf("no shards found")
	}

	return query.NewQueryIterator(ctx, shards, req), nil
}
