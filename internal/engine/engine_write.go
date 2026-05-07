package engine

import (
	"context"
	"fmt"

	"codeberg.org/micro-ts/mts/types"
)

// Write 写入单个数据点到存储引擎。
func (e *Engine) Write(ctx context.Context, point *types.Point) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if e.isClosed() {
		return fmt.Errorf("engine is closed")
	}

	if point == nil {
		return ErrNilPoint
	}
	if point.Database == "" {
		return ErrEmptyDatabase
	}
	if point.Measurement == "" {
		return ErrEmptyMeasurement
	}
	if point.Timestamp < 0 {
		return ErrInvalidTimestamp
	}

	cat := e.manager.Catalog()
	if !cat.DatabaseExists(point.Database) {
		_ = cat.CreateDatabase(point.Database)
	}
	if !cat.MeasurementExists(point.Database, point.Measurement) {
		_ = cat.CreateMeasurement(point.Database, point.Measurement)
	}

	s, err := e.shardManager.GetShard(point.Database, point.Measurement, point.Timestamp)
	if err != nil {
		return fmt.Errorf("get shard: %w", err)
	}

	if err := s.Write(point); err != nil {
		return fmt.Errorf("write to shard: %w", err)
	}
	return nil
}

// WriteBatch 批量写入数据点。
func (e *Engine) WriteBatch(ctx context.Context, points []*types.Point) error {
	for _, p := range points {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := e.Write(ctx, p); err != nil {
			return fmt.Errorf("write point (timestamp=%d): %w", p.Timestamp, err)
		}
	}
	return nil
}
