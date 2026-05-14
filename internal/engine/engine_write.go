package engine

import (
	"context"
	"fmt"
	"log/slog"

	"codeberg.org/micro-ts/mts/internal/storage/shard"
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
		if err := cat.CreateDatabase(point.Database); err != nil {
			slog.Warn("auto-create database failed", "database", point.Database, "error", err)
		}
	}
	if !cat.MeasurementExists(point.Database, point.Measurement) {
		if err := cat.CreateMeasurement(point.Database, point.Measurement); err != nil {
			slog.Warn("auto-create measurement failed", "database", point.Database, "measurement", point.Measurement, "error", err)
		}
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
//
// 优化策略：按 Shard 分组后对每组调用 Shard.WriteBatch，
// 减少锁获取次数并利用 WAL 批量写入减少 fsync。
//
// 批量写入不是原子操作，部分失败不会回滚已写入的点。
func (e *Engine) WriteBatch(ctx context.Context, points []*types.Point) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if e.isClosed() {
		return fmt.Errorf("engine is closed")
	}

	if len(points) == 0 {
		return nil
	}

	// 验证并自动创建 database/measurement，按 *Shard 分组
	cat := e.manager.Catalog()
	groups := make(map[*shard.Shard][]*types.Point)

	for _, p := range points {
		if p == nil {
			return ErrNilPoint
		}
		if p.Database == "" {
			return ErrEmptyDatabase
		}
		if p.Measurement == "" {
			return ErrEmptyMeasurement
		}
		if p.Timestamp < 0 {
			return ErrInvalidTimestamp
		}

		if !cat.DatabaseExists(p.Database) {
			if err := cat.CreateDatabase(p.Database); err != nil {
				slog.Warn("auto-create database failed", "database", p.Database, "error", err)
			}
		}
		if !cat.MeasurementExists(p.Database, p.Measurement) {
			if err := cat.CreateMeasurement(p.Database, p.Measurement); err != nil {
				slog.Warn("auto-create measurement failed", "database", p.Database, "measurement", p.Measurement, "error", err)
			}
		}

		s, err := e.shardManager.GetShard(p.Database, p.Measurement, p.Timestamp)
		if err != nil {
			return fmt.Errorf("get shard: %w", err)
		}

		groups[s] = append(groups[s], p)
	}

	// 对每组调用 Shard.WriteBatch
	for s, group := range groups {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := s.WriteBatch(group)
		if err != nil {
			return fmt.Errorf("write batch to shard: wrote %d/%d: %w", n, len(group), err)
		}
		if n != len(group) {
			slog.Warn("write batch to shard: partial write with nil error",
				"written", n, "expected", len(group))
		}
	}

	return nil
}
