package engine

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"codeberg.org/micro-ts/mts/internal/storage/writer"
	"codeberg.org/micro-ts/mts/types"
)

// getOrCreateWriter 获取或创建指定 measurement 的 Writer。
//
// 使用双检查（double-check）模式避免重复创建：
//  1. 先不加锁查询 coordinator
//  2. 加锁后再次确认
//  3. 不存在则创建并注册
func (e *Engine) getOrCreateWriter(db, measurement string) (Writer, error) {
	// 先检查是否已存在
	if w := e.coordinator.GetWriter(db, measurement); w != nil {
		return w, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// 双检查
	if w := e.coordinator.GetWriter(db, measurement); w != nil {
		return w, nil
	}

	measDir := filepath.Join(e.dataDir, db, measurement)
	slog.Info("getOrCreateWriter: creating new writer", "db", db, "meas", measurement, "dir", measDir)
	mw, err := writer.New(writer.Config{
		DB:          db,
		Measurement: measurement,
		Dir:         measDir,
		SeriesStore: e.seriesStore,
		MemTableCfg: e.memTableCfg,
	})
	if err != nil {
		return nil, fmt.Errorf("create writer: %w", err)
	}

	e.coordinator.RegisterWriter(db, measurement, mw)
	return mw, nil
}

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

	if !e.catalog.DatabaseExists(point.Database) {
		if err := e.catalog.CreateDatabase(point.Database); err != nil {
			slog.Warn("auto-create database failed", "database", point.Database, "error", err)
		}
	}
	if !e.catalog.MeasurementExists(point.Database, point.Measurement) {
		if err := e.catalog.CreateMeasurement(point.Database, point.Measurement); err != nil {
			slog.Warn("auto-create measurement failed", "database", point.Database, "measurement", point.Measurement, "error", err)
		}
	}

	w, err := e.getOrCreateWriter(point.Database, point.Measurement)
	if err != nil {
		return fmt.Errorf("get writer: %w", err)
	}

	if err := w.Write(point); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}

// WriteBatch 批量写入数据点。
//
// 优化策略：按 Measurement 分组后对每组调用 Writer.WriteBatch，
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

	// 验证并自动创建 database/measurement，按 writer 分组
	groups := make(map[Writer][]*types.Point)

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

		if !e.catalog.DatabaseExists(p.Database) {
			if err := e.catalog.CreateDatabase(p.Database); err != nil {
				slog.Warn("auto-create database failed", "database", p.Database, "error", err)
			}
		}
		if !e.catalog.MeasurementExists(p.Database, p.Measurement) {
			if err := e.catalog.CreateMeasurement(p.Database, p.Measurement); err != nil {
				slog.Warn("auto-create measurement failed", "database", p.Database, "measurement", p.Measurement, "error", err)
			}
		}

		w, err := e.getOrCreateWriter(p.Database, p.Measurement)
		if err != nil {
			return fmt.Errorf("get writer: %w", err)
		}

		groups[w] = append(groups[w], p)
	}

	// 对每组调用 Writer.WriteBatch
	for w, group := range groups {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := w.WriteBatch(group)
		if err != nil {
			return fmt.Errorf("write batch: wrote %d/%d: %w", n, len(group), err)
		}
		if n != len(group) {
			slog.Warn("write batch: partial write with nil error",
				"written", n, "expected", len(group))
		}
	}

	return nil
}
