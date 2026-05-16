package engine

import (
	"context"
	"fmt"
	"log/slog"

	"codeberg.org/micro-ts/mts/internal/storage/wal"
	"codeberg.org/micro-ts/mts/types"
)

// Write 写入单个数据点到存储引擎。
// 直接写入全局 WAL 和全局 MemTable。
func (e *Engine) Write(ctx context.Context, point *types.Point) error {
	if e.isClosed() {
		return fmt.Errorf("engine closed")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
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

	// 自动创建 db/measurement 元数据
	if !e.catalog.DatabaseExists(point.Database) {
		if err := e.catalog.CreateDatabase(point.Database); err != nil {
			slog.Warn("auto-create database failed", "database", point.Database, "error", err)
		}
	}
	if !e.catalog.MeasurementExists(point.Database, point.Measurement) {
		if err := e.catalog.CreateMeasurement(point.Database, point.Measurement); err != nil {
			return err
		}
	}

	// 分配 SID
	sid, err := e.seriesStore.AllocateSID(point.Database, point.Measurement, point.Tags)
	if err != nil {
		return fmt.Errorf("allocate sid: %w", err)
	}

	// 序列化为 MemPoint
	mp := types.PointToMemPoint(point, sid)

	// 序列化 WAL 数据
	walPayload, release := wal.SerializePoint(mp.Timestamp, mp.Sid, mp.FieldData)
	defer release()

	// 写 WAL
	if _, err := e.wal.Write(walPayload); err != nil {
		return fmt.Errorf("wal write: %w", err)
	}

	// 写 MemTable（背压检查）
	if e.memTable.ActiveFull() {
		return fmt.Errorf("memtable full")
	}

	if err := e.memTable.Write(mp); err != nil {
		return fmt.Errorf("memtable write: %w", err)
	}
	return nil
}

// WriteBatch 批量写入数据点。
//
// 批量写入直接使用全局 WAL 和全局 MemTable。
// 批量写入不是原子操作，部分失败不会回滚已写入的点。
func (e *Engine) WriteBatch(ctx context.Context, points []*types.Point) error {
	if e.isClosed() {
		return fmt.Errorf("engine closed")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(points) == 0 {
		return nil
	}

	// 预分配
	walData := make([][]byte, 0, len(points))
	memPoints := make([]types.MemPoint, 0, len(points))
	releases := make([]func(), 0, len(points))

	for _, point := range points {
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
				return err
			}
		}

		sid, err := e.seriesStore.AllocateSID(point.Database, point.Measurement, point.Tags)
		if err != nil {
			return fmt.Errorf("allocate sid: %w", err)
		}

		mp := types.PointToMemPoint(point, sid)
		memPoints = append(memPoints, mp)

		wp, release := wal.SerializePoint(mp.Timestamp, mp.Sid, mp.FieldData)
		walData = append(walData, wp)
		releases = append(releases, release)
	}

	// 确保释放
	defer func() {
		for _, r := range releases {
			r()
		}
	}()

	// 批量写 WAL
	if _, err := e.wal.WriteBatch(walData); err != nil {
		return fmt.Errorf("wal write batch: %w", err)
	}

	// 背压检查
	if e.memTable.ActiveFull() {
		return fmt.Errorf("memtable full")
	}

	// 批量写 MemTable
	for _, mp := range memPoints {
		if err := e.memTable.Write(mp); err != nil {
			return fmt.Errorf("memtable write: %w", err)
		}
	}
	return nil
}
