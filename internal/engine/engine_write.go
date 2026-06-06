package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/wal"
	"codeberg.org/micro-ts/mts/types"
)

const maxFlushRetries = 10

// Write 写入单个数据点到存储引擎。
// 直接写入全局 WAL 和全局 MemTable。
func (e *Engine) Write(ctx context.Context, point *types.Point) error {
	if e.isClosed() {
		return fmt.Errorf("engine closed")
	}

	// 等待启动恢复完成（WAL replay + shard 发现）
	select {
	case <-e.recoveryDone:
	case <-ctx.Done():
		return ctx.Err()
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
	walPayload, release := wal.SerializePoint(point.Database, point.Measurement, mp.Timestamp, mp.Sid, mp.FieldData)
	defer release()

	// 写 WAL（先持久化，保证数据不丢）
	if _, err := e.wal.Write(walPayload); err != nil {
		return fmt.Errorf("wal write: %w", err)
	}

	// 写 MemTable，若满则刷盘重试
	for range maxFlushRetries {
		err := e.memTable.Write(mp)
		if err == nil {
			return nil
		}
		if errors.Is(err, memtable.ErrMemTableFull) {
			if fErr := e.coordinator.FlushAll(); fErr != nil {
				return fmt.Errorf("write failed: %w", errors.Join(err, fErr))
			}
			continue
		}
		// 非容量类失败（极罕见）→ 保护 WAL entry 防止被 truncation
		e.recordPendingSeg()
		return fmt.Errorf("memtable write: %w", err)
	}
	return fmt.Errorf("memtable write failed after %d retries", maxFlushRetries)
}

// recordPendingSeg 记录当前 WAL segment 为 pending，防止被 truncation 回收。
// 当 MemTable 写入因非容量类失败时调用，保证 WAL 中的 entry 重启后可重放。
func (e *Engine) recordPendingSeg() {
	seg := e.wal.SegmentNum()
	e.pendingSeg.Store(seg)
	e.coordinator.SetPendingSeg(seg)
}

// WriteBatch 批量写入数据点。
//
// 批量写入直接使用全局 WAL 和全局 MemTable。
// 批量写入不是原子操作，部分失败不会回滚已写入的点。
func (e *Engine) WriteBatch(ctx context.Context, points []*types.Point) error {
	if e.isClosed() {
		return fmt.Errorf("engine closed")
	}

	// 等待启动恢复完成（WAL replay + shard 发现）
	select {
	case <-e.recoveryDone:
	case <-ctx.Done():
		return ctx.Err()
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

		wp, release := wal.SerializePoint(point.Database, point.Measurement, mp.Timestamp, mp.Sid, mp.FieldData)
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

	// 批量写 MemTable，若满则刷盘重试
	for _, mp := range memPoints {
		for range maxFlushRetries {
			err := e.memTable.Write(mp)
			if err == nil {
				break
			}
			if errors.Is(err, memtable.ErrMemTableFull) {
				if fErr := e.coordinator.FlushAll(); fErr != nil {
					return fmt.Errorf("batch write failed: %w", errors.Join(err, fErr))
				}
				continue
			}
			e.recordPendingSeg()
			return fmt.Errorf("memtable write: %w", err)
		}
	}
	return nil
}
