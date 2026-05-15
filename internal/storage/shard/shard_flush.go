package shard

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/micro-ts/mts/internal/metrics"
	"codeberg.org/micro-ts/mts/internal/storage"
	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/internal/storage/wal"
	"codeberg.org/micro-ts/mts/types"
)

// Flush 将 MemTable 数据刷写到 SSTable（同步，用于手动调用和 Close）。
func (s *Shard) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 等待正在进行的异步 flush 完成，避免 Swap() 合并冲突导致数据重复
	for s.memTable.IsFlushing() {
		s.mu.Unlock()
		time.Sleep(backpressureSleep)
		s.mu.Lock()
	}

	return s.flushLocked()
}

// flushLocked 内部同步刷写方法（已持有锁），用于 Close、手动 Flush、WAL replay。
func (s *Shard) flushLocked() error {
	passive := s.memTable.Swap()
	if len(passive) == 0 {
		return nil
	}

	if err := s.writeSSTableSync(passive); err != nil {
		// 失败时数据合并回 active
		s.memTable.MergePassiveBack()
		return err
	}

	s.memTable.ClearPassive()

	metrics.Incr(metrics.FlushTotal, 1)
	metrics.Incr(metrics.FlushPoints, int64(len(passive)))

	// WAL 清理（replay 期间跳过）
	if !s.replaying && s.wal != nil {
		if err := s.wal.TruncateAfterFlush(); err != nil {
			slog.Warn("failed to truncate WAL after flush", "error", err)
		}
	}

	s.triggerBackgroundCompaction()

	return nil
}

// writeSSTableSync 同步写入 SSTable（持锁调用）。
func (s *Shard) writeSSTableSync(points []types.MemPoint) error {
	useFlatCompaction := s.compaction != nil && !s.levelCompactionEnabled()

	var sstSeq uint64
	var sstPath string

	if s.levelCompaction != nil {
		sstSeq = s.levelCompaction.NextSeq()
		l0Dir := filepath.Join(s.dir, "data", "L0")
		if err := storage.SafeMkdirAll(l0Dir, 0700); err != nil {
			return fmt.Errorf("create L0 dir: %w", err)
		}
		sstPath = filepath.Join(l0Dir, fmt.Sprintf("sst_%d.bin", sstSeq))
	} else {
		sstSeq = s.sstSeq
		sstPath = filepath.Join(s.dir, "data", fmt.Sprintf("sst_%d.bin", sstSeq))
	}

	flushSucceeded := false
	if useFlatCompaction {
		if err := s.compaction.MarkWriting(sstPath); err != nil {
			slog.Warn("failed to mark sstable in write", "path", sstPath, "error", err)
		}
	}
	defer func() {
		if useFlatCompaction && !flushSucceeded {
			if unmarkErr := s.compaction.UnmarkWriting(sstPath); unmarkErr != nil {
				slog.Warn("failed to unmark sstable write", "path", sstPath, "error", unmarkErr)
			}
		}
	}()

	w, err := sstable.NewWriter(s.dir, sstSeq, 0, s.compressionAlgo)
	if err != nil {
		return fmt.Errorf("create sstable writer: %w", err)
	}

	if err := w.WriteMemPoints(points); err != nil {
		_ = w.Close()
		return fmt.Errorf("write mempoints to sstable: %w", err)
	}

	if s.schemaStore != nil {
		metaSchema := SSTableSchemaToMetadataSchema(w.Schema())
		if err := s.schemaStore.SetSchema(s.db, s.measurement, metaSchema); err != nil {
			slog.Warn("failed to persist schema", "error", err)
		}
		s.UpdateSchemaInMemory(metaSchema)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close sstable writer: %w", err)
	}

	if useFlatCompaction {
		if err := s.compaction.UnmarkWriting(sstPath); err != nil {
			slog.Warn("failed to unmark sstable write", "path", sstPath, "error", err)
		}
	}
	flushSucceeded = true

	if s.levelCompaction != nil {
		srcPath := filepath.Join(s.dir, "data", fmt.Sprintf("sst_%d.bin", sstSeq))
		dstPath := sstPath
		if srcPath != dstPath {
			if err := os.Rename(srcPath, dstPath); err != nil {
				return fmt.Errorf("move SSTable to L0: %w", err)
			}
		}

		minTime, maxTime := s.calcPointTimeRange(points)

		var size int64
		if info, err := os.Stat(dstPath); err == nil {
			size = info.Size()
		}

		s.levelCompaction.AddPart(0, compaction.PartInfo{
			Name:    fmt.Sprintf("sst_%d", sstSeq),
			Size:    size,
			MinTime: minTime,
			MaxTime: maxTime,
		})
	} else {
		s.sstSeq++
	}

	return nil
}

// tryTriggerAsyncFlush 尝试触发异步 flush。CAS 保证只有一个 goroutine 执行。
func (s *Shard) tryTriggerAsyncFlush() {
	if !s.memTable.TrySetFlushing() {
		return
	}

	s.compactionWg.Go(func() {
		s.executeAsyncFlush()
	})
}

// asyncFlushInfo 保存异步 flush Phase 2 产出的 SSTable 信息，供 Phase 3 注册使用。
type asyncFlushInfo struct {
	sstSeq             uint64
	finalPath          string
	tmpPath            string
	minTime            int64
	maxTime            int64
	useFlatCompaction  bool
	useLevelCompaction bool
}

// executeAsyncFlush 后台执行：swap + SSTable 写入 + 清理。
func (s *Shard) executeAsyncFlush() {
	// Phase 1: 持锁交换 + WAL 切分 + 获取 seq/path
	s.mu.Lock()

	if s.wal != nil {
		if err := s.wal.Rotate(); err != nil {
			slog.Warn("WAL rotate before async flush failed", "error", err)
			s.mu.Unlock()
			s.memTable.ClearFlushing()
			return
		}
	}

	passive := s.memTable.Swap()

	useFlatCompaction := s.compaction != nil && !s.levelCompactionEnabled()
	useLevelCompaction := s.levelCompaction != nil

	var sstSeq uint64
	var dataDir string
	if useLevelCompaction {
		sstSeq = s.levelCompaction.NextSeq()
		dataDir = filepath.Join(s.dir, "data", "L0")
	} else {
		sstSeq = s.sstSeq
		s.sstSeq++ // 立即预留 seq，防止后台 compaction 复用
		dataDir = filepath.Join(s.dir, "data")
	}
	s.mu.Unlock()

	if len(passive) == 0 {
		s.memTable.ClearPassive()
		return
	}

	// 创建目标目录
	if err := storage.SafeMkdirAll(dataDir, 0700); err != nil {
		slog.Error("async flush: create data dir failed", "error", err)
		s.mu.Lock()
		s.memTable.MergePassiveBack()
		s.mu.Unlock()
		return
	}

	// Phase 2: 写 SSTable（不持锁），输出到临时文件
	info, err := s.writeSSTableAsync(passive, sstSeq, dataDir, useFlatCompaction)
	if err != nil {
		slog.Error("async flush: write SSTable failed", "error", err)
		s.mu.Lock()
		s.memTable.MergePassiveBack()
		s.mu.Unlock()
		return
	}

	// Phase 3: 持锁 → ClearPassive → 原子 rename → 注册 → WAL 清理
	s.mu.Lock()

	s.memTable.ClearPassive()

	// 原子 rename：仅在 passive 清空后 SSTable 才对读者可见
	if err := os.Rename(info.tmpPath, info.finalPath); err != nil {
		s.mu.Unlock()
		slog.Error("async flush: rename tmp to final failed", "tmp", info.tmpPath, "final", info.finalPath, "error", err)
		_ = os.Remove(info.tmpPath)
		s.triggerBackgroundCompaction()
		return
	}

	// 注册 SSTable（seq 已在 Phase 1 预留）
	if info.useLevelCompaction {
		var size int64
		if fi, statErr := os.Stat(info.finalPath); statErr == nil {
			size = fi.Size()
		}
		s.levelCompaction.AddPart(0, compaction.PartInfo{
			Name:    fmt.Sprintf("sst_%d", info.sstSeq),
			Size:    size,
			MinTime: info.minTime,
			MaxTime: info.maxTime,
		})
	}

	if s.wal != nil {
		if walErr := s.wal.TruncateCurrent(); walErr != nil {
			slog.Warn("failed to truncate WAL after async flush", "error", walErr)
		}
		// 写入 checkpoint，记录当前已持久化的 WAL 位置
		cp := &wal.Checkpoint{
			Generation: s.wal.Generation(),
			Segment:    s.wal.SegmentNum(),
		}
		if cpErr := cp.Save(filepath.Join(s.dir, "wal")); cpErr != nil {
			slog.Warn("failed to save WAL checkpoint", "error", cpErr)
		}
	}

	metrics.Incr(metrics.FlushTotal, 1)
	metrics.Incr(metrics.FlushPoints, int64(len(passive)))

	s.mu.Unlock()

	s.triggerBackgroundCompaction()

	// 链接触发：若 active 已积累足够数据，立即开始下一轮异步 flush
	if s.memTable.ShouldSwap() {
		s.tryTriggerAsyncFlush()
	}
}

// writeSSTableAsync 异步写入 SSTable（不持锁）。
// 写入最终文件后立即 rename 到临时路径，由 Phase 3 原子 rename 回最终路径，
// 确保 SSTable 仅在 passive 清空后才对读者可见。
func (s *Shard) writeSSTableAsync(points []types.MemPoint, sstSeq uint64, dataDir string, useFlatCompaction bool) (*asyncFlushInfo, error) {
	finalPath := filepath.Join(dataDir, fmt.Sprintf("sst_%d.bin", sstSeq))
	tmpPath := filepath.Join(dataDir, fmt.Sprintf(".sst_%d.bin.tmp", sstSeq))

	flushSucceeded := false
	if useFlatCompaction {
		_ = s.compaction.MarkWriting(finalPath)
	}
	defer func() {
		if useFlatCompaction && !flushSucceeded {
			_ = s.compaction.UnmarkWriting(finalPath)
		}
	}()

	w, err := sstable.NewWriter(s.dir, sstSeq, 0, s.compressionAlgo)
	if err != nil {
		return nil, fmt.Errorf("create sstable writer: %w", err)
	}

	if err := w.WriteMemPoints(points); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("write mempoints: %w", err)
	}

	if s.schemaStore != nil {
		s.schemaMu.Lock()
		metaSchema := SSTableSchemaToMetadataSchema(w.Schema())
		s.schemaMu.Unlock()
		if err := s.schemaStore.SetSchema(s.db, s.measurement, metaSchema); err != nil {
			slog.Warn("failed to persist schema", "error", err)
		}
		s.UpdateSchemaInMemory(metaSchema)
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("close sstable writer: %w", err)
	}

	// NewWriter + Close 输出到 <shardDir>/data/sst_N.bin；rename 到 dataDir 下的目标路径
	srcPath := filepath.Join(s.dir, "data", fmt.Sprintf("sst_%d.bin", sstSeq))
	if srcPath != finalPath {
		if err := os.Rename(srcPath, finalPath); err != nil {
			return nil, fmt.Errorf("move SSTable to dest: %w", err)
		}
	}

	// 立即 rename 到临时路径，Phase 3 持锁后原子 rename 到 finalPath
	if err := os.Rename(finalPath, tmpPath); err != nil {
		return nil, fmt.Errorf("rename sst to tmp: %w", err)
	}

	if useFlatCompaction {
		_ = s.compaction.UnmarkWriting(finalPath)
	}
	flushSucceeded = true

	minTime, maxTime := s.calcPointTimeRange(points)

	return &asyncFlushInfo{
		sstSeq:             sstSeq,
		finalPath:          finalPath,
		tmpPath:            tmpPath,
		minTime:            minTime,
		maxTime:            maxTime,
		useFlatCompaction:  useFlatCompaction,
		useLevelCompaction: s.levelCompaction != nil,
	}, nil
}

// calcPointTimeRange 计算 points 的时间范围。
func (s *Shard) calcPointTimeRange(points []types.MemPoint) (int64, int64) {
	minTime := int64(0)
	maxTime := int64(0)
	for i, p := range points {
		if i == 0 || p.Timestamp < minTime {
			minTime = p.Timestamp
		}
		if i == 0 || p.Timestamp > maxTime {
			maxTime = p.Timestamp
		}
	}
	return minTime, maxTime
}

// triggerBackgroundCompaction 在后台触发 compaction。
func (s *Shard) triggerBackgroundCompaction() {
	if s.closed.Load() {
		return
	}

	if s.levelCompaction != nil && s.levelCompaction.ShouldCompact() {
		s.compactionWg.Go(func() {
			if s.closed.Load() {
				return
			}
			ctx, cancel := context.WithTimeout(s.levelCompaction.Context(), s.levelCompaction.Timeout())
			defer cancel()
			if _, _, err := s.levelCompaction.Compact(ctx); err != nil {
				if !s.closed.Load() {
					slog.Error("background level compaction failed", "error", err)
				}
			}
		})
	} else if s.compaction != nil && s.compaction.ShouldCompactWithLock() {
		s.compactionWg.Go(func() {
			if s.closed.Load() {
				return
			}
			ctx, cancel := context.WithTimeout(s.compaction.Context(), s.compaction.Timeout())
			defer cancel()
			if _, _, err := s.compaction.Compact(ctx); err != nil {
				if !s.closed.Load() {
					slog.Error("background compaction failed", "error", err)
				}
			} else {
				s.compaction.ResetTimer()
			}
		})
	}
}

// levelCompactionEnabled 检查是否启用了 Level Compaction。
func (s *Shard) levelCompactionEnabled() bool {
	return s.levelCompaction != nil
}
