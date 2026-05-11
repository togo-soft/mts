package shard

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// Flush 将 MemTable 数据刷写到 SSTable。
func (s *Shard) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.flushLocked()
}

// flushLocked 内部刷写方法（已持有锁）
func (s *Shard) flushLocked() error {
	points := s.memTable.Flush()
	if len(points) == 0 {
		return nil
	}

	var sstSeq uint64
	var sstPath string

	if s.levelCompaction != nil {
		sstSeq = s.levelCompaction.NextSeq()
		l0Dir := filepath.Join(s.dir, "data", "L0")
		if err := os.MkdirAll(l0Dir, 0700); err != nil {
			return fmt.Errorf("create L0 dir: %w", err)
		}
		sstPath = filepath.Join(l0Dir, fmt.Sprintf("sst_%d.bin", sstSeq))
	} else {
		sstSeq = s.sstSeq
		sstPath = filepath.Join(s.dir, "data", fmt.Sprintf("sst_%d.bin", sstSeq))
	}

	// 在 NewWriter 之前标记写入状态，防止 CollectSSTables 收集到不完整的 SSTable
	if s.compaction != nil && !s.levelCompactionEnabled() {
		if err := s.compaction.MarkWriting(sstPath); err != nil {
			slog.Warn("failed to mark sstable in write", "path", sstPath, "error", err)
		}
	}

	w, err := sstable.NewWriter(s.dir, sstSeq, 0)
	if err != nil {
		// 清理 .writing 标记
		if s.compaction != nil && !s.levelCompactionEnabled() {
			if unmarkErr := s.compaction.UnmarkWriting(sstPath); unmarkErr != nil {
				slog.Warn("failed to unmark sstable after writer error", "path", sstPath, "error", unmarkErr)
			}
		}
		return fmt.Errorf("create sstable writer: %w", err)
	}

	if err := w.WritePoints(points); err != nil {
		if closeErr := w.Close(); closeErr != nil {
			slog.Warn("failed to close sstable writer after write error", "error", closeErr)
		}
		if s.compaction != nil && !s.levelCompactionEnabled() {
			if unmarkErr := s.compaction.UnmarkWriting(sstPath); unmarkErr != nil {
				slog.Warn("failed to unmark sstable after write error", "error", unmarkErr)
			}
		}
		return fmt.Errorf("write points to sstable: %w", err)
	}

	// 将检测到的 schema 写入 boltDB 并更新内存缓存
	if s.schemaStore != nil {
		metaSchema := SSTableSchemaToMetadataSchema(w.Schema())
		if err := s.schemaStore.SetSchema(s.db, s.measurement, metaSchema); err != nil {
			slog.Warn("failed to persist schema", "error", err)
		}
		// 同步更新内存 schema
		s.UpdateSchemaInMemory(metaSchema)
	}

	if err := w.Close(); err != nil {
		if s.compaction != nil && !s.levelCompactionEnabled() {
			if unmarkErr := s.compaction.UnmarkWriting(sstPath); unmarkErr != nil {
				slog.Warn("failed to unmark sstable after close error", "error", unmarkErr)
			}
		}
		return fmt.Errorf("close sstable writer: %w", err)
	}

	if s.compaction != nil && !s.levelCompactionEnabled() {
		if err := s.compaction.UnmarkWriting(sstPath); err != nil {
			slog.Warn("failed to unmark sstable write", "path", sstPath, "error", err)
		}
	}

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

	// 不在 flush 时清理 WAL segment
	// WAL segment 清理由 compaction 模块负责

	s.triggerBackgroundCompaction()

	return nil
}

// calcPointTimeRange 计算 points 的时间范围。
func (s *Shard) calcPointTimeRange(points []types.InternalPoint) (int64, int64) {
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
		s.compactionWg.Add(1)
		go func() {
			defer s.compactionWg.Done()
			if s.closed.Load() {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), s.levelCompaction.Timeout())
			defer cancel()
			if _, _, err := s.levelCompaction.Compact(ctx); err != nil {
				if !s.closed.Load() {
					slog.Error("background level compaction failed", "error", err)
				}
			}
		}()
	} else if s.compaction != nil && s.compaction.ShouldCompactWithLock() {
		s.compactionWg.Add(1)
		go func() {
			defer s.compactionWg.Done()
			if s.closed.Load() {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), s.compaction.Timeout())
			defer cancel()
			if _, _, err := s.compaction.Compact(ctx); err != nil {
				if !s.closed.Load() {
					slog.Error("background compaction failed", "error", err)
				}
			} else {
				s.compaction.ResetTimer()
			}
		}()
	}
}

// levelCompactionEnabled 检查是否启用了 Level Compaction。
func (s *Shard) levelCompactionEnabled() bool {
	return s.levelCompaction != nil
}
