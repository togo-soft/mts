package shard

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

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
		sstPath = filepath.Join(l0Dir, fmt.Sprintf("sst_%d", sstSeq))
	} else {
		sstSeq = s.sstSeq
		sstPath = filepath.Join(s.dir, "data", fmt.Sprintf("sst_%d", sstSeq))
	}

	w, err := sstable.NewWriter(s.dir, sstSeq, 0)
	if err != nil {
		return fmt.Errorf("create sstable writer: %w", err)
	}

	if s.compaction != nil && !s.levelCompactionEnabled() {
		if err := s.compaction.markSSTableWriting(sstPath); err != nil {
			slog.Warn("failed to mark sstable in write", "path", sstPath, "error", err)
		}
	}

	if err := w.WritePoints(points, s.tsSidMap); err != nil {
		_ = w.Close()
		if s.compaction != nil && !s.levelCompactionEnabled() {
			_ = s.compaction.unmarkSSTableWriting(sstPath)
		}
		return fmt.Errorf("write points to sstable: %w", err)
	}

	for _, p := range points {
		delete(s.tsSidMap, p.Timestamp)
	}

	if err := w.Close(); err != nil {
		if s.compaction != nil && !s.levelCompactionEnabled() {
			_ = s.compaction.unmarkSSTableWriting(sstPath)
		}
		return fmt.Errorf("close sstable writer: %w", err)
	}

	if s.compaction != nil && !s.levelCompactionEnabled() {
		if err := s.compaction.unmarkSSTableWriting(sstPath); err != nil {
			slog.Warn("failed to unmark sstable write", "path", sstPath, "error", err)
		}
	}

	if s.levelCompaction != nil {
		srcPath := filepath.Join(s.dir, "data", fmt.Sprintf("sst_%d", sstSeq))
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

		s.levelCompaction.AddPart(0, PartInfo{
			Name:    fmt.Sprintf("sst_%d", sstSeq),
			Size:    size,
			MinTime: minTime,
			MaxTime: maxTime,
		})
	} else {
		s.sstSeq++
	}

	if s.wal != nil {
		_ = s.wal.TruncateCurrent()
	}

	for i := range points {
		points[i] = nil
	}

	s.triggerBackgroundCompaction()

	return nil
}

// calcPointTimeRange 计算 points 的时间范围。
func (s *Shard) calcPointTimeRange(points []*types.Point) (int64, int64) {
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
	if s.levelCompaction != nil && s.levelCompaction.ShouldCompact() {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), s.levelCompaction.Config().Timeout)
			defer cancel()
			if _, _, err := s.levelCompaction.Compact(ctx); err != nil {
				slog.Error("background level compaction failed", "error", err)
			}
		}()
	} else if s.compaction != nil && s.compaction.ShouldCompactWithLock() {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), s.compaction.config.Timeout)
			defer cancel()
			if _, _, err := s.compaction.Compact(ctx); err != nil {
				slog.Error("background compaction failed", "error", err)
			} else {
				s.compaction.resetTimer()
			}
		}()
	}
}

// levelCompactionEnabled 检查是否启用了 Level Compaction。
func (s *Shard) levelCompactionEnabled() bool {
	return s.levelCompaction != nil
}
