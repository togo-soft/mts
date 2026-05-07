package shard

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// Compact 执行 compaction。
func (lcm *LevelCompactionManager) Compact(ctx context.Context) (string, []string, error) {
	if !atomic.CompareAndSwapInt32(&lcm.compactInProgress, 0, 1) {
		return "", nil, nil
	}
	defer atomic.StoreInt32(&lcm.compactInProgress, 0)

	lcm.manifestMu.Lock()

	var targetLevel int
	for level := 0; level < len(lcm.config.LevelConfigs); level++ {
		if lcm.shouldCompactLevel(level) {
			targetLevel = level
			break
		}
	}

	selectedParts := lcm.selectPartsForMerge(targetLevel)
	if len(selectedParts) < 2 {
		lcm.manifestMu.Unlock()
		return "", nil, nil
	}

	overlaps := lcm.collectOverlapParts(targetLevel, selectedParts)
	if len(overlaps) == 0 {
		lcm.manifestMu.Unlock()
		return "", nil, nil
	}

	outputSeq := lcm.NextSeq()
	outputPath := filepath.Join(lcm.manifest.GetLevelPath(targetLevel+1), fmt.Sprintf("sst_%d", outputSeq))

	var cp *CompactionCheckpoint
	if lcm.config.EnableCheckpoint {
		cp = &CompactionCheckpoint{
			Version:    1,
			Level:      targetLevel,
			OutputSeq:  outputSeq,
			OutputPath: outputPath,
			StartedAt:  time.Now().Unix(),
		}
		inputNames := make([]string, len(overlaps))
		for i, p := range overlaps {
			inputNames[i] = p.Name
		}
		cp.InputParts = inputNames

		dataDir := filepath.Join(lcm.shard.Dir(), "data")
		if err := cp.Save(dataDir); err != nil {
			slog.Warn("failed to save checkpoint", "error", err)
		}
	}

	lcm.manifestMu.Unlock()

	inputPaths := make([]string, len(overlaps))
	for i, p := range overlaps {
		inputPaths[i] = filepath.Join(lcm.manifest.GetLevelPath(targetLevel), p.Name)
	}

	if err := lcm.merge(ctx, targetLevel, inputPaths, outputPath); err != nil {
		if cp != nil {
			dataDir := filepath.Join(lcm.shard.Dir(), "data")
			_ = cp.Clear(dataDir)
		}
		return "", nil, fmt.Errorf("merge: %w", err)
	}

	lcm.manifestMu.Lock()
	defer lcm.manifestMu.Unlock()

	inputNames := make([]string, len(overlaps))
	for i, p := range overlaps {
		inputNames[i] = p.Name
	}
	lcm.manifest.RemoveParts(targetLevel, inputNames)

	var newPartSize int64
	if info, err := os.Stat(outputPath); err == nil {
		newPartSize = info.Size()
	}

	newPart := PartInfo{
		Name:    fmt.Sprintf("sst_%d", outputSeq),
		Size:    newPartSize,
		MinTime: overlaps[0].MinTime,
		MaxTime: overlaps[len(overlaps)-1].MaxTime,
	}
	lcm.manifest.AddPart(targetLevel+1, newPart)

	if err := lcm.manifest.Save(); err != nil {
		return "", nil, fmt.Errorf("save manifest: %w", err)
	}

	for _, path := range inputPaths {
		if !lcm.shard.IsSSTUnused(path) {
			slog.Warn("sstable still in use, deferring cleanup", "path", path)
			continue
		}
		_ = os.RemoveAll(path)
	}

	if cp != nil {
		dataDir := filepath.Join(lcm.shard.Dir(), "data")
		_ = cp.Clear(dataDir)
	}

	return outputPath, inputPaths, nil
}

// merge 执行流式合并。
func (lcm *LevelCompactionManager) merge(ctx context.Context, level int, inputPaths []string, outputPath string) error {
	readers := make([]*sstable.Reader, 0, len(inputPaths))
	for _, path := range inputPaths {
		r, err := sstable.NewReader(path)
		if err != nil {
			for _, r := range readers {
				_ = r.Close()
			}
			return fmt.Errorf("open sstable reader for %s: %w", path, err)
		}
		readers = append(readers, r)
	}

	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	tombstones := collectInputTombstones(inputPaths)

	iterators := make([]*sstable.Iterator, 0, len(readers))
	for _, r := range readers {
		it, err := r.NewIterator()
		if err != nil {
			return err
		}
		iterators = append(iterators, it)
	}

	merged := newMergeIterator(iterators)

	seq := uint64(0)
	if parts := strings.Split(filepath.Base(outputPath), "_"); len(parts) == 2 {
		_, _ = fmt.Sscanf(parts[1], "%d", &seq)
	}

	w, err := sstable.NewWriter(lcm.shard.Dir(), seq, 0)
	if err != nil {
		return err
	}

	seen := make(map[string]bool)
	var pointsToWrite []*types.Point
	var tsSidMap map[int64]uint64
	const batchSize = 1000

	flushBatch := func() error {
		if len(pointsToWrite) == 0 {
			return nil
		}
		if err := w.WritePoints(pointsToWrite, tsSidMap); err != nil {
			return err
		}
		pointsToWrite = pointsToWrite[:0]
		tsSidMap = make(map[int64]uint64)
		return nil
	}

	for merged.Next() {
		select {
		case <-ctx.Done():
			_ = w.Close()
			return ctx.Err()
		default:
		}

		row := merged.Point()
		key := fmt.Sprintf("%d-%d", row.Timestamp, row.Sid)

		if seen[key] {
			continue
		}
		if tombstones.ShouldDelete(row.Sid, row.Timestamp) {
			continue
		}
		seen[key] = true

		point := &types.Point{
			Timestamp: row.Timestamp,
			Tags:      row.Tags,
			Fields:    row.Fields,
		}
		pointsToWrite = append(pointsToWrite, point)
		if tsSidMap == nil {
			tsSidMap = make(map[int64]uint64)
		}
		tsSidMap[row.Timestamp] = row.Sid

		if len(pointsToWrite) >= batchSize {
			if err := flushBatch(); err != nil {
				_ = w.Close()
				return err
			}
		}
	}

	if err := merged.Error(); err != nil {
		_ = w.Close()
		return err
	}

	if err := flushBatch(); err != nil {
		_ = w.Close()
		return err
	}

	if err := w.Close(); err != nil {
		return err
	}

	flatPath := filepath.Join(lcm.shard.Dir(), "data", fmt.Sprintf("sst_%d", seq))
	if flatPath != outputPath {
		if err := os.Rename(flatPath, outputPath); err != nil {
			return fmt.Errorf("move sstable to level path: %w", err)
		}
	}

	return saveTombstones(outputPath, tombstones)
}
