package writer

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/micro-ts/mts/internal/metrics"
	"codeberg.org/micro-ts/mts/internal/storage"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/internal/storage/wal"
	"codeberg.org/micro-ts/mts/types"
)

// flushGroup 表示一次 flush 中某个 shard 的 MemPoint 集合。
type flushGroup struct {
	shard   *ShardInfo
	points  []types.MemPoint
	sstSeq  uint64
	dataDir string
}

// tryTriggerAsyncFlush 尝试触发异步 flush。CAS 保证只有一个 goroutine 执行。
func (mw *MeasurementWriter) tryTriggerAsyncFlush() {
	if !mw.memTable.TrySetFlushing() {
		return
	}
	mw.flushWg.Go(func() {
		mw.executeAsyncFlush()
	})
}

// asyncFlushResult 保存异步 flush 阶段 2 产出的结果。
type asyncFlushResult struct {
	shard     *ShardInfo
	sstSeq    uint64
	tmpPath   string
	finalPath string
	minTime   int64
	maxTime   int64
}

// executeAsyncFlush 后台执行：swap + 按 shard 分组 + 写入 SSTable + 清理。
func (mw *MeasurementWriter) executeAsyncFlush() {
	// Phase 1: 持锁交换 + WAL 切分
	mw.mu.Lock()

	if mw.wal != nil {
		if err := mw.wal.Rotate(); err != nil {
			slog.Warn("WAL rotate before async flush failed", "error", err)
			mw.mu.Unlock()
			mw.memTable.ClearFlushing()
			return
		}
	}

	passive := mw.memTable.Swap()
	mw.mu.Unlock()

	if len(passive) == 0 {
		mw.memTable.ClearPassive()
		return
	}

	// Phase 2: 按 shard 分组（不持锁）
	groups := mw.groupByShardKey(passive)

	var results []asyncFlushResult
	for _, g := range groups {
		result, err := mw.writeGroupSSTable(g)
		if err != nil {
			slog.Error("async flush: write SSTable failed", "shard", g.shard.Dir, "error", err)
			// 失败时将所有 passive 数据合并回 active
			mw.mu.Lock()
			mw.memTable.MergePassiveBack()
			mw.mu.Unlock()
			// 清理已成功的临时文件
			for _, r := range results {
				_ = os.Remove(r.tmpPath)
			}
			return
		}
		results = append(results, result)
	}

	// Phase 3: 持锁 → ClearPassive → 原子 rename → 注册 → WAL 清理
	mw.mu.Lock()

	mw.memTable.ClearPassive()

	for _, r := range results {
		if err := os.Rename(r.tmpPath, r.finalPath); err != nil {
			slog.Error("async flush: rename tmp to final failed",
				"tmp", r.tmpPath, "final", r.finalPath, "error", err)
			_ = os.Remove(r.tmpPath)
			continue
		}

		var size int64
		if fi, statErr := os.Stat(r.finalPath); statErr == nil {
			size = fi.Size()
		}
		mw.shardStore.RegisterSSTable(r.shard, r.sstSeq, r.finalPath, r.minTime, r.maxTime, size)
		mw.shardStore.TriggerCompaction(r.shard)
	}

	if mw.wal != nil {
		if walErr := mw.wal.TruncateCurrent(); walErr != nil {
			slog.Warn("failed to truncate WAL after async flush", "error", walErr)
		}
		cp := &wal.Checkpoint{
			Generation: mw.wal.Generation(),
			Segment:    mw.wal.SegmentNum(),
		}
		if cpErr := cp.Save(filepath.Join(mw.dir, "wal")); cpErr != nil {
			slog.Warn("failed to save WAL checkpoint", "error", cpErr)
		}
	}

	metrics.Incr(metrics.FlushTotal, 1)
	metrics.Incr(metrics.FlushPoints, int64(len(passive)))

	mw.mu.Unlock()

	// 链接触发：若 active 已积累足够数据，立即开始下一轮异步 flush
	if mw.memTable.ShouldSwap() {
		mw.tryTriggerAsyncFlush()
	}
}

// groupByShardKey 将已排序的 MemPoint 按 shard 时间窗口分组。
// MemTable 数据已按 Timestamp 排序，只需一次线性扫描。
func (mw *MeasurementWriter) groupByShardKey(points []types.MemPoint) []flushGroup {
	if len(points) == 0 {
		return nil
	}

	shardDur := mw.shardDur
	type groupKey struct {
		startTime int64
	}
	groupMap := make(map[groupKey]*flushGroup)
	var groupOrder []groupKey

	for _, mp := range points {
		startTime := (mp.Timestamp / shardDur) * shardDur
		gk := groupKey{startTime: startTime}

		g, ok := groupMap[gk]
		if !ok {
			shard, err := mw.shardStore.GetOrCreateShard(mw.db, mw.measurement, startTime)
			if err != nil {
				slog.Warn("failed to get shard for flush group", "startTime", startTime, "error", err)
				continue
			}
			g = &flushGroup{
				shard:   shard,
				points:  make([]types.MemPoint, 0, 1024),
				sstSeq:  mw.shardStore.NextSSTSeq(shard),
				dataDir: shard.DataDir,
			}
			groupMap[gk] = g
			groupOrder = append(groupOrder, gk)
		}
		g.points = append(g.points, mp)
	}

	result := make([]flushGroup, 0, len(groupOrder))
	for _, gk := range groupOrder {
		result = append(result, *groupMap[gk])
	}
	return result
}

// writeGroupSSTable 将一组 MemPoint 写入 SSTable 文件（临时文件）。
func (mw *MeasurementWriter) writeGroupSSTable(g flushGroup) (asyncFlushResult, error) {
	shard := g.shard
	dataDir := g.dataDir
	sstSeq := g.sstSeq

	if err := storage.SafeMkdirAll(dataDir, 0700); err != nil {
		return asyncFlushResult{}, fmt.Errorf("create data dir: %w", err)
	}

	finalPath := filepath.Join(dataDir, fmt.Sprintf("sst_%d.bin", sstSeq))
	tmpPath := filepath.Join(dataDir, fmt.Sprintf(".sst_%d.bin.tmp", sstSeq))

	w, err := sstable.NewWriter(shard.Dir, sstSeq, 0, mw.compressionAlgorithm)
	if err != nil {
		return asyncFlushResult{}, fmt.Errorf("create sstable writer: %w", err)
	}
	w.SetSyncOnClose(false) // flush 走 tmp+rename，WAL 保证持久性

	if err := w.WriteMemPoints(g.points); err != nil {
		_ = w.Close()
		return asyncFlushResult{}, fmt.Errorf("write mempoints: %w", err)
	}

	if mw.schemaStore != nil {
		mw.schemaMu.Lock()
		metaSchema := sstableSchemaToMetaSchema(w.Schema())
		mw.schemaMu.Unlock()
		if err := mw.schemaStore.SetSchema(mw.db, mw.measurement, metaSchema); err != nil {
			slog.Warn("failed to persist schema", "error", err)
		}
		mw.schemaMu.Lock()
		mw.schema = metaSchema
		mw.schemaMu.Unlock()
	}

	if err := w.Close(); err != nil {
		return asyncFlushResult{}, fmt.Errorf("close sstable writer: %w", err)
	}

	// NewWriter + Close 输出到 <dir>/data/sst_N.bin；移动到目标 dataDir
	srcPath := filepath.Join(shard.Dir, "data", fmt.Sprintf("sst_%d.bin", sstSeq))
	if srcPath != finalPath {
		if err := os.Rename(srcPath, finalPath); err != nil {
			_ = os.Remove(srcPath)
			return asyncFlushResult{}, fmt.Errorf("move SSTable to dest: %w", err)
		}
	}

	// 重命名为临时文件，Phase 3 原子 rename 到 finalPath
	if err := os.Rename(finalPath, tmpPath); err != nil {
		return asyncFlushResult{}, fmt.Errorf("rename sst to tmp: %w", err)
	}

	minTime, maxTime := calcPointTimeRange(g.points)

	return asyncFlushResult{
		shard:     shard,
		sstSeq:    sstSeq,
		tmpPath:   tmpPath,
		finalPath: finalPath,
		minTime:   minTime,
		maxTime:   maxTime,
	}, nil
}

// calcPointTimeRange 计算 points 的时间范围。
func calcPointTimeRange(points []types.MemPoint) (int64, int64) {
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

// Flush 同步刷写所有 MemTable 数据（用于 Close 和手动调用）。
func (mw *MeasurementWriter) Flush() error {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	for mw.memTable.IsFlushing() {
		mw.mu.Unlock()
		time.Sleep(backpressureSleep)
		mw.mu.Lock()
	}

	return mw.flushLocked()
}

// flushLocked 内部同步刷写方法（已持有锁）。
func (mw *MeasurementWriter) flushLocked() error {
	passive := mw.memTable.Swap()
	if len(passive) == 0 {
		return nil
	}

	groups := mw.groupByShardKey(passive)

	for _, g := range groups {
		if err := mw.writeGroupSSTableSync(g); err != nil {
			mw.memTable.MergePassiveBack()
			return err
		}
	}

	mw.memTable.ClearPassive()

	metrics.Incr(metrics.FlushTotal, 1)
	metrics.Incr(metrics.FlushPoints, int64(len(passive)))

	if mw.wal != nil {
		if err := mw.wal.TruncateAfterFlush(); err != nil {
			slog.Warn("failed to truncate WAL after flush", "error", err)
		}
	}

	for _, g := range groups {
		mw.shardStore.TriggerCompaction(g.shard)
	}

	return nil
}

// writeGroupSSTableSync 同步写入 SSTable（持锁调用，直接写 finalPath）。
func (mw *MeasurementWriter) writeGroupSSTableSync(g flushGroup) error {
	dataDir := g.dataDir
	sstSeq := g.sstSeq

	if err := storage.SafeMkdirAll(dataDir, 0700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	finalPath := filepath.Join(dataDir, fmt.Sprintf("sst_%d.bin", sstSeq))

	w, err := sstable.NewWriter(g.shard.Dir, sstSeq, 0, mw.compressionAlgorithm)
	if err != nil {
		return fmt.Errorf("create sstable writer: %w", err)
	}
	w.SetSyncOnClose(false) // flush 场景，WAL 保证持久性

	if err := w.WriteMemPoints(g.points); err != nil {
		_ = w.Close()
		return fmt.Errorf("write mempoints: %w", err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close sstable writer: %w", err)
	}

	srcPath := filepath.Join(g.shard.Dir, "data", fmt.Sprintf("sst_%d.bin", sstSeq))
	if srcPath != finalPath {
		if err := os.Rename(srcPath, finalPath); err != nil {
			_ = os.Remove(srcPath)
			return fmt.Errorf("move SSTable to dest: %w", err)
		}
	}

	minTime, maxTime := calcPointTimeRange(g.points)

	var size int64
	if fi, err := os.Stat(finalPath); err == nil {
		size = fi.Size()
	}
	mw.shardStore.RegisterSSTable(g.shard, sstSeq, finalPath, minTime, maxTime, size)
	mw.shardStore.TriggerCompaction(g.shard)

	return nil
}

// startPeriodicFlushCheck 启动 MemTable 定期刷盘检查。
func (mw *MeasurementWriter) startPeriodicFlushCheck() {
	idleDuration := mw.memTable.IdleTimeout()
	var interval time.Duration
	if idleDuration > 0 {
		interval = idleDuration / 2
		if interval < 100*time.Millisecond {
			interval = 100 * time.Millisecond
		} else if interval > 30*time.Second {
			interval = 30 * time.Second
		}
	} else {
		interval = 10 * time.Second
	}

	mw.flushTicker = time.NewTicker(interval)
	mw.flushWg.Go(func() {
		for {
			select {
			case <-mw.flushTicker.C:
				mw.doPeriodicFlush()
			case <-mw.flushDone:
				mw.flushTicker.Stop()
				return
			}
		}
	})
}

func (mw *MeasurementWriter) doPeriodicFlush() {
	if mw.memTable.ShouldSwap() {
		mw.tryTriggerAsyncFlush()
	}
}

// Close 关闭 Writer，刷盘数据并释放资源。
func (mw *MeasurementWriter) Close() error {
	var err error
	mw.closeOnce.Do(func() {
		if mw.flushDone != nil {
			close(mw.flushDone)
			mw.flushWg.Wait()
		}

		err = mw.closeWithLock()
	})
	return err
}

func (mw *MeasurementWriter) closeWithLock() error {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	for mw.memTable.IsFlushing() {
		mw.mu.Unlock()
		time.Sleep(time.Millisecond)
		mw.mu.Lock()
	}

	flushErr := mw.flushLocked()
	if flushErr != nil {
		if mw.wal != nil {
			if closeErr := mw.wal.Close(); closeErr != nil {
				slog.Warn("wal close failed after flush error",
					"flushErr", flushErr, "walCloseErr", closeErr)
			}
		}
		return fmt.Errorf("flush memtable: %w", flushErr)
	}

	if mw.wal != nil {
		if closeErr := mw.wal.Close(); closeErr != nil {
			slog.Warn("failed to close WAL", "error", closeErr)
		}
		if purgeErr := mw.wal.Purge(); purgeErr != nil {
			slog.Warn("failed to purge WAL", "error", purgeErr)
		}
	}

	mw.closed.Store(true)
	return nil
}

// ReplayWAL 重放 WAL 数据恢复到 MemTable。
func (mw *MeasurementWriter) ReplayWAL() error {
	if mw.wal == nil {
		return nil
	}

	err := mw.wal.Replay(func(data []byte) error {
		mp, err := deserializeFromWAL(data)
		if err != nil {
			slog.Warn("WAL replay: failed to deserialize point, skipping", "error", err)
			return nil
		}

		if mw.seriesStore != nil {
			mw.seriesStore.GetTagsBySID(mp.Sid)
		}

		if err := mw.memTable.Write(mp); err != nil {
			return fmt.Errorf("WAL replay: write to memtable: %w", err)
		}

		if mw.memTable.ShouldFlush() {
			if err := mw.flushLocked(); err != nil {
				slog.Warn("WAL replay: flush failed", "error", err)
			}
		}

		return nil
	})

	mw.memTable.Sort()

	if mw.memTable.Count() > 0 {
		if err := mw.flushLocked(); err != nil {
			slog.Warn("WAL replay final flush failed", "error", err)
		}
	}

	metrics.Incr(metrics.WALReplayTotal, 1)

	return err
}

// Schema 返回当前 schema（供查询使用）。
func (mw *MeasurementWriter) Schema() *metadata.Schema {
	mw.schemaMu.RLock()
	defer mw.schemaMu.RUnlock()
	return mw.schema
}

// IsClosed 检查 Writer 是否已关闭。
func (mw *MeasurementWriter) IsClosed() bool {
	return mw.closed.Load()
}
