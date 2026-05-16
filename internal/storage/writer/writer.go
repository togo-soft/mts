// Package writer 实现 measurement 级别的写入器。
//
// MeasurementWriter 是全局唯一的写入入口，接收一个 measurement 的所有写入，
// 管理全局 MemTable 和 WAL。Flush 由外部 FlushCoordinator 编排，
// 将 MemPoint 按时间窗口分组写入对应 Shard 的 SSTable。
//
// 架构：
//
//	写入 → WAL → MemTable
//	Flush → ShardManager.Flush(...) → Shard.WriteSSTable
//	查询 → MemTable + Shard SSTables → 归并排序
package writer

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/internal/metrics"
	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/wal"
	"codeberg.org/micro-ts/mts/types"
)

// Config 定义 MeasurementWriter 的配置。
type Config struct {
	DB          string
	Measurement string
	Dir         string // measurement 数据根目录
	SeriesStore metadata.SeriesStore
	MemTableCfg *memtable.MemTableConfig
	Logger      *slog.Logger
}

// MeasurementWriter 是单个 measurement 的写入入口。
//
// 所有该 measurement 的数据先写入全局 WAL + MemTable，
// flush 由外部 FlushCoordinator 编排，将 MemTable 数据传入 ShardManager.Flush。
type MeasurementWriter struct {
	db          string
	measurement string
	dir         string

	memTable    *memtable.MemTable
	wal         *wal.WAL
	seriesStore metadata.SeriesStore

	mu        sync.Mutex
	closed    atomic.Bool
	closeOnce sync.Once
}

const backpressureSleep = time.Millisecond

// Write 写入单个数据点。
func (mw *MeasurementWriter) Write(point *types.Point) error {
	for mw.memTable.ActiveFull() {
		time.Sleep(backpressureSleep)
		if mw.closed.Load() {
			return fmt.Errorf("writer closed during backpressure wait")
		}
	}

	mw.mu.Lock()

	sid, err := mw.seriesStore.AllocateSID(mw.db, mw.measurement, point.Tags)
	if err != nil {
		mw.mu.Unlock()
		return fmt.Errorf("allocate SID: %w", err)
	}

	mp := types.PointToMemPoint(point, sid)
	mw.mu.Unlock()

	// WAL 序列化（LZ4 压缩 + 编码）移出临界区，减少锁持有时间
	var walData []byte
	var walRelease func()
	if mw.wal != nil {
		walData, walRelease = serializePointForWALPooled(mp.Timestamp, mp.Sid, mp.FieldData)
	}

	mw.mu.Lock()
	if mw.wal != nil {
		_, err := mw.wal.Write(walData)
		if walRelease != nil {
			walRelease()
		}
		if err != nil {
			metrics.Incr(metrics.WriteErrors, 1)
			mw.mu.Unlock()
			return fmt.Errorf("write to wal: %w", err)
		}
	}

	if err := mw.memTable.Write(mp); err != nil {
		metrics.Incr(metrics.WriteErrors, 1)
		mw.mu.Unlock()
		return fmt.Errorf("write to memtable: %w", err)
	}

	metrics.Incr(metrics.WriteTotal, 1)

	mw.mu.Unlock()

	return nil
}

// WriteBatch 批量写入数据点，使用单次锁获取 + 单次 WAL 批量写入。
func (mw *MeasurementWriter) WriteBatch(points []*types.Point) (int, error) {
	if len(points) == 0 {
		return 0, nil
	}

	for mw.memTable.ActiveFull() {
		time.Sleep(backpressureSleep)
		if mw.closed.Load() {
			return 0, fmt.Errorf("writer closed during backpressure wait")
		}
	}

	mw.mu.Lock()

	if mw.memTable.ActiveFull() {
		mw.mu.Unlock()
		return mw.WriteBatch(points)
	}

	mps := make([]types.MemPoint, 0, len(points))
	walData := make([][]byte, 0, len(points))
	walReleases := make([]func(), 0, len(points))

	for i, point := range points {
		sid, err := mw.seriesStore.AllocateSID(mw.db, mw.measurement, point.Tags)
		if err != nil {
			metrics.Incr(metrics.WriteErrors, 1)
			mw.mu.Unlock()
			for _, r := range walReleases {
				r()
			}
			return i, fmt.Errorf("allocate SID for point %d: %w", i, err)
		}
		mp := types.PointToMemPoint(point, sid)
		mps = append(mps, mp)

		if mw.wal != nil {
			data, release := serializePointForWALPooled(mp.Timestamp, mp.Sid, mp.FieldData)
			walData = append(walData, data)
			walReleases = append(walReleases, release)
		}
	}

	if mw.wal != nil && len(walData) > 0 {
		var batchErr error
		_, batchErr = mw.wal.WriteBatch(walData)
		for _, r := range walReleases {
			r()
		}
		if batchErr != nil {
			metrics.Incr(metrics.WriteErrors, 1)
			mw.mu.Unlock()
			return 0, fmt.Errorf("wal write batch: %w", batchErr)
		}
	}

	for i, mp := range mps {
		if err := mw.memTable.Write(mp); err != nil {
			metrics.Incr(metrics.WriteErrors, 1)
			mw.mu.Unlock()
			return i, fmt.Errorf("write to memtable at %d: %w", i, err)
		}
	}

	metrics.Incr(metrics.WriteBatchTotal, 1)
	metrics.Incr(metrics.WriteTotal, int64(len(mps)))

	mw.mu.Unlock()

	return len(mps), nil
}

// MemTable 返回内部 MemTable（供查询使用）。
func (mw *MeasurementWriter) MemTable() *memtable.MemTable {
	return mw.memTable
}

// SeriesStore 返回内部 SeriesStore（供查询时解析 SID → Tags）。
func (mw *MeasurementWriter) SeriesStore() metadata.SeriesStore {
	return mw.seriesStore
}

// New 创建新的 MeasurementWriter。
func New(cfg Config) (*MeasurementWriter, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	walDir := filepath.Join(cfg.Dir, "wal")
	logger.Info("writer.New: opening WAL", "walDir", walDir)
	w, err := wal.Open(wal.Config{
		Dir:          walDir,
		SegmentSize:  64 * 1024 * 1024,
		MaxSegments:  5,
		SyncMode:     wal.SyncPeriodic,
		SyncInterval: time.Minute,
		Logger:       logger,
	})
	if err != nil {
		w = nil
		logger.Warn("failed to open WAL, writes will not be durable",
			"walDir", walDir, "error", err)
	}

	mt := memtable.NewMemTable(cfg.MemTableCfg)

	return &MeasurementWriter{
		db:          cfg.DB,
		measurement: cfg.Measurement,
		dir:         cfg.Dir,
		memTable:    mt,
		wal:         w,
		seriesStore: cfg.SeriesStore,
	}, nil
}

// Close 关闭 Writer，清理 WAL 并标记 closed。
func (mw *MeasurementWriter) Close() error {
	var err error
	mw.closeOnce.Do(func() {
		mw.mu.Lock()
		mw.closed.Store(true)
		mw.mu.Unlock()

		if mw.wal != nil {
			if closeErr := mw.wal.Close(); closeErr != nil {
				slog.Warn("failed to close WAL", "error", closeErr)
			}
			if purgeErr := mw.wal.Purge(); purgeErr != nil {
				slog.Warn("failed to purge WAL", "error", purgeErr)
			}
		}
	})
	return err
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
			mw.seriesStore.GetTags(mw.db, mw.measurement, mp.Sid)
		}

		if err := mw.memTable.Write(mp); err != nil {
			return fmt.Errorf("WAL replay: write to memtable: %w", err)
		}

		return nil
	})

	mw.memTable.Sort()

	metrics.Incr(metrics.WALReplayTotal, 1)

	return err
}
