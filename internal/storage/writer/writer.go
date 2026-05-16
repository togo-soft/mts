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

const (
	backpressureSleep    = time.Millisecond
	backpressureTimeout  = 30 * time.Second
)

var errBackpressureTimeout = fmt.Errorf("backpressure timeout: memtable still full after 30s")

// Write 写入单个数据点。
func (mw *MeasurementWriter) Write(point *types.Point) error {
	// 快速路径：无背压时跳过 deadline 计算
	if mw.memTable.ActiveFull() {
		deadline := time.Now().Add(backpressureTimeout)
		for mw.memTable.ActiveFull() {
			if time.Now().After(deadline) {
				return errBackpressureTimeout
			}
			time.Sleep(backpressureSleep)
			if mw.closed.Load() {
				return fmt.Errorf("writer closed during backpressure wait")
			}
		}
	}

	mw.mu.Lock()

	sid, err := mw.seriesStore.AllocateSID(mw.db, mw.measurement, point.Tags)
	if err != nil {
		mw.mu.Unlock()
		return fmt.Errorf("allocate SID: %w", err)
	}

	var mp types.MemPoint
	var walData []byte
	var walRelease func()
	if mw.wal != nil {
		// 合并字段+WAL 序列化，单次 map 遍历直接写入 WAL 池缓冲区
		mp, walData, walRelease = serializePointDirect(point.Timestamp, sid, point.Fields)
	} else {
		mp = types.PointToMemPoint(point, sid)
	}
	mw.mu.Unlock()

	mw.mu.Lock()
	if mw.closed.Load() {
		mw.mu.Unlock()
		if walRelease != nil {
			walRelease()
		}
		return fmt.Errorf("writer closed")
	}

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

	if mw.memTable.ActiveFull() {
		deadline := time.Now().Add(backpressureTimeout)
		for mw.memTable.ActiveFull() {
			if time.Now().After(deadline) {
				return 0, errBackpressureTimeout
			}
			time.Sleep(backpressureSleep)
			if mw.closed.Load() {
				return 0, fmt.Errorf("writer closed during backpressure wait")
			}
		}
	}

	mw.mu.Lock()

	if mw.closed.Load() {
		mw.mu.Unlock()
		return 0, fmt.Errorf("writer closed")
	}

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

		if mw.wal != nil {
			mp, data, release := serializePointDirect(point.Timestamp, sid, point.Fields)
			mps = append(mps, mp)
			walData = append(walData, data)
			walReleases = append(walReleases, release)
		} else {
			mps = append(mps, types.PointToMemPoint(point, sid))
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
	logger.Debug("writer.New: opening WAL", "walDir", walDir)

	type walResult struct {
		w   *wal.WAL
		err error
	}
	walCh := make(chan walResult, 1)
	go func() {
		w, err := wal.Open(wal.Config{
			Dir:          walDir,
			SegmentSize:  64 * 1024 * 1024,
			MaxSegments:  5,
			SyncMode:     wal.SyncPeriodic,
			SyncInterval: time.Minute,
			Logger:       logger,
		})
		walCh <- walResult{w, err}
	}()

	var w *wal.WAL
	select {
	case res := <-walCh:
		w = res.w
		if res.err != nil {
			w = nil
			logger.Warn("failed to open WAL, writes will not be durable",
				"walDir", walDir, "error", res.err)
		}
	case <-time.After(10 * time.Second):
		w = nil
		logger.Warn("WAL open timed out after 10s, writes will not be durable",
			"walDir", walDir)
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
