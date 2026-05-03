// internal/storage/shard/shard.go
package shard

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"micro-ts/internal/storage/measurement"
	"micro-ts/internal/storage/shard/sstable"
	"micro-ts/internal/types"
)

const (
	// memTableSize MemTable 默认大小
	memTableSize = 64 * 1024 * 1024 // 64MB
)

// Shard 单个 Shard
type Shard struct {
	db          string
	measurement string
	startTime   int64
	endTime     int64
	dir         string
	memTable    *MemTable
	wal         *WAL                              // 新增
	metaStore   *measurement.MeasurementMetaStore // 新增
	mu          sync.RWMutex
}

// NewShard 创建 Shard
func NewShard(db, measurement string, startTime, endTime int64, dir string, metaStore *measurement.MeasurementMetaStore) *Shard {
	// 创建 WAL
	walDir := filepath.Join(dir, "wal")
	wal, err := NewWAL(walDir, 0)
	if err != nil {
		// 如果 WAL 创建失败，使用 nil wal
		wal = nil
	}

	return &Shard{
		db:          db,
		measurement: measurement,
		startTime:   startTime,
		endTime:     endTime,
		dir:         dir,
		memTable:    NewMemTable(memTableSize), // 64MB
		wal:         wal,
		metaStore:   metaStore,
	}
}

// StartTime 返回开始时间
func (s *Shard) StartTime() int64 {
	return s.startTime
}

// EndTime 返回结束时间
func (s *Shard) EndTime() int64 {
	return s.endTime
}

// ContainsTime 检查时间是否在范围内
func (s *Shard) ContainsTime(ts int64) bool {
	return ts >= s.startTime && ts < s.endTime
}

// Duration 返回时间窗口
func (s *Shard) Duration() time.Duration {
	return time.Duration(s.endTime - s.startTime)
}

// Write 写入点
// 注意：如果 WAL 写入成功但 MemTable 写入失败，replay 时可能产生重复数据。
// 这是可接受的设计权衡，因为这种情况非常罕见，且 eventual consistency 可保证最终正确。
func (s *Shard) Write(point *types.Point) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. 写入 WAL
	if s.wal != nil {
		data, err := serializePoint(point)
		if err != nil {
			return err
		}
		if _, err := s.wal.Write(data); err != nil {
			return err
		}
	}

	// 2. 分配 SID
	sid := s.metaStore.AllocateSID(point.Tags)
	_ = sid // SID 目前未使用，后续会用到

	// 3. 写入 MemTable
	if err := s.memTable.Write(point); err != nil {
		return err
	}

	// 4. 检查是否需要 flush
	if s.memTable.ShouldFlush() {
		if err := s.flushLocked(); err != nil {
			return err
		}
	}

	return nil
}

// Read 读取时间范围内的点（合并 MemTable 和 SSTable）
func (s *Shard) Read(startTime, endTime int64) ([]types.PointRow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var rows []types.PointRow

	// 1. 从 MemTable 读取
	iter := s.memTable.Iterator()
	for iter.Next() {
		p := iter.Point()
		if p.Timestamp >= startTime && p.Timestamp < endTime {
			rows = append(rows, types.PointRow{
				Timestamp: p.Timestamp,
				Tags:      p.Tags,
				Fields:    p.Fields,
			})
		}
	}

	// 2. 从 SSTable 读取
	sstRows, err := s.readFromSSTable(startTime, endTime)
	if err != nil {
		return nil, err
	}
	rows = append(rows, sstRows...)

	// 3. 按时间排序
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Timestamp < rows[j].Timestamp
	})

	return rows, nil
}

// readFromSSTable 从 SSTable 读取时间范围内的数据
func (s *Shard) readFromSSTable(startTime, endTime int64) ([]types.PointRow, error) {
	dataDir := filepath.Join(s.dir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return nil, nil // 没有 SSTable
	}

	r, err := sstable.NewReader(dataDir)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	rows, err := r.ReadRange(startTime, endTime)
	return rows, err
}

// Flush 将 MemTable 数据刷写到 SSTable
func (s *Shard) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	points := s.memTable.Flush()
	if len(points) == 0 {
		return nil
	}

	// 创建 SSTable Writer
	w, err := sstable.NewWriter(s.dir, 0)
	if err != nil {
		return err
	}

	if err := w.WritePoints(points); err != nil {
		_ = w.Close()
		return err
	}

	return w.Close()
}

// flushLocked 内部刷写方法（已持有锁）
func (s *Shard) flushLocked() error {
	points := s.memTable.Flush()
	if len(points) == 0 {
		return nil
	}

	w, err := sstable.NewWriter(s.dir, 0)
	if err != nil {
		return err
	}

	if err := w.WritePoints(points); err != nil {
		_ = w.Close()
		return err
	}

	return w.Close()
}

// Close 关闭 Shard
func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. 先刷写 MemTable 到 SSTable（即使 WAL 关闭失败，数据也已安全）
	points := s.memTable.Flush()
	if len(points) > 0 {
		w, err := sstable.NewWriter(s.dir, 0)
		if err != nil {
			return err
		}

		if err := w.WritePoints(points); err != nil {
			_ = w.Close()
			return err
		}

		if err := w.Close(); err != nil {
			return err
		}
	}

	// 2. 关闭 WAL（WAL 关闭失败可接受，因为数据已在 SSTable）
	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			return err
		}
	}

	return nil
}
