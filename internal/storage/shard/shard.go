// internal/storage/shard/shard.go
package shard

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"micro-ts/internal/storage/shard/sstable"
	"micro-ts/internal/types"
)

// Shard 单个 Shard
type Shard struct {
	db          string
	measurement string
	startTime   int64
	endTime     int64
	dir         string
	memTable    *MemTable
	mu          sync.RWMutex
}

// NewShard 创建 Shard
func NewShard(db, measurement string, startTime, endTime int64, dir string) *Shard {
	return &Shard{
		db:          db,
		measurement: measurement,
		startTime:   startTime,
		endTime:     endTime,
		dir:         dir,
		memTable:    NewMemTable(1024 * 1024), // 1MB default
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
func (s *Shard) Write(point *types.Point) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memTable.Write(point)
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

	rows, err := r.ReadRange(startTime, endTime)
	if closeErr := r.Close(); closeErr != nil {
		return nil, closeErr
	}

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

// Close 关闭 Shard
func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 如果还有未刷写的数据，先刷写
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

	return nil
}
