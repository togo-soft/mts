// internal/storage/shard/shard.go
package shard

import (
	"sync"
	"time"

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
