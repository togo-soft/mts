// internal/storage/shard/shard.go
package shard

import (
    "time"
)

// Shard 单个 Shard
type Shard struct {
    db          string
    measurement string
    startTime   int64
    endTime     int64
    dir         string
}

// NewShard 创建 Shard
func NewShard(db, measurement string, startTime, endTime int64, dir string) *Shard {
    return &Shard{
        db:          db,
        measurement: measurement,
        startTime:   startTime,
        endTime:     endTime,
        dir:         dir,
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
