// internal/engine/engine.go
package engine

import (
	"sync"
	"time"

	"micro-ts/internal/storage/measurement"
	"micro-ts/internal/storage/shard"
	"micro-ts/internal/types"
)

// Config 存储引擎配置
type Config struct {
	DataDir       string
	ShardDuration time.Duration
}

// Engine 存储引擎
type Engine struct {
	cfg          *Config
	shardManager *shard.ShardManager
	metaStores   map[string]*measurement.MemoryMetaStore
	mu           sync.RWMutex
}

// NewEngine 创建引擎
func NewEngine(cfg *Config) (*Engine, error) {
	return &Engine{
		cfg:          cfg,
		shardManager: shard.NewShardManager(cfg.DataDir, cfg.ShardDuration),
		metaStores:   make(map[string]*measurement.MemoryMetaStore),
	}, nil
}

// Close 关闭引擎
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.metaStores = nil
	return nil
}

// Write 写入单个点
func (e *Engine) Write(point *types.Point) error {
	// 获取或创建 Shard
	s, err := e.shardManager.GetShard(point.Database, point.Measurement, point.Timestamp)
	if err != nil {
		return err
	}

	// 写入 Shard
	return s.Write(point)
}

// WriteBatch 批量写入
func (e *Engine) WriteBatch(points []*types.Point) error {
	for _, p := range points {
		if err := e.Write(p); err != nil {
			return err
		}
	}
	return nil
}

// Query 范围查询
func (e *Engine) Query(req *types.QueryRangeRequest) (*types.QueryRangeResponse, error) {
	// 获取相交的 Shard
	shards := e.shardManager.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)

	var rows []types.PointRow
	for _, s := range shards {
		r, err := s.Read(req.StartTime, req.EndTime)
		if err != nil {
			return nil, err
		}
		rows = append(rows, r...)
	}

	return &types.QueryRangeResponse{
		Database:    req.Database,
		Measurement: req.Measurement,
		StartTime:   req.StartTime,
		EndTime:     req.EndTime,
		TotalCount:  int64(len(rows)),
		Rows:        rows,
	}, nil
}
