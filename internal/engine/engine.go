// internal/engine/engine.go
package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"micro-ts/internal/query"
	"micro-ts/internal/storage/measurement"
	"micro-ts/internal/storage/shard"
	"micro-ts/types"
)

// Config 存储引擎配置
type Config struct {
	DataDir       string
	ShardDuration time.Duration
	MemTableCfg   shard.MemTableConfig
}

// Engine 存储引擎
type Engine struct {
	cfg          *Config
	shardManager *shard.ShardManager
	metaStores   map[string]*measurement.MemoryMetaStore
	mu           sync.RWMutex
}

// New 创建引擎
func New(cfg *Config) (*Engine, error) {
	// 默认 MemTable 配置
	memTableCfg := cfg.MemTableCfg
	if memTableCfg.MaxSize == 0 {
		memTableCfg = shard.DefaultMemTableConfig()
	}
	return &Engine{
		cfg:          cfg,
		shardManager: shard.NewShardManager(cfg.DataDir, cfg.ShardDuration, memTableCfg),
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
		return fmt.Errorf("get shard: %w", err)
	}

	// 写入 Shard
	if err := s.Write(point); err != nil {
		return fmt.Errorf("write to shard: %w", err)
	}
	return nil
}

// WriteBatch 批量写入
func (e *Engine) WriteBatch(points []*types.Point) error {
	for _, p := range points {
		if err := e.Write(p); err != nil {
			return fmt.Errorf("write point (timestamp=%d): %w", p.Timestamp, err)
		}
	}
	return nil
}

// Query 范围查询（使用流式迭代器避免加载所有数据到内存）
func (e *Engine) Query(ctx context.Context, req *types.QueryRangeRequest) (*types.QueryRangeResponse, error) {
	// 获取相交的 Shard
	shards := e.shardManager.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)

	if len(shards) == 0 {
		return &types.QueryRangeResponse{
			Database:    req.Database,
			Measurement: req.Measurement,
			StartTime:   req.StartTime,
			EndTime:     req.EndTime,
			TotalCount:  0,
			Rows:        []types.PointRow{},
		}, nil
	}

	// 创建流式查询迭代器
	qit, err := e.QueryIterator(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("create query iterator: %w", err)
	}
	defer qit.Close()

	// 流式收集结果，跳过 offset 行，然后收集 limit 行
	var rows []types.PointRow
	targetCount := int(req.Limit) + int(req.Offset)
	hasExplicitLimit := req.Limit > 0
	// 如果没有指定 limit，不设置目标数量上限（使用最大 int）
	if !hasExplicitLimit {
		targetCount = int(^uint(0) >> 1) // MaxInt
	}

	skipped := 0
	collected := 0
	hasLimit := req.Limit > 0
	for qit.Next(ctx) {
		row := qit.Points()
		if row == nil {
			continue
		}
		// 跳过 offset 指定的行
		if int64(skipped) < req.Offset {
			skipped++
			continue
		}
		rows = append(rows, *row)
		collected++
		// 已收集足够的行（仅在指定了 limit 时提前停止）
		if hasLimit && collected >= int(req.Limit) {
			// 停止收集，但继续检查是否有更多数据
			break
		}
		// 也检查是否已达到目标数量上限
		if collected >= targetCount-int(req.Offset) {
			break
		}
	}

	// 检查是否有更多数据
	// 流式语义：如果收集满 limit 行，认为可能还有更多数据
	hasMore := false
	if hasLimit && collected >= int(req.Limit) {
		hasMore = true
	}

	// 计算 totalCount
	// 流式语义：当 HasMore=true 时，无法知道精确总数
	// 当 HasMore=false 时，表示已处理完所有数据，可以报告精确总数
	var totalCount int64
	if hasMore {
		// 有更多数据，只报告已处理的数量
		totalCount = int64(skipped + collected)
	} else {
		// 已处理完所有数据，报告精确总数
		totalCount = int64(skipped + collected)
	}

	return &types.QueryRangeResponse{
		Database:    req.Database,
		Measurement: req.Measurement,
		StartTime:   req.StartTime,
		EndTime:     req.EndTime,
		TotalCount:  totalCount,
		HasMore:     hasMore,
		Rows:        rows,
	}, nil
}

// QueryIterator 创建流式查询迭代器
func (e *Engine) QueryIterator(ctx context.Context, req *types.QueryRangeRequest) (*query.QueryIterator, error) {
	// 使用原始时间（假设为纳秒）
	startTimeNs := req.StartTime
	endTimeNs := req.EndTime

	// 获取相交的 Shards
	shards := e.shardManager.GetShards(req.Database, req.Measurement, startTimeNs, endTimeNs)
	if len(shards) == 0 {
		return nil, errors.New("no shards found")
	}

	return query.NewQueryIterator(ctx, shards, req), nil
}
