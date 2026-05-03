// internal/engine/engine.go
package engine

import (
	"sort"
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

	// 并发读取所有 Shard
	rowsCh := make(chan []types.PointRow, len(shards))
	var wg sync.WaitGroup

	for _, s := range shards {
		wg.Add(1)
		go func(s *shard.Shard) {
			defer wg.Done()
			rows, err := s.Read(req.StartTime, req.EndTime)
			if err != nil {
				return
			}
			rowsCh <- rows
		}(s)
	}

	go func() {
		wg.Wait()
		close(rowsCh)
	}()

	// 合并结果
	var allRows []types.PointRow
	for rows := range rowsCh {
		allRows = append(allRows, rows...)
	}

	// 按时间排序
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].Timestamp < allRows[j].Timestamp
	})

	// Tag 过滤
	if len(req.Tags) > 0 {
		allRows = e.filterTags(allRows, req.Tags)
	}

	// 字段过滤
	if len(req.Fields) > 0 {
		allRows = e.filterFields(allRows, req.Fields)
	}

	// 分页
	totalCount := int64(len(allRows))
	if req.Offset > 0 {
		if req.Offset < int64(len(allRows)) {
			allRows = allRows[req.Offset:]
		} else {
			allRows = nil
		}
	}
	if req.Limit > 0 && int64(len(allRows)) > req.Limit {
		allRows = allRows[:req.Limit]
	}

	return &types.QueryRangeResponse{
		Database:    req.Database,
		Measurement: req.Measurement,
		StartTime:   req.StartTime,
		EndTime:     req.EndTime,
		TotalCount:  totalCount,
		HasMore:     req.Limit > 0 && int64(len(allRows)) >= req.Limit,
		Rows:        allRows,
	}, nil
}

// filterFields 根据指定字段列表过滤行数据
func (e *Engine) filterFields(rows []types.PointRow, fields []string) []types.PointRow {
	fieldSet := make(map[string]bool)
	for _, f := range fields {
		fieldSet[f] = true
	}

	result := make([]types.PointRow, len(rows))
	for i, row := range rows {
		filtered := make(map[string]any)
		for name, val := range row.Fields {
			if fieldSet[name] {
				filtered[name] = val
			}
		}
		result[i] = types.PointRow{
			Timestamp: row.Timestamp,
			Tags:      row.Tags,
			Fields:    filtered,
		}
	}
	return result
}

// filterTags 根据指定 Tag 键值对过滤行数据
func (e *Engine) filterTags(rows []types.PointRow, tags map[string]string) []types.PointRow {
	result := make([]types.PointRow, 0, len(rows))
	for _, row := range rows {
		match := true
		for k, v := range tags {
			if row.Tags[k] != v {
				match = false
				break
			}
		}
		if match {
			result = append(result, row)
		}
	}
	return result
}
