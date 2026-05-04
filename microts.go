// microts.go
package microts

import (
	"context"
	"time"

	"micro-ts/internal/engine"
	"micro-ts/internal/query"
	"micro-ts/internal/storage/shard"
	"micro-ts/internal/types"
)

// Config 配置
type Config struct {
	DataDir       string
	ShardDuration time.Duration
	MemTableCfg   shard.MemTableConfig
}

// DefaultMemTableConfig 返回默认的 MemTable 配置
func DefaultMemTableConfig() shard.MemTableConfig {
	return shard.DefaultMemTableConfig()
}

// DB 数据库
type DB struct {
	engine *engine.Engine
}

// Open 打开数据库
func Open(cfg Config) (*DB, error) {
	// 默认ShardDuration为7天
	shardDuration := cfg.ShardDuration
	if shardDuration == 0 {
		shardDuration = 7 * 24 * time.Hour
	}

	// 默认 MemTable 配置
	memTableCfg := cfg.MemTableCfg
	if memTableCfg.MaxSize == 0 {
		memTableCfg = DefaultMemTableConfig()
	}

	eng, err := engine.NewEngine(&engine.Config{
		DataDir:       cfg.DataDir,
		ShardDuration: shardDuration,
		MemTableCfg:   memTableCfg,
	})
	if err != nil {
		return nil, err
	}
	return &DB{engine: eng}, nil
}

// Write 写入单个点
func (db *DB) Write(ctx context.Context, point *types.Point) error {
	return db.engine.Write(point)
}

// WriteBatch 批量写入
func (db *DB) WriteBatch(ctx context.Context, points []*types.Point) error {
	return db.engine.WriteBatch(points)
}

// QueryRange 范围查询
func (db *DB) QueryRange(ctx context.Context, req *types.QueryRangeRequest) (*types.QueryRangeResponse, error) {
	return db.engine.Query(req)
}

// QueryIterator 创建流式查询迭代器
func (db *DB) QueryIterator(ctx context.Context, req *types.QueryRangeRequest) (*query.QueryIterator, error) {
	return db.engine.QueryIterator(ctx, req)
}

// ListMeasurements 列出 Measurement
func (db *DB) ListMeasurements(ctx context.Context, database string) ([]string, error) {
	return []string{}, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	return db.engine.Close()
}
