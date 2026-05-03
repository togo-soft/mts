// internal/engine/engine.go
package engine

import (
	"sync"
	"time"

	"micro-ts/internal/storage/measurement"
	"micro-ts/internal/storage/shard"
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
