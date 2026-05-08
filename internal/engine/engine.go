// Package engine 实现微时序数据库的存储引擎。
//
// Engine 是数据库的核心组件，负责协调写入和查询操作。
// 它管理 Shard 的创建和回收，以及元数据的访问。
//
// 架构说明：
//
//	Engine → ShardManager → Shards → MemTable/SSTable
//	Engine → Manager → Catalog / SeriesStore / ShardIndex
//
// Engine 是并发安全的，所有公共方法都可以从多个 goroutine 调用。
package engine

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/types"
)

// 错误定义
var (
	ErrNilPoint            = errors.New("point is nil")
	ErrEmptyDatabase       = errors.New("database name is empty")
	ErrEmptyMeasurement    = errors.New("measurement name is empty")
	ErrInvalidTimestamp    = errors.New("timestamp is negative")
	ErrDatabaseNotFound    = errors.New("database not found")
	ErrMeasurementNotFound = errors.New("measurement not found")
)

// Config 定义存储引擎的配置。
type Config struct {
	DataDir                string
	ShardDuration          time.Duration
	MemTableCfg            *types.MemTableConfig
	CompactionCfg          *shard.CompactionConfig
	RetentionPeriod        time.Duration
	RetentionCheckInterval time.Duration
}

// Engine 是微时序数据库的存储引擎。
type Engine struct {
	cfg              *Config
	shardManager     *shard.ShardManager
	retentionService *shard.RetentionService
	manager          *metadata.Manager
	shutdownMu       sync.Mutex
	queryWg          sync.WaitGroup
	closed           bool
}

// New 创建新的存储引擎实例。
func New(cfg *Config) (*Engine, error) {
	var memTableCfg *types.MemTableConfig
	if cfg.MemTableCfg == nil || cfg.MemTableCfg.MaxSize == 0 {
		memTableCfg = shard.DefaultMemTableConfig()
	} else {
		memTableCfg = cfg.MemTableCfg
	}

	retentionCheckInterval := cfg.RetentionCheckInterval
	if retentionCheckInterval == 0 {
		retentionCheckInterval = time.Hour
	}

	mgr, err := metadata.NewManager(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("create metadata manager: %w", err)
	}

	if err := mgr.Load(); err != nil {
		return nil, fmt.Errorf("load metadata: %w", err)
	}

	engine := &Engine{
		cfg:          cfg,
		shardManager: shard.NewShardManager(cfg.DataDir, cfg.ShardDuration, memTableCfg, cfg.CompactionCfg, mgr),
		manager:      mgr,
	}

	if cfg.RetentionPeriod > 0 {
		engine.retentionService = shard.NewRetentionService(
			engine.shardManager,
			cfg.RetentionPeriod,
			retentionCheckInterval,
		)
		engine.retentionService.Start()
	}

	return engine, nil
}

// Close 关闭引擎，释放所有资源。
func (e *Engine) Close() error {
	e.shutdownMu.Lock()
	if e.closed {
		e.shutdownMu.Unlock()
		return nil
	}
	e.closed = true
	e.shutdownMu.Unlock()

	if e.retentionService != nil {
		e.retentionService.Stop()
	}

	e.queryWg.Wait()

	_ = e.shardManager.FlushAll()

	if err := e.manager.Sync(); err != nil {
		return fmt.Errorf("sync metadata: %w", err)
	}

	if err := e.manager.Close(); err != nil {
		return fmt.Errorf("close metadata manager: %w", err)
	}
	return nil
}

// isClosed 检查引擎是否已关闭。
func (e *Engine) isClosed() bool {
	e.shutdownMu.Lock()
	defer e.shutdownMu.Unlock()
	return e.closed
}

// Flush 将所有 MemTable 数据刷写到 SSTable。
func (e *Engine) Flush() error {
	return e.shardManager.FlushAll()
}

// DataDir 返回引擎的数据目录。
func (e *Engine) DataDir() string {
	return e.cfg.DataDir
}
