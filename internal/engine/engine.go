// Package engine 实现微时序数据库的存储引擎。
//
// Engine 是数据库的核心组件，负责协调写入和查询操作。
// 它管理 Shard 的创建和回收，以及元数据的访问。
//
// 架构说明：
//
//	Engine → Flusher → ShardManager → Shards → SSTable
//	Engine → Catalog / SeriesStore / ShardIndex → metadata.Manager
//	Engine → FlushCoordinator → Writer(WAL+MemTable)
//
// Engine 是并发安全的，所有公共方法都可以从多个 goroutine 调用。
package engine

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/internal/storage/writer"
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
	CompactionCfg          *compaction.Config
	CompressionAlgorithm   sstable.CompressionAlgorithm
	RetentionPeriod        time.Duration
	RetentionCheckInterval time.Duration
}

// Engine 是微时序数据库的存储引擎。
type Engine struct {
	cfg         *Config
	dataDir     string
	catalog     Catalog
	seriesStore SeriesStore
	shardIndex  ShardIndex
	flusher     Flusher
	coordinator *FlushCoordinator
	memTableCfg *types.MemTableConfig
	metaManager *metadata.Manager
	mu          sync.RWMutex
	queryWg     sync.WaitGroup
	closed      bool
	shutdownMu  sync.Mutex
}

// New 创建新的存储引擎实例。
func New(cfg *Config) (*Engine, error) {
	var memTableCfg *types.MemTableConfig
	if cfg.MemTableCfg == nil || cfg.MemTableCfg.MaxSize == 0 {
		memTableCfg = memtable.DefaultMemTableConfig()
	} else {
		memTableCfg = cfg.MemTableCfg
	}

	mgr, err := metadata.NewManager(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("create metadata manager: %w", err)
	}

	if err := mgr.Load(); err != nil {
		return nil, fmt.Errorf("load metadata: %w", err)
	}

	flusher := shard.NewShardManager(
		cfg.DataDir,
		cfg.ShardDuration,
		cfg.CompactionCfg,
		cfg.CompressionAlgorithm,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	coordinator := NewFlushCoordinator(flusher)
	coordinator.StartPeriodicCheck(time.Second)

	engine := &Engine{
		cfg:         cfg,
		dataDir:     cfg.DataDir,
		catalog:     mgr.Catalog(),
		seriesStore: mgr.Series(),
		shardIndex:  mgr.Shards(),
		flusher:     flusher,
		coordinator: coordinator,
		memTableCfg: memTableCfg,
		metaManager: mgr,
	}

	// 后台发现已有 measurement 的 WAL 并重放
	go engine.discoverAndRecover()

	return engine, nil
}

// discoverAndRecover 发现已存在的 measurement 并重放 WAL。
func (e *Engine) discoverAndRecover() {
	databases := e.catalog.ListDatabases()
	for _, db := range databases {
		measurements, err := e.catalog.ListMeasurements(db)
		if err != nil {
			continue
		}
		for _, meas := range measurements {
			// 发现已有 Shard（填充 ShardManager 缓存）
			_ = e.flusher.GetShards(db, meas, 0, 1<<62)

			// 创建 Writer 并重放 WAL
			w, err := e.getOrCreateWriter(db, meas)
			if err != nil {
				continue
			}
			if mw, ok := w.(*writer.MeasurementWriter); ok {
				if err := mw.ReplayWAL(); err != nil {
					slog.Warn("WAL replay failed", "db", db, "meas", meas, "error", err)
				}
			}
		}
	}
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

	e.queryWg.Wait()

	// 同步刷写所有数据
	_ = e.coordinator.FlushAll()

	// 关闭所有 Writer
	_ = e.coordinator.CloseAllWriters()

	// 关闭所有 Shard
	_ = e.flusher.CloseAll()

	// 同步 metadata
	if err := e.metaManager.Sync(); err != nil {
		return fmt.Errorf("sync metadata: %w", err)
	}

	if err := e.metaManager.Close(); err != nil {
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

// Flush 将所有 MeasurementWriter 的 MemTable 数据刷写到 SSTable。
func (e *Engine) Flush() error {
	return e.coordinator.FlushAll()
}

// DataDir 返回引擎的数据目录。
func (e *Engine) DataDir() string {
	return e.cfg.DataDir
}

// SetConfig 运行时更新所有 Shard 的 Compaction 配置。
func (e *Engine) SetConfig(config *compaction.Config) {
	e.flusher.SetConfig(config)
}
