// Package engine 实现微时序数据库的存储引擎。
//
// Engine 是数据库的核心组件，负责协调写入和查询操作。
// 它管理 Shard 的创建和回收，以及元数据的访问。
//
// 架构说明：
//
//	Engine → Flusher → ShardManager → Shards → SSTable
//	Engine → Catalog / SeriesStore / ShardIndex → metadata.Manager
//	Engine → FlushCoordinator(全局WAL+全局MemTable) → unordered
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
	"codeberg.org/micro-ts/mts/internal/storage/downsample"
	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/internal/storage/unordered"
	"codeberg.org/micro-ts/mts/internal/storage/wal"
	"codeberg.org/micro-ts/mts/types"
)

const (
	// maxTimeSentinel 用作最大时间哨兵值，表示无穷远的时间边界。
	maxTimeSentinel = 1 << 62
	// unorderedCompactionInterval 是 unordered → L0 compaction 的定时检查间隔。
	unorderedCompactionInterval = 500 * time.Millisecond
	// defaultMaxWALSegments 是 WAL 默认最大 segment 数量。
	defaultMaxWALSegments = 5
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
	CompressionAlgorithm   types.CompressionAlgorithm
	RetentionPeriod        time.Duration
	RetentionCheckInterval time.Duration
}

// Engine 是微时序数据库的存储引擎。
type Engine struct {
	cfg           *Config
	dataDir       string
	catalog       Catalog
	seriesStore   SeriesStore
	shardIndex    ShardIndex
	flusher       Flusher
	coordinator   *FlushCoordinator
	memTable      *memtable.MemTable // 全局 MemTable
	wal           *wal.WAL           // 全局 WAL
	memTableCfg   *types.MemTableConfig
	metaManager   *metadata.Manager
	shardMgr      *shard.ShardManager
	retentionSvc  *shard.RetentionService
	downsampleSvc *downsample.Service
	queryWg       sync.WaitGroup
	closed        bool
	shutdownMu    sync.Mutex

	// 启动恢复同步：recovery 完成后关闭此 channel，
	// Write/Query 在继续之前等待此信号。
	recoveryDone chan struct{}

	// unordered → L0 compaction
	compactionStopCh chan struct{}
}

// New 创建新的存储引擎实例。
func New(cfg *Config) (*Engine, error) {
	var memTableCfg *types.MemTableConfig
	if cfg.MemTableCfg == nil || cfg.MemTableCfg.FlushMemorySize == 0 {
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

	// 初始化全局 WAL
	walDir := wal.GlobalDir(cfg.DataDir)
	walCfg := wal.Config{
		Dir:          walDir,
		SegmentSize:  64 * 1024 * 1024,
		MaxSegments:  defaultMaxWALSegments,
		SyncMode:     wal.SyncPeriodic,
		SyncInterval: time.Minute,
		Compressed:   true,
	}
	globalWAL, err := wal.Open(walCfg)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	// 初始化全局 MemTable
	globalMT := memtable.NewMemTable(memTableCfg)

	// 初始化 unordered 目录和序列号恢复
	if err := unordered.EnsureDir(cfg.DataDir); err != nil {
		return nil, fmt.Errorf("create unordered dir: %w", err)
	}
	if err := unordered.RecoverSeq(cfg.DataDir); err != nil {
		return nil, fmt.Errorf("recover unordered seq: %w", err)
	}

	// 先构建 engine 结构体（部分字段后补）
	engine := &Engine{
		cfg:         cfg,
		dataDir:     cfg.DataDir,
		catalog:     mgr.Catalog(),
		seriesStore: mgr.Series(),
		shardIndex:  mgr.Shards(),
		flusher:     flusher,
		memTable:    globalMT,
		wal:         globalWAL,
		memTableCfg: memTableCfg,
		metaManager: mgr,
		shardMgr:    flusher,
	}

	// 创建 unordered → L0 compactor（需要在 coordinator 之前，以便 flush 后立即触发 compaction）
	uc := compaction.NewUnorderedCompactor(cfg.DataDir, engine.shardMgr, cfg.CompressionAlgorithm)

	coordinator := NewFlushCoordinator(globalMT, globalWAL, flusher, uc, cfg.DataDir, cfg.CompressionAlgorithm)
	coordinator.StartPeriodicCheck(time.Second)
	engine.coordinator = coordinator

	// 启动数据保留清理服务
	if cfg.RetentionPeriod > 0 {
		checkInterval := cfg.RetentionCheckInterval
		if checkInterval <= 0 {
			checkInterval = time.Hour
		}
		engine.retentionSvc = shard.NewRetentionService(engine.shardMgr, cfg.RetentionPeriod, checkInterval, engine.catalog)
		engine.retentionSvc.Start()
	}

	// 启动降采样服务
	dsAdapter := &downsampleCatalogAdapter{catalog: engine.catalog}
	engine.downsampleSvc = downsample.NewService(cfg.DataDir, dsAdapter, cfg.CompressionAlgorithm)
	engine.downsampleSvc.Start()

	// 启动 unordered → stable/L0 compaction 定时任务（每 500ms）
	engine.compactionStopCh = make(chan struct{})
	go func() {
		ticker := time.NewTicker(unorderedCompactionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = uc.Compact()
			case <-engine.compactionStopCh:
				return
			}
		}
	}()

	// 后台发现已有 measurement 的 Shard 并重放全局 WAL
	engine.recoveryDone = make(chan struct{})
	go func() {
		engine.discoverAndRecover()
		close(engine.recoveryDone)
	}()

	return engine, nil
}

// discoverAndRecover 发现已存在的 measurement 的 Shard，并重放全局 WAL 到全局 MemTable。
func (e *Engine) discoverAndRecover() {
	databases := e.catalog.ListDatabases()
	for _, db := range databases {
		measurements, err := e.catalog.ListMeasurements(db)
		if err != nil {
			continue
		}
		for _, meas := range measurements {
			// 发现已有 Shard（填充 ShardManager 缓存）
			_ = e.flusher.GetShards(db, meas, 0, maxTimeSentinel)
		}
	}

	// 全局 WAL 重放到全局 MemTable
	if err := e.wal.Replay(func(payload []byte) error {
		mp, err := wal.DeserializePoint(payload)
		if err != nil {
			return err
		}
		// 如果 MemTable 接近满，先刷盘
		if e.memTable.NearFull() {
			_ = e.coordinator.FlushAll()
		}
		return e.memTable.Write(mp)
	}); err != nil {
		slog.Warn("global WAL replay failed", "error", err)
	}

	// Replay 完成后排序
	e.memTable.Sort()

	// 删除已 replay 的 WAL 段
	_ = e.wal.TruncateBefore(e.wal.SegmentNum() + 1)
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

	// 停止数据保留清理服务
	if e.retentionSvc != nil {
		e.retentionSvc.Stop()
	}

	// 停止降采样服务
	if e.downsampleSvc != nil {
		e.downsampleSvc.Stop()
	}

	// 同步刷写所有数据
	_ = e.coordinator.FlushAll()

	// 停止 unordered compaction（FlushAll 之后关闭，确保最后的数据被处理）
	if e.compactionStopCh != nil {
		close(e.compactionStopCh)
	}

	// 停止 FlushCoordinator 的周期性检查 goroutine
	e.coordinator.Close()

	// 关闭全局 WAL
	if e.wal != nil {
		_ = e.wal.Close()
	}

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

// Flush 将全局 MemTable 的数据刷写到 SSTable。
func (e *Engine) Flush() error {
	return e.coordinator.FlushAll()
}

// DataDir 返回引擎的数据目录。
func (e *Engine) DataDir() string {
	return e.cfg.DataDir
}

// ForceDownsample 手动触发一次降采样处理。
func (e *Engine) ForceDownsample() {
	if e.downsampleSvc != nil {
		e.downsampleSvc.ForceRun()
	}
}

// SetConfig 运行时更新所有 Shard 的 Compaction 配置。
func (e *Engine) SetConfig(config *compaction.Config) {
	e.flusher.SetConfig(config)
}
