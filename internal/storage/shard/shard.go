// Package shard 实现分片存储管理。
//
// Shard 是数据管理的基本单元，负责：
//   - 管理时间窗口内的数据写入（WAL + MemTable）
//   - 提供数据读取（合并 MemTable 和 SSTable）
//   - 控制 MemTable 刷盘到 SSTable
//
// 数据流：
//
//	写入 → WAL → MemTable → SSTable
//	读取 → SSTable + MemTable → 归并排序 → 结果
//
// 核心组件：
//
//	Shard:          分片数据容器
//	ShardManager:   管理所有 Shard 的创建和获取
//	MemTable:       内存写入缓冲区
//	WAL:            预写日志，保证持久化
//	SSTable:        持久化的列式存储
package shard

import (
	"log/slog"
	"path/filepath"
	"sync"
	"time"
)

// SeriesStore 是 Shard 所需的 Series 操作接口。
//
// 通过接口解耦，Shard 不直接依赖 measurement.MeasurementMetaStore 具体类型。
type SeriesStore interface {
	AllocateSID(tags map[string]string) (uint64, error)
	GetTagsBySID(sid uint64) (map[string]string, bool)
}

// ===================================
// ShardConfig 定义 Shard 的配置。
//
// 字段说明：
//
//   - DB:          所属数据库名称
//   - Measurement: 所属 Measurement 名称
//   - StartTime:   Shard 时间窗口起始（包含），纳秒
//   - EndTime:     Shard 时间窗口结束（不包含），纳秒
//   - Dir:         数据存储目录路径
//   - SeriesStore: Series 存储接口，用于分配 SID 和查询 Tags
//   - MemTableCfg: MemTable 配置
//   - CompactionCfg: Compaction 配置（可选，nil 表示禁用 compaction）
//   - LevelCompactionCfg: Level Compaction 配置（可选，nil 表示使用平坦 compaction）
//   - Logger:      日志记录器（nil 使用 slog.Default()）
type ShardConfig struct {
	DB                 string
	Measurement        string
	StartTime          int64
	EndTime            int64
	Dir                string
	SeriesStore        SeriesStore
	MemTableCfg        *MemTableConfig
	CompactionCfg      *CompactionConfig
	LevelCompactionCfg *LevelCompactionConfig
	Logger             *slog.Logger
}

// Shard 是数据存储的基本单元，管理一个时间窗口内的所有数据。
//
// 每个 Shard 包含：
//
//   - MemTable: 内存写入缓冲区（活跃数据）
//   - WAL:      预写日志（持久化恢复）
//   - SSTable:  磁盘数据文件（已刷盘数据）
//
// 生命周期：
//
//	创建 → 写入 → 刷盘 → 读取 → 关闭
//
// 并发安全：
//
//	所有公共方法都是线程安全的，使用读写锁保护。
//	读操作可以并发，写操作会阻塞其他写。
//
// 字段说明：
//
//   - db, measurement: 标识信息
//   - startTime, endTime: 时间窗口边界
//   - dir: 数据存储目录
//   - memTable: 内存表
//   - wal: 预写日志
//   - seriesStore: Series ID 分配与查询
//   - sidCache: Sid→Tags 缓存（用于从 SSTable 恢复 Tags）
//   - tsSidMap: Timestamp→Sid 映射（用于 flush 时获取 Sid）
//   - mu: 读写锁
//   - sstSeq: SSTable 序列号（文件名生成）
//   - compaction: Compaction 管理器
//   - levelCompaction: Level Compaction 管理器（可选）
type Shard struct {
	db              string
	measurement     string
	startTime       int64
	endTime         int64
	dir             string
	memTable        *MemTable
	wal             *WAL
	walDone         chan struct{} // WAL 定期同步停止信号
	seriesStore     SeriesStore
	sidCache        map[uint64]map[string]string // sid → tags 缓存
	tsSidMap        map[int64]uint64             // timestamp → sid 映射
	mu              sync.RWMutex
	sstSeq          uint64 // SSTable序列号，用于生成唯一的文件名
	sstRefs         *sstRefs
	compaction      *CompactionManager
	levelCompaction *LevelCompactionManager
}

// NewShard 创建新的 Shard 实例。
//
// 参数：
//   - cfg: Shard 配置
//
// 返回：
//   - *Shard: 初始化后的 Shard
//
// 初始化过程：
//
//  1. 创建 WAL（如果失败，wal 设为 nil，继续运行）
//  2. 从 WAL 恢复数据到 MemTable（如果 WAL 存在）
//  3. 创建空的 MemTable
//
// 错误处理：
//
//	WAL 创建失败不会阻止 Shard 创建，数据可能仅保存在内存中。
//	这种情况下，系统重启后 MemTable 数据会丢失。
//
//	WAL 恢复失败会记录日志，但继续启动 Shard（可能丢失部分数据）。
func NewShard(cfg ShardConfig) *Shard {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// 创建 WAL
	walDir := filepath.Join(cfg.Dir, "wal")
	wal, err := NewWALWithLogger(walDir, 0, logger)
	if err != nil {
		// 如果 WAL 创建失败，使用 nil wal
		wal = nil
		logger.Warn("failed to create WAL, writes will not be durable",
			"walDir", walDir,
			"error", err)
	}

	// 创建空的 MemTable
	memTable := NewMemTable(cfg.MemTableCfg)

	// 从 WAL 恢复数据到 MemTable
	if wal != nil {
		points, err := ReplayWAL(walDir)
		if err != nil {
			logger.Error("failed to replay WAL, some data may be lost",
				"walDir", walDir,
				"error", err)
		} else {
			for _, p := range points {
				if writeErr := memTable.Write(p); writeErr != nil {
					logger.Error("failed to replay WAL point, skipping",
						"walDir", walDir,
						"timestamp", p.Timestamp,
						"error", writeErr)
				}
			}
			if len(points) > 0 {
				logger.Info("replayed WAL data into MemTable",
					"walDir", walDir,
					"pointCount", len(points))
			}
		}
	}

	// 创建 Shard 实例
	shard := &Shard{
		db:          cfg.DB,
		measurement: cfg.Measurement,
		startTime:   cfg.StartTime,
		endTime:     cfg.EndTime,
		dir:         cfg.Dir,
		memTable:    memTable,
		wal:         wal,
		walDone:     make(chan struct{}),
		seriesStore: cfg.SeriesStore,
		sidCache:    make(map[uint64]map[string]string),
		tsSidMap:    make(map[int64]uint64),
		sstRefs:     newSSTRefs(),
	}

	// 初始化 CompactionManager（如果配置了）
	if cfg.CompactionCfg != nil {
		shard.compaction = NewCompactionManager(shard, cfg.CompactionCfg)
	}

	// 初始化 LevelCompactionManager（如果配置了）
	if cfg.LevelCompactionCfg != nil {
		shard.levelCompaction, err = NewLevelCompactionManager(shard, cfg.LevelCompactionCfg)
		if err != nil {
			slog.Warn("failed to create LevelCompactionManager, level compaction disabled",
				"error", err)
			shard.levelCompaction = nil
		}
	}

	// 启动 WAL 定期同步（如果 WAL 存在）
	if wal != nil {
		go wal.StartPeriodicSync(time.Minute, shard.walDone)
	}

	// 启动定期 Compaction 检查（如果启用了）
	if shard.compaction != nil {
		shard.compaction.StartPeriodicCheck()
	}

	// 启动定期 Level Compaction 检查（如果启用了）
	if shard.levelCompaction != nil {
		shard.levelCompaction.StartPeriodicCheck()
	}

	return shard
}

// StartTime 返回 Shard 时间窗口的起始时间。
//
// 返回：
//   - int64: 起始时间戳（纳秒，包含）
func (s *Shard) StartTime() int64 {
	return s.startTime
}

// EndTime 返回 Shard 时间窗口的结束时间。
//
// 返回：
//   - int64: 结束时间戳（纳秒，不包含）
func (s *Shard) EndTime() int64 {
	return s.endTime
}

// DB 返回 Shard 所属的数据库名称。
func (s *Shard) DB() string {
	return s.db
}

// Measurement 返回 Shard 所属的 Measurement 名称。
func (s *Shard) Measurement() string {
	return s.measurement
}

// Dir 返回 Shard 的数据目录。
func (s *Shard) Dir() string {
	return s.dir
}

// ContainsTime 检查给定时间戳是否在 Shard 的时间窗口内。
//
// 参数：
//   - ts: 时间戳（纳秒）
//
// 返回：
//   - bool: true 表示在范围内（startTime <= ts < endTime）
func (s *Shard) ContainsTime(ts int64) bool {
	return ts >= s.startTime && ts < s.endTime
}

// Duration 返回 Shard 时间窗口的持续时间。
//
// 返回：
//   - time.Duration: 时间窗口长度
func (s *Shard) Duration() time.Duration {
	return time.Duration(s.endTime - s.startTime)
}
