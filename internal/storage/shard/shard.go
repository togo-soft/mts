// Package shard 实现分片存储管理。
//
// Shard 是 SSTable 的容器，负责：
//   - 管理时间窗口内的 SSTable 文件
//   - 提供数据读取（合并 SSTable）
//   - 控制 Compaction
//
// 数据流：
//
//	写入 → Writer (WAL + MemTable)
//	Flush → ShardManager → Shard.WriteSSTable
//	读取 → SSTable 归并排序 → 结果
//
// 核心组件：
//
//	Shard:         SSTable 容器
//	ShardManager: 管理所有 Shard 的创建和获取
//	SSTable:      持久化的列式存储
package shard

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// SeriesStore 是 Shard 所需的 Series 操作接口。
//
// 通过接口解耦，Shard 不直接依赖具体类型。
// 与 metadata.SeriesStore 兼容（通过 Go 隐式接口满足）。
type SeriesStore interface {
	AllocateSID(database, measurement string, tags map[string]string) (uint64, error)
	GetTags(database, measurement string, sid uint64) (map[string]string, bool)
}

// ===================================
// ShardConfig 定义 Shard 的配置。
type ShardConfig struct {
	DB                   string
	Measurement          string
	StartTime            int64
	EndTime              int64
	Dir                  string
	SeriesStore          SeriesStore
	SchemaStore          SchemaStore
	CompactionCfg        *compaction.Config
	LevelCompactionCfg   *compaction.LevelConfig
	CompressionAlgorithm sstable.CompressionAlgorithm
	Logger               *slog.Logger
}

// SchemaStore 是 schema 的存储接口。
type SchemaStore interface {
	GetSchema(db, measurement string) (*metadata.Schema, error)
	SetSchema(db, measurement string, s *metadata.Schema) error
}

// Shard 是数据存储的基本单元，管理一个时间窗口内的所有 SSTable。
//
// 每个 Shard 包含：
//
//   - SSTable: 磁盘数据文件
//
// 生命周期：
//
//	创建 → 写入 SSTable → 读取 → 关闭
//
// 并发安全：
//
//	所有公共方法都是线程安全的，使用读写锁保护。
type Shard struct {
	db              string
	measurement     string
	startTime       int64
	endTime         int64
	dir             string
	compactionWg    sync.WaitGroup // 等待后台 compaction goroutine 完成
	closeOnce       sync.Once      // 防止 Close 重复调用
	closed          atomic.Bool    // 标记 Shard 已关闭
	seriesStore     SeriesStore
	schemaStore     SchemaStore
	mu              sync.RWMutex
	sstSeq          uint64 // SSTable 序列号，用于生成唯一的文件名
	sstRefs         *sstRefs
	compaction      *compaction.Manager
	levelCompaction *compaction.LevelManager
	compressionAlgo sstable.CompressionAlgorithm
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
//  1. 恢复 SSTable 序列号
//  2. 初始化 Compaction
func NewShard(cfg ShardConfig) *Shard {
	shard := &Shard{
		db:              cfg.DB,
		measurement:     cfg.Measurement,
		startTime:       cfg.StartTime,
		endTime:         cfg.EndTime,
		dir:             cfg.Dir,
		seriesStore:     cfg.SeriesStore,
		schemaStore:     cfg.SchemaStore,
		sstRefs:         newSSTRefs(),
		compressionAlgo: cfg.CompressionAlgorithm,
	}

	// 恢复 SSTable 序列号
	shard.sstSeq = recoverSSTSeq(cfg.Dir)

	shard.initCompaction(cfg)

	return shard
}

// initCompaction 初始化 compaction manager。
func (s *Shard) initCompaction(cfg ShardConfig) {
	if cfg.CompactionCfg != nil {
		s.compaction = compaction.NewManager(s, cfg.CompactionCfg)
		s.compaction.StartPeriodicCheck()
	}
	if cfg.LevelCompactionCfg != nil {
		var err error
		s.levelCompaction, err = compaction.NewLevelManager(s, cfg.LevelCompactionCfg)
		if err != nil {
			slog.Warn("failed to create LevelManager, level compaction disabled", "error", err)
			s.levelCompaction = nil
		} else {
			s.levelCompaction.StartPeriodicCheck()
		}
	}
}

// writeSSTableWithTimeout 在独立的 goroutine 中执行 SSTable 写入，并带有 5 秒超时。
// 如果超时，主函数立即返回错误（goroutine 被 abandon，但不会泄漏，因为写入完成后会自然结束）。
// 这种设计确保了即使在 Windows 上 I/O 阻塞，也不会永久卡死调用方。
func (s *Shard) writeSSTableWithTimeout(points []types.MemPoint, seq uint64) (sstPath string, sstSeq uint64, minTime, maxTime int64, err error) {
	type result struct {
		sstPath string
		sstSeq  uint64
		minTime int64
		maxTime int64
		err     error
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resCh := make(chan result, 1)
	go func() {
		sstPath := filepath.Join(s.DataDir(), fmt.Sprintf("sst_%d.bin", seq))

		// MkdirAll can block on Windows (antivirus, filesystem issues)
		if mkdirErr := os.MkdirAll(s.DataDir(), 0700); mkdirErr != nil {
			resCh <- result{"", 0, 0, 0, fmt.Errorf("create data dir: %w", mkdirErr)}
			return
		}

		// 检查上下文是否已取消（避免在 I/O 期间长时间阻塞）
		select {
		case <-ctx.Done():
			resCh <- result{"", 0, 0, 0, fmt.Errorf("write sstable cancelled")}
			return
		default:
		}

		w, wErr := sstable.NewWriter(s.dir, seq, 0, s.compressionAlgo)
		if wErr != nil {
			resCh <- result{"", 0, 0, 0, fmt.Errorf("create sstable writer: %w", wErr)}
			return
		}

		if err := w.WriteMemPoints(points); err != nil {
			_ = w.Close()
			resCh <- result{"", 0, 0, 0, fmt.Errorf("write mempoints: %w", err)}
			return
		}

		// Close triggers fsync and file operations that can block on Windows
		if closeErr := w.Close(); closeErr != nil {
			resCh <- result{"", 0, 0, 0, fmt.Errorf("close sstable writer: %w", closeErr)}
			return
		}

		srcPath := filepath.Join(s.dir, "data", fmt.Sprintf("sst_%d.bin", seq))
		if srcPath != sstPath {
			if renameErr := os.Rename(srcPath, sstPath); renameErr != nil {
				_ = os.Remove(srcPath)
				resCh <- result{"", 0, 0, 0, fmt.Errorf("move sstable: %w", renameErr)}
				return
			}
		}

		minTime, maxTime = calcTimeRange(points)
		resCh <- result{sstPath, seq, minTime, maxTime, nil}
	}()

	select {
	case <-ctx.Done():
		return "", 0, 0, 0, fmt.Errorf("write sstable timeout after 5s")
	case res := <-resCh:
		return res.sstPath, res.sstSeq, res.minTime, res.maxTime, res.err
	}
}

// WriteSSTable 将 MemPoint 写入 SSTable 文件。
func (s *Shard) WriteSSTable(points []types.MemPoint) (sstPath string, sstSeq uint64, minTime, maxTime int64, err error) {
	s.mu.Lock()
	sstSeq = s.sstSeq
	s.sstSeq++
	s.mu.Unlock()

	return s.writeSSTableWithTimeout(points, sstSeq)
}

// calcTimeRange 计算 points 的时间范围。
func calcTimeRange(points []types.MemPoint) (int64, int64) {
	var minTime, maxTime int64
	for i, p := range points {
		if i == 0 || p.Timestamp < minTime {
			minTime = p.Timestamp
		}
		if i == 0 || p.Timestamp > maxTime {
			maxTime = p.Timestamp
		}
	}
	return minTime, maxTime
}

// RegisterSSTable 注册新写入的 SSTable 到 Shard 的 compaction 系统。
// 用于 ShardManager flush 后注册 SSTable。
func (s *Shard) RegisterSSTable(sstSeq uint64, minTime, maxTime int64, size int64) {
	if s.levelCompaction != nil {
		// 将 SSTable 从 data/ 移动到 data/L0/，使 compaction 能通过 level 路径找到它
		sstName := fmt.Sprintf("sst_%d.bin", sstSeq)
		srcPath := filepath.Join(s.dir, "data", sstName)
		dstDir := filepath.Join(s.dir, "data", "L0")
		dstPath := filepath.Join(dstDir, sstName)
		if srcPath != dstPath {
			if mkErr := os.MkdirAll(dstDir, 0700); mkErr == nil {
				if _, err := os.Stat(srcPath); err == nil {
					_ = os.Rename(srcPath, dstPath)
				}
			}
		}

		s.levelCompaction.AddPart(0, compaction.PartInfo{
			Name:    fmt.Sprintf("sst_%d", sstSeq),
			Size:    size,
			MinTime: minTime,
			MaxTime: maxTime,
		})
	}
}

// TriggerCompaction 在后台触发 compaction，成功后会级联检查是否仍有文件需要合并。
func (s *Shard) TriggerCompaction() {
	if s.closed.Load() {
		return
	}

	if s.levelCompaction != nil && s.levelCompaction.ShouldCompact() {
		s.compactionWg.Go(func() {
			if s.closed.Load() {
				return
			}
			ctx, cancel := context.WithTimeout(s.levelCompaction.Context(), s.levelCompaction.Timeout())
			defer cancel()
			_, _, err := s.levelCompaction.Compact(ctx)
			if err != nil {
				if !s.closed.Load() {
					slog.Error("background level compaction failed", "error", err)
				}
				return
			}
			// 级联：如果仍有文件需要合并，立即触发下一轮
			if !s.closed.Load() && s.levelCompaction.ShouldCompact() {
				s.TriggerCompaction()
			}
		})
	} else if s.compaction != nil && s.compaction.ShouldCompactWithLock() {
		s.compactionWg.Go(func() {
			if s.closed.Load() {
				return
			}
			ctx, cancel := context.WithTimeout(s.compaction.Context(), s.compaction.Timeout())
			defer cancel()
			_, _, err := s.compaction.Compact(ctx)
			if err != nil {
				if !s.closed.Load() {
					slog.Error("background compaction failed", "error", err)
				}
				return
			}
			s.compaction.ResetTimer()

			// 级联：如果仍有文件需要合并，立即触发下一轮
			if !s.closed.Load() && s.compaction.ShouldCompactWithLock() {
				s.TriggerCompaction()
			}
		})
	}
}

// recoverSSTSeq 扫描数据目录，恢复 SSTable 序列号。
// 返回 max(sst_N 中的 N) + 1，确保新创建的 SSTable 不会覆盖已有数据。
// 识别单文件格式 .bin 文件，同时兼容旧格式 sst_*/ 目录。
// 如果数据目录不存在或没有 SSTable，返回 0。
func recoverSSTSeq(shardDir string) uint64 {
	dataDir := filepath.Join(shardDir, "data")
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return 0
	}

	var maxSeq uint64
	found := false
	for _, entry := range entries {
		name := entry.Name()

		// 新格式: sst_N.bin 文件
		if !entry.IsDir() && strings.HasPrefix(name, "sst_") && strings.HasSuffix(name, ".bin") {
			inner := strings.TrimPrefix(name, "sst_")
			inner = strings.TrimSuffix(inner, ".bin")
			seq, err := strconv.ParseUint(inner, 10, 64)
			if err != nil {
				continue
			}
			if seq >= maxSeq {
				maxSeq = seq
				found = true
			}
			continue
		}

		// 旧格式兼容: sst_N/ 目录
		if entry.IsDir() && strings.HasPrefix(name, "sst_") {
			seq, err := strconv.ParseUint(strings.TrimPrefix(name, "sst_"), 10, 64)
			if err != nil {
				continue
			}
			if seq >= maxSeq {
				maxSeq = seq
				found = true
			}
		}
	}
	if !found {
		return 0
	}
	return maxSeq + 1
}

// StartTime 返回 Shard 时间窗口的起始时间。
func (s *Shard) StartTime() int64 {
	return s.startTime
}

// EndTime 返回 Shard 时间窗口的结束时间。
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

// CompressionAlgorithm 返回配置的 SSTable 块压缩算法。
func (s *Shard) CompressionAlgorithm() sstable.CompressionAlgorithm {
	return s.compressionAlgo
}

// GetSchema 返回 Shard 的 schema（sstable.Schema 格式）。
func (s *Shard) GetSchema() (sstable.Schema, error) {
	if s.schemaStore == nil {
		return sstable.Schema{}, fmt.Errorf("schema store not available")
	}
	metaSchema, err := s.schemaStore.GetSchema(s.db, s.measurement)
	if err != nil {
		return sstable.Schema{}, err
	}
	return MetadataSchemaToSSTableSchema(metaSchema), nil
}

// MetadataSchemaToSSTableSchema 将 metadata.Schema 转换为 sstable.Schema。
func MetadataSchemaToSSTableSchema(metaSchema *metadata.Schema) sstable.Schema {
	fields := make(map[string]sstable.FieldType)
	if metaSchema != nil {
		for _, f := range metaSchema.Fields {
			fields[f.Name] = MetadataFieldTypeToSSTableFieldType(f.Type)
		}
	}
	return sstable.Schema{Fields: fields}
}

// MetadataFieldTypeToSSTableFieldType 将 metadata 字段类型转换为 sstable 字段类型。
//
// 类型映射：
//   - 1: float64
//   - 2: int64
//   - 3: string
//   - 4: bool
func MetadataFieldTypeToSSTableFieldType(t int32) sstable.FieldType {
	switch t {
	case 1:
		return sstable.FieldTypeFloat64
	case 2:
		return sstable.FieldTypeInt64
	case 3:
		return sstable.FieldTypeString
	case 4:
		return sstable.FieldTypeBool
	default:
		return sstable.FieldTypeFloat64
	}
}

// ContainsTime 检查给定时间戳是否在 Shard 的时间窗口内。
func (s *Shard) ContainsTime(ts int64) bool {
	return ts >= s.startTime && ts < s.endTime
}

// Duration 返回 Shard 时间窗口的持续时间。
func (s *Shard) Duration() time.Duration {
	return time.Duration(s.endTime - s.startTime)
}

// SSTSeq 返回当前 SSTable 序列号（不递增，公开给 ShardStore）。
func (s *Shard) SSTSeq() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sstSeq
}
