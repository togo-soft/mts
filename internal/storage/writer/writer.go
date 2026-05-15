// Package writer 实现 measurement 级别的写入器。
//
// MeasurementWriter 是全局唯一的写入入口，接收一个 measurement 的所有写入，
// 管理全局 MemTable 和 WAL。Flush 时按时间窗口分组写入对应 Shard 的 SSTable。
//
// 架构：
//
//	写入 → WAL → MemTable → Flush(按 Shard 分组) → Shard SSTables
//	查询 → MemTable + Shard SSTables → 归并排序
package writer

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/internal/metrics"
	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/internal/storage/wal"
	"codeberg.org/micro-ts/mts/types"
)

// SeriesStore 是 Writer 所需的 Series 操作接口。
type SeriesStore interface {
	AllocateSID(tags map[string]string) (uint64, error)
	GetTagsBySID(sid uint64) (map[string]string, bool)
}

// SchemaStore 是 schema 的存储接口。
type SchemaStore interface {
	GetSchema(db, measurement string) (*metadata.Schema, error)
	SetSchema(db, measurement string, s *metadata.Schema) error
}

// ShardStore 是 Writer 在 flush 时访问 Shard 的接口。
// Shard 在此架构中退化为纯磁盘分区，只管理 SSTable 文件。
type ShardStore interface {
	// GetOrCreateShard 获取或创建指定时间窗口的 Shard（纯磁盘，无 WAL/MemTable）。
	GetOrCreateShard(db, measurement string, startTime int64) (*ShardInfo, error)
	// NextSSTSeq 获取 shard 的下一个 SSTable 序列号。
	NextSSTSeq(info *ShardInfo) uint64
	// RegisterSSTable 在 shard 中注册新写入的 SSTable。
	RegisterSSTable(info *ShardInfo, sstSeq uint64, path string, minTime, maxTime int64, size int64)
	// TriggerCompaction 触发指定 shard 的后台 compaction。
	TriggerCompaction(info *ShardInfo)
}

// ShardInfo 是 Writer 可见的 Shard 元数据。
type ShardInfo struct {
	StartTime int64
	EndTime   int64
	Dir       string
	DataDir   string

	// Internal 是 ShardStore 实现者使用的内部引用。
	Internal any
}

// Config 定义 MeasurementWriter 的配置。
type Config struct {
	DB                   string
	Measurement          string
	Dir                  string // measurement 数据根目录
	ShardDuration        int64
	SeriesStore          SeriesStore
	SchemaStore          SchemaStore
	ShardStore           ShardStore
	MemTableCfg          *memtable.MemTableConfig
	CompactionCfg        *compaction.Config
	LevelCompactionCfg   *compaction.LevelConfig
	CompressionAlgorithm sstable.CompressionAlgorithm
	Logger               *slog.Logger
}

// MeasurementWriter 是单个 measurement 的写入入口。
//
// 所有该 measurement 的数据先写入全局 WAL + MemTable，
// flush 时按时间窗口分组写入对应 Shard 的 SSTable。
type MeasurementWriter struct {
	db          string
	measurement string
	dir         string
	shardDur    int64

	memTable    *memtable.MemTable
	wal         *wal.WAL
	seriesStore SeriesStore
	schemaStore SchemaStore
	shardStore  ShardStore
	schema      *metadata.Schema
	schemaMu    sync.RWMutex

	compactionCfg        *compaction.Config
	levelCompactionCfg   *compaction.LevelConfig
	compressionAlgorithm sstable.CompressionAlgorithm

	mu          sync.Mutex
	flushDone   chan struct{}
	flushTicker *time.Ticker
	flushWg     sync.WaitGroup

	closed    atomic.Bool
	closeOnce sync.Once
}

const backpressureSleep = time.Millisecond

// Write 写入单个数据点。
func (mw *MeasurementWriter) Write(point *types.Point) error {
	for mw.memTable.ActiveFull() {
		if !mw.memTable.IsFlushing() {
			mw.tryTriggerAsyncFlush()
		}
		time.Sleep(backpressureSleep)
		if mw.closed.Load() {
			return fmt.Errorf("writer closed during backpressure wait")
		}
	}

	mw.mu.Lock()

	sid, err := mw.seriesStore.AllocateSID(point.Tags)
	if err != nil {
		mw.mu.Unlock()
		return fmt.Errorf("allocate SID: %w", err)
	}

	if err := mw.validateFieldTypes(point); err != nil {
		metrics.Incr(metrics.WriteErrors, 1)
		mw.mu.Unlock()
		return fmt.Errorf("validate field types: %w", err)
	}

	mp := types.PointToMemPoint(point, sid)

	if mw.wal != nil {
		data, release := serializePointForWALPooled(mp.Timestamp, mp.Sid, mp.FieldData)
		_, err := mw.wal.Write(data)
		release()
		if err != nil {
			metrics.Incr(metrics.WriteErrors, 1)
			mw.mu.Unlock()
			return fmt.Errorf("write to wal: %w", err)
		}
	}

	if err := mw.memTable.Write(mp); err != nil {
		metrics.Incr(metrics.WriteErrors, 1)
		mw.mu.Unlock()
		return fmt.Errorf("write to memtable: %w", err)
	}

	metrics.Incr(metrics.WriteTotal, 1)

	shouldFlush := mw.memTable.ShouldSwap()
	mw.mu.Unlock()

	if shouldFlush {
		mw.tryTriggerAsyncFlush()
	}

	return nil
}

// WriteBatch 批量写入数据点，使用单次锁获取 + 单次 WAL 批量写入。
func (mw *MeasurementWriter) WriteBatch(points []*types.Point) (int, error) {
	if len(points) == 0 {
		return 0, nil
	}

	for mw.memTable.ActiveFull() {
		if !mw.memTable.IsFlushing() {
			mw.tryTriggerAsyncFlush()
		}
		time.Sleep(backpressureSleep)
		if mw.closed.Load() {
			return 0, fmt.Errorf("writer closed during backpressure wait")
		}
	}

	mw.mu.Lock()

	if mw.memTable.ActiveFull() {
		mw.mu.Unlock()
		return mw.WriteBatch(points)
	}

	mps := make([]types.MemPoint, 0, len(points))
	walData := make([][]byte, 0, len(points))
	walReleases := make([]func(), 0, len(points))

	for i, point := range points {
		sid, err := mw.seriesStore.AllocateSID(point.Tags)
		if err != nil {
			metrics.Incr(metrics.WriteErrors, 1)
			mw.mu.Unlock()
			for _, r := range walReleases {
				r()
			}
			return i, fmt.Errorf("allocate SID for point %d: %w", i, err)
		}
		if err := mw.validateFieldTypes(point); err != nil {
			mw.mu.Unlock()
			for _, r := range walReleases {
				r()
			}
			return i, fmt.Errorf("validate field types for point %d: %w", i, err)
		}
		mp := types.PointToMemPoint(point, sid)
		mps = append(mps, mp)

		if mw.wal != nil {
			data, release := serializePointForWALPooled(mp.Timestamp, mp.Sid, mp.FieldData)
			walData = append(walData, data)
			walReleases = append(walReleases, release)
		}
	}

	if mw.wal != nil && len(walData) > 0 {
		var batchErr error
		_, batchErr = mw.wal.WriteBatch(walData)
		for _, r := range walReleases {
			r()
		}
		if batchErr != nil {
			metrics.Incr(metrics.WriteErrors, 1)
			mw.mu.Unlock()
			return 0, fmt.Errorf("wal write batch: %w", batchErr)
		}
	}

	for i, mp := range mps {
		if err := mw.memTable.Write(mp); err != nil {
			metrics.Incr(metrics.WriteErrors, 1)
			mw.mu.Unlock()
			return i, fmt.Errorf("write to memtable at %d: %w", i, err)
		}
	}

	metrics.Incr(metrics.WriteBatchTotal, 1)
	metrics.Incr(metrics.WriteTotal, int64(len(mps)))

	mw.mu.Unlock()

	if mw.memTable.ShouldSwap() {
		mw.tryTriggerAsyncFlush()
	}

	return len(mps), nil
}

// validateFieldTypes 验证 point 的字段类型与当前 schema 一致。
func (mw *MeasurementWriter) validateFieldTypes(point *types.Point) error {
	mw.schemaMu.Lock()
	defer mw.schemaMu.Unlock()

	if mw.schema == nil {
		mw.schema = &metadata.Schema{
			Version:   1,
			Fields:    make([]metadata.FieldDef, 0),
			TagKeys:   nil,
			UpdatedAt: time.Now().UnixNano(),
		}
	}

	for name, fieldValue := range point.Fields {
		if fieldValue == nil || fieldValue.Value == nil {
			continue
		}

		newType := detectFieldType(fieldValue)
		existingIdx := -1
		for i, f := range mw.schema.Fields {
			if f.Name == name {
				existingIdx = i
				break
			}
		}

		if existingIdx == -1 {
			mw.schema.Fields = append(mw.schema.Fields, metadata.FieldDef{
				Name: name,
				Type: sstableFieldTypeToMetadataType(newType),
			})
			continue
		}

		existingType := metadataFieldTypeToSSTableType(mw.schema.Fields[existingIdx].Type)
		if newType != existingType {
			return fmt.Errorf("field type mismatch: field %q has type %s, cannot accept %s",
				name, existingType, newType)
		}
	}

	return nil
}

// MemTable 返回内部 MemTable（供查询使用）。
func (mw *MeasurementWriter) MemTable() *memtable.MemTable {
	return mw.memTable
}

// SeriesStore 返回内部 SeriesStore（供查询时解析 SID → Tags）。
func (mw *MeasurementWriter) SeriesStore() SeriesStore {
	return mw.seriesStore
}

// Dir 返回 measurement 数据根目录。
func (mw *MeasurementWriter) Dir() string {
	return mw.dir
}

// detectFieldType 从 FieldValue 检测字段类型。
func detectFieldType(fv *types.FieldValue) sstable.FieldType {
	if fv == nil || fv.Value == nil {
		return sstable.FieldTypeFloat64
	}
	switch fv.Value.(type) {
	case *types.FieldValue_FloatValue:
		return sstable.FieldTypeFloat64
	case *types.FieldValue_IntValue:
		return sstable.FieldTypeInt64
	case *types.FieldValue_StringValue:
		return sstable.FieldTypeString
	case *types.FieldValue_BoolValue:
		return sstable.FieldTypeBool
	default:
		return sstable.FieldTypeFloat64
	}
}

func metadataFieldTypeToSSTableType(t int32) sstable.FieldType {
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

func sstableFieldTypeToMetadataType(t sstable.FieldType) int32 {
	switch t {
	case sstable.FieldTypeFloat64:
		return 1
	case sstable.FieldTypeInt64:
		return 2
	case sstable.FieldTypeString:
		return 3
	case sstable.FieldTypeBool:
		return 4
	default:
		return 1
	}
}

// sstableSchemaToMetaSchema 将 sstable.Schema 转换为 metadata.Schema。
func sstableSchemaToMetaSchema(sstSchema sstable.Schema) *metadata.Schema {
	fields := make([]metadata.FieldDef, 0, len(sstSchema.Fields))
	for name, fieldType := range sstSchema.Fields {
		fields = append(fields, metadata.FieldDef{
			Name: name,
			Type: sstableFieldTypeToMetadataType(fieldType),
		})
	}
	return &metadata.Schema{
		Version:   1,
		Fields:    fields,
		TagKeys:   nil,
		UpdatedAt: time.Now().UnixNano(),
	}
}

// New 创建新的 MeasurementWriter。
func New(cfg Config) (*MeasurementWriter, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	walDir := filepath.Join(cfg.Dir, "wal")
	w, err := wal.Open(wal.Config{
		Dir:          walDir,
		SegmentSize:  64 * 1024 * 1024,
		MaxSegments:  5,
		SyncMode:     wal.SyncPeriodic,
		SyncInterval: time.Minute,
		Logger:       logger,
	})
	if err != nil {
		w = nil
		logger.Warn("failed to open WAL, writes will not be durable",
			"walDir", walDir, "error", err)
	}

	mt := memtable.NewMemTable(cfg.MemTableCfg)

	mw := &MeasurementWriter{
		db:                   cfg.DB,
		measurement:          cfg.Measurement,
		dir:                  cfg.Dir,
		shardDur:             cfg.ShardDuration,
		memTable:             mt,
		wal:                  w,
		seriesStore:          cfg.SeriesStore,
		schemaStore:          cfg.SchemaStore,
		shardStore:           cfg.ShardStore,
		compactionCfg:        cfg.CompactionCfg,
		levelCompactionCfg:   cfg.LevelCompactionCfg,
		compressionAlgorithm: cfg.CompressionAlgorithm,
		flushDone:            make(chan struct{}),
	}

	if mw.schemaStore != nil {
		if metaSchema, err := mw.schemaStore.GetSchema(cfg.DB, cfg.Measurement); err == nil {
			mw.schema = metaSchema
		}
	}

	mw.startPeriodicFlushCheck()

	return mw, nil
}
