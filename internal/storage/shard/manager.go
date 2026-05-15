// Package shard 实现分片存储管理。
//
// ShardManager 是核心协调组件，管理所有 Shard 的生命周期。
package shard

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/internal/storage/writer"
)

// ShardManager 管理所有 Shard 的生命周期。
type ShardManager struct {
	dir                    string
	shardDuration          time.Duration
	memTableCfg            *memtable.MemTableConfig
	compactionCfg          *compaction.Config
	compressionAlgo        sstable.CompressionAlgorithm
	manager                *metadata.Manager
	shards                 map[string]*Shard
	writers                map[string]*writerEntry
	discoveredMeasurements map[string]bool
	mu                     sync.RWMutex
	discoveryDone          chan struct{}
	discoveryWg            sync.WaitGroup
}

// NewShardManager 创建新的 Shard 管理器。
func NewShardManager(dir string, shardDuration time.Duration, memTableCfg *memtable.MemTableConfig, compactionCfg *compaction.Config, mgr *metadata.Manager, compressionAlgo sstable.CompressionAlgorithm) *ShardManager {
	sm := &ShardManager{
		dir:                    dir,
		shardDuration:          shardDuration,
		memTableCfg:            memTableCfg,
		compactionCfg:          compactionCfg,
		compressionAlgo:        compressionAlgo,
		manager:                mgr,
		shards:                 make(map[string]*Shard),
		discoveredMeasurements: make(map[string]bool),
		discoveryDone:          make(chan struct{}),
	}

	// 后台触发主动发现，不阻塞构造函数
	sm.discoveryWg.Go(func() {
		if err := sm.discoverAndReplayWAL(); err != nil {
			slog.Warn("failed to discover and replay WAL", "error", err)
		}
		close(sm.discoveryDone)
	})

	return sm
}

// GetShard 获取或创建指定时间戳对应的 Shard。
func (m *ShardManager) GetShard(db, measurementName string, timestamp int64) (*Shard, error) {
	// 防止路径遍历注入
	if !isNameSafe(db) || !isNameSafe(measurementName) {
		return nil, fmt.Errorf("invalid database or measurement name")
	}

	// 等待 discovery 完成，避免在 discovery 完成前创建重复的 Shard
	m.discoveryWg.Wait()

	startTime := m.calcShardStart(timestamp)
	endTime := startTime + int64(m.shardDuration)

	key := m.makeKey(db, measurementName, startTime)

	m.mu.RLock()
	s, ok := m.shards[key]
	m.mu.RUnlock()

	if ok {
		return s, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok = m.shards[key]; ok {
		return s, nil
	}

	// 通过 Manager 获取或创建 SeriesStore
	seriesStore := m.manager.GetOrCreateSeriesStore(db, measurementName)

	shardDir := filepath.Join(m.dir, db, measurementName, formatTimeRange(startTime, endTime))
	s = NewShard(ShardConfig{
		DB:                   db,
		Measurement:          measurementName,
		StartTime:            startTime,
		EndTime:              endTime,
		Dir:                  shardDir,
		SeriesStore:          seriesStore,
		SchemaStore:          m.manager.Catalog(),
		MemTableCfg:          m.memTableCfg,
		CompactionCfg:        m.compactionCfg,
		CompressionAlgorithm: m.compressionAlgo,
	})
	if err := s.ReplayWAL(); err != nil {
		slog.Warn("failed to replay WAL for new shard", "key", key, "error", err)
	}
	m.shards[key] = s

	// 注册到 shardIndex
	if err := m.manager.Shards().RegisterShard(db, measurementName, metadata.ShardInfo{
		ID:        key,
		StartTime: startTime,
		EndTime:   endTime,
		DataDir:   shardDir,
	}); err != nil {
		slog.Warn("failed to register shard", "key", key, "error", err)
	}

	return s, nil
}

// GetShards 获取与指定时间范围相交的所有 Shard。
func (m *ShardManager) GetShards(db, measurementName string, startTime, endTime int64) []*Shard {
	if !isNameSafe(db) || !isNameSafe(measurementName) {
		return nil
	}
	m.mu.RLock()
	alreadyDiscovered := m.discoveredMeasurements[db+"/"+measurementName]
	m.mu.RUnlock()

	if !alreadyDiscovered {
		m.discoverShardsLocked(db, measurementName)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*Shard

	shardDuration := int64(m.shardDuration)
	shardStart := (startTime / shardDuration) * shardDuration

	for ts := shardStart; ts < endTime; ts += shardDuration {
		key := m.makeKey(db, measurementName, ts)
		if s, ok := m.shards[key]; ok {
			result = append(result, s)
		}
	}

	return result
}

func (m *ShardManager) discoverShardsLocked(db, measurementName string) {
	if !isNameSafe(db) || !isNameSafe(measurementName) {
		return
	}
	metaKey := db + "/" + measurementName

	m.mu.Lock()
	m.discoveredMeasurements[metaKey] = true
	m.mu.Unlock()

	measurementDir := filepath.Join(m.dir, db, measurementName)
	entries, err := os.ReadDir(measurementDir)
	if err != nil {
		return
	}

	// 通过 Manager 获取或创建 SeriesStore
	seriesStore := m.manager.GetOrCreateSeriesStore(db, measurementName)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		parts := strings.Split(entry.Name(), "_")
		if len(parts) != 2 {
			continue
		}

		startTime, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			continue
		}
		endTime, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}

		key := m.makeKey(db, measurementName, startTime)

		if _, ok := m.shards[key]; ok {
			continue
		}

		shardDir := filepath.Join(measurementDir, entry.Name())
		shard := NewShard(ShardConfig{
			DB:                   db,
			Measurement:          measurementName,
			StartTime:            startTime,
			EndTime:              endTime,
			Dir:                  shardDir,
			SeriesStore:          seriesStore,
			MemTableCfg:          m.memTableCfg,
			CompactionCfg:        m.compactionCfg,
			CompressionAlgorithm: m.compressionAlgo,
		})
		if err := shard.ReplayWAL(); err != nil {
			slog.Warn("failed to replay WAL for discovered shard", "key", key, "error", err)
		}
		m.shards[key] = shard
	}
}

func (m *ShardManager) discoverAndReplayWAL() error {
	databases := m.manager.ListAllDatabases()
	for _, db := range databases {
		measurements, err := m.manager.ListMeasurements(db)
		if err != nil {
			slog.Warn("failed to list measurements", "db", db, "error", err)
		}
		for _, meas := range measurements {
			metaKey := db + "/" + meas

			// 1. 重放 measurement 级别 WAL（新架构 Writer）
			m.replayWriterWAL(db, meas)

			// 2. 查询 shardIndex 获取已注册 shard（旧架构 shard 级别 WAL）
			shards := m.manager.Shards().ListShards(db, meas)
			for _, info := range shards {
				s := m.loadShardFromIndex(db, meas, info)
				if s != nil {
					key := m.makeKey(db, meas, info.StartTime)
					m.mu.Lock()
					if _, ok := m.shards[key]; ok {
						m.mu.Unlock()
						continue
					}
					if err := s.ReplayWAL(); err != nil {
						slog.Warn("failed to replay WAL for discovered shard", "key", info.ID, "error", err)
					}
					m.shards[key] = s
					m.mu.Unlock()
				}
			}
			m.mu.Lock()
			m.discoveredMeasurements[metaKey] = true
			m.mu.Unlock()
		}
	}
	return nil
}

// replayWriterWAL 重放 measurement 级别的 WAL（新架构）。
func (m *ShardManager) replayWriterWAL(db, meas string) {
	measDir := filepath.Join(m.dir, db, meas)
	walDir := filepath.Join(measDir, "wal")
	if _, err := os.Stat(walDir); os.IsNotExist(err) {
		return
	}

	w, err := m.GetWriter(db, meas)
	if err != nil {
		slog.Warn("failed to create writer for WAL replay", "db", db, "meas", meas, "error", err)
		return
	}

	if err := w.ReplayWAL(); err != nil {
		slog.Warn("failed to replay measurement WAL", "db", db, "meas", meas, "error", err)
	}
}

func (m *ShardManager) loadShardFromIndex(db, measurement string, info metadata.ShardInfo) *Shard {
	seriesStore := m.manager.GetOrCreateSeriesStore(db, measurement)
	return NewShard(ShardConfig{
		DB:                   db,
		Measurement:          measurement,
		StartTime:            info.StartTime,
		EndTime:              info.EndTime,
		Dir:                  info.DataDir,
		SeriesStore:          seriesStore,
		SchemaStore:          m.manager.Catalog(),
		MemTableCfg:          m.memTableCfg,
		CompactionCfg:        m.compactionCfg,
		CompressionAlgorithm: m.compressionAlgo,
	})
}

func (m *ShardManager) WaitForDiscovery() {
	m.discoveryWg.Wait()
}

func (m *ShardManager) calcShardStart(timestamp int64) int64 {
	shardDuration := int64(m.shardDuration)
	if shardDuration <= 0 {
		return 0
	}
	return (timestamp / shardDuration) * shardDuration
}

func (m *ShardManager) makeKey(db, measurementName string, startTime int64) string {
	return db + "/" + measurementName + "/" + formatInt64(startTime)
}

func formatTimeRange(start, end int64) string {
	return formatInt64(start) + "_" + formatInt64(end)
}

func formatInt64(n int64) string {
	return strconv.FormatInt(n, 10)
}

// isNameSafe 检查数据库名/measurement 名不包含路径遍历字符。
func isNameSafe(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	cleaned := filepath.Clean(name)
	if cleaned == "." || cleaned == ".." {
		return false
	}
	if strings.Contains(cleaned, string(os.PathSeparator)) {
		return false
	}
	return true
}

// FlushAll 刷新所有 Shard 的 MemTable 到 SSTable。
func (m *ShardManager) FlushAll() error {
	m.mu.RLock()
	shards := make([]*Shard, 0, len(m.shards))
	for _, s := range m.shards {
		shards = append(shards, s)
	}
	m.mu.RUnlock()

	var firstErr error
	for _, s := range shards {
		if err := s.Flush(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// CloseAll 关闭所有 Shard，释放资源。
func (m *ShardManager) CloseAll() error {
	m.mu.RLock()
	shards := make([]*Shard, 0, len(m.shards))
	for _, s := range m.shards {
		shards = append(shards, s)
	}
	m.mu.RUnlock()

	var firstErr error
	for _, s := range shards {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// PersistAll 持久化所有元数据到磁盘（通过 Manager）。
func (m *ShardManager) PersistAll() error {
	return m.manager.Sync()
}

// SetConfig 运行时更新所有现有 Shard 的 Compaction 配置。
func (m *ShardManager) SetConfig(config *compaction.Config) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, s := range m.shards {
		if s.compaction != nil {
			s.compaction.SetConfig(config)
		}
	}
}

// GetAllShards 返回所有 Shard 的快照。
func (m *ShardManager) GetAllShards() []*Shard {
	m.mu.RLock()
	shards := make([]*Shard, 0, len(m.shards))
	for _, s := range m.shards {
		shards = append(shards, s)
	}
	m.mu.RUnlock()
	return shards
}

// DeleteShard 删除指定的 Shard。
func (m *ShardManager) DeleteShard(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	shard, ok := m.shards[key]
	if !ok {
		return nil
	}

	if err := shard.Flush(); err != nil {
		return fmt.Errorf("flush shard: %w", err)
	}

	if err := shard.Close(); err != nil {
		return fmt.Errorf("close shard: %w", err)
	}

	dir := shard.Dir()
	if dir != "" {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("remove shard dir: %w", err)
		}
	}

	delete(m.shards, key)
	return nil
}

// ===================================
// ShardStore 接口实现（供 MeasurementWriter 使用）
// ===================================

// GetOrCreateDiskShard 获取或创建磁盘模式的 Shard（无 WAL/MemTable）。
func (m *ShardManager) GetOrCreateDiskShard(db, measurement string, startTime int64) (*writer.ShardInfo, error) {
	if !isNameSafe(db) || !isNameSafe(measurement) {
		return nil, fmt.Errorf("invalid database or measurement name")
	}

	endTime := startTime + int64(m.shardDuration)
	key := m.makeKey(db, measurement, startTime)

	m.mu.RLock()
	s, ok := m.shards[key]
	m.mu.RUnlock()

	if ok {
		return &writer.ShardInfo{
			StartTime: s.startTime,
			EndTime:   s.endTime,
			Dir:       s.dir,
			DataDir:   s.DataDir(),
			Internal:  s,
		}, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok = m.shards[key]; ok {
		return &writer.ShardInfo{
			StartTime: s.startTime,
			EndTime:   s.endTime,
			Dir:       s.dir,
			DataDir:   s.DataDir(),
			Internal:  s,
		}, nil
	}

	seriesStore := m.manager.GetOrCreateSeriesStore(db, measurement)

	shardDir := filepath.Join(m.dir, db, measurement, formatTimeRange(startTime, endTime))
	s = NewShard(ShardConfig{
		DB:                   db,
		Measurement:          measurement,
		StartTime:            startTime,
		EndTime:              endTime,
		Dir:                  shardDir,
		SeriesStore:          seriesStore,
		SchemaStore:          m.manager.Catalog(),
		CompactionCfg:        m.compactionCfg,
		CompressionAlgorithm: m.compressionAlgo,
		DiskOnly:             true,
	})
	m.shards[key] = s

	if err := m.manager.Shards().RegisterShard(db, measurement, metadata.ShardInfo{
		ID:        key,
		StartTime: startTime,
		EndTime:   endTime,
		DataDir:   shardDir,
	}); err != nil {
		slog.Warn("failed to register shard", "key", key, "error", err)
	}

	return &writer.ShardInfo{
		StartTime: s.startTime,
		EndTime:   s.endTime,
		Dir:       s.dir,
		DataDir:   s.DataDir(),
		Internal:  s,
	}, nil
}

// NextSSTSeqForShard 获取 shard 的下一个 SSTable 序列号。
func (m *ShardManager) NextSSTSeqForShard(info *writer.ShardInfo) uint64 {
	s, ok := info.Internal.(*Shard)
	if !ok {
		return 0
	}
	return s.NextSSTSeq()
}

// RegisterSSTableInShard 在 shard 中注册新写入的 SSTable。
func (m *ShardManager) RegisterSSTableInShard(info *writer.ShardInfo, sstSeq uint64, path string, minTime, maxTime int64, size int64) {
	s, ok := info.Internal.(*Shard)
	if !ok {
		return
	}
	_ = path
	s.RegisterSSTable(sstSeq, minTime, maxTime, size)
}

// TriggerCompactionInShard 触发 shard 的后台 compaction。
func (m *ShardManager) TriggerCompactionInShard(info *writer.ShardInfo) {
	s, ok := info.Internal.(*Shard)
	if !ok {
		return
	}
	s.TriggerCompaction()
}

// writerShardStore 是 writer.ShardStore 接口的适配器，将 ShardManager 方法映射到 writer 接口。
type writerShardStore struct {
	m *ShardManager
}

func (a *writerShardStore) GetOrCreateShard(db, measurement string, startTime int64) (*writer.ShardInfo, error) {
	return a.m.GetOrCreateDiskShard(db, measurement, startTime)
}

func (a *writerShardStore) NextSSTSeq(info *writer.ShardInfo) uint64 {
	return a.m.NextSSTSeqForShard(info)
}

func (a *writerShardStore) RegisterSSTable(info *writer.ShardInfo, sstSeq uint64, path string, minTime, maxTime int64, size int64) {
	a.m.RegisterSSTableInShard(info, sstSeq, path, minTime, maxTime, size)
}

func (a *writerShardStore) TriggerCompaction(info *writer.ShardInfo) {
	a.m.TriggerCompactionInShard(info)
}

// NewShardStore 创建符合 writer.ShardStore 接口的适配器。
func NewShardStore(m *ShardManager) writer.ShardStore {
	return &writerShardStore{m: m}
}

// ===================================
// MeasurementWriter 管理
// ===================================

// writerEntry 保存 writer 及其 shard store 适配器。
type writerEntry struct {
	writer     *writer.MeasurementWriter
	shardStore writer.ShardStore
}

// GetWriter 获取或创建指定 db.measurement 的 MeasurementWriter。
func (m *ShardManager) GetWriter(db, measurement string) (*writer.MeasurementWriter, error) {
	if !isNameSafe(db) || !isNameSafe(measurement) {
		return nil, fmt.Errorf("invalid database or measurement name")
	}

	key := db + "/" + measurement

	// 先检查缓存（需要初始化 writersMu）
	m.mu.Lock()
	if m.writers == nil {
		m.writers = make(map[string]*writerEntry)
	}
	if entry, ok := m.writers[key]; ok {
		m.mu.Unlock()
		return entry.writer, nil
	}
	m.mu.Unlock()

	// 创建 writer
	store := NewShardStore(m)
	measDir := filepath.Join(m.dir, db, measurement)

	mw, err := writer.New(writer.Config{
		DB:                   db,
		Measurement:          measurement,
		Dir:                  measDir,
		ShardDuration:        int64(m.shardDuration),
		SeriesStore:          m.manager.GetOrCreateSeriesStore(db, measurement),
		SchemaStore:          m.manager.Catalog(),
		ShardStore:           store,
		MemTableCfg:          m.memTableCfg,
		CompactionCfg:        m.compactionCfg,
		CompressionAlgorithm: m.compressionAlgo,
	})
	if err != nil {
		return nil, fmt.Errorf("create writer: %w", err)
	}

	m.mu.Lock()
	// 双检查
	if entry, ok := m.writers[key]; ok {
		m.mu.Unlock()
		_ = mw.Close()
		return entry.writer, nil
	}
	m.writers[key] = &writerEntry{writer: mw, shardStore: store}
	m.mu.Unlock()

	return mw, nil
}

// CloseAllWriters 关闭所有 MeasurementWriter。
func (m *ShardManager) CloseAllWriters() error {
	m.mu.Lock()
	writers := make([]*writer.MeasurementWriter, 0, len(m.writers))
	for _, entry := range m.writers {
		writers = append(writers, entry.writer)
	}
	m.mu.Unlock()

	var firstErr error
	for _, w := range writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// FlushAllWriters 刷新所有 MeasurementWriter 的 MemTable。
func (m *ShardManager) FlushAllWriters() error {
	m.mu.RLock()
	writers := make([]*writer.MeasurementWriter, 0, len(m.writers))
	for _, entry := range m.writers {
		writers = append(writers, entry.writer)
	}
	m.mu.RUnlock()

	var firstErr error
	for _, w := range writers {
		if err := w.Flush(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// GetWriterIfExists 获取已存在的 writer，不创建新的。
func (m *ShardManager) GetWriterIfExists(db, measurement string) *writer.MeasurementWriter {
	if !isNameSafe(db) || !isNameSafe(measurement) {
		return nil
	}
	key := db + "/" + measurement
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.writers == nil {
		return nil
	}
	if entry, ok := m.writers[key]; ok {
		return entry.writer
	}
	return nil
}
