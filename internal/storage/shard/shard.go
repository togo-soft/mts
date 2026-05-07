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
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/measurement"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// copyTags 复制 tags map
func copyTags(tags map[string]string) map[string]string {
	if tags == nil {
		return nil
	}
	result := make(map[string]string, len(tags))
	for k, v := range tags {
		result[k] = v
	}
	return result
}

// ShardConfig 定义 Shard 的配置。
//
// 字段说明：
//
//   - DB:          所属数据库名称
//   - Measurement: 所属 Measurement 名称
//   - StartTime:   Shard 时间窗口起始（包含），纳秒
//   - EndTime:     Shard 时间窗口结束（不包含），纳秒
//   - Dir:         数据存储目录路径
//   - MetaStore:   测量元数据存储，用于分配 Series IDs
//   - MemTableCfg: MemTable 配置
//   - CompactionCfg: Compaction 配置（可选，nil 表示禁用 compaction）
//
// 时间窗口：
//
//	Shard 只包含 [StartTime, EndTime) 范围内的数据。
//	时间戳小于 StartTime 或大于等于 EndTime 的点不会写入此 Shard。
type ShardConfig struct {
	DB            string
	Measurement   string
	StartTime     int64
	EndTime       int64
	Dir           string
	MetaStore     *measurement.MeasurementMetaStore
	MemTableCfg   *MemTableConfig
	CompactionCfg *CompactionConfig
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
//   - metaStore: 元数据存储（用于 SID 分配）
//   - sidCache: Sid→Tags 缓存（用于从 SSTable 恢复 Tags）
//   - tsSidMap: Timestamp→Sid 映射（用于 flush 时获取 Sid）
//   - mu: 读写锁
//   - sstSeq: SSTable 序列号（文件名生成）
//   - compaction: Compaction 管理器
type Shard struct {
	db          string
	measurement string
	startTime   int64
	endTime     int64
	dir         string
	memTable    *MemTable
	wal         *WAL
	walDone     chan struct{} // WAL 定期同步停止信号
	metaStore   *measurement.MeasurementMetaStore
	sidCache    map[uint64]map[string]string // sid → tags 缓存
	tsSidMap    map[int64]uint64             // timestamp → sid 映射
	mu          sync.RWMutex
	sstSeq      uint64 // SSTable序列号，用于生成唯一的文件名
	compaction  *CompactionManager
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
	// 创建 WAL
	walDir := filepath.Join(cfg.Dir, "wal")
	wal, err := NewWAL(walDir, 0)
	if err != nil {
		// 如果 WAL 创建失败，使用 nil wal
		wal = nil
		slog.Warn("failed to create WAL, writes will not be durable",
			"walDir", walDir,
			"error", err)
	}

	// 创建空的 MemTable
	memTable := NewMemTable(cfg.MemTableCfg)

	// 从 WAL 恢复数据到 MemTable
	if wal != nil {
		points, err := ReplayWAL(walDir)
		if err != nil {
			slog.Error("failed to replay WAL, some data may be lost",
				"walDir", walDir,
				"error", err)
		} else {
			for _, p := range points {
				if writeErr := memTable.Write(p); writeErr != nil {
					slog.Error("failed to replay WAL point, skipping",
						"walDir", walDir,
						"timestamp", p.Timestamp,
						"error", writeErr)
				}
			}
			if len(points) > 0 {
				slog.Info("replayed WAL data into MemTable",
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
		metaStore:   cfg.MetaStore,
		sidCache:    make(map[uint64]map[string]string),
		tsSidMap:    make(map[int64]uint64),
	}

	// 初始化 CompactionManager（如果配置了）
	if cfg.CompactionCfg != nil {
		shard.compaction = NewCompactionManager(shard, cfg.CompactionCfg)
	}

	// 启动 WAL 定期同步（如果 WAL 存在）
	if wal != nil {
		go wal.StartPeriodicSync(time.Minute, shard.walDone)
	}

	// 启动定期 Compaction 检查（如果启用了）
	if shard.compaction != nil {
		shard.compaction.StartPeriodicCheck()
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

// Write 写入单个数据点到 Shard。
//
// 写入流程：
//
//  1. 锁定 Shard（写锁）
//  2. 序列化数据点
//  3. 写入 WAL（如果 WAL 可用）
//  4. 分配 Series ID
//  5. 写入 MemTable
//  6. 检查是否需要刷盘，如需要则执行刷盘
//
// 参数：
//   - point: 要写入的数据点
//
// 返回：
//   - error: 写入失败时返回错误
//
// 错误情况：
//
//   - WAL 写入失败
//   - MemTable 写入失败
//   - 刷盘失败
//
// 注意：
//
//	如果 WAL 写入成功但 MemTable 写入失败，replay 时可能产生重复数据。
//	这是可接受的设计权衡，因为这种情况非常罕见，且最终一致性可保证正确。
func (s *Shard) Write(point *types.Point) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.wal != nil {
		data, err := serializePoint(point)
		if err != nil {
			return fmt.Errorf("serialize point: %w", err)
		}
		if _, err := s.wal.Write(data); err != nil {
			return fmt.Errorf("write to wal: %w", err)
		}
	}

	// 2. 分配 SID 并更新 sidCache 和 tsSidMap
	sid, err := s.metaStore.AllocateSID(point.Tags)
	if err != nil {
		return fmt.Errorf("allocate SID: %w", err)
	}
	s.sidCache[sid] = copyTags(point.Tags)
	s.tsSidMap[point.Timestamp] = sid

	// 3. 写入 MemTable
	if err := s.memTable.Write(point); err != nil {
		return fmt.Errorf("write to memtable: %w", err)
	}

	// 4. 检查是否需要 flush
	if s.memTable.ShouldFlush() {
		if err := s.flushLocked(); err != nil {
			return fmt.Errorf("flush memtable: %w", err)
		}
	}

	return nil
}

// Read 读取指定时间范围内的数据点。
//
// 读取流程：
//
//  1. 从 MemTable 读取匹配的数据
//  2. 从 SSTable 读取匹配的数据
//  3. 合并并排序结果
//
// 参数：
//   - startTime: 起始时间（包含，纳秒）
//   - endTime:   结束时间（不包含，纳秒）
//
// 返回：
//   - []*types.PointRow: 按时间排序的数据点
//   - error:            读取失败时返回错误
//
// 注意：
//
//	返回的结果是 MemTable 和 SSTable 的合并，按时间升序排列。
//	对于大数据集，建议使用迭代器模式避免内存压力。
func (s *Shard) Read(startTime, endTime int64) ([]*types.PointRow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var rows []*types.PointRow

	// 1. 从 MemTable 读取（可能有 WAL replay 重复数据，需要去重）
	memTableSeen := make(map[int64]bool)
	iter := s.memTable.Iterator()
	for iter.Next() {
		p := iter.Point()
		if p.Timestamp >= startTime && p.Timestamp < endTime {
			// MemTable 去重：基于 timestamp（同一时间戳只有一条数据）
			if memTableSeen[p.Timestamp] {
				continue
			}
			memTableSeen[p.Timestamp] = true
			rows = append(rows, &types.PointRow{
				Timestamp: p.Timestamp,
				Tags:      p.Tags,
				Fields:    p.Fields,
			})
		}
	}

	// 2. 从 SSTable 读取（Tags 已在内部通过 Sid 填充）
	sstRows, err := s.readFromSSTable(startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("read from sstable: %w", err)
	}
	rows = append(rows, sstRows...)

	// 3. 按时间排序
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Timestamp < rows[j].Timestamp
	})

	return rows, nil
}

// readFromSSTable 从 SSTable 读取时间范围内的数据
func (s *Shard) readFromSSTable(startTime, endTime int64) ([]*types.PointRow, error) {
	dataDir := filepath.Join(s.dir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return nil, nil // 没有 SSTable
	}

	var allRows []*types.PointRow

	// 读取所有 SSTable 子目录 (sst_0, sst_1, ...)
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("read data dir: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// 检查是否是 SSTable 目录
		if !strings.HasPrefix(entry.Name(), "sst_") {
			continue
		}

		sstDir := filepath.Join(dataDir, entry.Name())
		r, err := sstable.NewReader(sstDir)
		if err != nil {
			slog.Warn("failed to open SSTable, skipping",
				"sstDir", sstDir,
				"error", err)
			continue
		}

		rows, err := r.ReadRange(startTime, endTime)
		if closeErr := r.Close(); closeErr != nil {
			slog.Warn("failed to close SSTable reader",
				"sstDir", sstDir,
				"closeError", closeErr)
		}
		if err != nil {
			slog.Error("failed to read SSTable range, skipping",
				"sstDir", sstDir,
				"startTime", startTime,
				"endTime", endTime,
				"error", err)
			continue
		}
		allRows = append(allRows, rows...)
	}

	// 为所有 SSTable 行填充 Tags（通过 Sid 从 metaStore 获取）
	for i := range allRows {
		if allRows[i].Sid != 0 {
			if tags, ok := s.metaStore.GetTagsBySID(allRows[i].Sid); ok {
				allRows[i].Tags = tags
			}
		}
	}

	// 按时间戳排序
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].Timestamp < allRows[j].Timestamp
	})

	return allRows, nil
}

// Flush 将 MemTable 数据刷写到 SSTable。
//
// 刷盘流程：
//
//  1. 获取 MemTable 中的数据（同时清空 MemTable）
//  2. 创建 SSTable Writer
//  3. 写入数据到 SSTable
//  4. 检查是否需要触发 compaction
//
// 返回：
//   - error: 刷盘失败时返回错误
//
// 使用场景：
//
//	通常由 MemTable 自动触发（达到阈值）。
//	也可手动调用以强制刷盘（如优雅关闭时）。
func (s *Shard) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	points := s.memTable.Flush()
	if len(points) == 0 {
		return nil
	}

	// 获取即将写入的 SSTable 路径
	sstPath := filepath.Join(s.dir, "data", fmt.Sprintf("sst_%d", s.sstSeq))

	// 创建 SSTable Writer
	w, err := sstable.NewWriter(s.dir, s.sstSeq, 0)
	if err != nil {
		return fmt.Errorf("create sstable writer: %w", err)
	}
	s.sstSeq++

	// 创建写入标志（防止 compaction 选中正在写入的 SSTable）
	// 注意：必须在 NewWriter 之后调用，因为 NewWriter 会创建目录
	if s.compaction != nil {
		if err := s.compaction.markSSTableWriting(sstPath); err != nil {
			slog.Warn("failed to mark sstable in write", "path", sstPath, "error", err)
		}
	}

	if err := w.WritePoints(points, s.tsSidMap); err != nil {
		_ = w.Close()
		if s.compaction != nil {
			_ = s.compaction.unmarkSSTableWriting(sstPath)
		}
		return fmt.Errorf("write points to sstable: %w", err)
	}

	// 写入完成后清除已刷盘的 timestamp→sid 映射
	for _, p := range points {
		delete(s.tsSidMap, p.Timestamp)
	}

	if err := w.Close(); err != nil {
		if s.compaction != nil {
			_ = s.compaction.unmarkSSTableWriting(sstPath)
		}
		return fmt.Errorf("close sstable writer: %w", err)
	}

	// 删除写入标志
	if s.compaction != nil {
		if err := s.compaction.unmarkSSTableWriting(sstPath); err != nil {
			slog.Warn("failed to unmark sstable write", "path", sstPath, "error", err)
		}
	}

	// 清空当前 WAL 文件（数据已持久化到 SSTable，不再需要）
	if s.wal != nil {
		_ = s.wal.TruncateCurrent()
	}

	// 显式清空 points 引用，帮助 GC 回收内存
	for i := range points {
		points[i] = nil
	}

	// 检查是否需要触发 compaction（后台执行）
	if s.compaction != nil && s.compaction.ShouldCompactWithLock() {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), s.compaction.config.Timeout)
			defer cancel()
			if _, _, err := s.compaction.Compact(ctx); err != nil {
				slog.Error("background compaction failed", "error", err)
			} else {
				s.compaction.resetTimer()
			}
		}()
	}

	return nil
}

// flushLocked 内部刷写方法（已持有锁）
func (s *Shard) flushLocked() error {
	points := s.memTable.Flush()
	if len(points) == 0 {
		return nil
	}

	// 获取即将写入的 SSTable 路径
	sstPath := filepath.Join(s.dir, "data", fmt.Sprintf("sst_%d", s.sstSeq))

	w, err := sstable.NewWriter(s.dir, s.sstSeq, 0)
	if err != nil {
		return fmt.Errorf("create sstable writer: %w", err)
	}
	s.sstSeq++ // 递增序列号，确保下次 flush 使用不同的文件名

	// 创建写入标志（防止 compaction 选中正在写入的 SSTable）
	// 注意：必须在 NewWriter 之后调用，因为 NewWriter 会创建目录
	if s.compaction != nil {
		if err := s.compaction.markSSTableWriting(sstPath); err != nil {
			slog.Warn("failed to mark sstable in write", "path", sstPath, "error", err)
		}
	}

	if err := w.WritePoints(points, s.tsSidMap); err != nil {
		_ = w.Close()
		if s.compaction != nil {
			_ = s.compaction.unmarkSSTableWriting(sstPath)
		}
		return fmt.Errorf("write points to sstable: %w", err)
	}

	// 写入完成后清除已刷盘的 timestamp→sid 映射
	for _, p := range points {
		delete(s.tsSidMap, p.Timestamp)
	}

	if err := w.Close(); err != nil {
		if s.compaction != nil {
			_ = s.compaction.unmarkSSTableWriting(sstPath)
		}
		return fmt.Errorf("close sstable writer: %w", err)
	}

	// 删除写入标志
	if s.compaction != nil {
		if err := s.compaction.unmarkSSTableWriting(sstPath); err != nil {
			slog.Warn("failed to unmark sstable write", "path", sstPath, "error", err)
		}
	}

	// 清空当前 WAL 文件（数据已持久化到 SSTable，不再需要）
	if s.wal != nil {
		_ = s.wal.TruncateCurrent()
	}

	// 显式清空 points 引用，帮助 GC 回收内存
	for i := range points {
		points[i] = nil
	}

	// 检查是否需要触发 compaction（后台执行）
	// 注意：这里不需要再次检查，因为 ShouldCompact 在 Write 时已经被调用

	return nil
}

// Close 关闭 Shard，释放资源。
//
// 关闭流程：
//
//  1. 刷盘 MemTable 数据到 SSTable
//  2. 关闭 WAL
//  3. 清理 tsSidMap 和 sstSeq
//
// 错误处理：
//
//	优先确保数据安全（刷盘）。
//	如果刷盘成功但 WAL 关闭失败，数据已在 SSTable 中，不会丢失。
//	关闭后 Shard 不可再使用。
func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. 先刷写 MemTable 到 SSTable
	points := s.memTable.Flush()
	if len(points) > 0 {
		w, err := sstable.NewWriter(s.dir, s.sstSeq, 0)
		if err != nil {
			// 即使 writer 创建失败，也要继续关闭 WAL
			if s.wal != nil {
				_ = s.wal.Close()
			}
			return fmt.Errorf("create sstable writer: %w", err)
		}
		s.sstSeq++

		if err := w.WritePoints(points, s.tsSidMap); err != nil {
			_ = w.Close()
			if s.wal != nil {
				_ = s.wal.Close()
			}
			return fmt.Errorf("write points to sstable: %w", err)
		}

		if err := w.Close(); err != nil {
			if s.wal != nil {
				_ = s.wal.Close()
			}
			return fmt.Errorf("close sstable writer: %w", err)
		}

		// 清理已刷盘的 timestamp→sid 映射
		for _, p := range points {
			delete(s.tsSidMap, p.Timestamp)
		}
	}

	// 2. 清理剩余的 tsSidMap（不再需要）
	for ts := range s.tsSidMap {
		delete(s.tsSidMap, ts)
	}

	// 3. 停止 WAL 定期同步 goroutine
	if s.wal != nil && s.walDone != nil {
		close(s.walDone)
	}

	// 4. 关闭 WAL
	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			return fmt.Errorf("close wal: %w", err)
		}
	}

	// 5. 持久化 MetaStore（如果脏了）
	if s.metaStore != nil {
		if err := s.metaStore.Persist(); err != nil {
			return fmt.Errorf("persist metastore: %w", err)
		}
	}

	return nil
}

// DataDir 返回 Shard 的数据目录。
//
// 返回：
//   - string: 数据目录路径 (shardDir/data)
func (s *Shard) DataDir() string {
	return filepath.Join(s.dir, "data")
}

// NextSSTSeq 返回下一个 SSTable 序列号并递增。
//
// 返回：
//   - uint64: 下一个可用的序列号
//
// 注意：
//
//	调用此方法会递增内部序列号，确保每次调用返回不同的值。
func (s *Shard) NextSSTSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.sstSeq
	s.sstSeq++
	return seq
}
