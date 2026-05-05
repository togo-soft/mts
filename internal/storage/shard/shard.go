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
	"fmt"
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
//
// 时间窗口：
//
//	Shard 只包含 [StartTime, EndTime) 范围内的数据。
//	时间戳小于 StartTime 或大于等于 EndTime 的点不会写入此 Shard。
type ShardConfig struct {
	DB          string
	Measurement string
	StartTime   int64
	EndTime     int64
	Dir         string
	MetaStore   *measurement.MeasurementMetaStore
	MemTableCfg MemTableConfig
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
//   - mu: 读写锁
//   - sstSeq: SSTable 序列号（文件名生成）
type Shard struct {
	db          string
	measurement string
	startTime   int64
	endTime     int64
	dir         string
	memTable    *MemTable
	wal         *WAL
	metaStore   *measurement.MeasurementMetaStore
	mu          sync.RWMutex
	sstSeq      uint64 // SSTable序列号，用于生成唯一的文件名
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
//  2. 创建空的 MemTable
//
// 错误处理：
//
//	WAL 创建失败不会阻止 Shard 创建，数据可能仅保存在内存中。
//	这种情况下，系统重启后 MemTable 数据会丢失。
func NewShard(cfg ShardConfig) *Shard {
	// 创建 WAL
	walDir := filepath.Join(cfg.Dir, "wal")
	wal, err := NewWAL(walDir, 0)
	if err != nil {
		// 如果 WAL 创建失败，使用 nil wal
		wal = nil
	}

	return &Shard{
		db:          cfg.DB,
		measurement: cfg.Measurement,
		startTime:   cfg.StartTime,
		endTime:     cfg.EndTime,
		dir:         cfg.Dir,
		memTable:    NewMemTable(cfg.MemTableCfg),
		wal:         wal,
		metaStore:   cfg.MetaStore,
	}
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

	// 2. 分配 SID
	sid := s.metaStore.AllocateSID(point.Tags)
	_ = sid // SID 目前未使用，后续会用到

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
//   - []types.PointRow: 按时间排序的数据点
//   - error:            读取失败时返回错误
//
// 注意：
//
//	返回的结果是 MemTable 和 SSTable 的合并，按时间升序排列。
//	对于大数据集，建议使用迭代器模式避免内存压力。
func (s *Shard) Read(startTime, endTime int64) ([]types.PointRow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var rows []types.PointRow

	// 1. 从 MemTable 读取
	iter := s.memTable.Iterator()
	for iter.Next() {
		p := iter.Point()
		if p.Timestamp >= startTime && p.Timestamp < endTime {
			rows = append(rows, types.PointRow{
				Timestamp: p.Timestamp,
				Tags:      p.Tags,
				Fields:    p.Fields,
			})
		}
	}

	// 2. 从 SSTable 读取
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
func (s *Shard) readFromSSTable(startTime, endTime int64) ([]types.PointRow, error) {
	dataDir := filepath.Join(s.dir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return nil, nil // 没有 SSTable
	}

	var allRows []types.PointRow

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
			continue // 跳过无法读取的 SSTable
		}

		rows, err := r.ReadRange(startTime, endTime)
		r.Close()
		if err != nil {
			continue
		}
		allRows = append(allRows, rows...)
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

	// 创建 SSTable Writer
	w, err := sstable.NewWriter(s.dir, s.sstSeq)
	if err != nil {
		return fmt.Errorf("create sstable writer: %w", err)
	}
	s.sstSeq++

	if err := w.WritePoints(points); err != nil {
		_ = w.Close()
		return fmt.Errorf("write points to sstable: %w", err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close sstable writer: %w", err)
	}
	return nil
}

// flushLocked 内部刷写方法（已持有锁）
func (s *Shard) flushLocked() error {
	points := s.memTable.Flush()
	if len(points) == 0 {
		return nil
	}

	w, err := sstable.NewWriter(s.dir, s.sstSeq)
	if err != nil {
		return fmt.Errorf("create sstable writer: %w", err)
	}
	s.sstSeq++ // 递增序列号，确保下次 flush 使用不同的文件名

	if err := w.WritePoints(points); err != nil {
		_ = w.Close()
		return fmt.Errorf("write points to sstable: %w", err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close sstable writer: %w", err)
	}

	// 显式清空 points 引用，帮助 GC 回收内存
	for i := range points {
		points[i] = nil
	}

	return nil
}

// Close 关闭 Shard，释放资源。
//
// 关闭流程：
//
//  1. 刷盘 MemTable 数据（即使 WAL 关闭失败也继续）
//  2. 关闭 WAL
//
// 返回：
//   - error: 关闭失败时返回错误
//
// 错误处理：
//
//	优先确保数据安全（刷盘），WAL 关闭失败不影响数据完整性。
//	关闭后 Shard 不可再使用。
func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. 先刷写 MemTable 到 SSTable（即使 WAL 关闭失败，数据也已安全）
	points := s.memTable.Flush()
	if len(points) > 0 {
		w, err := sstable.NewWriter(s.dir, s.sstSeq)
		if err != nil {
			return fmt.Errorf("create sstable writer: %w", err)
		}

		if err := w.WritePoints(points); err != nil {
			_ = w.Close()
			return fmt.Errorf("write points to sstable: %w", err)
		}

		if err := w.Close(); err != nil {
			return fmt.Errorf("close sstable writer: %w", err)
		}
	}

	// 2. 关闭 WAL（WAL 关闭失败可接受，因为数据已在 SSTable）
	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			return fmt.Errorf("close wal: %w", err)
		}
	}

	return nil
}
