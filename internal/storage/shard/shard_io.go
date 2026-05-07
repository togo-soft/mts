package shard

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

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
	sid, err := s.seriesStore.AllocateSID(point.Tags)
	if err != nil {
		return fmt.Errorf("allocate SID: %w", err)
	}
	s.sidCache[sid] = copyTagsMap(point.Tags)
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

	// 如果使用 Level Compaction，扫描 L0, L1, L2 等目录
	if s.levelCompaction != nil {
		for level := 0; ; level++ {
			levelDir := filepath.Join(dataDir, fmt.Sprintf("L%d", level))
			if _, err := os.Stat(levelDir); os.IsNotExist(err) {
				break // 没有更多层级目录
			}

			entries, err := os.ReadDir(levelDir)
			if err != nil {
				slog.Warn("failed to read level dir", "levelDir", levelDir, "error", err)
				continue
			}

			for _, entry := range entries {
				if !entry.IsDir() {
					continue
				}
				if !strings.HasPrefix(entry.Name(), "sst_") {
					continue
				}

				sstDir := filepath.Join(levelDir, entry.Name())
				if err := s.readSSTableDir(sstDir, startTime, endTime, &allRows); err != nil {
					slog.Warn("failed to read SSTable in level", "sstDir", sstDir, "error", err)
				}
			}
		}
	} else {
		// 读取所有 SSTable 子目录 (sst_0, sst_1, ...) - 平坦结构
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
			if err := s.readSSTableDir(sstDir, startTime, endTime, &allRows); err != nil {
				slog.Warn("failed to read SSTable", "sstDir", sstDir, "error", err)
			}
		}
	}

	// 通过 Sid 从 seriesStore 获取 Tags
	for i := range allRows {
		if allRows[i].Sid != 0 {
			if tags, ok := s.seriesStore.GetTagsBySID(allRows[i].Sid); ok {
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

// readSSTableDir 读取单个 SSTable 目录的数据
func (s *Shard) readSSTableDir(sstDir string, startTime, endTime int64, rows *[]*types.PointRow) error {
	s.AcquireSSTRef(sstDir)
	defer s.ReleaseSSTRef(sstDir)

	r, err := sstable.NewReader(sstDir)
	if err != nil {
		return fmt.Errorf("open sstable: %w", err)
	}

	readRows, err := r.ReadRange(startTime, endTime)
	if closeErr := r.Close(); closeErr != nil {
		slog.Warn("failed to close SSTable reader", "sstDir", sstDir, "error", closeErr)
	}
	if err != nil {
		return fmt.Errorf("read range: %w", err)
	}
	*rows = append(*rows, readRows...)
	return nil
}
