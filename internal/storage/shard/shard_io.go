package shard

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"codeberg.org/micro-ts/mts/types"
)

// Write 写入单个数据点到 Shard。
//
// 写入流程：
//
//  1. 背压检查（active 超限时等待）
//  2. 持锁：分配 SID → 验证字段 → 写 WAL → 写 MemTable
//  3. 释放锁后异步触发 flush（不阻塞写入）
//
// 参数：
//   - point: 要写入的数据点
//
// 返回：
//   - error: 写入失败时返回错误
//
// 注意：
//
//	如果 WAL 写入成功但 MemTable 写入失败，replay 时可能产生重复数据。
//	这是可接受的设计权衡，因为这种情况非常罕见，且最终一致性可保证正确。
func (s *Shard) Write(point *types.Point) error {
	// 背压：如果 active 超过硬限制（2x 阈值），等待正在进行的 flush 完成。
	// 若 flush 已完成但 active 仍超限（积压数据未清理），自行触发新 flush 清理。
	for s.memTable.ActiveFull() {
		if !s.memTable.IsFlushing() {
			s.tryTriggerAsyncFlush()
		}
		time.Sleep(time.Millisecond)
		if s.closed.Load() {
			return fmt.Errorf("shard closed during backpressure wait")
		}
	}

	s.mu.Lock()

	// 1. 分配 SID（Tags→Sid 映射持久化到 boltDB，WAL 无需重复存储 Tags）
	sid, err := s.seriesStore.AllocateSID(point.Tags)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("allocate SID: %w", err)
	}

	// 2. 验证字段类型一致性
	if err := s.ValidateFieldTypes(point); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("validate field types: %w", err)
	}

	// 3. 转换为 InternalPoint
	ip := types.PointToInternal(point, sid)

	// 4. 写入 WAL（紧凑 InternalPoint 格式，不含 Tags）
	if s.wal != nil {
		data, err := serializeInternalPoint(ip)
		if err != nil {
			s.mu.Unlock()
			return fmt.Errorf("serialize point: %w", err)
		}
		if _, err := s.wal.Write(data); err != nil {
			s.mu.Unlock()
			return fmt.Errorf("write to wal: %w", err)
		}
	}

	// 5. 写入 MemTable
	if err := s.memTable.Write(ip); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("write to memtable: %w", err)
	}

	// 6. 检查是否需要异步 flush（释放锁后触发，避免阻塞写入）
	shouldFlush := s.memTable.ShouldSwap()
	s.mu.Unlock()

	if shouldFlush {
		s.tryTriggerAsyncFlush()
	}

	return nil
}

// listSSTableFiles 列出 Shard 中所有可读的 SSTable 文件路径。
// 自动处理 flat（data/sst_*.bin）和 leveled（data/L0/sst_*.bin, ...）两种目录结构。
func (s *Shard) listSSTableFiles() []string {
	dataDir := filepath.Join(s.dir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return nil
	}

	var files []string

	if s.levelCompaction != nil {
		for level := 0; ; level++ {
			levelDir := filepath.Join(dataDir, fmt.Sprintf("L%d", level))
			entries, err := os.ReadDir(levelDir)
			if err != nil {
				break
			}
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				if !strings.HasPrefix(entry.Name(), "sst_") || !strings.HasSuffix(entry.Name(), ".bin") {
					continue
				}
				files = append(files, filepath.Join(levelDir, entry.Name()))
			}
		}
		return files
	}

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), "sst_") || !strings.HasSuffix(entry.Name(), ".bin") {
			continue
		}
		files = append(files, filepath.Join(dataDir, entry.Name()))
	}
	return files
}
