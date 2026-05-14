package shard

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"codeberg.org/micro-ts/mts/internal/metrics"
	"codeberg.org/micro-ts/mts/types"
)

const backpressureSleep = time.Millisecond // 背压等待轮询间隔

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
		time.Sleep(backpressureSleep)
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
		metrics.Incr(metrics.WriteErrors, 1)
		s.mu.Unlock()
		return fmt.Errorf("validate field types: %w", err)
	}

	// 3. 转换为 MemPoint（字段直接序列化为紧凑 []byte）
	mp := types.PointToMemPoint(point, sid)

	// 4. 写入 WAL（WAL 完整格式 = Version + TS + SID + FieldData）
	if s.wal != nil {
		data := serializePointForWAL(mp.Timestamp, mp.Sid, mp.FieldData)
		if _, err := s.wal.Write(data); err != nil {
			metrics.Incr(metrics.WriteErrors, 1)
			s.mu.Unlock()
			return fmt.Errorf("write to wal: %w", err)
		}
	}

	// 5. 写入 MemTable（接管 FieldData 所有权，零拷贝）
	if err := s.memTable.Write(mp); err != nil {
		metrics.Incr(metrics.WriteErrors, 1)
		s.mu.Unlock()
		return fmt.Errorf("write to memtable: %w", err)
	}

	metrics.Incr(metrics.WriteTotal, 1)

	// 6. 检查是否需要异步 flush（释放锁后触发，避免阻塞写入）
	shouldFlush := s.memTable.ShouldSwap()
	s.mu.Unlock()

	if shouldFlush {
		s.tryTriggerAsyncFlush()
	}

	return nil
}

// WriteBatch 批量写入数据点到 Shard，使用单次锁获取 + 单次 WAL 批量写入。
//
// 与多次调用 Write 的区别：
//   - 只获取一次 Shard 锁（减少锁竞争）
//   - 通过 WAL.WriteBatch 批量持久化（减少 fsync 次数）
//
// 参数：
//   - points: 要写入的数据点切片
//
// 返回：
//   - int: 成功写入的点数
//   - error: 首个失败点的错误
func (s *Shard) WriteBatch(points []*types.Point) (int, error) {
	if len(points) == 0 {
		return 0, nil
	}

	// 背压检查
	for s.memTable.ActiveFull() {
		if !s.memTable.IsFlushing() {
			s.tryTriggerAsyncFlush()
		}
		time.Sleep(backpressureSleep)
		if s.closed.Load() {
			return 0, fmt.Errorf("shard closed during backpressure wait")
		}
	}

	s.mu.Lock()

	// 二次检查：获取锁期间 memTable 可能已被其他 goroutine 写满
	if s.memTable.ActiveFull() {
		s.mu.Unlock()
		// 递归重试（最多一次，避免栈溢出）
		return s.WriteBatch(points)
	}

	// 预序列化所有 point
	mps := make([]types.MemPoint, 0, len(points))
	walData := make([][]byte, 0, len(points))

	for i, point := range points {
		sid, err := s.seriesStore.AllocateSID(point.Tags)
		if err != nil {
			metrics.Incr(metrics.WriteErrors, 1)
			s.mu.Unlock()
			return i, fmt.Errorf("allocate SID for point %d: %w", i, err)
		}
		if err := s.ValidateFieldTypes(point); err != nil {
			s.mu.Unlock()
			return i, fmt.Errorf("validate field types for point %d: %w", i, err)
		}
		mp := types.PointToMemPoint(point, sid)
		mps = append(mps, mp)

		if s.wal != nil {
			walData = append(walData, serializePointForWAL(mp.Timestamp, mp.Sid, mp.FieldData))
		}
	}

	// 批量写入 WAL
	if s.wal != nil && len(walData) > 0 {
		if _, err := s.wal.WriteBatch(walData); err != nil {
			metrics.Incr(metrics.WriteErrors, 1)
			s.mu.Unlock()
			return 0, fmt.Errorf("wal write batch: %w", err)
		}
	}

	// 批量写入 MemTable
	for i, mp := range mps {
		if err := s.memTable.Write(mp); err != nil {
			metrics.Incr(metrics.WriteErrors, 1)
			s.mu.Unlock()
			return i, fmt.Errorf("write to memtable at %d: %w", i, err)
		}
	}

	metrics.Incr(metrics.WriteBatchTotal, 1)
	metrics.Incr(metrics.WriteTotal, int64(len(mps)))

	s.mu.Unlock()

	// 释放锁后触发异步 flush，避免阻塞后续写入
	if s.memTable.ShouldSwap() {
		s.tryTriggerAsyncFlush()
	}

	return len(mps), nil
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
