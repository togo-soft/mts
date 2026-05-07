package shard

import (
	"fmt"
	"path/filepath"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
)

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
	// 如果使用 Level Compaction，调用 flushLocked 以保持一致的处理逻辑
	if s.levelCompaction != nil {
		if err := s.flushLocked(); err != nil {
			// 即使失败也要继续关闭 WAL
			if s.wal != nil {
				_ = s.wal.Close()
			}
			return fmt.Errorf("flush memtable: %w", err)
		}
	} else {
		// 平坦 Compaction 的刷盘逻辑
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

	// 5. 停止 Compaction Manager
	if s.compaction != nil {
		s.compaction.Stop()
	}

	// 6. 停止 Level Compaction Manager
	if s.levelCompaction != nil {
		s.levelCompaction.Stop()
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

// copyTagsMap 复制 tags map，避免共享底层数据结构。
func copyTagsMap(tags map[string]string) map[string]string {
	if tags == nil {
		return nil
	}
	copied := make(map[string]string, len(tags))
	for k, v := range tags {
		copied[k] = v
	}
	return copied
}
