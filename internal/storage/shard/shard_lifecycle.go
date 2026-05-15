package shard

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"time"
)

// Close 关闭 Shard，释放资源。
//
// 关闭流程：
//
//  1. 使用 sync.Once 确保 Close 只执行一次
//  2. 停止 MemTable 定期刷盘检查（需要先于 s.mu 获取，以避免死锁）
//  3. 刷盘 MemTable 数据到 SSTable
//  4. WAL 清理（仅当 flush 成功时）
//  5. 关闭 WAL
//  6. 标记 closed，阻止新的后台 compaction
//  7. 停止 Compaction Manager
//  8. 停止 Level Compaction Manager
//  9. 等待所有后台 compaction goroutine 完成
//
// 错误处理：
//
//	优先确保数据安全（刷盘）。
//	如果刷盘成功但 WAL 关闭失败，数据已在 SSTable 中，不会丢失。
//	关闭后 Shard 不可再使用。
func (s *Shard) Close() error {
	var err error
	s.closeOnce.Do(func() {
		// 停止 MemTable 定期刷盘（仅完整模式）
		if s.flushDone != nil {
			close(s.flushDone)
			s.flushWg.Wait()
		}

		// 刷盘 + WAL 清理（仅完整模式）
		err = s.closeWithLock()

		// 停止 compaction managers
		if s.compaction != nil {
			s.compaction.Stop()
		}
		if s.levelCompaction != nil {
			s.levelCompaction.Stop()
		}
		s.compactionWg.Wait()

		slog.Info("Shard.Close: completed")
	})
	return err
}

// IsDiskOnly 返回 Shard 是否为磁盘模式（无 WAL/MemTable）。
func (s *Shard) IsDiskOnly() bool {
	return s.memTable == nil
}

// closeWithLock 在持有 s.mu 的情况下执行刷盘和 WAL 清理。
func (s *Shard) closeWithLock() error {
	// 磁盘模式：只需标记关闭
	if s.memTable == nil {
		s.mu.Lock()
		s.closed.Store(true)
		s.mu.Unlock()
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	slog.Info("Shard.Close: starting", "db", s.db, "measurement", s.measurement, "dir", s.dir)

	// 先等待正在进行的异步 flush 完成，然后最终同步刷盘
	for s.memTable.IsFlushing() {
		s.mu.Unlock()
		time.Sleep(time.Millisecond)
		s.mu.Lock()
	}

	// 使用 flushLocked 统一处理（Swap → writeSSTableSync → ClearPassive）
	flushErr := s.flushLocked()
	if flushErr != nil {
		// 即使刷盘失败也继续关闭 WAL
		if s.wal != nil {
			if closeErr := s.wal.Close(); closeErr != nil {
				slog.Warn("wal close failed after memtable flush error",
					"flushErr", flushErr, "walCloseErr", closeErr)
			}
		}
		return fmt.Errorf("flush memtable: %w", flushErr)
	}

	// WAL 清理
	slog.Info("Shard.Close: flushed, about to close and purge WAL", "wal", s.wal != nil)
	if s.wal != nil {
		if closeErr := s.wal.Close(); closeErr != nil {
			slog.Warn("failed to close WAL", "error", closeErr)
		}
		if purgeErr := s.wal.Purge(); purgeErr != nil {
			slog.Warn("failed to purge WAL", "error", purgeErr)
		}
		slog.Info("Shard.Close: WAL closed and purged")
	}

	// 标记关闭，阻止新的后台 compaction 触发
	s.closed.Store(true)

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
