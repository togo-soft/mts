package shard

import (
	"fmt"
	"log/slog"
	"path/filepath"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
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
		// 1. 先停止 MemTable 定期刷盘检查，避免与 s.mu 形成死锁
		// 注意：这里需要在获取 s.mu 之前停止定时任务，因为 doPeriodicFlush
		// 会尝试获取 s.mu，如果我们在持有 s.mu 时等待 flushWg，
		// 而定时任务恰好在执行 doPeriodicFlush，会导致死锁。
		if s.flushDone != nil {
			close(s.flushDone)
			s.flushWg.Wait()
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		slog.Info("Shard.Close: starting", "db", s.db, "measurement", s.measurement, "dir", s.dir)

		flushed := false

		// 2. 先刷写 MemTable 到 SSTable
		// 如果使用 Level Compaction，调用 flushLocked 以保持一致的处理逻辑
		if s.levelCompaction != nil {
			slog.Info("Shard.Close: using level compaction flush", "memTableCount", s.memTable.Count())
			if flushErr := s.flushLocked(); flushErr != nil {
				// 即使失败也要继续关闭 WAL
				if s.wal != nil {
					if closeErr := s.wal.Close(); closeErr != nil {
						slog.Warn("wal close failed after memtable flush error",
							"flushErr", flushErr, "walCloseErr", closeErr)
					}
				}
				err = fmt.Errorf("flush memtable: %w", flushErr)
				return
			}
			flushed = true
			slog.Info("Shard.Close: level compaction flush completed")
		} else {
			// 平坦 Compaction 的刷盘逻辑
			points := s.memTable.Flush()
			slog.Info("Shard.Close: flat compaction flush", "pointsCount", len(points))
			if len(points) > 0 {
				w, wErr := sstable.NewWriter(s.dir, s.sstSeq, 0)
				if wErr != nil {
					// 即使 writer 创建失败，也要继续关闭 WAL
					if s.wal != nil {
						if closeErr := s.wal.Close(); closeErr != nil {
							slog.Warn("wal close failed after writer create error",
								"writerErr", wErr, "walCloseErr", closeErr)
						}
					}
					err = fmt.Errorf("create sstable writer: %w", wErr)
					return
				}
				s.sstSeq++

				if writeErr := w.WritePoints(points); writeErr != nil {
					_ = w.Close()
					if s.wal != nil {
						if closeErr := s.wal.Close(); closeErr != nil {
							slog.Warn("wal close failed after write error",
								"writeErr", writeErr, "walCloseErr", closeErr)
						}
					}
					err = fmt.Errorf("write points to sstable: %w", writeErr)
					return
				}

				if closeErr := w.Close(); closeErr != nil {
					if s.wal != nil {
						if walCloseErr := s.wal.Close(); walCloseErr != nil {
							slog.Warn("wal close failed after writer close error",
								"writerCloseErr", closeErr, "walCloseErr", walCloseErr)
						}
					}
					err = fmt.Errorf("close sstable writer: %w", closeErr)
					return
				}
				flushed = true
			} else {
				flushed = true
			}
		}

		// 4. WAL 清理（仅当 flush 成功时）
		// 先调用 WAL.Close() 确保 periodic sync goroutine 退出并关闭 segment，
		// 然后调用 WAL.Purge() 删除 segment 文件。
		// WAL.Purge() 会正确处理 WAL 已关闭的情况。
		slog.Info("Shard.Close: flushed, about to close and purge WAL", "flushed", flushed, "wal", s.wal != nil)
		if flushed && s.wal != nil {
			if closeErr := s.wal.Close(); closeErr != nil {
				slog.Warn("failed to close WAL", "error", closeErr)
			}
			if purgeErr := s.wal.Purge(); purgeErr != nil {
				slog.Warn("failed to purge WAL", "error", purgeErr)
			}
			slog.Info("Shard.Close: WAL closed and purged")
		}

		// 6. 标记关闭，阻止新的后台 compaction 触发
		s.closed.Store(true)

		// 7. 停止 Compaction Manager（阻止新的周期性触发）
		if s.compaction != nil {
			s.compaction.Stop()
		}

		// 8. 停止 Level Compaction Manager
		if s.levelCompaction != nil {
			s.levelCompaction.Stop()
		}

		// 9. 等待所有后台 compaction goroutine 完成
		s.compactionWg.Wait()

		slog.Info("Shard.Close: completed")
	})
	return err
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
