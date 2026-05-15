package shard

import (
	"log/slog"
	"path/filepath"
)

// Close 关闭 Shard，释放资源。
//
// 关闭流程：
//
//  1. 使用 sync.Once 确保 Close 只执行一次
//  2. 标记 closed，阻止新的后台 compaction
//  3. 停止 Compaction Manager
//  4. 停止 Level Compaction Manager
//  5. 等待所有后台 compaction goroutine 完成
func (s *Shard) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed.Store(true)
		s.mu.Unlock()

		if s.compaction != nil {
			s.compaction.Stop()
		}
		if s.levelCompaction != nil {
			s.levelCompaction.Stop()
		}
		s.compactionWg.Wait()

		slog.Debug("Shard.Close: completed")
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
