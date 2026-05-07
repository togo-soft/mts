package shard

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

// ShouldCompact 检查是否应该触发 compaction。
func (cm *CompactionManager) ShouldCompact() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	return cm.shouldCompactLocked()
}

// shouldCompactLocked 检查是否应该触发 compaction（已持有锁）。
func (cm *CompactionManager) shouldCompactLocked() bool {
	files, err := cm.collectSSTables()
	if err != nil {
		return false
	}

	if len(files) < cm.config.MaxSSTableCount {
		return false
	}

	shardSize, err := cm.calculateShardSize()
	if err != nil {
		return false
	}

	if shardSize >= cm.config.ShardSizeLimit {
		slog.Info("shard size exceeds limit, skipping compaction",
			"shard", cm.shard.Dir(),
			"size", shardSize,
			"limit", cm.config.ShardSizeLimit)
		return false
	}

	return true
}

// ShouldCompactWithLock 检查是否应该触发 compaction（调用者需持有锁）。
func (cm *CompactionManager) ShouldCompactWithLock() bool {
	return cm.shouldCompactLocked()
}

// calculateShardSize 计算 Shard 数据目录总大小。
func (cm *CompactionManager) calculateShardSize() (int64, error) {
	dataDir := filepath.Join(cm.shard.Dir(), "data")
	var totalSize int64

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "sst_") {
			continue
		}
		sstPath := filepath.Join(dataDir, entry.Name())
		size, err := dirSize(sstPath)
		if err != nil {
			continue
		}
		totalSize += size
	}

	return totalSize, nil
}

// dirSize 计算目录大小（递归）。
func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// StartPeriodicCheck 启动定期检查。
func (cm *CompactionManager) StartPeriodicCheck() {
	if cm.config.CheckInterval <= 0 {
		return
	}

	cm.ticker = time.NewTicker(cm.config.CheckInterval)
	ticker := cm.ticker
	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		for {
			select {
			case <-ticker.C:
				cm.doPeriodicCompaction()
			case <-cm.stopCh:
				ticker.Stop()
				return
			}
		}
	}()
}

// Stop 停止定期检查。
func (cm *CompactionManager) Stop() {
	cm.stopOnce.Do(func() {
		close(cm.stopCh)
	})
	cm.wg.Wait()
}

// doPeriodicCompaction 定时执行的 compaction。
func (cm *CompactionManager) doPeriodicCompaction() {
	if !cm.tryAcquireCompactLock() {
		return
	}
	defer cm.releaseCompactLock()

	ctx, cancel := context.WithTimeout(context.Background(), cm.config.Timeout)
	defer cancel()

	if !cm.shouldCompactLocked() {
		return
	}

	_, _, err := cm.Compact(ctx)
	if err != nil {
		slog.Error("periodic compaction failed", "error", err)
	}

	cm.resetTimer()
}

// tryAcquireCompactLock 尝试获取 compaction 执行锁。
func (cm *CompactionManager) tryAcquireCompactLock() bool {
	return atomic.CompareAndSwapInt32(&cm.compactInProgress, 0, 1)
}

// releaseCompactLock 释放 compaction 执行锁。
func (cm *CompactionManager) releaseCompactLock() {
	atomic.StoreInt32(&cm.compactInProgress, 0)
}

// resetTimer 重置定时器。
func (cm *CompactionManager) resetTimer() {
	cm.compactMu.Lock()
	cm.lastCompact = time.Now()
	cm.compactMu.Unlock()

	if cm.ticker != nil {
		cm.ticker.Reset(cm.config.CheckInterval)
	}
}
