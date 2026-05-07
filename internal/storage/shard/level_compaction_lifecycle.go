package shard

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage"
)

// StartPeriodicCheck 启动定期检查。
func (lcm *LevelCompactionManager) StartPeriodicCheck() {
	if lcm.config.CheckInterval <= 0 {
		return
	}

	lcm.ticker = time.NewTicker(lcm.config.CheckInterval)
	ticker := lcm.ticker
	lcm.wg.Add(1)
	go func() {
		defer lcm.wg.Done()
		for {
			select {
			case <-ticker.C:
				lcm.doPeriodicCompaction()
			case <-lcm.stopCh:
				ticker.Stop()
				return
			}
		}
	}()
}

// Stop 停止定期检查。
func (lcm *LevelCompactionManager) Stop() {
	lcm.stopOnce.Do(func() {
		close(lcm.stopCh)
	})
	lcm.wg.Wait()
}

// doPeriodicCompaction 定时执行的 compaction。
func (lcm *LevelCompactionManager) doPeriodicCompaction() {
	if !atomic.CompareAndSwapInt32(&lcm.compactInProgress, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&lcm.compactInProgress, 0)

	if !lcm.ShouldCompact() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), lcm.config.Timeout)
	defer cancel()

	_, _, err := lcm.Compact(ctx)
	if err != nil {
		slog.Error("periodic compaction failed", "error", err)
	}
}

// Recover 启动时恢复检查。
func (lcm *LevelCompactionManager) Recover() error {
	dataDir := filepath.Join(lcm.shard.Dir(), "data")
	cp := &CompactionCheckpoint{}

	if err := cp.Load(dataDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("load checkpoint: %w", err)
	}

	if cp.OutputPath != "" {
		_ = os.RemoveAll(cp.OutputPath)
	}

	_ = cp.Clear(dataDir)

	slog.Info("cleaned up incomplete compaction", "level", cp.Level)
	return nil
}

// IsOldFormat 检测是否为旧的扁平结构。
func (lcm *LevelCompactionManager) IsOldFormat() bool {
	dataDir := filepath.Join(lcm.shard.Dir(), "data")

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "sst_") {
			return true
		}
	}

	return false
}

// MigrateFromOldFormat 从旧格式迁移。
func (lcm *LevelCompactionManager) MigrateFromOldFormat() error {
	dataDir := filepath.Join(lcm.shard.Dir(), "data")

	l0Dir := filepath.Join(dataDir, "L0")
	if err := storage.SafeMkdirAll(l0Dir, 0700); err != nil {
		return fmt.Errorf("create L0 dir: %w", err)
	}

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("read data dir: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "sst_") {
			continue
		}

		oldPath := filepath.Join(dataDir, entry.Name())
		newPath := filepath.Join(l0Dir, entry.Name())

		if err := os.Rename(oldPath, newPath); err != nil {
			slog.Warn("failed to migrate SSTable", "from", oldPath, "to", newPath, "error", err)
		}
	}

	if err := lcm.manifest.Save(); err != nil {
		return fmt.Errorf("save manifest: %w", err)
	}

	return nil
}
