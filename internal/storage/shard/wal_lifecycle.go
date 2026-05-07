package shard

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// StartPeriodicSync 启动定期同步的 goroutine。
func (w *WAL) StartPeriodicSync(interval time.Duration, done <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		for {
			select {
			case <-ticker.C:
				if err := w.Sync(); err != nil {
					w.logger.Error("wal sync failed", slog.Any("error", err))
				}
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()
}

// Sequence 返回当前 WAL 的序列号。
func (w *WAL) Sequence() uint64 {
	return w.seq
}

// Close 关闭 WAL，释放资源。
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushLocked(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

// Cleanup 删除旧的 WAL 文件。
func (w *WAL) Cleanup() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	pattern := filepath.Join(w.dir, "*.wal")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	for _, path := range matches {
		filename := filepath.Base(path)
		seqStr := filename[:len(filename)-4]
		var seq uint64
		if _, err := fmt.Sscanf(seqStr, "%020d", &seq); err != nil {
			continue
		}
		if seq < w.seq {
			if err := os.Remove(path); err != nil {
				w.logger.Warn("failed to remove old WAL file", "path", path, "error", err)
			}
		}
	}
	return nil
}

// TruncateCurrent 清空当前 WAL 文件。
func (w *WAL) TruncateCurrent() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushLocked(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}

	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	w.fileSize = 0
	return nil
}
