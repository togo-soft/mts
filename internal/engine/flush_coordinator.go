package engine

import (
	"log/slog"
	"strings"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/metrics"
)

// FlushCoordinator 编排 Writer → Flusher 的异步刷盘流程。
// 管理 Writer 注册、查询和同步刷盘。
type FlushCoordinator struct {
	writers map[string]Writer // key: "db/meas"
	flusher Flusher
	mu      sync.RWMutex
	closed  bool
}

// NewFlushCoordinator 创建新的 FlushCoordinator。
func NewFlushCoordinator(flusher Flusher) *FlushCoordinator {
	return &FlushCoordinator{
		writers: make(map[string]Writer),
		flusher: flusher,
	}
}

// RegisterWriter 注册一个 Writer（按 db/meas 索引）。
func (fc *FlushCoordinator) RegisterWriter(db, measurement string, w Writer) {
	key := db + "/" + measurement
	fc.mu.Lock()
	fc.writers[key] = w
	fc.mu.Unlock()
}

// GetWriter 获取已注册的 Writer（不创建新的）。
func (fc *FlushCoordinator) GetWriter(db, measurement string) Writer {
	key := db + "/" + measurement
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return fc.writers[key]
}

// FlushWriter 同步刷写指定 measurement 的 MemTable 到 SSTable。
// 调用者负责确保 Writer 已注册且未关闭。
func (fc *FlushCoordinator) FlushWriter(db, measurement string) error {
	key := db + "/" + measurement
	fc.mu.RLock()
	w, ok := fc.writers[key]
	fc.mu.RUnlock()
	if !ok {
		return nil
	}

	return fc.flushWriterLocked(db, measurement, w)
}

// FlushAll 同步刷写所有 Writer 的 MemTable。
func (fc *FlushCoordinator) FlushAll() error {
	fc.mu.RLock()
	defer fc.mu.RUnlock()

	var firstErr error
	for key, w := range fc.writers {
		parts := strings.SplitN(key, "/", 2)
		if len(parts) != 2 {
			continue
		}
		if err := fc.flushWriterLocked(parts[0], parts[1], w); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// CloseAllWriters 关闭所有注册的 Writer。
func (fc *FlushCoordinator) CloseAllWriters() error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.closed = true
	var firstErr error
	for _, w := range fc.writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// flushWriterLocked 内部同步刷写（调用者持有锁或保证线程安全）。
func (fc *FlushCoordinator) flushWriterLocked(db, measurement string, w Writer) error {
	// 等待正在进行的 flush 完成
	waitStart := time.Now()
	for w.MemTable().IsFlushing() {
		time.Sleep(time.Millisecond)
		if time.Since(waitStart) > 3*time.Second {
			slog.Warn("flushWriterLocked: IsFlushing timeout waiting", "db", db, "meas", measurement)
		}
	}
	if time.Since(waitStart) > time.Second {
		slog.Info("flushWriterLocked: waited for IsFlushing", "db", db, "meas", measurement, "waited", time.Since(waitStart))
	}

	passive := w.MemTable().Swap()
	if len(passive) == 0 {
		w.MemTable().ClearPassive()
		return nil
	}

	flushStart := time.Now()
	if err := fc.flusher.Flush(db, measurement, passive); err != nil {
		slog.Error("flushWriterLocked: Flush failed", "db", db, "meas", measurement, "error", err)
		w.MemTable().MergePassiveBack()
		return err
	}
	if time.Since(flushStart) > time.Second {
		slog.Info("flushWriterLocked: Flush completed", "db", db, "meas", measurement, "duration", time.Since(flushStart))
	}

	w.MemTable().ClearPassive()

	metrics.Incr(metrics.FlushTotal, 1)
	metrics.Incr(metrics.FlushPoints, int64(len(passive)))

	return nil
}
