package engine

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/internal/metrics"
)

// FlushCoordinator 编排 Writer → Flusher 的异步刷盘流程。
// 管理 Writer 注册、查询和同步刷盘。
type FlushCoordinator struct {
	writers             map[string]Writer    // key: "db/meas"
	lastFlushAt         map[string]time.Time // 上次刷盘完成时间，用于冷却期判断
	lastMu              sync.Mutex           // 保护 lastFlushAt
	flusher             Flusher
	mu                  sync.RWMutex
	closed              bool
	stopCh              chan struct{}
	stopOnce            sync.Once
	flushAllInProgress  atomic.Bool // 标记 FlushAll 正在执行，防止 checkAndFlush 并发冲突
}

const flushSpinTimeout = 30 * time.Second // IsFlushing 自旋等待超时

const flushCooldown = 3 * time.Second // 刷盘冷却期，防止背靠背刷盘导致 I/O 竞争

// NewFlushCoordinator 创建新的 FlushCoordinator。
func NewFlushCoordinator(flusher Flusher) *FlushCoordinator {
	return &FlushCoordinator{
		writers:     make(map[string]Writer),
		lastFlushAt: make(map[string]time.Time),
		flusher:     flusher,
		stopCh:      make(chan struct{}),
	}
}

// StartPeriodicCheck 启动周期性自动检查，当 ShouldSwap 为 true 时触发刷盘。
// interval 为检查间隔，建议 1s。
func (fc *FlushCoordinator) StartPeriodicCheck(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fc.checkAndFlush()
			case <-fc.stopCh:
				return
			}
		}
	}()
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

	return fc.flushWriterLocked(db, measurement, key, w)
}

// FlushAll 同步刷写所有 Writer 的 MemTable。
func (fc *FlushCoordinator) FlushAll() error {
	fc.flushAllInProgress.Store(true)
	defer fc.flushAllInProgress.Store(false)

	fc.mu.RLock()
	defer fc.mu.RUnlock()

	var firstErr error
	for key, w := range fc.writers {
		parts := strings.SplitN(key, "/", 2)
		if len(parts) != 2 {
			continue
		}
		if err := fc.flushWriterLocked(parts[0], parts[1], key, w); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// CloseAllWriters 关闭所有注册的 Writer。
func (fc *FlushCoordinator) CloseAllWriters() error {
	fc.mu.Lock()
	fc.closed = true
	fc.mu.Unlock()

	fc.stopOnce.Do(func() { close(fc.stopCh) })

	fc.mu.Lock()
	defer fc.mu.Unlock()

	var firstErr error
	for _, w := range fc.writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// checkAndFlush 检查所有已注册 Writer 的状态，按需触发刷盘。
//
// 触发条件：
//   - NearFull 且自上次刷盘完成已超过冷却期（3s），防止刷盘 I/O 与写入 I/O 持续竞争
//   - IdleExceeded：空闲超时立即触发，不受冷却期限制（无写入竞争）
func (fc *FlushCoordinator) checkAndFlush() {
	if fc.flushAllInProgress.Load() {
		return
	}

	fc.mu.RLock()
	if fc.closed {
		fc.mu.RUnlock()
		return
	}

	type entry struct {
		db, meas string
		w        Writer
		key      string
	}
	entries := make([]entry, 0, len(fc.writers))
	now := time.Now()
	for key, w := range fc.writers {
		parts := strings.SplitN(key, "/", 2)
		if len(parts) != 2 {
			continue
		}
		entries = append(entries, entry{parts[0], parts[1], w, key})
	}
	fc.mu.RUnlock()

	for _, e := range entries {
		mt := e.w.MemTable()
		if mt.IsFlushing() {
			continue
		}
		if mt.IdleExceeded() {
			_ = fc.flushWriterLocked(e.db, e.meas, e.key, e.w)
			continue
		}
		fc.lastMu.Lock()
		lastAt := fc.lastFlushAt[e.key]
		fc.lastMu.Unlock()
		if mt.NearFull() && now.Sub(lastAt) >= flushCooldown {
			_ = fc.flushWriterLocked(e.db, e.meas, e.key, e.w)
		}
	}
}

// flushWriterLocked 内部同步刷写。
// flushKey 是 "db/meas" 格式的键，用于更新冷却期记录。
func (fc *FlushCoordinator) flushWriterLocked(db, measurement, flushKey string, w Writer) error {
	spinDeadline := time.Now().Add(flushSpinTimeout)
	for w.MemTable().IsFlushing() {
		if time.Now().After(spinDeadline) {
			return fmt.Errorf("timeout waiting for flush to complete on %s/%s (30s)", db, measurement)
		}
		time.Sleep(time.Millisecond)
	}

	passive := w.MemTable().Swap()
	if len(passive) == 0 {
		w.MemTable().ClearPassive()
		return nil
	}

	if err := fc.flusher.Flush(db, measurement, passive); err != nil {
		w.MemTable().MergePassiveBack()
		return err
	}

	w.MemTable().ClearPassive()

	fc.lastMu.Lock()
	fc.lastFlushAt[flushKey] = time.Now()
	fc.lastMu.Unlock()

	metrics.Incr(metrics.FlushTotal, 1)
	metrics.Incr(metrics.FlushPoints, int64(len(passive)))

	return nil
}
