package engine

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/unordered"
	"codeberg.org/micro-ts/mts/internal/storage/wal"
	"codeberg.org/micro-ts/mts/types"
)

const (
	flushCooldown     = 500 * time.Millisecond
	maxUnorderedFiles = 50 // 无序文件超过此阈值时跳过 flush，等待 compaction 消化
)

// Compactor 是 flush 后需要执行的 unordered→L0 compaction 接口。
type Compactor interface {
	Compact() error
}

// FlushCoordinator 编排全局 MemTable → unordered 的异步刷盘流程。
type FlushCoordinator struct {
	memTable    *memtable.MemTable
	wal         *wal.WAL
	flusher     Flusher
	compactor   Compactor
	dataDir     string
	compression types.CompressionAlgorithm
	lastFlush   time.Time
	mu          sync.Mutex
	closed      bool
	stopCh      chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
	pendingSeg  atomic.Uint64
}

// NewFlushCoordinator 创建新的 FlushCoordinator。
func NewFlushCoordinator(mt *memtable.MemTable, w *wal.WAL, flusher Flusher, compactor Compactor, dataDir string, compression types.CompressionAlgorithm) *FlushCoordinator {
	return &FlushCoordinator{
		memTable:    mt,
		wal:         w,
		flusher:     flusher,
		compactor:   compactor,
		dataDir:     dataDir,
		compression: compression,
		stopCh:      make(chan struct{}),
	}
}

// StartPeriodicCheck 启动周期性检查（每 1 秒检查是否需要刷盘）。
func (fc *FlushCoordinator) StartPeriodicCheck(interval time.Duration) {
	fc.wg.Add(1)
	go func() {
		defer fc.wg.Done()
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

// checkAndFlush 检查触发条件并执行刷盘。
func (fc *FlushCoordinator) checkAndFlush() {
	if fc.memTable.IsFlushing() {
		return
	}

	// 空闲超时：不检查冷却时间
	if fc.memTable.IdleExceeded() {
		if err := fc.doFlush(); err != nil {
			slog.Warn("idle flush failed", "error", err)
		}
		return
	}

	// NearFull：需要检查冷却时间
	if !fc.memTable.NearFull() {
		return
	}

	// 无序文件反压：当堆积超过阈值时跳过 flush，等待 compaction 消化。
	// 若 memtable 持续满，Write() 会被阻塞，形成自然的端到端反压。
	files, err := unordered.ListFiles(fc.dataDir)
	if err != nil {
		slog.Warn("failed to list unordered files, skipping flush", "error", err)
		return
	}
	if len(files) >= maxUnorderedFiles {
		return
	}

	fc.mu.Lock()
	if time.Since(fc.lastFlush) < flushCooldown {
		fc.mu.Unlock()
		return
	}
	fc.mu.Unlock()

	if err := fc.doFlush(); err != nil {
		slog.Warn("near-full flush failed", "error", err)
	}
}

// doFlush 执行实际的刷盘操作。
func (fc *FlushCoordinator) doFlush() error {
	if !fc.memTable.TrySetFlushing() {
		return nil
	}
	defer fc.memTable.ClearFlushing()

	passive := fc.memTable.Swap()
	if len(passive) == 0 {
		return nil
	}

	// 调用 Flusher.Flush（当前为桩，实际在 unordered 写入中处理）
	if err := fc.flusher.Flush(passive); err != nil {
		slog.Warn("flusher.Flush failed", "error", err)
	}

	// 写入 unordered 目录
	_, err := unordered.Write(fc.dataDir, passive, fc.compression)
	if err != nil {
		fc.memTable.MergePassiveBack()
		return err
	}

	fc.memTable.ClearPassive()

	// 更新最后刷盘时间
	fc.mu.Lock()
	fc.lastFlush = time.Now()
	fc.mu.Unlock()

	// 销毁已刷盘的 WAL 段，但需保护 pending entry 所在段不被回收
	truncateBefore := fc.wal.SegmentNum() + 1
	if pending := fc.pendingSeg.Load(); pending > 0 && pending < truncateBefore {
		truncateBefore = pending
	}
	if err := fc.wal.TruncateBefore(truncateBefore); err != nil {
		slog.Warn("failed to truncate WAL after flush", "error", err)
	}

	return nil
}

// FlushAll 强制刷写所有数据。
func (fc *FlushCoordinator) FlushAll() error {
	return fc.doFlush()
}

// Close 停止周期性检查并等待 goroutine 退出。
func (fc *FlushCoordinator) Close() {
	fc.stopOnce.Do(func() {
		fc.mu.Lock()
		fc.closed = true
		fc.mu.Unlock()
		close(fc.stopCh)
	})
	fc.wg.Wait()
}

// MemTable 返回全局 MemTable。
func (fc *FlushCoordinator) MemTable() *memtable.MemTable {
	return fc.memTable
}

// SetPendingSeg 设置 pending WAL segment，该 segment 内的 entry 不会被 truncation。
// 用于 MemTable 写入失败但 WAL 已写入的场景，保证重启后 WAL replay 可恢复数据。
func (fc *FlushCoordinator) SetPendingSeg(seg uint64) {
	fc.pendingSeg.Store(seg)
}
