package engine

import (
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/internal/storage/unordered"
	"codeberg.org/micro-ts/mts/internal/storage/wal"
)

const (
	flushCooldown = 3 * time.Second
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
	compression sstable.CompressionAlgorithm
	lastFlush   time.Time
	mu          sync.Mutex
	closed      bool
	stopCh      chan struct{}
	stopOnce    sync.Once
}

// NewFlushCoordinator 创建新的 FlushCoordinator。
func NewFlushCoordinator(mt *memtable.MemTable, w *wal.WAL, flusher Flusher, compactor Compactor, dataDir string, compression sstable.CompressionAlgorithm) *FlushCoordinator {
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

// checkAndFlush 检查触发条件并执行刷盘。
func (fc *FlushCoordinator) checkAndFlush() {
	if fc.memTable.IsFlushing() {
		return
	}

	// 空闲超时：不检查冷却时间
	if fc.memTable.IdleExceeded() {
		_ = fc.doFlush()
		return
	}

	// NearFull：需要检查冷却时间
	if !fc.memTable.NearFull() {
		return
	}

	fc.mu.Lock()
	if time.Since(fc.lastFlush) < flushCooldown {
		fc.mu.Unlock()
		return
	}
	fc.mu.Unlock()

	_ = fc.doFlush()
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
	_ = fc.flusher.Flush(passive)

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

	// 销毁已刷盘的 WAL 段
	_ = fc.wal.TruncateBefore(fc.wal.SegmentNum() + 1)

	return nil
}

// FlushAll 强制刷写所有数据。
func (fc *FlushCoordinator) FlushAll() error {
	return fc.doFlush()
}

// Close 停止周期性检查。
func (fc *FlushCoordinator) Close() {
	fc.stopOnce.Do(func() {
		fc.mu.Lock()
		fc.closed = true
		fc.mu.Unlock()
		close(fc.stopCh)
	})
}

// MemTable 返回全局 MemTable。
func (fc *FlushCoordinator) MemTable() *memtable.MemTable {
	return fc.memTable
}
