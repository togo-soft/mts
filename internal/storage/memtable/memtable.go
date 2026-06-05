package memtable

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/internal/metrics"
	"codeberg.org/micro-ts/mts/types"
)

// MemTableConfig 是 types.MemTableConfig 的别名，用于 memtable 包。
type MemTableConfig = types.MemTableConfig

// DefaultMemTableConfig 返回默认配置
func DefaultMemTableConfig() *MemTableConfig {
	return &MemTableConfig{
		FlushMemorySize: 64 * 1024 * 1024, // 64MB
		FlushPointCount: 50000,
		FlushIdleNanos:  int64(time.Minute),
	}
}

// MemTable 是内存中的写入缓冲区，使用 active/passive 双缓冲设计。
//
// active 接收新写入，passive 等待后台 flush。
// 写入路径仅操作 active（持锁），flush 在后台 goroutine 中处理 passive。
type MemTable struct {
	mu          sync.RWMutex
	active      []types.MemPoint
	passive     []types.MemPoint
	flushing    atomic.Bool
	maxSize     int64
	maxCount    int
	idleTimeout time.Duration
	lastWrite   time.Time
	activeCount int
	sorted      bool
	flushMu     sync.Mutex
	flushCond   *sync.Cond
	activeBytes int64
}

const memPointBaseSize = 128 // MemPoint 结构体 + 字符串/切片头部的近似开销

// NewMemTable 创建新的 MemTable 实例。
func NewMemTable(cfg *MemTableConfig) *MemTable {
	mt := &MemTable{
		active:      make([]types.MemPoint, 0, 1024),
		maxSize:     cfg.FlushMemorySize,
		maxCount:    int(cfg.FlushPointCount),
		idleTimeout: time.Duration(cfg.FlushIdleNanos),
		lastWrite:   time.Now(),
	}
	mt.flushCond = sync.NewCond(&mt.flushMu)
	return mt
}

// Write 写入 MemPoint 到 active。直接接管 FieldData 所有权，无需拷贝。
func (m *MemTable) Write(mp types.MemPoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.active = append(m.active, mp)
	m.activeCount++
	m.activeBytes += memPointBaseSize + int64(len(mp.Database)+len(mp.Measurement)+len(mp.FieldData))
	m.lastWrite = time.Now()

	// 快速路径：时间戳单调递增（最常见场景），跳过排序
	if m.activeCount <= 1 || mp.Timestamp >= m.active[m.activeCount-2].Timestamp {
		m.sorted = true
		return nil
	}
	// 乱序插入：全量重排
	m.sortActive()
	m.sorted = true
	return nil
}

// Count 返回 active 中的条目数。
func (m *MemTable) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeCount
}

// ActiveCount 返回 active 中的条目数（导出用于外部检查）。
func (m *MemTable) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeCount
}

// PassiveCount 返回 passive 中的条目数，用于查询时检查是否有未刷盘的 swap 数据。
func (m *MemTable) PassiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.passive)
}

// ShouldFlush 检查 MemTable 是否满足刷盘条件（向后兼容）。
func (m *MemTable) ShouldFlush() bool {
	return m.ShouldSwap()
}

// ShouldSwap 检查 active 是否需要交换到 passive。
func (m *MemTable) ShouldSwap() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.shouldSwapUnsafe()
}

func (m *MemTable) shouldSwapUnsafe() bool {
	if m.flushing.Load() {
		return false
	}
	if m.activeBytes >= m.maxSize {
		return true
	}
	if m.maxCount > 0 && m.activeCount >= m.maxCount {
		return true
	}
	if m.idleTimeout > 0 && m.activeCount > 0 {
		if time.Since(m.lastWrite) >= m.idleTimeout {
			return true
		}
	}
	return false
}

// Flush 将 active 交换到 passive 并返回 passive（向后兼容）。
// 调用者应确保当前无 flush 进行中。
func (m *MemTable) Flush() []types.MemPoint {
	return m.Swap()
}

// Swap 交换 active 和 passive。返回旧的 active（现为 passive）用于 flush。
//
// 如果 passive 仍有未处理数据（上次 swap 未 clear/merge），会先合并回 active 再 swap，
// 确保数据不丢失。正常流程中 passive 应在 swap 前已被 ClearPassive 或 MergePassiveBack 清空。
func (m *MemTable) Swap() []types.MemPoint {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 安全兜底：如果 passive 还有数据，合并回 active（防止竞态导致数据丢失）
	if len(m.passive) > 0 {
		m.active = append(m.passive, m.active...)
		m.passive = nil
		m.activeCount = len(m.active)
		m.recalcActiveBytes()
		m.sortActive()
	}

	if len(m.active) == 0 {
		m.flushing.Store(false)
		return nil
	}

	m.passive = m.active
	// 使用上一次 active 的实际长度作为新 active 的初始容量
	// 既避免小容量频繁扩容，也避免为新 shard 过度预分配
	newCap := len(m.passive)
	if newCap < 1024 {
		newCap = 1024
	}
	m.active = make([]types.MemPoint, 0, newCap)
	m.activeCount = 0
	m.activeBytes = 0
	m.sorted = false
	m.flushing.Store(true)

	metrics.Incr(metrics.MemTableSwapTotal, 1)

	return m.passive
}

// ClearPassive 在后台 flush 成功后清空 passive，释放内存。
// FieldData 由 unordered.Write 统一归还 pool，此处仅释放切片引用。
func (m *MemTable) ClearPassive() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.passive = nil
	m.flushing.Store(false)
	m.flushCond.Broadcast()
}

// MergePassiveBack 将 flush 失败的 passive 数据合并回 active。
func (m *MemTable) MergePassiveBack() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.passive) == 0 {
		m.flushing.Store(false)
		m.flushCond.Broadcast()
		return
	}

	// 将 passive 插入 active 前面，然后整体排序
	m.active = append(m.passive, m.active...)
	for _, mp := range m.passive {
		m.activeBytes += memPointBaseSize + int64(len(mp.Database)+len(mp.Measurement)+len(mp.FieldData))
	}
	m.passive = nil
	m.activeCount = len(m.active)
	m.sortActive()
	m.flushing.Store(false)
	m.flushCond.Broadcast()
}

// IsFlushing 返回是否有后台 flush 进行中。
func (m *MemTable) IsFlushing() bool {
	return m.flushing.Load()
}

// TrySetFlushing CAS 设置 flushing 标志，返回是否成功获取。
func (m *MemTable) TrySetFlushing() bool {
	return m.flushing.CompareAndSwap(false, true)
}

// ClearFlushing 清除 flushing 标志并通知等待的 goroutine。
func (m *MemTable) ClearFlushing() {
	m.flushing.Store(false)
	m.flushMu.Lock()
	m.flushCond.Broadcast()
	m.flushMu.Unlock()
}

// WaitNotFullNoCtx 阻塞等待直到可能已完成刷盘（条件变量通知）。
// 返回后调用方需重新检查 ActiveFull() 以确认状态。
func (m *MemTable) WaitNotFullNoCtx() {
	m.flushMu.Lock()
	m.flushCond.Wait()
	m.flushMu.Unlock()
}

// ActiveFull 检查 active 是否超过硬限制（5x maxSize/maxCount），需要背压。
func (m *MemTable) ActiveFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeBytes >= m.maxSize*5 || (m.maxCount > 0 && m.activeCount >= m.maxCount*5)
}

// NearFull 检查 active 是否接近容量上限（2x 阈值），用于触发预刷盘。
func (m *MemTable) NearFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.flushing.Load() {
		return false
	}
	return m.activeBytes >= m.maxSize*2 ||
		(m.maxCount > 0 && m.activeCount >= m.maxCount*2)
}

// IdleExceeded 检查自上次写入后空闲时间是否超过配置的 idle 超时。
func (m *MemTable) IdleExceeded() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.idleTimeout > 0 && m.activeCount > 0 &&
		time.Since(m.lastWrite) >= m.idleTimeout
}

// Sort 对 active 进行排序，确保数据有序。
// 用于 WAL Replay 后或任何需要防御性排序的场景。
func (m *MemTable) Sort() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activeCount > 1 {
		m.sortActive()
	} else {
		m.sorted = true
	}
}

// Iterator 返回合并 active 和 passive 的迭代器。
func (m *MemTable) Iterator() *MemTableIterator {
	m.mu.RLock()
	active := m.active
	passive := m.passive
	m.mu.RUnlock()

	return &MemTableIterator{
		active:  active,
		passive: passive,
	}
}

// sortActive 对 active 切片按 Timestamp 升序排序。
func (m *MemTable) sortActive() {
	sort.Slice(m.active, func(i, j int) bool {
		return m.active[i].Timestamp < m.active[j].Timestamp
	})
	m.sorted = true
}

func (m *MemTable) recalcActiveBytes() {
	var total int64
	for _, mp := range m.active {
		total += memPointBaseSize + int64(len(mp.Database)+len(mp.Measurement)+len(mp.FieldData))
	}
	m.activeBytes = total
}

// IdleTimeout 返回空闲超时配置。
func (m *MemTable) IdleTimeout() time.Duration {
	return m.idleTimeout
}

// MemTableIterator 迭代器，二路归并 active 和 passive。
type MemTableIterator struct {
	active     []types.MemPoint
	passive    []types.MemPoint
	idxA       int // active 中下一条待读取的位置
	idxP       int // passive 中下一条待读取的位置
	current    types.InternalPoint
	hasCurrent bool
	err        error
}

// Next 移动到下一个条目（按 timestamp 升序归并），惰性解码 MemPoint → InternalPoint。
func (it *MemTableIterator) Next() bool {
	var mp types.MemPoint
	aHas := it.idxA < len(it.active)
	pHas := it.idxP < len(it.passive)

	if aHas && pHas {
		if it.active[it.idxA].Timestamp <= it.passive[it.idxP].Timestamp {
			mp = it.active[it.idxA]
			it.idxA++
		} else {
			mp = it.passive[it.idxP]
			it.idxP++
		}
	} else if aHas {
		mp = it.active[it.idxA]
		it.idxA++
	} else if pHas {
		mp = it.passive[it.idxP]
		it.idxP++
	} else {
		it.hasCurrent = false
		return false
	}

	ip, err := types.MemPointToInternal(mp)
	if err != nil {
		it.err = err
		it.hasCurrent = false
		return false
	}
	it.current = ip
	it.hasCurrent = true
	return true
}

// Err 返回迭代过程中的解码错误。
func (it *MemTableIterator) Err() error {
	return it.err
}

// Point 返回当前位置的 InternalPoint。
func (it *MemTableIterator) Point() types.InternalPoint {
	if !it.hasCurrent {
		return types.InternalPoint{}
	}
	return it.current
}
