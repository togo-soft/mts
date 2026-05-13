package memtable

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/types"
)

// MemTableConfig 是 types.MemTableConfig 的别名，用于 memtable 包。
type MemTableConfig = types.MemTableConfig

// DefaultMemTableConfig 返回默认配置
func DefaultMemTableConfig() *MemTableConfig {
	return &MemTableConfig{
		MaxSize:           64 * 1024 * 1024, // 64MB
		MaxCount:          3000,
		IdleDurationNanos: int64(time.Minute),
	}
}

// MemTable 是内存中的写入缓冲区，使用 active/passive 双缓冲设计。
//
// active 接收新写入，passive 等待后台 flush。
// 写入路径仅操作 active（持锁），flush 在后台 goroutine 中处理 passive。
type MemTable struct {
	mu          sync.RWMutex
	active      []types.InternalPoint
	passive     []types.InternalPoint
	flushing    atomic.Bool
	maxSize     int64
	maxCount    int
	idleTimeout time.Duration
	lastWrite   time.Time
	activeCount int
	sorted      bool
}

// NewMemTable 创建新的 MemTable 实例。
func NewMemTable(cfg *MemTableConfig) *MemTable {
	return &MemTable{
		active:      make([]types.InternalPoint, 0, 1024),
		maxSize:     cfg.MaxSize,
		maxCount:    int(cfg.MaxCount),
		idleTimeout: time.Duration(cfg.IdleDurationNanos),
		lastWrite:   time.Now(),
	}
}

// Write 写入 InternalPoint 到 active。
func (m *MemTable) Write(ip types.InternalPoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fields := make([]types.InternalField, len(ip.Fields))
	copy(fields, ip.Fields)

	m.active = append(m.active, types.InternalPoint{
		Timestamp: ip.Timestamp,
		Fields:    fields,
		Sid:       ip.Sid,
	})
	m.activeCount++
	m.lastWrite = time.Now()

	if !m.sorted || (m.activeCount > 1 && m.active[m.activeCount-1].Timestamp < m.active[m.activeCount-2].Timestamp) {
		m.sortActive()
	}
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
	estimatedSize := int64(len(m.active)) * 1024
	if estimatedSize >= m.maxSize {
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
func (m *MemTable) Flush() []types.InternalPoint {
	return m.Swap()
}

// Swap 交换 active 和 passive。返回旧的 active（现为 passive）用于 flush。
//
// 如果 passive 仍有未处理数据（上次 swap 未 clear/merge），会先合并回 active 再 swap，
// 确保数据不丢失。正常流程中 passive 应在 swap 前已被 ClearPassive 或 MergePassiveBack 清空。
func (m *MemTable) Swap() []types.InternalPoint {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 安全兜底：如果 passive 还有数据，合并回 active（防止竞态导致数据丢失）
	if len(m.passive) > 0 {
		m.active = append(m.passive, m.active...)
		m.passive = nil
		m.activeCount = len(m.active)
		m.sortActive()
	}

	if len(m.active) == 0 {
		m.flushing.Store(false)
		return nil
	}

	m.passive = m.active
	m.active = make([]types.InternalPoint, 0, 1024)
	m.activeCount = 0
	m.sorted = false
	m.flushing.Store(true)

	return m.passive
}

// ClearPassive 在后台 flush 成功后清空 passive，释放内存。
func (m *MemTable) ClearPassive() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.passive = nil
	m.flushing.Store(false)
}

// MergePassiveBack 将 flush 失败的 passive 数据合并回 active。
func (m *MemTable) MergePassiveBack() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.passive) == 0 {
		m.flushing.Store(false)
		return
	}

	// 将 passive 插入 active 前面，然后整体排序
	m.active = append(m.passive, m.active...)
	m.passive = nil
	m.activeCount = len(m.active)
	m.sortActive()
	m.flushing.Store(false)
}

// IsFlushing 返回是否有后台 flush 进行中。
func (m *MemTable) IsFlushing() bool {
	return m.flushing.Load()
}

// TrySetFlushing CAS 设置 flushing 标志，返回是否成功获取。
func (m *MemTable) TrySetFlushing() bool {
	return m.flushing.CompareAndSwap(false, true)
}

// ClearFlushing 清除 flushing 标志。
func (m *MemTable) ClearFlushing() {
	m.flushing.Store(false)
}

// ActiveFull 检查 active 是否超过硬限制（2x maxSize/maxCount），需要背压。
func (m *MemTable) ActiveFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	estimatedSize := int64(len(m.active)) * 1024
	return estimatedSize >= m.maxSize*2 || (m.maxCount > 0 && m.activeCount >= m.maxCount*2)
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

// IdleTimeout 返回空闲超时配置。
func (m *MemTable) IdleTimeout() time.Duration {
	return m.idleTimeout
}

// MemTableIterator 迭代器，二路归并 active 和 passive。
type MemTableIterator struct {
	active     []types.InternalPoint
	passive    []types.InternalPoint
	idxA       int // active 中下一条待读取的位置
	idxP       int // passive 中下一条待读取的位置
	current    types.InternalPoint
	hasCurrent bool
}

// Next 移动到下一个条目（按 timestamp 升序归并）。
func (it *MemTableIterator) Next() bool {
	aHas := it.idxA < len(it.active)
	pHas := it.idxP < len(it.passive)

	if aHas && pHas {
		if it.active[it.idxA].Timestamp <= it.passive[it.idxP].Timestamp {
			it.current = it.active[it.idxA]
			it.idxA++
		} else {
			it.current = it.passive[it.idxP]
			it.idxP++
		}
		it.hasCurrent = true
		return true
	}
	if aHas {
		it.current = it.active[it.idxA]
		it.idxA++
		it.hasCurrent = true
		return true
	}
	if pHas {
		it.current = it.passive[it.idxP]
		it.idxP++
		it.hasCurrent = true
		return true
	}
	it.hasCurrent = false
	return false
}

// Point 返回当前位置的 InternalPoint。
func (it *MemTableIterator) Point() types.InternalPoint {
	if !it.hasCurrent {
		return types.InternalPoint{}
	}
	return it.current
}
