// internal/storage/shard/memtable.go
package shard

import (
	"sort"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/types"
)

// MemTableConfig 是 types.MemTableConfig 的别名，用于 shard 包。
//
// 为了保持兼容性，我们保留此别名，但内部统一使用 types.MemTableConfig。
type MemTableConfig = types.MemTableConfig

// DefaultMemTableConfig 返回默认配置
func DefaultMemTableConfig() *MemTableConfig {
	return &MemTableConfig{
		MaxSize:           64 * 1024 * 1024, // 64MB
		MaxCount:          3000,
		IdleDurationNanos: int64(time.Minute),
	}
}

// entry 是 MemTable 中的条目
type entry struct {
	Point types.Point
}

// MemTable 是内存中的写入缓冲区，按时间戳排序存储数据点。
//
// 功能：
//
//   - 缓冲写入操作，批量刷盘
//   - 维护按时间排序的数据
//   - 支持顺序和迭代器遍历
//
// 字段说明：
//
//   - mu:          读写锁，保护所有字段
//   - entries:     存储的数据点条目
//   - maxSize:     触发刷盘的大小阈值
//   - maxCount:    触发刷盘的条目数阈值
//   - idleTimeout: 触发刷盘的空闲时间阈值
//   - lastWrite:   上次写入时间，用于空闲检测
//   - count:       当前条目数（缓存优化）
//   - sorted:      标记 entries 是否已排序（避免重复排序）
//
// 并发安全：
//
//	所有公共方法都是线程安全的。
//	内部使用锁保护，不建议在外部加锁。
//
// 使用模式：
//
//	写入：m.Write(point)
//	检查：m.ShouldFlush()
//	刷盘：points := m.Flush()
//	遍历：it := m.Iterator(); for it.Next() { ... }
type MemTable struct {
	mu          sync.RWMutex
	entries     []*entry
	maxSize     int64
	maxCount    int
	idleTimeout time.Duration
	lastWrite   time.Time // 上次写入时间
	count       int       // 当前条目数
	sorted      bool      // 标记是否已排序，避免重复排序
}

// NewMemTable 创建新的 MemTable 实例。
func NewMemTable(cfg *MemTableConfig) *MemTable {
	return &MemTable{
		entries:     make([]*entry, 0, 1024),
		maxSize:     cfg.MaxSize,
		maxCount:    int(cfg.MaxCount),
		idleTimeout: time.Duration(cfg.IdleDurationNanos),
		lastWrite:   time.Now(),
	}
}

// Write 写入一个数据点到 MemTable。
//
// 参数：
//   - p: 要写入的数据点
//
// 返回：
//   - error: 写入失败时返回错误
//
// 数据拷贝：
//
//	为保证数据独立性，会深拷贝 Tags 和 Fields 的 map。
//	避免客户端和存储层共享同一底层数据。
//
// 排序维护：
//
//	当检测到乱序写入（新时间小于前一个），触发全局排序。
//	频繁乱序写入会影响性能，建议尽量顺序写入。
func (m *MemTable) Write(p *types.Point) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 深拷贝：创建新的 map，切断对客户端 map 的引用
	// 避免客户端和服务端共享同一个 map 底层数据
	tags := make(map[string]string, len(p.Tags))
	for k, v := range p.Tags {
		tags[k] = v
	}

	fields := make(map[string]*types.FieldValue, len(p.Fields))
	for k, v := range p.Fields {
		// v 已经是 *types.FieldValue，直接复制指针
		// FieldValue 是不可变的，共享指针是安全的
		fields[k] = v
	}

	m.entries = append(m.entries, &entry{
		Point: types.Point{
			Database:    p.Database,
			Measurement: p.Measurement,
			Tags:        tags,
			Timestamp:   p.Timestamp,
			Fields:      fields,
		},
	})
	m.count++
	m.lastWrite = time.Now()

	// 检查是否需要排序
	if m.count > 1 && m.entries[m.count-1].Point.Timestamp < m.entries[m.count-2].Point.Timestamp {
		sort.Slice(m.entries, func(i, j int) bool {
			return m.entries[i].Point.Timestamp < m.entries[j].Point.Timestamp
		})
		m.sorted = true
	} else {
		m.sorted = true
	}

	return nil
}

// Count 返回 MemTable 中的条目数。
//
// 返回：
//   - int: 当前存储的数据点数量
//
// 线程安全：
//
//	使用读锁，可并发调用。
func (m *MemTable) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

// ShouldFlush 检查 MemTable 是否满足刷盘条件。
//
// 返回：
//   - bool: true 表示应该执行刷盘
//
// 刷盘条件：
//
//   - 估算大小 >= MaxSize（按每个条目1KB估算）
//   - 条目数 >= MaxCount（如果 MaxCount > 0）
//   - 空闲时间 >= IdleDuration（如果 IdleDuration > 0 且有数据）
//
// 线程安全：
//
//	使用读锁，可并发调用。
func (m *MemTable) ShouldFlush() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.shouldFlushUnsafe()
}

// shouldFlushUnsafe 检查是否应该刷盘（已持有锁）
func (m *MemTable) shouldFlushUnsafe() bool {
	// 估算每个条目的平均大小约为1KB
	estimatedSize := int64(len(m.entries)) * 1024
	if estimatedSize >= m.maxSize {
		return true
	}
	if m.maxCount > 0 && m.count >= m.maxCount {
		return true
	}
	// 检查空闲时间
	if m.idleTimeout > 0 && m.count > 0 {
		if time.Since(m.lastWrite) >= m.idleTimeout {
			return true
		}
	}
	return false
}

// Flush 将 MemTable 数据刷盘并返回。
//
// 返回：
//   - []*types.Point: 刷盘的数据点列表
//
// 线程安全：
//
//	函数内部加锁，获取数据后清空 entries。
//	返回的数据点引用原内存，但在清空后不再受保护。
//
// GC 优化：
//
//	返回后显式清空原 entries 数组，帮助 GC。
func (m *MemTable) Flush() []*types.Point {
	m.mu.Lock()
	result := m.entries
	m.entries = nil // 彻底清空底层数组
	m.count = 0
	m.sorted = false
	m.mu.Unlock()

	if len(result) == 0 {
		return nil
	}

	points := make([]*types.Point, len(result))
	for i, e := range result {
		points[i] = &e.Point
	}

	// 显式清空 result 帮助 GC
	for i := range result {
		result[i] = nil
	}

	return points
}

// Iterator 返回 MemTable 的迭代器。
//
// 返回：
//   - *MemTableIterator: 新创建的迭代器，初始位置在第一个元素之前
//
// 注意：
//
//	迭代器不持有锁，返回后 MemTable 可能被修改。
//	遍历前应考虑是否加锁或使用快照。
func (m *MemTable) Iterator() *MemTableIterator {
	return &MemTableIterator{
		entries: m.entries,
		pos:     -1,
	}
}

// MemTableIterator 迭代器
type MemTableIterator struct {
	entries []*entry
	pos     int
}

// Next 移动到下一个条目
func (i *MemTableIterator) Next() bool {
	i.pos++
	return i.pos < len(i.entries)
}

// Point 返回当前位置的 Point
func (i *MemTableIterator) Point() *types.Point {
	return &i.entries[i.pos].Point
}
