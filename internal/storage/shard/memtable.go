// internal/storage/shard/memtable.go
package shard

import (
	"sort"
	"sync"
	"time"

	"micro-ts/internal/types"
)

// MemTableConfig MemTable 配置
type MemTableConfig struct {
	// MaxSize 最大内存大小（字节），默认 64MB
	MaxSize int64
	// MaxCount 最大条目数，默认 3000
	MaxCount int
	// IdleDuration 空闲时间阈值，数据持续该时间没有写入则触发 flush
	IdleDuration time.Duration
}

// DefaultMemTableConfig 返回默认配置
func DefaultMemTableConfig() MemTableConfig {
	return MemTableConfig{
		MaxSize:      64 * 1024 * 1024, // 64MB
		MaxCount:     3000,
		IdleDuration: time.Minute,
	}
}

// entry 是 MemTable 中的条目
type entry struct {
	Point types.Point
}

// MemTable 内存跳表
type MemTable struct {
	mu          sync.RWMutex
	entries     []*entry
	maxSize     int64
	maxCount    int
	idleTimeout time.Duration
	lastWrite   time.Time // 上次写入时间
	count       int       // 当前条目数
}

// NewMemTable 创建 MemTable
func NewMemTable(cfg MemTableConfig) *MemTable {
	return &MemTable{
		entries:     make([]*entry, 0, 1024),
		maxSize:     cfg.MaxSize,
		maxCount:    cfg.MaxCount,
		idleTimeout: cfg.IdleDuration,
		lastWrite:   time.Now(),
	}
}

// Write 写入
func (m *MemTable) Write(p *types.Point) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 深拷贝：创建新的 map，切断对客户端 map 的引用
	// 避免客户端和服务端共享同一个 map 底层数据
	tags := make(map[string]string, len(p.Tags))
	for k, v := range p.Tags {
		tags[k] = v
	}

	fields := make(map[string]any, len(p.Fields))
	for k, v := range p.Fields {
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
	}

	return nil
}

// Count 返回条数
func (m *MemTable) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

// ShouldFlush 检查是否应该刷盘
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

// Flush 刷盘（返回数据用于生成 SSTable）
func (m *MemTable) Flush() []*types.Point {
	m.mu.Lock()
	result := m.entries
	m.entries = nil // 彻底清空底层数组
	m.count = 0
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

// Iterator 迭代器
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
