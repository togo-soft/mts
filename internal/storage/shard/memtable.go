// internal/storage/shard/memtable.go
package shard

import (
	"sort"
	"sync"

	"micro-ts/internal/types"
)

// entry 是 MemTable 中的条目
type entry struct {
	Point types.Point
}

// MemTable 内存跳表
type MemTable struct {
	mu      sync.RWMutex
	entries []*entry
	maxSize int64
	count   int
}

// NewMemTable 创建 MemTable
func NewMemTable(maxSize int64) *MemTable {
	return &MemTable{
		entries: make([]*entry, 0, 1024),
		maxSize: maxSize,
	}
}

// Write 写入
func (m *MemTable) Write(p *types.Point) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries = append(m.entries, &entry{Point: *p})
	m.count++

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
	return int64(len(m.entries)*16) >= m.maxSize || m.count >= 10000
}

// Flush 刷盘（返回数据用于生成 SSTable）
func (m *MemTable) Flush() []*types.Point {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*types.Point, len(m.entries))
	for i, e := range m.entries {
		p := e.Point
		result[i] = &p
	}

	m.entries = m.entries[:0]
	m.count = 0
	return result
}

// Iterator 迭代器
func (m *MemTable) Iterator() *MemTableIterator {
	m.mu.RLock()
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
