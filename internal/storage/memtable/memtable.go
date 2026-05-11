package memtable

import (
	"sort"
	"sync"
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

// FieldEntry 紧凑字段条目，避免每行分配 map。
type FieldEntry struct {
	Key   string
	Value *types.FieldValue
}

// entry 是 MemTable 中的紧凑条目。
// Database/Measurement 为 Shard 级别常量，不存储。
// Tags 通过 Sid 从 SeriesStore 恢复，不存储。
// Fields 使用紧凑切片存储，消除 map 开销。
type entry struct {
	Timestamp int64
	Fields    []FieldEntry
	Sid       uint64
}

// MemTable 是内存中的写入缓冲区，按时间戳排序存储数据点。
type MemTable struct {
	mu          sync.RWMutex
	entries     []*entry
	maxSize     int64
	maxCount    int
	idleTimeout time.Duration
	lastWrite   time.Time
	count       int
	sorted      bool
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
// 仅存储 Timestamp + Fields（紧凑切片） + Sid，不存储 Database/Measurement/Tags。
func (m *MemTable) Write(p *types.Point, sid uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fields := make([]FieldEntry, 0, len(p.Fields))
	for k, v := range p.Fields {
		fields = append(fields, FieldEntry{Key: k, Value: v})
	}

	m.entries = append(m.entries, &entry{
		Timestamp: p.Timestamp,
		Fields:    fields,
		Sid:       sid,
	})
	m.count++
	m.lastWrite = time.Now()

	if m.count > 1 && m.entries[m.count-1].Timestamp < m.entries[m.count-2].Timestamp {
		sort.Slice(m.entries, func(i, j int) bool {
			return m.entries[i].Timestamp < m.entries[j].Timestamp
		})
		m.sorted = true
	} else {
		m.sorted = true
	}

	return nil
}

// Count 返回 MemTable 中的条目数。
func (m *MemTable) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

// ShouldFlush 检查 MemTable 是否满足刷盘条件。
func (m *MemTable) ShouldFlush() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.shouldFlushUnsafe()
}

func (m *MemTable) shouldFlushUnsafe() bool {
	estimatedSize := int64(len(m.entries)) * 1024
	if estimatedSize >= m.maxSize {
		return true
	}
	if m.maxCount > 0 && m.count >= m.maxCount {
		return true
	}
	if m.idleTimeout > 0 && m.count > 0 {
		if time.Since(m.lastWrite) >= m.idleTimeout {
			return true
		}
	}
	return false
}

// Flush 将 MemTable 数据刷盘并返回。
// 返回的 Point 仅填充 Timestamp 和 Fields（SSTable Writer 不使用 Tags/Database/Measurement）。
func (m *MemTable) Flush() ([]*types.Point, []uint64) {
	m.mu.Lock()
	result := m.entries
	m.entries = nil
	m.count = 0
	m.sorted = false
	m.mu.Unlock()

	if len(result) == 0 {
		return nil, nil
	}

	points := make([]*types.Point, len(result))
	sids := make([]uint64, len(result))
	for i, e := range result {
		points[i] = &types.Point{
			Timestamp: e.Timestamp,
			Fields:    fieldsToMap(e.Fields),
		}
		sids[i] = e.Sid
	}

	for i := range result {
		result[i] = nil
	}

	return points, sids
}

// Iterator 返回 MemTable 的迭代器。
func (m *MemTable) Iterator() *MemTableIterator {
	return &MemTableIterator{
		entries: m.entries,
		pos:     -1,
	}
}

// IdleTimeout 返回空闲超时配置。
func (m *MemTable) IdleTimeout() time.Duration {
	return m.idleTimeout
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

// Point 返回当前位置的 Point（Tags 为空，需通过 Sid 恢复）。
func (i *MemTableIterator) Point() *types.Point {
	e := i.entries[i.pos]
	return &types.Point{
		Timestamp: e.Timestamp,
		Fields:    fieldsToMap(e.Fields),
	}
}

// Sid 返回当前条目的 Series ID。
func (i *MemTableIterator) Sid() uint64 {
	return i.entries[i.pos].Sid
}

// fieldsToMap 将紧凑 FieldEntry 切片还原为 map[string]*types.FieldValue。
func fieldsToMap(fields []FieldEntry) map[string]*types.FieldValue {
	if len(fields) == 0 {
		return nil
	}
	m := make(map[string]*types.FieldValue, len(fields))
	for _, fe := range fields {
		m[fe.Key] = fe.Value
	}
	return m
}
