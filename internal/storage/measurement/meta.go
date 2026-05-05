// Package measurement 实现测量元数据管理。
//
// 提供 Measurement 级别的元数据存储，包括 Schema、Series 定义和 Tag 索引。
// 支持内存存储和磁盘持久化两种模式。
//
// 核心组件：
//
//	MetaStore:        元数据存储接口
//	MemoryMetaStore:  内存实现，支持持久化
//	MeasurementMetaStore: Measurement 级 Series 管理
//
// 元数据类型：
//
//   - Schema:    字段定义（名称、类型）
//   - Series:    唯一标签组合和对应的 SID
//   - TagIndex:  标签到 Series ID 的倒排索引
package measurement

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"codeberg.org/micro-ts/mts/types"
)

// ErrInvalidMetaFile 表示 meta 文件格式无效。
//
// 可能原因：
//   - 魔数不匹配（不是有效的 meta 文件）
//   - 版本不匹配
//   - 文件损坏或截断
var ErrInvalidMetaFile = errors.New("invalid meta file")

// MemoryMetaStore 是内存实现的元数据存储。
//
// 功能完整，支持持久化到磁盘。
// 适合作为嵌入式或开发场景的默认实现。
//
// 数据结构：
//
//   - meta:     Measurement Schema（字段定义、标签键）
//   - series:   Sid 到序列化标签的映射
//   - tagIndex: 标签值到 Sid 列表的倒排索引
//   - dirty:    是否被修改（用于懒写的优化）
//
// 持久化格式：
//
//	二进制文件，包含魔数（"MTSH"）、版本和结构化数据。
//	具体格式参见 Persist 和 Load 方法的实现。
//
// 线程安全：
//
//	所有公共方法都是线程安全的，使用读写锁保护。
type MemoryMetaStore struct {
	mu       sync.RWMutex
	meta     *types.MeasurementMeta
	series   map[uint64][]byte
	tagIndex map[string][]uint64
	dirty    bool
}

// NewMemoryMetaStore 创建 MemoryMetaStore
func NewMemoryMetaStore() *MemoryMetaStore {
	return &MemoryMetaStore{
		series:   make(map[uint64][]byte),
		tagIndex: make(map[string][]uint64),
	}
}

func (m *MemoryMetaStore) GetMeta(ctx context.Context) (*types.MeasurementMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.meta, nil
}

func (m *MemoryMetaStore) SetMeta(ctx context.Context, meta *types.MeasurementMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.meta = meta
	m.dirty = true
	return nil
}

func (m *MemoryMetaStore) GetSeries(ctx context.Context, sid uint64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.series[sid], nil
}

func (m *MemoryMetaStore) SetSeries(ctx context.Context, sid uint64, tags []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.series[sid] = tags
	m.dirty = true
	return nil
}

func (m *MemoryMetaStore) GetAllSeries(ctx context.Context) (map[uint64][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[uint64][]byte, len(m.series))
	for k, v := range m.series {
		result[k] = v
	}
	return result, nil
}

func (m *MemoryMetaStore) GetSidsByTag(ctx context.Context, tagKey, tagValue string) ([]uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := tagKey + "\x00" + tagValue
	return m.tagIndex[key], nil
}

func (m *MemoryMetaStore) AddTagIndex(ctx context.Context, tagKey, tagValue string, sid uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := tagKey + "\x00" + tagValue
	m.tagIndex[key] = append(m.tagIndex[key], sid)
	m.dirty = true
	return nil
}

func (m *MemoryMetaStore) Persist(ctx context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 确保目录存在
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	var buf bytes.Buffer

	// 写入 magic header
	if err := binary.Write(&buf, binary.BigEndian, uint32(0x4D545348)); err != nil { // "MTSH"
		return fmt.Errorf("write magic header: %w", err)
	}
	// 写入版本
	if err := binary.Write(&buf, binary.BigEndian, int64(1)); err != nil {
		return fmt.Errorf("write version: %w", err)
	}

	// 写入 MeasurementMeta
	if m.meta != nil {
		if err := binary.Write(&buf, binary.BigEndian, m.meta.Version); err != nil {
			return fmt.Errorf("write meta version: %w", err)
		}
		// FieldSchema
		if err := binary.Write(&buf, binary.BigEndian, int64(len(m.meta.FieldSchema))); err != nil {
			return fmt.Errorf("write field schema count: %w", err)
		}
		for _, fd := range m.meta.FieldSchema {
			if err := writeString(&buf, fd.Name); err != nil {
				return fmt.Errorf("write field name: %w", err)
			}
			if err := binary.Write(&buf, binary.BigEndian, int64(fd.Type)); err != nil {
				return fmt.Errorf("write field type: %w", err)
			}
		}
		// TagKeys
		if err := binary.Write(&buf, binary.BigEndian, int64(len(m.meta.TagKeys))); err != nil {
			return fmt.Errorf("write tag key count: %w", err)
		}
		for _, key := range m.meta.TagKeys {
			if err := writeString(&buf, key); err != nil {
				return fmt.Errorf("write tag key: %w", err)
			}
		}
		// NextSID
		if err := binary.Write(&buf, binary.BigEndian, m.meta.NextSID); err != nil {
			return fmt.Errorf("write next sid: %w", err)
		}
	} else {
		// meta 为空写入默认值
		if err := binary.Write(&buf, binary.BigEndian, int64(0)); err != nil {
			return fmt.Errorf("write default version: %w", err)
		}
		if err := binary.Write(&buf, binary.BigEndian, int64(0)); err != nil {
			return fmt.Errorf("write default field count: %w", err)
		}
		if err := binary.Write(&buf, binary.BigEndian, int64(0)); err != nil {
			return fmt.Errorf("write default tag count: %w", err)
		}
		if err := binary.Write(&buf, binary.BigEndian, int64(0)); err != nil {
			return fmt.Errorf("write default sid: %w", err)
		}
	}

	// 写入 series count
	if err := binary.Write(&buf, binary.BigEndian, int64(len(m.series))); err != nil {
		return fmt.Errorf("write series count: %w", err)
	}
	// 写入 series entries
	for sid, tags := range m.series {
		if err := binary.Write(&buf, binary.BigEndian, sid); err != nil {
			return fmt.Errorf("write series sid: %w", err)
		}
		if err := writeBytes(&buf, tags); err != nil {
			return fmt.Errorf("write series tags: %w", err)
		}
	}

	// 写入 tagIndex count
	if err := binary.Write(&buf, binary.BigEndian, int64(len(m.tagIndex))); err != nil {
		return fmt.Errorf("write tag index count: %w", err)
	}
	// 写入 tagIndex entries
	for key, sids := range m.tagIndex {
		if err := writeString(&buf, key); err != nil {
			return fmt.Errorf("write tag index key: %w", err)
		}
		if err := binary.Write(&buf, binary.BigEndian, int64(len(sids))); err != nil {
			return fmt.Errorf("write tag index sid count: %w", err)
		}
		for _, sid := range sids {
			if err := binary.Write(&buf, binary.BigEndian, sid); err != nil {
				return fmt.Errorf("write tag index sid: %w", err)
			}
		}
	}

	// 写入文件
	if err := os.WriteFile(path, buf.Bytes(), 0600); err != nil {
		return fmt.Errorf("write file: %w", err)
	}
	return nil
}

func (m *MemoryMetaStore) Load(ctx context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	buf := bytes.NewReader(data)

	// 读取 magic header
	var magic uint32
	if err := binary.Read(buf, binary.BigEndian, &magic); err != nil {
		return fmt.Errorf("read magic header: %w", err)
	}
	if magic != 0x4D545348 {
		return ErrInvalidMetaFile
	}

	// 读取版本
	var version int64
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return fmt.Errorf("read version: %w", err)
	}
	if version != 1 {
		return ErrInvalidMetaFile
	}

	// 读取 MeasurementMeta
	meta := &types.MeasurementMeta{}
	if err := binary.Read(buf, binary.BigEndian, &meta.Version); err != nil {
		return fmt.Errorf("read meta version: %w", err)
	}
	// FieldSchema
	var fieldCount int64
	if err := binary.Read(buf, binary.BigEndian, &fieldCount); err != nil {
		return fmt.Errorf("read field count: %w", err)
	}
	meta.FieldSchema = make([]types.FieldDef, fieldCount)
	for i := int64(0); i < fieldCount; i++ {
		name, err := readString(buf)
		if err != nil {
			return fmt.Errorf("read field name: %w", err)
		}
		var typeVal int64
		if err := binary.Read(buf, binary.BigEndian, &typeVal); err != nil {
			return fmt.Errorf("read field type: %w", err)
		}
		meta.FieldSchema[i] = types.FieldDef{Name: name, Type: types.FieldType(typeVal)}
	}
	// TagKeys
	var tagKeyCount int64
	if err := binary.Read(buf, binary.BigEndian, &tagKeyCount); err != nil {
		return fmt.Errorf("read tag key count: %w", err)
	}
	meta.TagKeys = make([]string, tagKeyCount)
	for i := int64(0); i < tagKeyCount; i++ {
		key, err := readString(buf)
		if err != nil {
			return fmt.Errorf("read tag key: %w", err)
		}
		meta.TagKeys[i] = key
	}
	// NextSID
	if err := binary.Read(buf, binary.BigEndian, &meta.NextSID); err != nil {
		return fmt.Errorf("read next sid: %w", err)
	}
	m.meta = meta

	// 读取 series count
	var seriesCount int64
	if err := binary.Read(buf, binary.BigEndian, &seriesCount); err != nil {
		return fmt.Errorf("read series count: %w", err)
	}
	// 读取 series entries
	m.series = make(map[uint64][]byte, seriesCount)
	for i := int64(0); i < seriesCount; i++ {
		var sid uint64
		if err := binary.Read(buf, binary.BigEndian, &sid); err != nil {
			return fmt.Errorf("read series sid: %w", err)
		}
		tags, err := readBytes(buf)
		if err != nil {
			return fmt.Errorf("read series tags: %w", err)
		}
		m.series[sid] = tags
	}

	// 读取 tagIndex count
	var tagIndexCount int64
	if err := binary.Read(buf, binary.BigEndian, &tagIndexCount); err != nil {
		return fmt.Errorf("read tag index count: %w", err)
	}
	// 读取 tagIndex entries
	m.tagIndex = make(map[string][]uint64, tagIndexCount)
	for i := int64(0); i < tagIndexCount; i++ {
		key, err := readString(buf)
		if err != nil {
			return fmt.Errorf("read tag index key: %w", err)
		}
		var sidCount int64
		if err := binary.Read(buf, binary.BigEndian, &sidCount); err != nil {
			return fmt.Errorf("read tag index sid count: %w", err)
		}
		sids := make([]uint64, sidCount)
		for j := int64(0); j < sidCount; j++ {
			var sid uint64
			if err := binary.Read(buf, binary.BigEndian, &sid); err != nil {
				return fmt.Errorf("read tag index sid: %w", err)
			}
			sids[j] = sid
		}
		m.tagIndex[key] = sids
	}

	return nil
}

// writeString writes a length-prefixed string
func writeString(buf *bytes.Buffer, s string) error {
	if err := binary.Write(buf, binary.BigEndian, int64(len(s))); err != nil {
		return fmt.Errorf("write string length: %w", err)
	}
	if _, err := buf.WriteString(s); err != nil {
		return fmt.Errorf("write string data: %w", err)
	}
	return nil
}

// readString reads a length-prefixed string
func readString(buf *bytes.Reader) (string, error) {
	var length int64
	if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
		return "", fmt.Errorf("read string length: %w", err)
	}
	b := make([]byte, length)
	if _, err := buf.Read(b); err != nil {
		return "", fmt.Errorf("read string data: %w", err)
	}
	return string(b), nil
}

// writeBytes writes a length-prefixed byte slice
func writeBytes(buf *bytes.Buffer, b []byte) error {
	if err := binary.Write(buf, binary.BigEndian, int64(len(b))); err != nil {
		return fmt.Errorf("write bytes length: %w", err)
	}
	if _, err := buf.Write(b); err != nil {
		return fmt.Errorf("write bytes data: %w", err)
	}
	return nil
}

// readBytes reads a length-prefixed byte slice
func readBytes(buf *bytes.Reader) ([]byte, error) {
	var length int64
	if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("read bytes length: %w", err)
	}
	b := make([]byte, length)
	if _, err := buf.Read(b); err != nil {
		return nil, fmt.Errorf("read bytes data: %w", err)
	}
	return b, nil
}

func (m *MemoryMetaStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.series = nil
	m.tagIndex = nil
	return nil
}
