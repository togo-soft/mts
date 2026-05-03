// internal/storage/measurement/meta.go
package measurement

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"micro-ts/internal/types"
)

// ErrInvalidMetaFile 无效的 meta 文件
var ErrInvalidMetaFile = errors.New("invalid meta file")

// MemoryMetaStore 内存实现的 MetaStore
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
		return err
	}

	var buf bytes.Buffer

	// 写入 magic header
	if err := binary.Write(&buf, binary.BigEndian, uint32(0x4D545348)); err != nil { // "MTSH"
		return err
	}
	// 写入版本
	if err := binary.Write(&buf, binary.BigEndian, int64(1)); err != nil {
		return err
	}

	// 写入 MeasurementMeta
	if m.meta != nil {
		if err := binary.Write(&buf, binary.BigEndian, m.meta.Version); err != nil {
			return err
		}
		// FieldSchema
		if err := binary.Write(&buf, binary.BigEndian, int64(len(m.meta.FieldSchema))); err != nil {
			return err
		}
		for _, fd := range m.meta.FieldSchema {
			if err := writeString(&buf, fd.Name); err != nil {
				return err
			}
			if err := binary.Write(&buf, binary.BigEndian, int64(fd.Type)); err != nil {
				return err
			}
		}
		// TagKeys
		if err := binary.Write(&buf, binary.BigEndian, int64(len(m.meta.TagKeys))); err != nil {
			return err
		}
		for _, key := range m.meta.TagKeys {
			if err := writeString(&buf, key); err != nil {
				return err
			}
		}
		// NextSID
		if err := binary.Write(&buf, binary.BigEndian, m.meta.NextSID); err != nil {
			return err
		}
	} else {
		// meta 为空写入默认值
		if err := binary.Write(&buf, binary.BigEndian, int64(0)); err != nil {
			return err
		}
		if err := binary.Write(&buf, binary.BigEndian, int64(0)); err != nil {
			return err
		}
		if err := binary.Write(&buf, binary.BigEndian, int64(0)); err != nil {
			return err
		}
		if err := binary.Write(&buf, binary.BigEndian, int64(0)); err != nil {
			return err
		}
	}

	// 写入 series count
	if err := binary.Write(&buf, binary.BigEndian, int64(len(m.series))); err != nil {
		return err
	}
	// 写入 series entries
	for sid, tags := range m.series {
		if err := binary.Write(&buf, binary.BigEndian, sid); err != nil {
			return err
		}
		if err := writeBytes(&buf, tags); err != nil {
			return err
		}
	}

	// 写入 tagIndex count
	if err := binary.Write(&buf, binary.BigEndian, int64(len(m.tagIndex))); err != nil {
		return err
	}
	// 写入 tagIndex entries
	for key, sids := range m.tagIndex {
		if err := writeString(&buf, key); err != nil {
			return err
		}
		if err := binary.Write(&buf, binary.BigEndian, int64(len(sids))); err != nil {
			return err
		}
		for _, sid := range sids {
			if err := binary.Write(&buf, binary.BigEndian, sid); err != nil {
				return err
			}
		}
	}

	// 写入文件
	return os.WriteFile(path, buf.Bytes(), 0600)
}

func (m *MemoryMetaStore) Load(ctx context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	buf := bytes.NewReader(data)

	// 读取 magic header
	var magic uint32
	if err := binary.Read(buf, binary.BigEndian, &magic); err != nil {
		return err
	}
	if magic != 0x4D545348 {
		return ErrInvalidMetaFile
	}

	// 读取版本
	var version int64
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return err
	}
	if version != 1 {
		return ErrInvalidMetaFile
	}

	// 读取 MeasurementMeta
	meta := &types.MeasurementMeta{}
	if err := binary.Read(buf, binary.BigEndian, &meta.Version); err != nil {
		return err
	}
	// FieldSchema
	var fieldCount int64
	if err := binary.Read(buf, binary.BigEndian, &fieldCount); err != nil {
		return err
	}
	meta.FieldSchema = make([]types.FieldDef, fieldCount)
	for i := int64(0); i < fieldCount; i++ {
		name, err := readString(buf)
		if err != nil {
			return err
		}
		var typeVal int64
		if err := binary.Read(buf, binary.BigEndian, &typeVal); err != nil {
			return err
		}
		meta.FieldSchema[i] = types.FieldDef{Name: name, Type: types.FieldType(typeVal)}
	}
	// TagKeys
	var tagKeyCount int64
	if err := binary.Read(buf, binary.BigEndian, &tagKeyCount); err != nil {
		return err
	}
	meta.TagKeys = make([]string, tagKeyCount)
	for i := int64(0); i < tagKeyCount; i++ {
		key, err := readString(buf)
		if err != nil {
			return err
		}
		meta.TagKeys[i] = key
	}
	// NextSID
	if err := binary.Read(buf, binary.BigEndian, &meta.NextSID); err != nil {
		return err
	}
	m.meta = meta

	// 读取 series count
	var seriesCount int64
	if err := binary.Read(buf, binary.BigEndian, &seriesCount); err != nil {
		return err
	}
	// 读取 series entries
	m.series = make(map[uint64][]byte, seriesCount)
	for i := int64(0); i < seriesCount; i++ {
		var sid uint64
		if err := binary.Read(buf, binary.BigEndian, &sid); err != nil {
			return err
		}
		tags, err := readBytes(buf)
		if err != nil {
			return err
		}
		m.series[sid] = tags
	}

	// 读取 tagIndex count
	var tagIndexCount int64
	if err := binary.Read(buf, binary.BigEndian, &tagIndexCount); err != nil {
		return err
	}
	// 读取 tagIndex entries
	m.tagIndex = make(map[string][]uint64, tagIndexCount)
	for i := int64(0); i < tagIndexCount; i++ {
		key, err := readString(buf)
		if err != nil {
			return err
		}
		var sidCount int64
		if err := binary.Read(buf, binary.BigEndian, &sidCount); err != nil {
			return err
		}
		sids := make([]uint64, sidCount)
		for j := int64(0); j < sidCount; j++ {
			var sid uint64
			if err := binary.Read(buf, binary.BigEndian, &sid); err != nil {
				return err
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
		return err
	}
	_, err := buf.WriteString(s)
	return err
}

// readString reads a length-prefixed string
func readString(buf *bytes.Reader) (string, error) {
	var length int64
	if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
		return "", err
	}
	b := make([]byte, length)
	if _, err := buf.Read(b); err != nil {
		return "", err
	}
	return string(b), nil
}

// writeBytes writes a length-prefixed byte slice
func writeBytes(buf *bytes.Buffer, b []byte) error {
	if err := binary.Write(buf, binary.BigEndian, int64(len(b))); err != nil {
		return err
	}
	_, err := buf.Write(b)
	return err
}

// readBytes reads a length-prefixed byte slice
func readBytes(buf *bytes.Reader) ([]byte, error) {
	var length int64
	if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	b := make([]byte, length)
	if _, err := buf.Read(b); err != nil {
		return nil, err
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
