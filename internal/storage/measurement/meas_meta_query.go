package measurement

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// GetTagsBySID 根据 Series ID 获取标签。
func (m *MeasurementMetaStore) GetTagsBySID(sid uint64) (map[string]string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tags, ok := m.series[sid]
	if !ok {
		return nil, false
	}
	return CopyTags(tags), true
}

// GetSidsByTag 根据标签键值查找所有匹配的 Series IDs。
func (m *MeasurementMetaStore) GetSidsByTag(tagKey, tagValue string) []uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sids := m.tagIndex[tagKey+"\x00"+tagValue]
	result := make([]uint64, len(sids))
	copy(result, sids)
	return result
}

// SeriesCount 返回当前 measurement 的 series 总数。
func (m *MeasurementMetaStore) SeriesCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.series)
}

// Close 关闭 MetaStore，释放资源。
func (m *MeasurementMetaStore) Close() error {
	if m.persistPath != "" && m.dirty {
		if err := m.Persist(); err != nil {
			return fmt.Errorf("persist before close: %w", err)
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.series = nil
	m.tagIndex = nil
	return nil
}

// Persist 将元数据持久化到磁盘。
func (m *MeasurementMetaStore) Persist() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.persistPath == "" {
		return nil
	}

	if !m.dirty {
		return nil
	}

	dir := filepath.Dir(m.persistPath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	data := struct {
		NextSID uint64              `json:"next_sid"`
		Series  map[uint64][]string `json:"series"`
	}{
		NextSID: m.nextSID,
		Series:  make(map[uint64][]string),
	}

	for sid, tags := range m.series {
		tagsList := make([]string, 0, len(tags)*2)
		for k, v := range tags {
			tagsList = append(tagsList, k, v)
		}
		data.Series[sid] = tagsList
	}

	tmpPath := m.persistPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("encode json: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("fsync: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	if err := os.Rename(tmpPath, m.persistPath); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	m.dirty = false
	return nil
}
