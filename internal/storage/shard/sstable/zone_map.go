package sstable

import (
	"encoding/binary"
	"fmt"
	"math"
)

// ZoneMapEntry 记录单个字段在单个 block 中的 min/max 值。
type ZoneMapEntry struct {
	FieldName string
	Min       float64
	Max       float64
}

// BlockZoneMap 单 block 内所有字段的 ZoneMap 汇总。
type BlockZoneMap struct {
	FieldZMaps []ZoneMapEntry
}

// ZoneMapIndex 所有 block 的 ZoneMap 汇总。
type ZoneMapIndex struct {
	Blocks []BlockZoneMap
}

// FilterCondition 用于 ZoneMap 块跳过的简化过滤条件（避免循环导入 types）。
type FilterCondition struct {
	Field string
	Op    int32
	Value float64
}

// Marshal 序列化 ZoneMapIndex。
// 格式: [block_count:4B][for each block: field_count:2B][for each field: name_len:2B, name:var, min:8B, max:8B]
func (zm *ZoneMapIndex) Marshal() []byte {
	size := 4
	for _, b := range zm.Blocks {
		size += 2
		for _, e := range b.FieldZMaps {
			size += 2 + len(e.FieldName) + 8 + 8
		}
	}
	buf := make([]byte, size)
	pos := 0
	binary.BigEndian.PutUint32(buf[pos:], uint32(len(zm.Blocks)))
	pos += 4
	for _, b := range zm.Blocks {
		binary.BigEndian.PutUint16(buf[pos:], uint16(len(b.FieldZMaps)))
		pos += 2
		for _, e := range b.FieldZMaps {
			binary.BigEndian.PutUint16(buf[pos:], uint16(len(e.FieldName)))
			pos += 2
			copy(buf[pos:], e.FieldName)
			pos += len(e.FieldName)
			binary.BigEndian.PutUint64(buf[pos:], math.Float64bits(e.Min))
			pos += 8
			binary.BigEndian.PutUint64(buf[pos:], math.Float64bits(e.Max))
			pos += 8
		}
	}
	return buf
}

// UnmarshalZoneMapIndex 反序列化 ZoneMapIndex。
func UnmarshalZoneMapIndex(data []byte) (*ZoneMapIndex, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("zone map data too short: %d bytes", len(data))
	}
	blockCount := binary.BigEndian.Uint32(data[0:4])
	zm := &ZoneMapIndex{Blocks: make([]BlockZoneMap, 0, blockCount)}
	pos := 4
	for bi := uint32(0); bi < blockCount; bi++ {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("zone map truncated at block %d", bi)
		}
		fieldCount := binary.BigEndian.Uint16(data[pos : pos+2])
		pos += 2
		bzm := BlockZoneMap{FieldZMaps: make([]ZoneMapEntry, 0, fieldCount)}
		for fi := uint16(0); fi < fieldCount; fi++ {
			if pos+2 > len(data) {
				return nil, fmt.Errorf("zone map field %d truncated", fi)
			}
			nameLen := binary.BigEndian.Uint16(data[pos : pos+2])
			pos += 2
			if pos+int(nameLen)+16 > len(data) {
				return nil, fmt.Errorf("zone map field %d data truncated", fi)
			}
			name := string(data[pos : pos+int(nameLen)])
			pos += int(nameLen)
			min := math.Float64frombits(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			max := math.Float64frombits(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			bzm.FieldZMaps = append(bzm.FieldZMaps, ZoneMapEntry{FieldName: name, Min: min, Max: max})
		}
		zm.Blocks = append(zm.Blocks, bzm)
	}
	return zm, nil
}

// Lookup 按 block 索引和字段名查找 ZoneMap 条目。
func (zm *ZoneMapIndex) Lookup(blockIdx int, fieldName string) (ZoneMapEntry, bool) {
	if blockIdx < 0 || blockIdx >= len(zm.Blocks) {
		return ZoneMapEntry{}, false
	}
	for _, e := range zm.Blocks[blockIdx].FieldZMaps {
		if e.FieldName == fieldName {
			return e, true
		}
	}
	return ZoneMapEntry{}, false
}

// zoneAccumulator 用于累积单个字段在当前 block 中的 min/max 值。
type zoneAccumulator struct {
	min, max    float64
	initialized bool
}

func (za *zoneAccumulator) update(v float64) {
	if !za.initialized {
		za.min = v
		za.max = v
		za.initialized = true
		return
	}
	if v < za.min {
		za.min = v
	}
	if v > za.max {
		za.max = v
	}
}
