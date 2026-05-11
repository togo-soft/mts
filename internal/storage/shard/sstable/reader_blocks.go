package sstable

import (
	"encoding/binary"
	"fmt"

	"codeberg.org/micro-ts/mts/internal/storage/shard/compression"
	"codeberg.org/micro-ts/mts/types"
)

// readTimestamps 读取并解码全部 timestamps。
func (r *Reader) readTimestamps() ([]int64, error) {
	tsOffset, tsSize, _ := r.sectionTable.LookupByType(SectionTimestamps)
	if tsSize == 0 {
		return nil, nil
	}
	data := make([]byte, tsSize)
	if _, err := r.file.ReadAt(data, int64(tsOffset)); err != nil {
		return nil, err
	}

	enc := r.sectionTable.LookupEncoding("_timestamps")
	rowCount := int(r.header.RowCount)

	switch enc {
	case EncodingDeltaVarint:
		return compression.DecodeTimestamps(data, rowCount)
	default:
		return decodeTimestampBatch(data), nil
	}
}

// readTimestampRange 读取指定行偏移和行数的 timestamps。
// 编码后的数据为变长，因此解码全部后切片。
func (r *Reader) readTimestampRange(offset uint32, numRows uint32) ([]int64, error) {
	all, err := r.readTimestamps()
	if err != nil {
		return nil, err
	}
	end := int(offset) + int(numRows)
	if end > len(all) {
		end = len(all)
	}
	if int(offset) >= len(all) {
		return nil, nil
	}
	return all[offset:end], nil
}

// readSids 读取并解码全部 sids。
func (r *Reader) readSids(expectedCount int) ([]uint64, error) {
	sidOffset, sidSize, _ := r.sectionTable.LookupByType(SectionSids)
	if sidSize == 0 {
		return make([]uint64, expectedCount), nil
	}
	data := make([]byte, sidSize)
	if _, err := r.file.ReadAt(data, int64(sidOffset)); err != nil {
		return nil, err
	}

	enc := r.sectionTable.LookupEncoding("_sids")
	switch enc {
	case EncodingVarint:
		return compression.DecodeSids(data, expectedCount)
	default:
		return decodeSidBatch(data), nil
	}
}

// readSidsRange 读取指定行偏移和行数的 sids。
func (r *Reader) readSidsRange(offset uint32, numRows uint32) ([]uint64, error) {
	all, err := r.readSids(int(r.header.RowCount))
	if err != nil {
		return nil, err
	}
	end := int(offset) + int(numRows)
	if end > len(all) {
		end = len(all)
	}
	if int(offset) >= len(all) {
		return nil, nil
	}
	return all[offset:end], nil
}

// decodeFieldSection 读取并解码整个字段段。
func (r *Reader) decodeFieldSection(name string, rowCount int) ([]*types.FieldValue, error) {
	fOffset, fSize := r.sectionTable.Lookup(name)
	if fSize == 0 {
		values := make([]*types.FieldValue, rowCount)
		ft := r.schema.Fields[name]
		for i := 0; i < rowCount; i++ {
			values[i] = zeroFieldValue(ft)
		}
		return values, nil
	}

	data := make([]byte, fSize)
	if _, err := r.file.ReadAt(data, int64(fOffset)); err != nil {
		return nil, err
	}

	enc := r.sectionTable.LookupEncoding(name)
	ft := r.schema.Fields[name]

	switch enc {
	case EncodingXORFloat:
		floatVals, err := compression.DecodeFloat64Values(data, rowCount)
		if err != nil {
			return nil, fmt.Errorf("decode xor float field %s: %w", name, err)
		}
		return float64ValuesToFieldValues(floatVals), nil

	case EncodingZigZagVarint:
		intVals, err := compression.DecodeInt64Values(data, rowCount)
		if err != nil {
			return nil, fmt.Errorf("decode zigzag int field %s: %w", name, err)
		}
		return int64ValuesToFieldValues(intVals), nil

	case EncodingDictString:
		strVals, err := compression.DecodeStringValues(data, rowCount, true)
		if err != nil {
			return nil, fmt.Errorf("decode dict string field %s: %w", name, err)
		}
		return stringValuesToFieldValues(strVals), nil

	case EncodingBitmapBool:
		boolVals := compression.DecodeBoolValues(data, rowCount)
		return boolValuesToFieldValues(boolVals), nil

	case EncodingRaw:
		return r.decodeRawFieldSection(data, rowCount, ft, name), nil

	default:
		return r.decodeRawFieldSection(data, rowCount, ft, name), nil
	}
}

// decodeRawFieldSection 解码原始编码的字段段。
func (r *Reader) decodeRawFieldSection(data []byte, rowCount int, ft FieldType, name string) []*types.FieldValue {
	values := make([]*types.FieldValue, rowCount)
	offsets := r.computeFieldOffsets(name, data, rowCount)
	for i := 0; i < rowCount; i++ {
		values[i] = r.decodeFieldValue(data, offsets[i], name)
	}
	return values
}

// ReadAllDecodedFieldSections 读取并解码所有字段段。
func (r *Reader) ReadAllDecodedFieldSections(fields []string, rowCount int) (map[string][]*types.FieldValue, error) {
	if len(fields) == 0 {
		fields = r.sectionTable.FieldNames()
	}

	decodedFields := make(map[string][]*types.FieldValue, len(fields))
	for _, name := range fields {
		vals, err := r.decodeFieldSection(name, rowCount)
		if err != nil {
			return nil, err
		}
		decodedFields[name] = vals
	}
	return decodedFields, nil
}

func float64ValuesToFieldValues(vals []float64) []*types.FieldValue {
	result := make([]*types.FieldValue, len(vals))
	for i, v := range vals {
		result[i] = types.NewFieldValue(v)
	}
	return result
}

func int64ValuesToFieldValues(vals []int64) []*types.FieldValue {
	result := make([]*types.FieldValue, len(vals))
	for i, v := range vals {
		result[i] = types.NewFieldValue(v)
	}
	return result
}

func stringValuesToFieldValues(vals []string) []*types.FieldValue {
	result := make([]*types.FieldValue, len(vals))
	for i, v := range vals {
		result[i] = types.NewFieldValue(v)
	}
	return result
}

func boolValuesToFieldValues(vals []bool) []*types.FieldValue {
	result := make([]*types.FieldValue, len(vals))
	for i, v := range vals {
		result[i] = types.NewFieldValue(v)
	}
	return result
}

func zeroFieldValue(ft FieldType) *types.FieldValue {
	switch ft {
	case FieldTypeFloat64:
		return types.NewFieldValue(float64(0))
	case FieldTypeInt64:
		return types.NewFieldValue(int64(0))
	case FieldTypeString:
		return types.NewFieldValue("")
	case FieldTypeBool:
		return types.NewFieldValue(false)
	default:
		return types.NewFieldValue(float64(0))
	}
}

func decodeTimestampBatch(data []byte) []int64 {
	timestamps := make([]int64, 0, len(data)/8)
	for i := 0; i+8 <= len(data); i += 8 {
		ts := int64(binary.BigEndian.Uint64(data[i : i+8]))
		timestamps = append(timestamps, ts)
	}
	return timestamps
}

func decodeSidBatch(data []byte) []uint64 {
	sids := make([]uint64, 0, len(data)/8)
	for i := 0; i+8 <= len(data); i += 8 {
		sid := binary.BigEndian.Uint64(data[i : i+8])
		sids = append(sids, sid)
	}
	return sids
}
