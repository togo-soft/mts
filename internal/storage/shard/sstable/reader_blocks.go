package sstable

import (
	"encoding/binary"
	"fmt"

	"codeberg.org/micro-ts/mts/internal/storage/shard/compression"
	"codeberg.org/micro-ts/mts/types"
)

// readTimestamps 逐 block 独立解码全部 timestamps。
func (r *Reader) readTimestamps() ([]int64, error) {
	var all []int64
	for i := 0; i < r.blockIndex.Len(); i++ {
		ts, err := r.readTimestampsBlock(i)
		if err != nil {
			return nil, err
		}
		all = append(all, ts...)
	}
	return all, nil
}

// readSids 逐 block 独立解码全部 sids。
func (r *Reader) readSids(expectedCount int) ([]uint64, error) {
	var all []uint64
	for i := 0; i < r.blockIndex.Len(); i++ {
		sids, err := r.readSidsBlock(i)
		if err != nil {
			return nil, err
		}
		all = append(all, sids...)
	}
	return all, nil
}

// decodeFieldSection 逐 block 独立解码全部字段段。
func (r *Reader) decodeFieldSection(name string, rowCount int) ([]*types.FieldValue, error) {
	var all []*types.FieldValue
	for i := 0; i < r.blockIndex.Len(); i++ {
		vals, err := r.decodeFieldSectionBlock(name, i)
		if err != nil {
			return nil, err
		}
		all = append(all, vals...)
	}
	return all, nil
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

// readTimestampsBlock 只解码指定块（按字节范围）的 timestamps。
func (r *Reader) readTimestampsBlock(blockIdx int) ([]int64, error) {
	bso := r.blockSectionMap.Lookup("_timestamps")
	if bso == nil {
		return nil, fmt.Errorf("no block map entry for _timestamps")
	}
	offset, size := bso.BlockRange(blockIdx)
	if size == 0 {
		return nil, nil
	}

	entry := r.blockIndex.Entry(blockIdx)
	rowCount := int(entry.RowCount)

	// 读取 section 中对应 block 的字节范围
	tsOffset, _, _ := r.sectionTable.LookupByType(SectionTimestamps)
	data := make([]byte, size)
	if _, err := r.file.ReadAt(data, int64(tsOffset+offset)); err != nil {
		return nil, err
	}

	comp := r.sectionTable.LookupCompression("_timestamps")
	var decErr error
	data, decErr = DecompressBlock(data, comp)
	if decErr != nil {
		return nil, fmt.Errorf("decompress timestamps block %d: %w", blockIdx, decErr)
	}

	enc := r.sectionTable.LookupEncoding("_timestamps")
	switch enc {
	case EncodingDeltaVarint:
		return compression.DecodeTimestamps(data, rowCount)
	default:
		return decodeTimestampBatch(data), nil
	}
}

// readSidsBlock 只解码指定块（按字节范围）的 sids。
func (r *Reader) readSidsBlock(blockIdx int) ([]uint64, error) {
	bso := r.blockSectionMap.Lookup("_sids")
	if bso == nil {
		return nil, fmt.Errorf("no block map entry for _sids")
	}
	offset, size := bso.BlockRange(blockIdx)
	if size == 0 {
		entry := r.blockIndex.Entry(blockIdx)
		return make([]uint64, entry.RowCount), nil
	}

	entry := r.blockIndex.Entry(blockIdx)
	rowCount := int(entry.RowCount)

	sidsOffset, _, _ := r.sectionTable.LookupByType(SectionSids)
	data := make([]byte, size)
	if _, err := r.file.ReadAt(data, int64(sidsOffset+offset)); err != nil {
		return nil, err
	}

	comp := r.sectionTable.LookupCompression("_sids")
	var decErr error
	data, decErr = DecompressBlock(data, comp)
	if decErr != nil {
		return nil, fmt.Errorf("decompress sids block %d: %w", blockIdx, decErr)
	}

	return compression.DecodeSidsDelta(data, rowCount)
}

// decodeFieldSectionBlock 只解码指定块（按字节范围）的字段数据。
func (r *Reader) decodeFieldSectionBlock(name string, blockIdx int) ([]*types.FieldValue, error) {
	bso := r.blockSectionMap.Lookup(name)
	if bso == nil {
		entry := r.blockIndex.Entry(blockIdx)
		rowCount := int(entry.RowCount)
		return make([]*types.FieldValue, rowCount), nil
	}
	offset, size := bso.BlockRange(blockIdx)
	if size == 0 {
		entry := r.blockIndex.Entry(blockIdx)
		ft := r.schema.Fields[name]
		rowCount := int(entry.RowCount)
		values := make([]*types.FieldValue, rowCount)
		for i := 0; i < rowCount; i++ {
			values[i] = zeroFieldValue(ft)
		}
		return values, nil
	}

	entry := r.blockIndex.Entry(blockIdx)
	rowCount := int(entry.RowCount)

	secOffset, _ := r.sectionTable.Lookup(name)
	data := make([]byte, size)
	if _, err := r.file.ReadAt(data, int64(secOffset+offset)); err != nil {
		return nil, err
	}

	comp := r.sectionTable.LookupCompression(name)
	var decErr error
	data, decErr = DecompressBlock(data, comp)
	if decErr != nil {
		return nil, fmt.Errorf("decompress field %s block %d: %w", name, blockIdx, decErr)
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
		if len(data) == 0 {
			return nil, fmt.Errorf("decode dict string field %s: empty data", name)
		}
		strVals, err := compression.DecodeStringValues(data[1:], rowCount, data[0] == 1)
		if err != nil {
			return nil, fmt.Errorf("decode dict string field %s: %w", name, err)
		}
		return stringValuesToFieldValues(strVals), nil
	case EncodingBitmapBool:
		boolVals := compression.DecodeBoolValues(data, rowCount)
		return boolValuesToFieldValues(boolVals), nil
	default:
		return r.decodeRawFieldSection(data, rowCount, ft, name), nil
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

// readFieldBlockRaw 读取并解压指定 block 的字段数据，但不解码。
func (r *Reader) readFieldBlockRaw(name string, blockIdx int) ([]byte, error) {
	bso := r.blockSectionMap.Lookup(name)
	if bso == nil {
		return nil, nil
	}
	offset, size := bso.BlockRange(blockIdx)
	if size == 0 {
		return nil, nil
	}

	secOffset, _ := r.sectionTable.Lookup(name)
	data := make([]byte, size)
	if _, err := r.file.ReadAt(data, int64(secOffset+offset)); err != nil {
		return nil, err
	}

	comp := r.sectionTable.LookupCompression(name)
	return DecompressBlock(data, comp)
}

// decodeFieldSectionBlockUpTo 解码指定 block 的字段数据，最多解码 maxRow 行。
func (r *Reader) decodeFieldSectionBlockUpTo(name string, blockIdx int, maxRow int) ([]*types.FieldValue, error) {
	bso := r.blockSectionMap.Lookup(name)
	if bso == nil {
		values := make([]*types.FieldValue, maxRow)
		for i := 0; i < maxRow; i++ {
			values[i] = zeroFieldValue(r.schema.Fields[name])
		}
		return values, nil
	}
	offset, size := bso.BlockRange(blockIdx)
	if size == 0 {
		values := make([]*types.FieldValue, maxRow)
		ft := r.schema.Fields[name]
		for i := 0; i < maxRow; i++ {
			values[i] = zeroFieldValue(ft)
		}
		return values, nil
	}

	secOffset, _ := r.sectionTable.Lookup(name)
	data := make([]byte, size)
	if _, err := r.file.ReadAt(data, int64(secOffset+offset)); err != nil {
		return nil, err
	}

	comp := r.sectionTable.LookupCompression(name)
	var decErr error
	data, decErr = DecompressBlock(data, comp)
	if decErr != nil {
		return nil, fmt.Errorf("decompress field %s block %d: %w", name, blockIdx, decErr)
	}

	enc := r.sectionTable.LookupEncoding(name)
	switch enc {
	case EncodingXORFloat:
		floatVals, err := compression.DecodeFloat64Values(data, maxRow)
		if err != nil {
			return nil, fmt.Errorf("decode xor float field %s: %w", name, err)
		}
		return float64ValuesToFieldValues(floatVals), nil
	case EncodingZigZagVarint:
		intVals, err := compression.DecodeInt64Values(data, maxRow)
		if err != nil {
			return nil, fmt.Errorf("decode zigzag int field %s: %w", name, err)
		}
		return int64ValuesToFieldValues(intVals), nil
	case EncodingDictString:
		if len(data) == 0 {
			return nil, fmt.Errorf("decode dict string field %s: empty data", name)
		}
		strVals, err := compression.DecodeStringValues(data[1:], maxRow, data[0] == 1)
		if err != nil {
			return nil, fmt.Errorf("decode dict string field %s: %w", name, err)
		}
		return stringValuesToFieldValues(strVals), nil
	case EncodingBitmapBool:
		boolVals := compression.DecodeBoolValues(data, maxRow)
		return boolValuesToFieldValues(boolVals), nil
	default:
		return r.decodeRawFieldSection(data, maxRow, r.schema.Fields[name], name), nil
	}
}

// decodeFieldSectionBlockFromData 从已解压的原始字节解码字段值。
func (r *Reader) decodeFieldSectionBlockFromData(name string, data []byte, rowCount int) ([]*types.FieldValue, error) {
	if data == nil {
		ft := r.schema.Fields[name]
		values := make([]*types.FieldValue, rowCount)
		for i := 0; i < rowCount; i++ {
			values[i] = zeroFieldValue(ft)
		}
		return values, nil
	}

	enc := r.sectionTable.LookupEncoding(name)

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
		if len(data) == 0 {
			return nil, fmt.Errorf("decode dict string field %s: empty data", name)
		}
		strVals, err := compression.DecodeStringValues(data[1:], rowCount, data[0] == 1)
		if err != nil {
			return nil, fmt.Errorf("decode dict string field %s: %w", name, err)
		}
		return stringValuesToFieldValues(strVals), nil
	case EncodingBitmapBool:
		boolVals := compression.DecodeBoolValues(data, rowCount)
		return boolValuesToFieldValues(boolVals), nil
	default:
		return r.decodeRawFieldSection(data, rowCount, r.schema.Fields[name], name), nil
	}
}
