package sstable

import (
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"

	"codeberg.org/micro-ts/mts/types"
)

// ReadRange 读取指定时间范围内的数据。
func (r *Reader) ReadRange(startTime, endTime int64) ([]*types.PointRow, error) {
	dataDir := r.dataDir

	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return nil, nil
	}

	if r.blockIndex != nil && r.blockIndex.Len() > 0 {
		return r.readRangeOptimized(dataDir, startTime, endTime)
	}

	return r.readRangeFullScan(dataDir, startTime, endTime)
}

func (r *Reader) readRangeOptimized(dataDir string, startTime, endTime int64) ([]*types.PointRow, error) {
	tsFile, err := os.Open(filepath.Join(dataDir, "_timestamps.bin"))
	if err != nil {
		return nil, err
	}
	defer func() { _ = tsFile.Close() }()

	startBlock := r.blockIndex.FindBlock(startTime)
	if startBlock >= r.blockIndex.Len() {
		return nil, nil
	}

	type blockInfo struct {
		blockIdx int
		offset   uint32
		rowCount uint32
		firstTs  int64
		lastTs   int64
	}

	var blocks []blockInfo
	for i := startBlock; i < r.blockIndex.Len(); i++ {
		entry := r.blockIndex.Entry(i)
		if entry.FirstTimestamp >= endTime && endTime > 0 {
			break
		}
		if entry.LastTimestamp < startTime {
			continue
		}
		blocks = append(blocks, blockInfo{
			blockIdx: i,
			offset:   entry.Offset,
			rowCount: entry.RowCount,
			firstTs:  entry.FirstTimestamp,
			lastTs:   entry.LastTimestamp,
		})
	}

	if len(blocks) == 0 {
		return nil, nil
	}

	var allTimestamps []int64
	var allSids []uint64

	for _, b := range blocks {
		ts, err := r.readTimestampRange(tsFile, b.offset, b.rowCount)
		if err != nil {
			return nil, err
		}

		sids, err := r.readSidsRange(dataDir, b.offset, b.rowCount)
		if err != nil {
			return nil, err
		}

		allTimestamps = append(allTimestamps, ts...)
		allSids = append(allSids, sids...)
	}

	var matchingIndices []int
	for i, ts := range allTimestamps {
		if ts >= startTime && (endTime <= 0 || ts < endTime) {
			matchingIndices = append(matchingIndices, i)
		}
	}

	if len(matchingIndices) == 0 {
		return nil, nil
	}

	entries, err := os.ReadDir(filepath.Join(dataDir, "fields"))
	if err != nil {
		return nil, err
	}

	fields := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			fields = append(fields, e.Name()[:len(e.Name())-4])
		}
	}

	fieldData := make(map[string][]byte)
	for _, name := range fields {
		f, err := os.Open(filepath.Join(dataDir, "fields", name+".bin"))
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(f)
		if closeErr := f.Close(); closeErr != nil {
			return nil, closeErr
		}
		if err != nil {
			return nil, err
		}
		fieldData[name] = data
	}

	offsets := r.computeOffsets(fields, fieldData, len(allTimestamps))

	rows := make([]*types.PointRow, 0, len(matchingIndices))
	for _, idx := range matchingIndices {
		row := &types.PointRow{
			Sid:       allSids[idx],
			Timestamp: allTimestamps[idx],
			Tags:      nil,
			Fields:    make(map[string]*types.FieldValue),
		}

		for _, name := range fields {
			row.Fields[name] = r.decodeFieldValue(fieldData[name], offsets[name][idx], name)
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func (r *Reader) readRangeFullScan(dataDir string, startTime, endTime int64) ([]*types.PointRow, error) {
	tsFile, err := os.Open(filepath.Join(dataDir, "_timestamps.bin"))
	if err != nil {
		return nil, err
	}
	timestamps, err := r.readTimestamps(tsFile)
	if closeErr := tsFile.Close(); closeErr != nil {
		return nil, closeErr
	}
	if err != nil {
		return nil, err
	}

	sids, err := r.readSids(dataDir, len(timestamps))
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(filepath.Join(dataDir, "fields"))
	if err != nil {
		return nil, err
	}

	fields := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			fields = append(fields, e.Name()[:len(e.Name())-4])
		}
	}

	fieldData := make(map[string][]byte)
	for _, name := range fields {
		f, err := os.Open(filepath.Join(dataDir, "fields", name+".bin"))
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(f)
		if closeErr := f.Close(); closeErr != nil {
			return nil, closeErr
		}
		if err != nil {
			return nil, err
		}
		fieldData[name] = data
	}

	offsets := r.computeOffsets(fields, fieldData, len(timestamps))

	var rows []*types.PointRow
	for i, ts := range timestamps {
		if ts >= startTime && (endTime <= 0 || ts < endTime) {
			row := &types.PointRow{
				Sid:       sids[i],
				Timestamp: ts,
				Tags:      nil,
				Fields:    make(map[string]*types.FieldValue),
			}

			for _, name := range fields {
				row.Fields[name] = r.decodeFieldValue(fieldData[name], offsets[name][i], name)
			}

			rows = append(rows, row)
		}
	}

	return rows, nil
}

// decodeFieldValue 解码字段值
func (r *Reader) decodeFieldValue(data []byte, offset int, fieldName string) *types.FieldValue {
	fieldType := r.schema.Fields[fieldName]

	switch fieldType {
	case FieldTypeFloat64:
		if offset+8 > len(data) {
			return types.NewFieldValue(float64(0))
		}
		bits := binary.BigEndian.Uint64(data[offset : offset+8])
		return types.NewFieldValue(math.Float64frombits(bits))
	case FieldTypeInt64:
		if offset+8 > len(data) {
			return types.NewFieldValue(int64(0))
		}
		bits := binary.BigEndian.Uint64(data[offset : offset+8])
		return types.NewFieldValue(int64(bits))
	case FieldTypeString:
		if offset+4 > len(data) {
			return types.NewFieldValue("")
		}
		strLen := binary.BigEndian.Uint32(data[offset : offset+4])
		start := offset + 4
		end := start + int(strLen)
		if end > len(data) {
			return types.NewFieldValue(string(data[start:]))
		}
		return types.NewFieldValue(string(data[start:end]))
	case FieldTypeBool:
		if offset >= len(data) {
			return types.NewFieldValue(false)
		}
		return types.NewFieldValue(data[offset] != 0)
	default:
		if offset+8 > len(data) {
			return types.NewFieldValue(nil)
		}
		bits := binary.BigEndian.Uint64(data[offset : offset+8])
		return types.NewFieldValue(bits)
	}
}
