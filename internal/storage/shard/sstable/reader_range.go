package sstable

import (
	"codeberg.org/micro-ts/mts/types"
)

// ReadRange 读取指定时间范围内的数据。
func (r *Reader) ReadRange(startTime, endTime int64) ([]*types.PointRow, error) {
	if r.blockIndex != nil && r.blockIndex.Len() > 0 {
		return r.readRangeOptimized(startTime, endTime)
	}
	return r.readRangeFullScan(startTime, endTime)
}

func (r *Reader) readRangeOptimized(startTime, endTime int64) ([]*types.PointRow, error) {
	startBlock := r.blockIndex.FindBlock(startTime)
	if startBlock >= r.blockIndex.Len() {
		return nil, nil
	}

	type blockInfo struct {
		offset   uint32
		rowCount uint32
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
			offset:   entry.Offset,
			rowCount: entry.RowCount,
		})
	}

	if len(blocks) == 0 {
		return nil, nil
	}

	var allTimestamps []int64
	var allSids []uint64

	for _, b := range blocks {
		ts, err := r.readTimestampRange(b.offset, b.rowCount)
		if err != nil {
			return nil, err
		}
		sids, err := r.readSidsRange(b.offset, b.rowCount)
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

	// 使用编码感知的字段解码器
	fields := r.sectionTable.FieldNames()
	rowCount := int(r.header.RowCount)
	decodedFields, err := r.ReadAllDecodedFieldSections(fields, rowCount)
	if err != nil {
		return nil, err
	}

	rows := make([]*types.PointRow, 0, len(matchingIndices))
	for _, idx := range matchingIndices {
		row := &types.PointRow{
			Sid:       allSids[idx],
			Timestamp: allTimestamps[idx],
			Tags:      nil,
			Fields:    make(map[string]*types.FieldValue),
		}
		for _, name := range fields {
			if vals, ok := decodedFields[name]; ok && idx < len(vals) {
				row.Fields[name] = vals[idx]
			}
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func (r *Reader) readRangeFullScan(startTime, endTime int64) ([]*types.PointRow, error) {
	timestamps, err := r.readTimestamps()
	if err != nil {
		return nil, err
	}

	sids, err := r.readSids(len(timestamps))
	if err != nil {
		return nil, err
	}

	fields := r.sectionTable.FieldNames()
	decodedFields, err := r.ReadAllDecodedFieldSections(fields, len(timestamps))
	if err != nil {
		return nil, err
	}

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
				if vals, ok := decodedFields[name]; ok && i < len(vals) {
					row.Fields[name] = vals[i]
				}
			}
			rows = append(rows, row)
		}
	}

	return rows, nil
}
