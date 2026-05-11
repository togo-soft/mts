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

	// 收集与 [startTime, endTime) 重叠的 block 索引
	var matchingBlocks []int
	for i := startBlock; i < r.blockIndex.Len(); i++ {
		entry := r.blockIndex.Entry(i)
		if entry.FirstTimestamp >= endTime && endTime > 0 {
			break
		}
		if entry.LastTimestamp < startTime {
			continue
		}
		matchingBlocks = append(matchingBlocks, i)
	}

	if len(matchingBlocks) == 0 {
		return nil, nil
	}

	fields := r.sectionTable.FieldNames()

	// v2 优化路径：逐 block 独立解码
	if r.HasBlockSectionMap() {
		return r.readRangeBlocksV2(matchingBlocks, startTime, endTime, fields)
	}

	// v1 路径：全量解码后切片
	return r.readRangeBlocksV1(matchingBlocks, startTime, endTime, fields)
}

// readRangeBlocksV2 使用 BlockSectionMap 逐 block 按需解码（v2 格式）。
func (r *Reader) readRangeBlocksV2(matchingBlocks []int, startTime, endTime int64, fields []string) ([]*types.PointRow, error) {
	var rows []*types.PointRow

	for _, blockIdx := range matchingBlocks {
		entry := r.blockIndex.Entry(blockIdx)

		timestamps, err := r.readTimestampsBlock(blockIdx)
		if err != nil {
			return nil, err
		}
		sids, err := r.readSidsBlock(blockIdx)
		if err != nil {
			return nil, err
		}

		// 预解码该 block 的所有字段
		decodedFields := make(map[string][]*types.FieldValue, len(fields))
		for _, name := range fields {
			vals, err := r.decodeFieldSectionBlock(name, blockIdx)
			if err != nil {
				return nil, err
			}
			decodedFields[name] = vals
		}

		// 在该 block 内按时间过滤
		for i, ts := range timestamps {
			if ts >= startTime && (endTime <= 0 || ts < endTime) {
				row := &types.PointRow{
					Timestamp: ts,
					Tags:      nil,
					Fields:    make(map[string]*types.FieldValue),
				}
				if i < len(sids) {
					row.Sid = sids[i]
				}
				for _, name := range fields {
					if vals, ok := decodedFields[name]; ok && i < len(vals) {
						row.Fields[name] = vals[i]
					}
				}
				rows = append(rows, row)
			}
		}
		_ = entry // keep reference for clarity
	}

	return rows, nil
}

// readRangeBlocksV1 全量解码后按匹配索引切片（v1 格式兼容）。
func (r *Reader) readRangeBlocksV1(matchingBlocks []int, startTime, endTime int64, fields []string) ([]*types.PointRow, error) {
	type blockInfo struct {
		offset   uint32
		rowCount uint32
	}

	var blocks []blockInfo
	for _, idx := range matchingBlocks {
		entry := r.blockIndex.Entry(idx)
		blocks = append(blocks, blockInfo{
			offset:   entry.Offset,
			rowCount: entry.RowCount,
		})
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
