package sstable

import (
	"codeberg.org/micro-ts/mts/types"
)

// ReadRange 读取指定时间范围内的数据。
// maxRows 限制返回行数（0 表示无限制）。
func (r *Reader) ReadRange(startTime, endTime int64, maxRows int) ([]*types.PointRow, error) {
	if r.blockIndex == nil || r.blockIndex.Len() == 0 {
		return nil, nil
	}
	startBlock := r.blockIndex.FindBlock(startTime)
	if startBlock >= r.blockIndex.Len() {
		return nil, nil
	}

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
	return r.readRangeBlocks(matchingBlocks, startTime, endTime, fields, maxRows)
}

// readRangeBlocks 使用 BlockSectionMap 逐 block 按需解码。
func (r *Reader) readRangeBlocks(matchingBlocks []int, startTime, endTime int64, fields []string, maxRows int) ([]*types.PointRow, error) {
	var rows []*types.PointRow

	for _, blockIdx := range matchingBlocks {
		timestamps, err := r.readTimestampsBlock(blockIdx)
		if err != nil {
			return nil, err
		}
		sids, err := r.readSidsBlock(blockIdx)
		if err != nil {
			return nil, err
		}

		decodedFields := make(map[string][]*types.FieldValue, len(fields))
		for _, name := range fields {
			vals, err := r.decodeFieldSectionBlock(name, blockIdx)
			if err != nil {
				return nil, err
			}
			decodedFields[name] = vals
		}

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
				if maxRows > 0 && len(rows) >= maxRows {
					return rows, nil
				}
			}
		}
	}

	return rows, nil
}
