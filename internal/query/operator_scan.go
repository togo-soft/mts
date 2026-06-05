package query

import (
	"context"

	"codeberg.org/micro-ts/mts/types"
)

// ScanOperator 包装现有 query.Iterator 实现数据源扫描。
// 它是 Pipeline 的起点，内部复用 min-heap 归并排序。
type ScanOperator struct {
	iter       *Iterator
	currentRow *types.PointRow
}

// NewScanOperator 创建扫描算子。
func NewScanOperator(iter *Iterator) *ScanOperator {
	return &ScanOperator{iter: iter}
}

// Open 无需额外初始化（Iterator 已在构造时完成初始化）。
func (s *ScanOperator) Open(_ context.Context) error {
	return nil
}

// Next 从 Iterator 获取下一行。
func (s *ScanOperator) Next() (*types.PointRow, error) {
	if s.iter.Next(bgCtx) {
		s.currentRow = s.iter.Points()
		return s.currentRow, nil
	}
	return nil, nil
}

// Close 释放底层 Iterator 资源。
func (s *ScanOperator) Close() error {
	return s.iter.Close()
}
