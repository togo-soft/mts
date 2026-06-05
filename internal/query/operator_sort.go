package query

import (
	"container/heap"
	"context"
	"sort"

	"codeberg.org/micro-ts/mts/types"
)

// SortOperator 对上游所有数据进行全量排序后逐行输出。
type SortOperator struct {
	upstream  Operator
	fields    []*types.SortField
	rows      []*types.PointRow
	pos       int
	limitHint int // 非零时使用堆选 Top-K 代替全量排序
}

// NewSortOperator 创建排序算子。
func NewSortOperator(upstream Operator, fields []*types.SortField) *SortOperator {
	return &SortOperator{upstream: upstream, fields: fields}
}

// Open 初始化上游算子并加载排序。
func (s *SortOperator) Open(ctx context.Context) error {
	if err := s.upstream.Open(ctx); err != nil {
		return err
	}
	return s.loadAndSort()
}

// loadAndSort 加载上游所有数据并排序。
func (s *SortOperator) loadAndSort() error {
	var rows []*types.PointRow
	for {
		row, err := s.upstream.Next()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}

	if s.limitHint > 0 && s.limitHint < len(rows) {
		s.rows = s.topK(rows, s.limitHint)
	} else {
		s.sortRows(rows)
		s.rows = rows
	}
	return nil
}

// topK 使用堆选择 Top-K 元素（避免全量排序）。
func (s *SortOperator) topK(rows []*types.PointRow, k int) []*types.PointRow {
	if k >= len(rows) {
		s.sortRows(rows)
		return rows
	}

	h := &maxHeap{fields: s.fields}
	for i := 0; i < k; i++ {
		heap.Push(h, rows[i])
	}
	for i := k; i < len(rows); i++ {
		if s.less(rows[i], h.rows[0]) {
			h.rows[0] = rows[i]
			heap.Fix(h, 0)
		}
	}

	result := make([]*types.PointRow, k)
	copy(result, h.rows)
	s.sortRows(result)
	return result
}

// maxHeap 用于 Top-K 选择（内部存储最大堆，堆顶为当前最大元素）。
type maxHeap struct {
	rows   []*types.PointRow
	fields []*types.SortField
}

func (h *maxHeap) Len() int { return len(h.rows) }

func (h *maxHeap) Less(i, j int) bool {
	return h.compare(h.rows[i], h.rows[j]) > 0
}

func (h *maxHeap) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }

func (h *maxHeap) Push(x any) { h.rows = append(h.rows, x.(*types.PointRow)) }

func (h *maxHeap) Pop() any {
	old := h.rows
	n := len(old)
	x := old[n-1]
	h.rows = old[:n-1]
	return x
}

func (h *maxHeap) compare(a, b *types.PointRow) int {
	return compareRows(a, b, h.fields)
}

// sortRows 对行进行全量排序。
func (s *SortOperator) sortRows(rows []*types.PointRow) {
	sort.Slice(rows, func(i, j int) bool {
		return s.less(rows[i], rows[j])
	})
}

// less 比较两行。
func (s *SortOperator) less(a, b *types.PointRow) bool {
	return compareRows(a, b, s.fields) < 0
}

// compareRows 按排序字段比较两行。
func compareRows(a, b *types.PointRow, fields []*types.SortField) int {
	for _, sf := range fields {
		cmp := compareField(a, b, sf)
		if cmp != 0 {
			if sf.Direction == types.SortDirection_DESC {
				return -cmp
			}
			return cmp
		}
	}
	return 0
}

// compareField 比较两行的单个字段。
func compareField(a, b *types.PointRow, sf *types.SortField) int {
	if sf.Field == "timestamp" {
		if a.Timestamp < b.Timestamp {
			return -1
		}
		if a.Timestamp > b.Timestamp {
			return 1
		}
		return 0
	}

	va := getFieldValue(a, sf.Field)
	vb := getFieldValue(b, sf.Field)
	return compareFieldValue(va, vb)
}

// getFieldValue 获取行中指定字段的 FieldValue。
func getFieldValue(row *types.PointRow, field string) *types.FieldValue {
	for _, fe := range row.Fields {
		if fe.Key == field {
			return fe.Value
		}
	}
	return nil
}

// Next 返回下一行排序结果。
func (s *SortOperator) Next() (*types.PointRow, error) {
	if s.pos >= len(s.rows) {
		return nil, nil
	}
	row := s.rows[s.pos]
	s.pos++
	return row, nil
}

// Close 关闭上游算子。
func (s *SortOperator) Close() error {
	return s.upstream.Close()
}
