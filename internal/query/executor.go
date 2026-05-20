// Package query 实现查询处理和执行。
//
// executor.go 负责将 QueryPlan（proto）解析为算子 Pipeline。
//
// 流程：
//
//	QueryPlan → BuildPipeline(scan, ops) → Operator chain → RowIterator
//
// RowIterator 封装算子链头，提供统一的 Next/Points/Close 接口。
package query

import (
	"context"
	"fmt"

	"codeberg.org/micro-ts/mts/types"
)

// RowIterator 封装算子 Pipeline 的执行结果。
//
// 提供与 Iterator 兼容的接口，调用方通过 Next/Points 逐行获取数据。
type RowIterator struct {
	head       Operator
	currentRow *types.PointRow
	closed     bool
}

// NewRowIterator 创建行迭代器。
func NewRowIterator(head Operator) *RowIterator {
	return &RowIterator{head: head}
}

// Open 初始化整个算子链。
func (r *RowIterator) Open(ctx context.Context) error {
	return r.head.Open(ctx)
}

// Next 移动到下一行。
func (r *RowIterator) Next(ctx context.Context) bool {
	if r.closed {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	default:
	}
	row, err := r.head.Next()
	if err != nil || row == nil {
		return false
	}
	r.currentRow = row
	return true
}

// Points 返回当前行。
func (r *RowIterator) Points() *types.PointRow {
	row := r.currentRow
	r.currentRow = nil
	return row
}

// Close 释放整个算子链的资源。
func (r *RowIterator) Close() error {
	r.closed = true
	return r.head.Close()
}

// BuildPipeline 根据 QueryPlan 中的算子列表构建算子链。
//
// 参数：
//   - scanIter: 已初始化的数据源迭代器（由 Engine 创建）
//   - ops: QueryPlan 中的算子规格列表
//
// 返回：
//   - Operator: 算子链头部（调用方通过 Next 拉取数据）
//   - error: 构建失败时返回错误
//
// 优化：检测 Sort → Project → Limit 模式，将 limitHint 下推到 Sort 算子，
// 使其使用堆选 Top-K 代替全量排序。
func BuildPipeline(scanIter *Iterator, ops []*types.OperatorSpec) (Operator, error) {
	if len(ops) == 0 {
		return nil, fmt.Errorf("empty operator list")
	}

	var head Operator = NewScanOperator(scanIter)
	var groupByTags []string
	var lastSort *SortOperator

	for _, spec := range ops[1:] { // 跳过第一个 Scan
		switch op := spec.Op.(type) {
		case *types.OperatorSpec_Scan:
			// skip
		case *types.OperatorSpec_Filter:
			head = NewFilterOperator(head, op.Filter.Conditions)
		case *types.OperatorSpec_GroupBy:
			groupByTags = op.GroupBy.Tags
		case *types.OperatorSpec_Aggregate:
			head = NewGroupAggregateOperator(head, groupByTags, op.Aggregate.Functions)
			lastSort = nil
		case *types.OperatorSpec_Sort:
			sortOp := NewSortOperator(head, op.Sort.Fields)
			head = sortOp
			lastSort = sortOp
		case *types.OperatorSpec_Project:
			head = NewProjectOperator(head, op.Project.Fields)
			// 穿透 Project：lastSort 保持
		case *types.OperatorSpec_Limit:
			if lastSort != nil {
				lastSort.limitHint = int(op.Limit.Offset + op.Limit.Limit)
			}
			head = NewLimitOperator(head, op.Limit.Offset, op.Limit.Limit)
		default:
			return nil, fmt.Errorf("unknown operator type: %T", op)
		}
	}

	return head, nil
}
