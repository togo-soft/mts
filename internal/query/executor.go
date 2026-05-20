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

	// 优化：检测 Sort→Project→Limit 模式，当 Sort 字段 ⊆ Project 字段时，
	// 将 Project 移到 Sort 之前（Sort 处理裁剪后的轻量行）
	ops = reorderSortProject(ops)

	var head Operator = NewScanOperator(scanIter)
	var groupByTags []string
	var lastSort *SortOperator

	for _, spec := range ops[1:] { // 跳过第一个 Scan
		switch op := spec.Op.(type) {
		case *types.OperatorSpec_Scan:
			// skip
		case *types.OperatorSpec_Filter:
			// Scan→Filter 融合：消除一层 interface 分发
			if _, isScan := head.(*ScanOperator); isScan {
				head = NewFilteredScanOperator(scanIter, op.Filter.Conditions)
			} else {
				head = NewFilterOperator(head, op.Filter.Conditions)
			}
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

// reorderSortProject 检测 Sort→Project→Limit 连续模式，
// 当 Sort 的排序字段是 Project 字段的子集时，交换 Sort 和 Project 位置。
func reorderSortProject(ops []*types.OperatorSpec) []*types.OperatorSpec {
	for i := 0; i+2 < len(ops); i++ {
		if ops[i].GetSort() == nil {
			continue
		}
		if ops[i+1].GetProject() == nil {
			continue
		}
		if ops[i+2].GetLimit() == nil {
			continue
		}
		sortFields := ops[i].GetSort().Fields
		projFields := ops[i+1].GetProject().Fields
		if sortFieldsSubsetOfProject(sortFields, projFields) {
			ops[i], ops[i+1] = ops[i+1], ops[i]
		}
	}
	return ops
}

// sortFieldsSubsetOfProject 检查所有排序字段是否都在投影字段中。
// timestamp 总是可用，无需在 Project 中显式声明。
func sortFieldsSubsetOfProject(sortFields []*types.SortField, projFields []string) bool {
	projSet := make(map[string]bool, len(projFields))
	for _, f := range projFields {
		projSet[f] = true
	}
	for _, sf := range sortFields {
		if sf.Field == "timestamp" {
			continue
		}
		if !projSet[sf.Field] {
			return false
		}
	}
	return true
}
