// Package query 实现查询处理和执行。
//
// 各算子文件定义：
//
//	operator_scan.go              — ScanOperator（数据源扫描）
//	operator_filtered_scan.go     — FilteredScanOperator（融合 Scan+Filter）
//	operator_filter.go            — FilterOperator（行级过滤）
//	operator_shared.go            — compareFieldValue、fieldValueFloat（共享工具）
//	operator_accumulators.go      — aggAccumulator 接口及 8 种累加器
//	operator_group_aggregate.go   — GroupAggregateOperator（分组聚合）
//	operator_sort.go              — SortOperator（排序 + Top-K 堆）
//	operator_project.go           — ProjectOperator（字段投影）
//	operator_limit.go             — LimitOperator（Offset/Limit 截断）
//
// 管道模式：Scan → Filter → GroupAggregate → Sort → Project → Limit
package query

import (
	"context"

	"codeberg.org/micro-ts/mts/types"
)

// bgCtx 是 package 级 context.Background() 单例，用于算子内部调用 Iterator.Next()，
// 避免每行分配新的 context.valueCtx。
var bgCtx = context.Background()

// Operator 是查询执行计划中的单个算子。
//
// 算子链按顺序连接：上游算子的输出作为下游算子的输入。
// Open 初始化资源，Next 返回下一行（无数据时返回 nil），Close 释放资源。
type Operator interface {
	Open(ctx context.Context) error
	Next() (*types.PointRow, error)
	Close() error
}
