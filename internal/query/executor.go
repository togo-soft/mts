// Package query 实现查询处理和执行。
//
// query 包包含查询执行器和迭代器，用于处理范围查询和流式查询。
//
// 组件：
//
//	Executor:   查询执行器，负责 SQL 解析和计划生成
//	QueryIterator: 流式迭代器，用于大数据集查询
//
// 查询流程：
//
//	请求 → Parser → Planner → Executor → 数据源 → 结果
package query

import (
	"context"

	"micro-ts/types"
)

// Executor 是查询执行器。
//
// Executor 负责解析和执行查询请求。
// 当前是框架实现，未来将支持完整的 SQL 查询解析。
//
// 字段说明：
//
//	目前无具体字段，未来会添加解析器和优化器。
type Executor struct {
	// 依赖注入，后续扩展
}

// NewExecutor 创建新的查询执行器。
//
// 参数：
//   - engine: 存储引擎实例（当前未使用，为接口预留）
//
// 返回：
//   - *Executor: 查询执行器实例
//
// 说明：
//
//	当前实现为基础框架，engine 参数用于未来依赖注入。
//	完整的实现需要与 engine 集成以实际查询数据。
func NewExecutor(engine any) *Executor {
	return &Executor{}
}

// Execute 执行查询请求。
//
// 参数：
//   - ctx: 上下文，用于取消查询
//   - req: 查询请求
//
// 返回：
//   - *types.QueryRangeResponse: 查询结果（当前为空结果）
//   - error: 执行成功时返回 nil
//
// 当前实现：
//
//	基础实现仅返回空结果，不报错。
//	后续需要与存储引擎集成以实际查询数据。
func (e *Executor) Execute(ctx context.Context, req *types.QueryRangeRequest) (*types.QueryRangeResponse, error) {
	// 基础实现：返回空结果
	// 完整实现需要在 engine 注入后完成
	return &types.QueryRangeResponse{
		Database:    req.Database,
		Measurement: req.Measurement,
		StartTime:   req.StartTime,
		EndTime:     req.EndTime,
		TotalCount:  0,
		HasMore:     false,
		Rows:        []types.PointRow{},
	}, nil
}
