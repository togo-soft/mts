// internal/query/executor.go
package query

import (
	"context"

	"micro-ts/types"
)

// Executor 查询执行器
type Executor struct {
	// 依赖注入，后续扩展
}

// NewExecutor 创建 Executor
func NewExecutor(engine interface{}) *Executor {
	return &Executor{}
}

// Execute 执行查询
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
