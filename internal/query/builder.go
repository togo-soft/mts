package query

import (
	"strings"

	"codeberg.org/micro-ts/mts/types"
)

// FilterOp 是过滤操作符的类型别名。
type FilterOp = types.FilterOp

// SortDirection 是排序方向的类型别名。
type SortDirection = types.SortDirection

// 导出的过滤操作符常量。
const (
	EQ  = types.FilterOp_EQ
	NE  = types.FilterOp_NE
	GT  = types.FilterOp_GT
	GTE = types.FilterOp_GTE
	LT  = types.FilterOp_LT
	LTE = types.FilterOp_LTE
)

// 导出的排序方向常量。
const (
	ASC  = types.SortDirection_ASC
	DESC = types.SortDirection_DESC
)

// Builder 通过链式调用构建 QueryPlan。
//
// 用法示例：
//
//	plan := query.NewBuilder().
//	    Select("avg(cpu)", "host").
//	    From("monitoring", "server_metrics").
//	    Where("host", query.EQ, "web-01").
//	    GroupBy("host").
//	    OrderBy("avg_cpu", query.DESC).
//	    Limit(100).
//	    Build()
type Builder struct {
	database    string
	measurement string
	startTime   int64
	endTime     int64
	selectArgs  []string
	whereConds  []*types.FilterCondition
	groupByTags []string
	sortFields  []*types.SortField
	offset      int64
	limit       int64
}

// NewBuilder 创建新的查询构建器。
func NewBuilder() *Builder {
	return &Builder{}
}

// Select 设置查询的输出列。
// 支持聚合函数格式 "avg(field)"、"sum(field)" 等，以及普通字段名。
func (b *Builder) Select(args ...string) *Builder {
	b.selectArgs = append(b.selectArgs, args...)
	return b
}

// From 设置查询的 database 和 measurement。
func (b *Builder) From(database, measurement string) *Builder {
	b.database = database
	b.measurement = measurement
	return b
}

// TimeRange 设置查询时间范围（纳秒时间戳）。
func (b *Builder) TimeRange(startTime, endTime int64) *Builder {
	b.startTime = startTime
	b.endTime = endTime
	return b
}

// Where 添加过滤条件，多次调用为 AND 关系。
func (b *Builder) Where(tagOrField string, op types.FilterOp, value *types.FieldValue) *Builder {
	cond := &types.FilterCondition{Op: op, Value: value}
	if strings.HasPrefix(tagOrField, "tags.") {
		cond.Tag = tagOrField[5:]
	} else {
		cond.Field = tagOrField
	}
	b.whereConds = append(b.whereConds, cond)
	return b
}

// GroupBy 设置分组标签。
func (b *Builder) GroupBy(tags ...string) *Builder {
	b.groupByTags = append(b.groupByTags, tags...)
	return b
}

// OrderBy 添加排序字段。
func (b *Builder) OrderBy(field string, direction types.SortDirection) *Builder {
	b.sortFields = append(b.sortFields, &types.SortField{
		Field:     field,
		Direction: direction,
	})
	return b
}

// Offset 设置跳过行数。
func (b *Builder) Offset(n int64) *Builder {
	b.offset = n
	return b
}

// Limit 设置最大返回行数。
func (b *Builder) Limit(n int64) *Builder {
	b.limit = n
	return b
}

// Build 构建 QueryPlan。
func (b *Builder) Build() *types.QueryPlan {
	plan := &types.QueryPlan{
		Database:    b.database,
		Measurement: b.measurement,
		StartTime:   b.startTime,
		EndTime:     b.endTime,
	}

	plan.Ops = append(plan.Ops, &types.OperatorSpec{
		Op: &types.OperatorSpec_Scan{Scan: &types.ScanSpec{}},
	})

	// 解析 Select 参数：区分聚合函数和普通字段
	aggFuncs, plainFields := parseSelectArgs(b.selectArgs)

	if len(b.whereConds) > 0 {
		plan.Ops = append(plan.Ops, &types.OperatorSpec{
			Op: &types.OperatorSpec_Filter{Filter: &types.FilterSpec{Conditions: b.whereConds}},
		})
	}

	if len(b.groupByTags) > 0 || len(aggFuncs) > 0 {
		plan.Ops = append(plan.Ops, &types.OperatorSpec{
			Op: &types.OperatorSpec_GroupBy{GroupBy: &types.GroupBySpec{Tags: b.groupByTags}},
		})
		plan.Ops = append(plan.Ops, &types.OperatorSpec{
			Op: &types.OperatorSpec_Aggregate{Aggregate: &types.AggregateSpec{Functions: aggFuncs}},
		})
	}

	if len(b.sortFields) > 0 {
		plan.Ops = append(plan.Ops, &types.OperatorSpec{
			Op: &types.OperatorSpec_Sort{Sort: &types.SortSpec{Fields: b.sortFields}},
		})
	}

	if len(plainFields) > 0 {
		plan.Ops = append(plan.Ops, &types.OperatorSpec{
			Op: &types.OperatorSpec_Project{Project: &types.ProjectSpec{Fields: plainFields}},
		})
	}

	if b.offset > 0 || b.limit > 0 {
		plan.Ops = append(plan.Ops, &types.OperatorSpec{
			Op: &types.OperatorSpec_Limit{Limit: &types.LimitSpec{Offset: b.offset, Limit: b.limit}},
		})
	}

	return plan
}

// parseSelectArgs 解析 Select 参数，区分聚合函数和普通字段。
// "avg(cpu)" → AggFunction{Function:"avg", Field:"cpu"}
// "host" → plain field
func parseSelectArgs(args []string) ([]*types.AggFunction, []string) {
	var aggFuncs []*types.AggFunction
	var plainFields []string

	for _, arg := range args {
		if fn, field, ok := parseAggArg(arg); ok {
			aggFuncs = append(aggFuncs, &types.AggFunction{
				Function: fn,
				Field:    field,
			})
		} else {
			plainFields = append(plainFields, arg)
		}
	}

	return aggFuncs, plainFields
}

// parseAggArg 解析单个聚合参数，如 "avg(cpu)" → ("avg", "cpu", true)。
func parseAggArg(arg string) (fn, field string, ok bool) {
	idx := strings.Index(arg, "(")
	if idx < 0 {
		return "", "", false
	}
	end := strings.Index(arg, ")")
	if end < 0 || end != len(arg)-1 || end <= idx+1 {
		return "", "", false
	}
	fn = arg[:idx]
	field = arg[idx+1 : end]
	if fn == "" || field == "" {
		return "", "", false
	}
	return fn, field, true
}
