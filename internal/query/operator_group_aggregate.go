package query

import (
	"context"
	"strings"

	"codeberg.org/micro-ts/mts/types"
)

// GroupAggregateOperator 按标签分组并对每个组执行聚合函数（流式累加器，O(1) 内存）。
type GroupAggregateOperator struct {
	upstream      Operator
	groupByTags   []string
	aggSpecs      []*types.AggFunction
	results       []*types.PointRow
	pos           int
	fieldIndex    map[string]int
	groupKeyCache map[string]string // 查询级缓存，随算子生命周期释放
}

// NewGroupAggregateOperator 创建分组聚合算子。
func NewGroupAggregateOperator(upstream Operator, groupByTags []string, aggSpecs []*types.AggFunction) *GroupAggregateOperator {
	return &GroupAggregateOperator{
		upstream:    upstream,
		groupByTags: groupByTags,
		aggSpecs:    aggSpecs,
	}
}

// Open 初始化上游算子并执行流式聚合。
func (g *GroupAggregateOperator) Open(ctx context.Context) error {
	if err := g.upstream.Open(ctx); err != nil {
		return err
	}
	return g.loadAndAggregate()
}

// createAccumulators 根据聚合规格创建累加器列表。
func (g *GroupAggregateOperator) createAccumulators() []aggAccumulator {
	accs := make([]aggAccumulator, 0, len(g.aggSpecs))
	for _, spec := range g.aggSpecs {
		switch spec.Function {
		case "avg":
			accs = append(accs, &avgAccumulator{field: spec.Field, outputKey: "avg_" + spec.Field})
		case "max":
			accs = append(accs, &maxAccumulator{field: spec.Field, outputKey: "max_" + spec.Field})
		case "min":
			accs = append(accs, &minAccumulator{field: spec.Field, outputKey: "min_" + spec.Field})
		case "sum":
			accs = append(accs, &sumAccumulator{field: spec.Field, outputKey: "sum_" + spec.Field})
		case "count":
			accs = append(accs, &countAccumulator{field: spec.Field, outputKey: "count_" + spec.Field})
		case "first":
			accs = append(accs, &firstAccumulator{field: spec.Field, outputKey: "first_" + spec.Field})
		case "last":
			accs = append(accs, &lastAccumulator{field: spec.Field, outputKey: "last_" + spec.Field})
		case "diff":
			accs = append(accs, &rangeAccumulator{field: spec.Field, outputKey: "diff_" + spec.Field, fn: "diff"})
		case "rate":
			accs = append(accs, &rangeAccumulator{field: spec.Field, outputKey: "rate_" + spec.Field, fn: "rate"})
		case "irate":
			accs = append(accs, &rangeAccumulator{field: spec.Field, outputKey: "irate_" + spec.Field, fn: "irate"})
		case "derivative":
			accs = append(accs, &rangeAccumulator{field: spec.Field, outputKey: "derivative_" + spec.Field, fn: "derivative"})
		}
	}
	return accs
}

// loadAndAggregate 流式读取上游数据，实时更新累加器（不物化全量行）。
func (g *GroupAggregateOperator) loadAndAggregate() error {
	groups := make(map[string][]aggAccumulator)

	for {
		row, err := g.upstream.Next()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}

		if g.fieldIndex == nil {
			g.fieldIndex = make(map[string]int, len(row.Fields))
			for i, f := range row.Fields {
				g.fieldIndex[f.Key] = i
			}
		}

		key := g.groupKey(row)
		accs, ok := groups[key]
		if !ok {
			accs = g.createAccumulators()
			groups[key] = accs
		}
		for _, acc := range accs {
			acc.update(row, g.fieldIndex)
		}
	}

	for key, accs := range groups {
		g.results = append(g.results, g.buildResultRow(key, accs))
	}
	return nil
}

// buildResultRow 从累加器构建结果行。
func (g *GroupAggregateOperator) buildResultRow(key string, accs []aggAccumulator) *types.PointRow {
	tags := g.parseGroupKey(key)
	fields := make([]*types.FieldEntry, 0, len(accs))
	for _, acc := range accs {
		fields = append(fields, acc.result())
	}
	return &types.PointRow{Tags: tags, Fields: fields}
}

// parseGroupKey 从缓存的 key 还原 tags。
func (g *GroupAggregateOperator) parseGroupKey(key string) map[string]string {
	if key == "global" {
		return nil
	}
	tags := make(map[string]string)
	parts := strings.Split(key, "\x00")
	for i, tag := range g.groupByTags {
		if i < len(parts) {
			tags[tag] = parts[i]
		}
	}
	return tags
}

// groupKey 计算分组的 key。
func (g *GroupAggregateOperator) groupKey(row *types.PointRow) string {
	if len(g.groupByTags) == 0 {
		return "global"
	}
	if len(g.groupByTags) == 1 {
		return row.Tags[g.groupByTags[0]]
	}
	var buf strings.Builder
	for i, tag := range g.groupByTags {
		if i > 0 {
			buf.WriteByte(0)
		}
		buf.WriteString(row.Tags[tag])
	}
	raw := buf.String()
	if cached, ok := g.groupKeyCache[raw]; ok {
		return cached
	}
	if g.groupKeyCache == nil {
		g.groupKeyCache = make(map[string]string)
	}
	g.groupKeyCache[raw] = raw
	return raw
}

// Next 返回下一行聚合结果。
func (g *GroupAggregateOperator) Next() (*types.PointRow, error) {
	if g.pos >= len(g.results) {
		return nil, nil
	}
	row := g.results[g.pos]
	g.pos++
	return row, nil
}

// Close 关闭上游算子。
func (g *GroupAggregateOperator) Close() error {
	return g.upstream.Close()
}
