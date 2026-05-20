// Package query 实现查询处理和执行。
//
// operator.go 定义算子接口及 7 种算子实现：
//
//	Scan → Filter → GroupAggregate → Sort → Project → Limit
//
// 每个算子实现统一的 Operator 接口，下游通过 Next() 拉取上游数据。
package query

import (
	"container/heap"
	"context"
	"sort"
	"strings"
	"sync"

	"codeberg.org/micro-ts/mts/types"
)

// Operator 是查询执行计划中的单个算子。
//
// 算子链按顺序连接：上游算子的输出作为下游算子的输入。
// Open 初始化资源，Next 返回下一行（无数据时返回 nil），Close 释放资源。
type Operator interface {
	Open(ctx context.Context) error
	Next() (*types.PointRow, error)
	Close() error
}

// ===================================
// ScanOperator —— 数据源扫描
// ===================================

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
	if s.iter.Next(context.Background()) {
		s.currentRow = s.iter.Points()
		return s.currentRow, nil
	}
	return nil, nil
}

// Close 释放底层 Iterator 资源。
func (s *ScanOperator) Close() error {
	return s.iter.Close()
}

// ===================================
// FilterOperator —— 行级过滤
// ===================================

// FilterOperator 按条件过滤行，多个条件为 AND 关系。
type FilterOperator struct {
	upstream    Operator
	conditions  []*types.FilterCondition
	currentRow  *types.PointRow
}

// NewFilterOperator 创建过滤算子。
func NewFilterOperator(upstream Operator, specs []*types.FilterCondition) *FilterOperator {
	return &FilterOperator{upstream: upstream, conditions: specs}
}

// Open 初始化上游算子。
func (f *FilterOperator) Open(ctx context.Context) error {
	return f.upstream.Open(ctx)
}

// Next 返回第一行通过所有条件的行。
func (f *FilterOperator) Next() (*types.PointRow, error) {
	for {
		row, err := f.upstream.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil
		}
		if f.matchAll(row) {
			return row, nil
		}
	}
}

// matchAll 检查行是否满足所有条件。
func (f *FilterOperator) matchAll(row *types.PointRow) bool {
	for _, cond := range f.conditions {
		if !f.matchOne(row, cond) {
			return false
		}
	}
	return true
}

// matchOne 检查行是否满足单个条件。
func (f *FilterOperator) matchOne(row *types.PointRow, cond *types.FilterCondition) bool {
	if cond.Tag != "" {
		return f.matchTag(row, cond)
	}
	return f.matchField(row, cond)
}

// matchTag 检查 tag 值是否满足条件。
func (f *FilterOperator) matchTag(row *types.PointRow, cond *types.FilterCondition) bool {
	val, ok := row.Tags[cond.Tag]
	if !ok {
		return false
	}
	switch cond.Op {
	case types.FilterOp_EQ:
		return val == cond.Value.GetStringValue()
	case types.FilterOp_NE:
		return val != cond.Value.GetStringValue()
	default:
		return false
	}
}

// matchField 检查字段值是否满足比较条件。
func (f *FilterOperator) matchField(row *types.PointRow, cond *types.FilterCondition) bool {
	var fieldVal *types.FieldValue
	for _, fe := range row.Fields {
		if fe.Key == cond.Field {
			fieldVal = fe.Value
			break
		}
	}
	if fieldVal == nil {
		return false
	}

	switch cond.Op {
	case types.FilterOp_EQ:
		return compareFieldValue(fieldVal, cond.Value) == 0
	case types.FilterOp_NE:
		return compareFieldValue(fieldVal, cond.Value) != 0
	case types.FilterOp_GT:
		return compareFieldValue(fieldVal, cond.Value) > 0
	case types.FilterOp_GTE:
		return compareFieldValue(fieldVal, cond.Value) >= 0
	case types.FilterOp_LT:
		return compareFieldValue(fieldVal, cond.Value) < 0
	case types.FilterOp_LTE:
		return compareFieldValue(fieldVal, cond.Value) <= 0
	default:
		return false
	}
}

// compareFieldValue 比较两个 FieldValue，返回 -1/0/1。
func compareFieldValue(a, b *types.FieldValue) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch v := a.Value.(type) {
	case *types.FieldValue_IntValue:
		switch w := b.Value.(type) {
		case *types.FieldValue_IntValue:
			if v.IntValue < w.IntValue {
				return -1
			}
			if v.IntValue > w.IntValue {
				return 1
			}
			return 0
		case *types.FieldValue_FloatValue:
			fv := float64(v.IntValue)
			if fv < w.FloatValue {
				return -1
			}
			if fv > w.FloatValue {
				return 1
			}
			return 0
		}
	case *types.FieldValue_FloatValue:
		switch w := b.Value.(type) {
		case *types.FieldValue_FloatValue:
			if v.FloatValue < w.FloatValue {
				return -1
			}
			if v.FloatValue > w.FloatValue {
				return 1
			}
			return 0
		case *types.FieldValue_IntValue:
			fw := float64(w.IntValue)
			if v.FloatValue < fw {
				return -1
			}
			if v.FloatValue > fw {
				return 1
			}
			return 0
		}
	case *types.FieldValue_StringValue:
		switch w := b.Value.(type) {
		case *types.FieldValue_StringValue:
			if v.StringValue < w.StringValue {
				return -1
			}
			if v.StringValue > w.StringValue {
				return 1
			}
			return 0
		}
	case *types.FieldValue_BoolValue:
		switch w := b.Value.(type) {
		case *types.FieldValue_BoolValue:
			if !v.BoolValue && w.BoolValue {
				return -1
			}
			if v.BoolValue && !w.BoolValue {
				return 1
			}
			return 0
		}
	}
	return 0
}

// Close 关闭上游算子。
func (f *FilterOperator) Close() error {
	return f.upstream.Close()
}

// ===================================
// GroupAggregateOperator —— 分组聚合
// ===================================

// aggAccumulator 聚合累加器接口，支持流式聚合：每行调用 update()，最终调用 result() 产出结果。
type aggAccumulator interface {
	update(row *types.PointRow, fieldIndex map[string]int)
	result() *types.FieldEntry
}

// avgAccumulator 计算平均值。
type avgAccumulator struct {
	field, outputKey string
	sum              float64
	count            int
}

func (a *avgAccumulator) update(row *types.PointRow, fieldIndex map[string]int) {
	idx, ok := fieldIndex[a.field]
	if !ok {
		return
	}
	if v, ok := fieldValueFloat(row.Fields[idx].Value); ok {
		a.sum += v
		a.count++
	}
}

func (a *avgAccumulator) result() *types.FieldEntry {
	if a.count == 0 {
		return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(float64(0))}
	}
	return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(a.sum / float64(a.count))}
}

// maxAccumulator 计算最大值。
type maxAccumulator struct {
	field, outputKey string
	max              float64
	init             bool
}

func (a *maxAccumulator) update(row *types.PointRow, fieldIndex map[string]int) {
	idx, ok := fieldIndex[a.field]
	if !ok {
		return
	}
	if v, ok := fieldValueFloat(row.Fields[idx].Value); ok {
		if !a.init || v > a.max {
			a.max = v
		}
		a.init = true
	}
}

func (a *maxAccumulator) result() *types.FieldEntry {
	if !a.init {
		return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(float64(0))}
	}
	return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(a.max)}
}

// minAccumulator 计算最小值。
type minAccumulator struct {
	field, outputKey string
	min              float64
	init             bool
}

func (a *minAccumulator) update(row *types.PointRow, fieldIndex map[string]int) {
	idx, ok := fieldIndex[a.field]
	if !ok {
		return
	}
	if v, ok := fieldValueFloat(row.Fields[idx].Value); ok {
		if !a.init || v < a.min {
			a.min = v
		}
		a.init = true
	}
}

func (a *minAccumulator) result() *types.FieldEntry {
	if !a.init {
		return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(float64(0))}
	}
	return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(a.min)}
}

// sumAccumulator 计算总和。
type sumAccumulator struct {
	field, outputKey string
	sum              float64
}

func (a *sumAccumulator) update(row *types.PointRow, fieldIndex map[string]int) {
	idx, ok := fieldIndex[a.field]
	if !ok {
		return
	}
	if v, ok := fieldValueFloat(row.Fields[idx].Value); ok {
		a.sum += v
	}
}

func (a *sumAccumulator) result() *types.FieldEntry {
	return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(a.sum)}
}

// countAccumulator 计算数量。
type countAccumulator struct {
	field, outputKey string
	count            int64
}

func (a *countAccumulator) update(row *types.PointRow, fieldIndex map[string]int) {
	if _, ok := fieldIndex[a.field]; ok {
		a.count++
	}
}

func (a *countAccumulator) result() *types.FieldEntry {
	return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(a.count)}
}

// firstAccumulator 返回第一个非空值。
type firstAccumulator struct {
	field, outputKey string
	val              *types.FieldValue
}

func (a *firstAccumulator) update(row *types.PointRow, fieldIndex map[string]int) {
	if a.val != nil {
		return
	}
	idx, ok := fieldIndex[a.field]
	if ok {
		a.val = row.Fields[idx].Value
	}
}

func (a *firstAccumulator) result() *types.FieldEntry {
	if a.val == nil {
		return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(float64(0))}
	}
	return &types.FieldEntry{Key: a.outputKey, Value: a.val}
}

// lastAccumulator 返回最后一个非空值。
type lastAccumulator struct {
	field, outputKey string
	val              *types.FieldValue
}

func (a *lastAccumulator) update(row *types.PointRow, fieldIndex map[string]int) {
	idx, ok := fieldIndex[a.field]
	if ok {
		a.val = row.Fields[idx].Value
	}
}

func (a *lastAccumulator) result() *types.FieldEntry {
	if a.val == nil {
		return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(float64(0))}
	}
	return &types.FieldEntry{Key: a.outputKey, Value: a.val}
}

// rangeAccumulator 记录首/尾/尾前值，用于 diff/rate/irate/derivative。
type rangeAccumulator struct {
	field, outputKey, fn     string
	firstVal, lastVal        float64
	firstTs, lastTs          int64
	prevVal                 float64
	prevTs                  int64
	count                    int
	hasFirst                  bool
}

func (a *rangeAccumulator) update(row *types.PointRow, fieldIndex map[string]int) {
	idx, ok := fieldIndex[a.field]
	if !ok {
		return
	}
	v, ok := fieldValueFloat(row.Fields[idx].Value)
	if !ok {
		return
	}
	if !a.hasFirst {
		a.firstVal = v
		a.firstTs = row.Timestamp
		a.hasFirst = true
	}
	a.prevVal = a.lastVal
	a.prevTs = a.lastTs
	a.lastVal = v
	a.lastTs = row.Timestamp
	a.count++
}

func (a *rangeAccumulator) result() *types.FieldEntry {
	if a.count < 2 {
		return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(float64(0))}
	}
	switch a.fn {
	case "diff":
		return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(a.lastVal - a.firstVal)}
	case "rate", "derivative":
		window := float64(a.lastTs - a.firstTs)
		if window <= 0 {
			return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(float64(0))}
		}
		return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue((a.lastVal - a.firstVal) / window * 1e9)}
	case "irate":
		window := float64(a.lastTs - a.prevTs)
		if window <= 0 {
			return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(float64(0))}
		}
		return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue((a.lastVal - a.prevVal) / window * 1e9)}
	}
	return &types.FieldEntry{Key: a.outputKey, Value: types.NewFieldValue(float64(0))}
}

// fieldValueFloat 从 FieldValue 提取 float64 值（支持 float64 和 int64）。
func fieldValueFloat(fv *types.FieldValue) (float64, bool) {
	if fv == nil {
		return 0, false
	}
	switch v := fv.Value.(type) {
	case *types.FieldValue_FloatValue:
		return v.FloatValue, true
	case *types.FieldValue_IntValue:
		return float64(v.IntValue), true
	}
	return 0, false
}

// GroupAggregateOperator 按标签分组并对每个组执行聚合函数（流式累加器，O(1) 内存）。
type GroupAggregateOperator struct {
	upstream    Operator
	groupByTags []string
	aggSpecs    []*types.AggFunction
	results     []*types.PointRow
	pos         int
	fieldIndex  map[string]int
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

		// 首行惰性构建 fieldIndex
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

// groupKeyCache 缓存 groupKey 计算结果，消除每行字符串分配。
var groupKeyCache sync.Map

// groupKey 计算分组的 key。
func (g *GroupAggregateOperator) groupKey(row *types.PointRow) string {
	if len(g.groupByTags) == 0 {
		return "global"
	}
	var buf strings.Builder
	for i, tag := range g.groupByTags {
		if i > 0 {
			buf.WriteByte(0)
		}
		buf.WriteString(row.Tags[tag])
	}
	raw := buf.String()
	if cached, ok := groupKeyCache.Load(raw); ok {
		return cached.(string)
	}
	groupKeyCache.Store(raw, raw)
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

// ===================================
// SortOperator —— 排序
// ===================================

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

	// 使用最大堆选出 Top-K 最小元素
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

	// 堆中元素为 Top-K，按排序顺序输出
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
	return h.compare(h.rows[i], h.rows[j]) > 0 // 最大堆
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

// ===================================
// ProjectOperator —— 字段投影
// ===================================

// ProjectOperator 选择输出指定字段。
type ProjectOperator struct {
	upstream   Operator
	fields     []string
	currentRow *types.PointRow
}

// NewProjectOperator 创建投影算子。
func NewProjectOperator(upstream Operator, fields []string) *ProjectOperator {
	return &ProjectOperator{upstream: upstream, fields: fields}
}

// Open 初始化上游算子。
func (p *ProjectOperator) Open(ctx context.Context) error {
	return p.upstream.Open(ctx)
}

// Next 返回只包含指定字段的行。
func (p *ProjectOperator) Next() (*types.PointRow, error) {
	row, err := p.upstream.Next()
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	if len(p.fields) == 0 {
		return row, nil
	}

	fieldSet := make(map[string]bool, len(p.fields))
	for _, name := range p.fields {
		fieldSet[name] = true
	}
	filtered := make([]*types.FieldEntry, 0, len(p.fields))
	for _, f := range row.Fields {
		if fieldSet[f.Key] {
			filtered = append(filtered, f)
		}
	}
	p.currentRow = &types.PointRow{
		Sid:       row.Sid,
		Timestamp: row.Timestamp,
		Tags:      row.Tags,
		Fields:    filtered,
	}
	return p.currentRow, nil
}

// Close 关闭上游算子。
func (p *ProjectOperator) Close() error {
	return p.upstream.Close()
}

// ===================================
// LimitOperator —— Offset + Limit 截断
// ===================================

// LimitOperator 跳过 Offset 行后限制输出行数。
type LimitOperator struct {
	upstream  Operator
	offset    int64
	limit     int64
	skipped   int64
	consumed  int64
}

// NewLimitOperator 创建截断算子。
func NewLimitOperator(upstream Operator, offset, limit int64) *LimitOperator {
	return &LimitOperator{upstream: upstream, offset: offset, limit: limit}
}

// Open 初始化上游算子。
func (l *LimitOperator) Open(ctx context.Context) error {
	return l.upstream.Open(ctx)
}

// Next 返回经过 Offset/Limit 截断后的行。
func (l *LimitOperator) Next() (*types.PointRow, error) {
	for {
		if l.limit > 0 && l.consumed >= l.limit {
			return nil, nil
		}
		row, err := l.upstream.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil
		}
		if l.skipped < l.offset {
			l.skipped++
			continue
		}
		l.consumed++
		return row, nil
	}
}

// Close 关闭上游算子。
func (l *LimitOperator) Close() error {
	return l.upstream.Close()
}
