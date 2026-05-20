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
	"math"
	"sort"
	"strings"

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

// GroupAggregateOperator 按标签分组并对每个组执行聚合函数。
type GroupAggregateOperator struct {
	upstream    Operator
	groupByTags []string
	aggSpecs    []*types.AggFunction
	rows        []*types.PointRow
	pos         int
}

// NewGroupAggregateOperator 创建分组聚合算子。
func NewGroupAggregateOperator(upstream Operator, groupByTags []string, aggSpecs []*types.AggFunction) *GroupAggregateOperator {
	return &GroupAggregateOperator{
		upstream:    upstream,
		groupByTags: groupByTags,
		aggSpecs:    aggSpecs,
	}
}

// Open 初始化上游算子并加载所有数据。
func (g *GroupAggregateOperator) Open(ctx context.Context) error {
	if err := g.upstream.Open(ctx); err != nil {
		return err
	}
	return g.loadAndAggregate()
}

// loadAndAggregate 从上游加载所有数据，分组聚合后排序输出。
func (g *GroupAggregateOperator) loadAndAggregate() error {
	// 读取所有上游数据并分组
	groups := make(map[string][]*types.PointRow)
	for {
		row, err := g.upstream.Next()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		key := g.groupKey(row)
		groups[key] = append(groups[key], row)
	}

	// 对每个分组执行聚合
	var result []*types.PointRow
	for _, rows := range groups {
		aggRow := g.aggregateGroup(rows)
		if aggRow != nil {
			result = append(result, aggRow)
		}
	}

	// 排序输出（按标签值保证确定性顺序）
	g.rows = result
	return nil
}

// groupKey 计算分组的 key。
func (g *GroupAggregateOperator) groupKey(row *types.PointRow) string {
	if len(g.groupByTags) == 0 {
		return "global"
	}
	var parts []string
	for _, tag := range g.groupByTags {
		parts = append(parts, row.Tags[tag])
	}
	return strings.Join(parts, "\x00")
}

// aggregateGroup 对分组执行聚合计算。
func (g *GroupAggregateOperator) aggregateGroup(rows []*types.PointRow) *types.PointRow {
	if len(rows) == 0 {
		return nil
	}

	// 按时间戳排序（确保聚合顺序确定）
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Timestamp < rows[j].Timestamp
	})

	// 保留分组标签
	tags := make(map[string]string)
	for _, tag := range g.groupByTags {
		if len(rows) > 0 {
			tags[tag] = rows[0].Tags[tag]
		}
	}

	// 聚合后的字段
	var fields []*types.FieldEntry

	for _, spec := range g.aggSpecs {
		switch spec.Function {
		case "avg":
			fields = append(fields, g.aggAvg(rows, spec.Field))
		case "max":
			fields = append(fields, g.aggMax(rows, spec.Field))
		case "min":
			fields = append(fields, g.aggMin(rows, spec.Field))
		case "sum":
			fields = append(fields, g.aggSum(rows, spec.Field))
		case "count":
			fields = append(fields, g.aggCount(rows, spec.Field))
		case "first":
			fields = append(fields, g.aggFirst(rows, spec.Field))
		case "last":
			fields = append(fields, g.aggLast(rows, spec.Field))
		case "diff":
			fields = append(fields, g.aggDiff(rows, spec.Field))
		case "rate":
			fields = append(fields, g.aggRate(rows, spec.Field))
		case "irate":
			fields = append(fields, g.aggIrate(rows, spec.Field))
		case "derivative":
			fields = append(fields, g.aggDerivative(rows, spec.Field))
		}
	}

	return &types.PointRow{
		Tags:   tags,
		Fields: fields,
	}
}

// getFieldFloat 获取行中指定字段的 float64 值。
func getFieldFloat(row *types.PointRow, field string) (float64, bool) {
	for _, fe := range row.Fields {
		if fe.Key == field {
			if fv := fe.Value.GetFloatValue(); fe.Value != nil {
				return fv, true
			}
			if iv := fe.Value.GetIntValue(); fe.Value != nil {
				return float64(iv), true
			}
		}
	}
	return 0, false
}

// aggAvg 计算平均值。
func (g *GroupAggregateOperator) aggAvg(rows []*types.PointRow, field string) *types.FieldEntry {
	var sum float64
	count := 0
	for _, row := range rows {
		if v, ok := getFieldFloat(row, field); ok {
			sum += v
			count++
		}
	}
	if count == 0 {
		return &types.FieldEntry{Key: "avg_" + field, Value: types.NewFieldValue(float64(0))}
	}
	return &types.FieldEntry{Key: "avg_" + field, Value: types.NewFieldValue(sum / float64(count))}
}

// aggMax 计算最大值。
func (g *GroupAggregateOperator) aggMax(rows []*types.PointRow, field string) *types.FieldEntry {
	max := -math.MaxFloat64
	for _, row := range rows {
		if v, ok := getFieldFloat(row, field); ok && v > max {
			max = v
		}
	}
	return &types.FieldEntry{Key: "max_" + field, Value: types.NewFieldValue(max)}
}

// aggMin 计算最小值。
func (g *GroupAggregateOperator) aggMin(rows []*types.PointRow, field string) *types.FieldEntry {
	min := math.MaxFloat64
	for _, row := range rows {
		if v, ok := getFieldFloat(row, field); ok && v < min {
			min = v
		}
	}
	return &types.FieldEntry{Key: "min_" + field, Value: types.NewFieldValue(min)}
}

// aggSum 计算总和。
func (g *GroupAggregateOperator) aggSum(rows []*types.PointRow, field string) *types.FieldEntry {
	var sum float64
	for _, row := range rows {
		if v, ok := getFieldFloat(row, field); ok {
			sum += v
		}
	}
	return &types.FieldEntry{Key: "sum_" + field, Value: types.NewFieldValue(sum)}
}

// aggCount 计算数量。
func (g *GroupAggregateOperator) aggCount(rows []*types.PointRow, field string) *types.FieldEntry {
	count := 0
	for _, row := range rows {
		if _, ok := getFieldFloat(row, field); ok {
			count++
		}
	}
	return &types.FieldEntry{Key: "count_" + field, Value: types.NewFieldValue(int64(count))}
}

// aggFirst 返回第一个值。
func (g *GroupAggregateOperator) aggFirst(rows []*types.PointRow, field string) *types.FieldEntry {
	for _, row := range rows {
		for _, fe := range row.Fields {
			if fe.Key == field {
				return &types.FieldEntry{Key: "first_" + field, Value: fe.Value}
			}
		}
	}
	return &types.FieldEntry{Key: "first_" + field, Value: types.NewFieldValue(float64(0))}
}

// aggLast 返回最后一个值。
func (g *GroupAggregateOperator) aggLast(rows []*types.PointRow, field string) *types.FieldEntry {
	for i := len(rows) - 1; i >= 0; i-- {
		for _, fe := range rows[i].Fields {
			if fe.Key == field {
				return &types.FieldEntry{Key: "last_" + field, Value: fe.Value}
			}
		}
	}
	return &types.FieldEntry{Key: "last_" + field, Value: types.NewFieldValue(float64(0))}
}

// aggDiff 计算最后一个值与第一个值的差。
func (g *GroupAggregateOperator) aggDiff(rows []*types.PointRow, field string) *types.FieldEntry {
	if len(rows) < 2 {
		return &types.FieldEntry{Key: "diff_" + field, Value: types.NewFieldValue(float64(0))}
	}
	first, ok1 := getFieldFloat(rows[0], field)
	last, ok2 := getFieldFloat(rows[len(rows)-1], field)
	if !ok1 || !ok2 {
		return &types.FieldEntry{Key: "diff_" + field, Value: types.NewFieldValue(float64(0))}
	}
	return &types.FieldEntry{Key: "diff_" + field, Value: types.NewFieldValue(last - first)}
}

// aggRate 计算变化率（diff / 时间窗口）。
func (g *GroupAggregateOperator) aggRate(rows []*types.PointRow, field string) *types.FieldEntry {
	if len(rows) < 2 {
		return &types.FieldEntry{Key: "rate_" + field, Value: types.NewFieldValue(float64(0))}
	}
	first, ok1 := getFieldFloat(rows[0], field)
	last, ok2 := getFieldFloat(rows[len(rows)-1], field)
	if !ok1 || !ok2 {
		return &types.FieldEntry{Key: "rate_" + field, Value: types.NewFieldValue(float64(0))}
	}
	window := float64(rows[len(rows)-1].Timestamp - rows[0].Timestamp)
	if window <= 0 {
		return &types.FieldEntry{Key: "rate_" + field, Value: types.NewFieldValue(float64(0))}
	}
	return &types.FieldEntry{Key: "rate_" + field, Value: types.NewFieldValue((last - first) / window * 1e9)}
}

// aggIrate 计算瞬时变化率（最后两个点的变化率）。
func (g *GroupAggregateOperator) aggIrate(rows []*types.PointRow, field string) *types.FieldEntry {
	if len(rows) < 2 {
		return &types.FieldEntry{Key: "irate_" + field, Value: types.NewFieldValue(float64(0))}
	}
	prev, ok1 := getFieldFloat(rows[len(rows)-2], field)
	last, ok2 := getFieldFloat(rows[len(rows)-1], field)
	if !ok1 || !ok2 {
		return &types.FieldEntry{Key: "irate_" + field, Value: types.NewFieldValue(float64(0))}
	}
	window := float64(rows[len(rows)-1].Timestamp - rows[len(rows)-2].Timestamp)
	if window <= 0 {
		return &types.FieldEntry{Key: "irate_" + field, Value: types.NewFieldValue(float64(0))}
	}
	return &types.FieldEntry{Key: "irate_" + field, Value: types.NewFieldValue((last - prev) / window * 1e9)}
}

// aggDerivative 计算导数（与 rate 相同，按秒归一化）。
func (g *GroupAggregateOperator) aggDerivative(rows []*types.PointRow, field string) *types.FieldEntry {
	return g.aggRate(rows, field)
}

// Next 返回下一行聚合结果。
func (g *GroupAggregateOperator) Next() (*types.PointRow, error) {
	if g.pos >= len(g.rows) {
		return nil, nil
	}
	row := g.rows[g.pos]
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
