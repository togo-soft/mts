package query

import "codeberg.org/micro-ts/mts/types"

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
	field, outputKey, fn string
	firstVal, lastVal    float64
	firstTs, lastTs      int64
	prevVal              float64
	prevTs               int64
	count                int
	hasFirst             bool
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
