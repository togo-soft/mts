package downsample

import (
	"fmt"
	"math"
	"sort"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// accumulator 累积单个字段的聚合状态。
type accumulator struct {
	min   float64
	max   float64
	sum   float64
	count int64
	first float64
	last  float64

	// 用于 rate / irate / derivative 计算
	secondLast   float64
	lastTs       int64
	secondLastTs int64
}

// newAccumulator 创建新的 accumulator。
func newAccumulator() *accumulator {
	return &accumulator{
		min: math.MaxFloat64,
		max: -math.MaxFloat64,
	}
}

// add 添加一个值及其时间戳。
func (a *accumulator) add(v float64, ts int64) {
	if a.count == 0 {
		a.first = v
		a.lastTs = ts
	} else {
		a.secondLast = a.last
		a.secondLastTs = a.lastTs
	}
	a.last = v
	a.lastTs = ts
	a.sum += v
	a.count++
	if v < a.min {
		a.min = v
	}
	if v > a.max {
		a.max = v
	}
}

// avg 返回平均值。
func (a *accumulator) avg() float64 {
	if a.count == 0 {
		return 0
	}
	return a.sum / float64(a.count)
}

// diff 返回最新值与最旧值的差值。
func (a *accumulator) diff() float64 {
	return a.last - a.first
}

// rate 计算每秒增长率：(last - first) / windowSeconds。
func (a *accumulator) rate(windowSeconds float64) float64 {
	if a.count < 2 || windowSeconds <= 0 {
		return 0
	}
	return (a.last - a.first) / windowSeconds
}

// irate 计算瞬时增长率（基于最后两个点）。
func (a *accumulator) irate() float64 {
	if a.count < 2 {
		return 0
	}
	deltaTs := a.lastTs - a.secondLastTs
	if deltaTs <= 0 {
		return 0
	}
	return (a.last - a.secondLast) / (float64(deltaTs) / 1e9)
}

// derivative 计算导数（基于窗口内的速率变化）。
func (a *accumulator) derivative(windowSeconds float64) float64 {
	if a.count < 2 || windowSeconds <= 0 {
		return 0
	}
	return (a.last - a.first) / windowSeconds
}

// bucket 表示一个时间窗口的聚合桶。
type bucket struct {
	windowStart  int64
	accumulators map[string]*accumulator // key → accumulator
}

// aggregateSSTFiles 读取 SSTable 并按窗口聚合。
func aggregateSSTFiles(files []string, windowNanos, shardStart int64, functions []string, schema sstable.Schema) ([]*bucket, error) {
	bucketMap := make(map[int64]*bucket)

	for _, f := range files {
		if err := aggregateFile(f, windowNanos, bucketMap, schema); err != nil {
			return nil, fmt.Errorf("aggregate file %s: %w", f, err)
		}
	}

	return sortedBuckets(bucketMap), nil
}

// aggregateFile 流式读取单个 SSTable 文件并聚合，避免全量内存加载。
func aggregateFile(path string, windowNanos int64, bucketMap map[int64]*bucket, schema sstable.Schema) error {
	reader, err := sstable.NewReader(path, schema)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()

	iter, err := reader.NewIterator(nil, nil)
	if err != nil {
		return fmt.Errorf("new iterator: %w", err)
	}

	for iter.Next() {
		row := iter.Point()
		windowStart := (row.Timestamp / windowNanos) * windowNanos
		bk, ok := bucketMap[windowStart]
		if !ok {
			bk = &bucket{
				windowStart:  windowStart,
				accumulators: make(map[string]*accumulator),
			}
			bucketMap[windowStart] = bk
		}

		for _, entry := range row.Fields {
			v := toFloat64(entry.Value)
			acc, ok := bk.accumulators[entry.Key]
			if !ok {
				acc = newAccumulator()
				bk.accumulators[entry.Key] = acc
			}
			acc.add(v, row.Timestamp)
		}
	}

	return nil
}

// toFloat64 将 FieldValue 转换为 float64。
func toFloat64(fv *types.FieldValue) float64 {
	if fv == nil {
		return 0
	}
	switch v := fv.Value.(type) {
	case *types.FieldValue_IntValue:
		return float64(v.IntValue)
	case *types.FieldValue_FloatValue:
		return v.FloatValue
	default:
		return 0
	}
}

// sortedBuckets 按窗口起始时间排序桶。
func sortedBuckets(m map[int64]*bucket) []*bucket {
	result := make([]*bucket, 0, len(m))
	for _, b := range m {
		result = append(result, b)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].windowStart < result[j].windowStart
	})
	return result
}
