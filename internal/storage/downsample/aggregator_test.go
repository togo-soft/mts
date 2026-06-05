package downsample

import (
	"math"
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

func TestAccumulator_Add(t *testing.T) {
	acc := newAccumulator()
	if acc.count != 0 {
		t.Error("initial count should be 0")
	}
	if acc.min != math.MaxFloat64 {
		t.Error("initial min should be MaxFloat64")
	}
	if acc.max != -math.MaxFloat64 {
		t.Error("initial max should be -MaxFloat64")
	}

	acc.add(5.0, 100)
	if acc.count != 1 {
		t.Fatal("count should be 1, got", acc.count)
	}
	if acc.min != 5.0 {
		t.Error("min should be 5.0, got", acc.min)
	}
	if acc.max != 5.0 {
		t.Error("max should be 5.0, got", acc.max)
	}
	if acc.first != 5.0 {
		t.Error("first should be 5.0, got", acc.first)
	}
	if acc.last != 5.0 {
		t.Error("last should be 5.0, got", acc.last)
	}
	if acc.lastTs != 100 {
		t.Error("lastTs should be 100, got", acc.lastTs)
	}
	if acc.sum != 5.0 {
		t.Error("sum should be 5.0, got", acc.sum)
	}

	acc.add(3.0, 200)
	if acc.count != 2 {
		t.Fatal("count should be 2, got", acc.count)
	}
	if acc.min != 3.0 {
		t.Error("min should be 3.0, got", acc.min)
	}
	if acc.max != 5.0 {
		t.Error("max should be 5.0, got", acc.max)
	}
	if acc.first != 5.0 {
		t.Error("first should stay 5.0, got", acc.first)
	}
	if acc.last != 3.0 {
		t.Error("last should be 3.0, got", acc.last)
	}
	if acc.secondLast != 5.0 {
		t.Error("secondLast should be 5.0, got", acc.secondLast)
	}
	if acc.lastTs != 200 {
		t.Error("lastTs should be 200, got", acc.lastTs)
	}
	if acc.secondLastTs != 100 {
		t.Error("secondLastTs should be 100, got", acc.secondLastTs)
	}
	if acc.sum != 8.0 {
		t.Error("sum should be 8.0, got", acc.sum)
	}

	acc.add(10.0, 300)
	if acc.count != 3 {
		t.Fatal("count should be 3, got", acc.count)
	}
	if acc.min != 3.0 {
		t.Error("min should be 3.0, got", acc.min)
	}
	if acc.max != 10.0 {
		t.Error("max should be 10.0, got", acc.max)
	}
	if acc.first != 5.0 {
		t.Error("first should stay 5.0, got", acc.first)
	}
	if acc.last != 10.0 {
		t.Error("last should be 10.0, got", acc.last)
	}
	if acc.secondLast != 3.0 {
		t.Error("secondLast should be 3.0, got", acc.secondLast)
	}
	if acc.lastTs != 300 {
		t.Error("lastTs should be 300, got", acc.lastTs)
	}
	if acc.secondLastTs != 200 {
		t.Error("secondLastTs should be 200, got", acc.secondLastTs)
	}
	if acc.sum != 18.0 {
		t.Error("sum should be 18.0, got", acc.sum)
	}
}

func TestAccumulator_Avg(t *testing.T) {
	tests := []struct {
		name     string
		values   []float64
		expected float64
	}{
		{"empty", nil, 0},
		{"single", []float64{5.0}, 5.0},
		{"multiple", []float64{1.0, 2.0, 3.0}, 2.0},
		{"with decimals", []float64{1.5, 2.5}, 2.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acc := newAccumulator()
			for i, v := range tt.values {
				acc.add(v, int64(i*100))
			}
			got := acc.avg()
			if got != tt.expected {
				t.Errorf("avg() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestAccumulator_Diff(t *testing.T) {
	tests := []struct {
		name string
		data []struct {
			v  float64
			ts int64
		}
		expected float64
	}{
		{"single", []struct {
			v  float64
			ts int64
		}{{5, 100}}, 0},
		{"two points", []struct {
			v  float64
			ts int64
		}{{10, 100}, {20, 200}}, 10},
		{"three points", []struct {
			v  float64
			ts int64
		}{{10, 100}, {15, 200}, {25, 300}}, 15},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acc := newAccumulator()
			for _, d := range tt.data {
				acc.add(d.v, d.ts)
			}
			got := acc.diff()
			if got != tt.expected {
				t.Errorf("diff() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestAccumulator_Rate(t *testing.T) {
	// counter 从 100 增长到 200，窗口 60 秒
	acc := newAccumulator()
	acc.add(100, 1000000000)
	acc.add(200, 106000000000)
	got := acc.rate(60.0)
	expected := 100.0 / 60.0
	if got != expected {
		t.Errorf("rate() = %v, want %v", got, expected)
	}
}

func TestAccumulator_Irate(t *testing.T) {
	// 最后两个点相距 5 秒，值差 50
	acc := newAccumulator()
	acc.add(100, 1000000000)  // t=1s
	acc.add(150, 6000000000)  // t=6s, secondLast
	acc.add(200, 11000000000) // t=11s, last
	got := acc.irate()
	expected := (200.0 - 150.0) / 5.0 // 50 / 5s = 10/sec
	if got != expected {
		t.Errorf("irate() = %v, want %v", got, expected)
	}
}

func TestAccumulator_Derivative(t *testing.T) {
	acc := newAccumulator()
	acc.add(10, 1000000000)
	acc.add(20, 31000000000)
	got := acc.derivative(30.0)
	expected := 10.0 / 30.0
	if got != expected {
		t.Errorf("derivative() = %v, want %v", got, expected)
	}
}

func TestAccumulator_Rate_SinglePoint(t *testing.T) {
	acc := newAccumulator()
	acc.add(100, 1000000000)
	if acc.rate(60.0) != 0 {
		t.Error("rate should be 0 with single point")
	}
	if acc.irate() != 0 {
		t.Error("irate should be 0 with single point")
	}
	if acc.derivative(60.0) != 0 {
		t.Error("derivative should be 0 with single point")
	}
}

func TestAccumulator_Irate_SameTimestamp(t *testing.T) {
	acc := newAccumulator()
	acc.add(10, 100)
	acc.add(20, 100) // 相同时间戳
	if acc.irate() != 0 {
		t.Error("irate should be 0 with same timestamp")
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    *types.FieldValue
		expected float64
	}{
		{"nil", nil, 0},
		{"int value", types.NewFieldValue(int64(42)), 42.0},
		{"float value", types.NewFieldValue(3.14), 3.14},
		{"string value", types.NewFieldValue("test"), 0},
		{"bool value", types.NewFieldValue(true), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toFloat64(tt.input)
			if got != tt.expected {
				t.Errorf("toFloat64() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestSortedBuckets(t *testing.T) {
	m := map[int64]*bucket{
		300: {windowStart: 300},
		100: {windowStart: 100},
		200: {windowStart: 200},
	}
	result := sortedBuckets(m)
	if len(result) != 3 {
		t.Fatal("expected 3 buckets, got", len(result))
	}
	if result[0].windowStart != 100 {
		t.Error("first bucket should be 100, got", result[0].windowStart)
	}
	if result[1].windowStart != 200 {
		t.Error("second bucket should be 200, got", result[1].windowStart)
	}
	if result[2].windowStart != 300 {
		t.Error("third bucket should be 300, got", result[2].windowStart)
	}
}

func TestSortedBuckets_Empty(t *testing.T) {
	result := sortedBuckets(map[int64]*bucket{})
	if len(result) != 0 {
		t.Error("expected 0 buckets, got", len(result))
	}
}

func TestNewAccumulator(t *testing.T) {
	acc := newAccumulator()
	if acc == nil {
		t.Fatal("newAccumulator returned nil")
	}
	if acc.min != math.MaxFloat64 {
		t.Error("min not initialized correctly")
	}
	if acc.max != -math.MaxFloat64 {
		t.Error("max not initialized correctly")
	}
}

func TestBuildDownsampledFields_Avg(t *testing.T) {
	acc := newAccumulator()
	acc.add(1.0, 100)
	acc.add(2.0, 200)
	acc.add(3.0, 300)

	b := &bucket{
		windowStart:  100,
		accumulators: map[string]*accumulator{"cpu": acc},
	}
	entries := buildDownsampledFields(b, []string{"avg"}, 60.0)
	if len(entries) != 1 {
		t.Fatal("expected 1 entry, got", len(entries))
	}
	if entries[0].Key != "avg_cpu" {
		t.Errorf("expected key avg_cpu, got %s", entries[0].Key)
	}
	if entries[0].Value.GetFloatValue() != 2.0 {
		t.Errorf("expected value 2.0, got %v", entries[0].Value.GetFloatValue())
	}
}

func TestBuildDownsampledFields_MultipleFunctions(t *testing.T) {
	acc := newAccumulator()
	acc.add(1.0, 100)
	acc.add(2.0, 200)
	acc.add(3.0, 300)

	b := &bucket{
		windowStart:  100,
		accumulators: map[string]*accumulator{"mem": acc},
	}
	fns := []string{"avg", "max", "min", "sum", "count", "first", "last"}
	entries := buildDownsampledFields(b, fns, 60.0)
	if len(entries) != len(fns) {
		t.Fatalf("expected %d entries, got %d", len(fns), len(entries))
	}

	expectedKeys := []string{
		"avg_mem", "max_mem", "min_mem", "sum_mem", "count_mem", "first_mem", "last_mem",
	}
	for i, ek := range expectedKeys {
		if entries[i].Key != ek {
			t.Errorf("entry %d: expected key %s, got %s", i, ek, entries[i].Key)
		}
	}
}

func TestBuildDownsampledFields_CountIsInt64(t *testing.T) {
	acc := newAccumulator()
	acc.add(1.0, 100)
	acc.add(2.0, 200)

	b := &bucket{
		windowStart:  100,
		accumulators: map[string]*accumulator{"cpu": acc},
	}
	entries := buildDownsampledFields(b, []string{"count"}, 60.0)
	if len(entries) != 1 {
		t.Fatal("expected 1 entry, got", len(entries))
	}
	if entries[0].Value.GetIntValue() != 2 {
		t.Errorf("expected int value 2, got %v", entries[0].Value.GetIntValue())
	}
}

func TestBuildDownsampledFields_EmptyFunctions(t *testing.T) {
	acc := newAccumulator()
	acc.add(1.0, 100)

	b := &bucket{
		windowStart:  100,
		accumulators: map[string]*accumulator{"cpu": acc},
	}
	entries := buildDownsampledFields(b, nil, 60.0)
	if len(entries) != 0 {
		t.Error("expected 0 entries for nil functions, got", len(entries))
	}

	entries = buildDownsampledFields(b, []string{}, 60.0)
	if len(entries) != 0 {
		t.Error("expected 0 entries for empty functions, got", len(entries))
	}
}

func TestBuildDownsampledFields_Diff(t *testing.T) {
	acc := newAccumulator()
	acc.add(10.0, 100)
	acc.add(15.0, 200)
	acc.add(25.0, 300)

	b := &bucket{
		windowStart:  100,
		accumulators: map[string]*accumulator{"cpu": acc},
	}
	entries := buildDownsampledFields(b, []string{"diff"}, 60.0)
	if len(entries) != 1 {
		t.Fatal("expected 1 entry, got", len(entries))
	}
	if entries[0].Key != "diff_cpu" {
		t.Errorf("expected key diff_cpu, got %s", entries[0].Key)
	}
	if entries[0].Value.GetFloatValue() != 15.0 {
		t.Errorf("expected value 15.0, got %v", entries[0].Value.GetFloatValue())
	}
}

func TestBuildDownsampledFields_Rate(t *testing.T) {
	acc := newAccumulator()
	acc.add(100.0, 1000000000)
	acc.add(200.0, 61000000000)

	b := &bucket{
		windowStart:  100,
		accumulators: map[string]*accumulator{"cpu": acc},
	}
	entries := buildDownsampledFields(b, []string{"rate"}, 60.0)
	if len(entries) != 1 {
		t.Fatal("expected 1 entry, got", len(entries))
	}
	if entries[0].Key != "rate_cpu" {
		t.Errorf("expected key rate_cpu, got %s", entries[0].Key)
	}
	expected := 100.0 / 60.0
	if entries[0].Value.GetFloatValue() != expected {
		t.Errorf("expected value %v, got %v", expected, entries[0].Value.GetFloatValue())
	}
}

func TestBuildDownsampledFields_Irate(t *testing.T) {
	acc := newAccumulator()
	acc.add(100.0, 1000000000)  // t=1s
	acc.add(150.0, 6000000000)  // t=6s
	acc.add(200.0, 11000000000) // t=11s

	b := &bucket{
		windowStart:  100,
		accumulators: map[string]*accumulator{"cpu": acc},
	}
	entries := buildDownsampledFields(b, []string{"irate"}, 60.0)
	if len(entries) != 1 {
		t.Fatal("expected 1 entry, got", len(entries))
	}
	if entries[0].Key != "irate_cpu" {
		t.Errorf("expected key irate_cpu, got %s", entries[0].Key)
	}
	expected := 50.0 / 5.0 // 10/sec
	if entries[0].Value.GetFloatValue() != expected {
		t.Errorf("expected value %v, got %v", expected, entries[0].Value.GetFloatValue())
	}
}

func TestBuildDownsampledFields_Derivative(t *testing.T) {
	acc := newAccumulator()
	acc.add(10.0, 1000000000)
	acc.add(20.0, 31000000000)

	b := &bucket{
		windowStart:  100,
		accumulators: map[string]*accumulator{"cpu": acc},
	}
	entries := buildDownsampledFields(b, []string{"derivative"}, 30.0)
	if len(entries) != 1 {
		t.Fatal("expected 1 entry, got", len(entries))
	}
	if entries[0].Key != "derivative_cpu" {
		t.Errorf("expected key derivative_cpu, got %s", entries[0].Key)
	}
	expected := 10.0 / 30.0
	if entries[0].Value.GetFloatValue() != expected {
		t.Errorf("expected value %v, got %v", expected, entries[0].Value.GetFloatValue())
	}
}
