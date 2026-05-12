package compression

import (
	"math"
	"testing"
)

func TestEncodeTimestamps_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []int64
	}{
		{"empty", nil},
		{"single", []int64{1000}},
		{"two", []int64{1000, 2000}},
		{"regularInterval", regularTimestamps(0, 1000000000, 100)},
		{"irregularInterval", []int64{0, 10, 15, 25, 30, 50}},
		{"negative", []int64{-100, -50, 0, 50, 100}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := EncodeTimestamps(tt.values)
			decoded, err := DecodeTimestamps(data, len(tt.values))
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if len(decoded) != len(tt.values) {
				t.Fatalf("len: want %d, got %d", len(tt.values), len(decoded))
			}
			for i, v := range tt.values {
				if decoded[i] != v {
					t.Errorf("idx %d: want %d, got %d", i, v, decoded[i])
				}
			}
		})
	}
}

func TestEncodeTimestamps_Compression(t *testing.T) {
	// 等间隔时间戳应极好压缩（1 纳秒间隔）
	values := regularTimestamps(0, 1, 1000)
	data := EncodeTimestamps(values)
	rawSize := len(values) * 8 // 8000
	if len(data) >= rawSize {
		t.Errorf("expected compression, got %d >= %d", len(data), rawSize)
	}
	t.Logf("1000 regular timestamps: %d bytes (raw=%d, ratio=%.1f%%)",
		len(data), rawSize, float64(len(data))/float64(rawSize)*100)
}

func TestEncodeSids_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []uint64
	}{
		{"empty", nil},
		{"single", []uint64{42}},
		{"smallSeq", []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
		{"largeValues", []uint64{1000000, 2000000, 3000000}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := EncodeSids(tt.values)
			decoded, err := DecodeSids(data, len(tt.values))
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if len(decoded) != len(tt.values) {
				t.Fatalf("len: want %d, got %d", len(tt.values), len(decoded))
			}
			for i, v := range tt.values {
				if decoded[i] != v {
					t.Errorf("idx %d: want %d, got %d", i, v, decoded[i])
				}
			}
		})
	}
}

func TestEncodeInt64Values_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []int64
	}{
		{"empty", nil},
		{"single", []int64{42}},
		{"smallPositive", []int64{1, 2, 3, 4, 5}},
		{"smallNegative", []int64{-1, -2, -3, -4}},
		{"mixed", []int64{-100, 0, 100, -50, 50}},
		{"zeros", []int64{0, 0, 0, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := EncodeInt64Values(tt.values)
			decoded, err := DecodeInt64Values(data, len(tt.values))
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if len(decoded) != len(tt.values) {
				t.Fatalf("len: want %d, got %d", len(tt.values), len(decoded))
			}
			for i, v := range tt.values {
				if decoded[i] != v {
					t.Errorf("idx %d: want %d, got %d", i, v, decoded[i])
				}
			}
		})
	}
}

func TestEncodeFloat64Values_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
	}{
		{"empty", nil},
		{"single", []float64{3.14}},
		{"zeros", []float64{0, 0, 0}},
		{"same", []float64{1.5, 1.5, 1.5}},
		{"random", []float64{3.14, 2.718, 1.414, 0.0, -1.5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := EncodeFloat64Values(tt.values)
			decoded, err := DecodeFloat64Values(data, len(tt.values))
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if len(decoded) != len(tt.values) {
				t.Fatalf("len: want %d, got %d", len(tt.values), len(decoded))
			}
			for i, v := range tt.values {
				if math.IsNaN(v) {
					if !math.IsNaN(decoded[i]) {
						t.Errorf("idx %d: want NaN, got %v", i, decoded[i])
					}
				} else if decoded[i] != v {
					t.Errorf("idx %d: want %v, got %v", i, v, decoded[i])
				}
			}
		})
	}
}

func TestEncodeStringValues_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []string
	}{
		{"empty", nil},
		{"dictSuccess", repeatStrings([]string{"foo", "bar"}, 100)},
		{"dictFallback", []string{"a", "b", "c"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, isDict := EncodeStringValues(tt.values)
			decoded, err := DecodeStringValues(data, len(tt.values), isDict)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if len(decoded) != len(tt.values) {
				t.Fatalf("len: want %d, got %d", len(tt.values), len(decoded))
			}
			for i, v := range tt.values {
				if decoded[i] != v {
					t.Errorf("idx %d: want %q, got %q", i, v, decoded[i])
				}
			}
		})
	}
}

func TestEncodeBoolValues_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []bool
	}{
		{"empty", nil},
		{"single", []bool{true}},
		{"mixed", []bool{true, false, true, false, false}},
		{"allTrue100", boolRepeat(true, 100)},
		{"allFalse100", boolRepeat(false, 100)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := EncodeBoolValues(tt.values)
			decoded := DecodeBoolValues(data, len(tt.values))
			if len(decoded) != len(tt.values) {
				t.Fatalf("len: want %d, got %d", len(tt.values), len(decoded))
			}
			for i, v := range tt.values {
				if decoded[i] != v {
					t.Errorf("idx %d: want %v, got %v", i, v, decoded[i])
				}
			}
		})
	}
}

func regularTimestamps(start, interval int64, count int) []int64 {
	values := make([]int64, count)
	for i := 0; i < count; i++ {
		values[i] = start + int64(i)*interval
	}
	return values
}

func boolRepeat(v bool, count int) []bool {
	values := make([]bool, count)
	for i := range values {
		values[i] = v
	}
	return values
}

func TestEncodeSidsDelta_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []uint64
	}{
		{"empty", nil},
		{"single", []uint64{42}},
		{"two", []uint64{1000, 1001}},
		{"smallSeq", []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
		{"largeDelta", []uint64{1000000, 1000100, 1000200, 1000300}},
		{"mixedDelta", []uint64{1, 100, 105, 110, 200}},
		{"repeating", []uint64{100, 100, 100, 100, 100}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := EncodeSidsDelta(tt.values)
			decoded, err := DecodeSidsDelta(data, len(tt.values))
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if len(decoded) != len(tt.values) {
				t.Fatalf("len: want %d, got %d", len(tt.values), len(decoded))
			}
			for i, v := range tt.values {
				if decoded[i] != v {
					t.Errorf("idx %d: want %d, got %d", i, v, decoded[i])
				}
			}
		})
	}
}

func TestEncodeSidsDelta_Compression(t *testing.T) {
	// 递增 SID：Delta 编码应该比直接 Varint 小很多
	values := make([]uint64, 1000)
	for i := range values {
		values[i] = uint64(1000000 + i)
	}

	deltaData := EncodeSidsDelta(values)
	varintData := EncodeSids(values)

	t.Logf("1000 递增 SID: Delta=%d bytes, Varint=%d bytes, 节省=%.1f%%",
		len(deltaData), len(varintData),
		float64(len(varintData)-len(deltaData))/float64(len(varintData))*100)

	if len(deltaData) >= len(varintData) {
		t.Errorf("Delta 编码应该更小")
	}
}

func TestEncodeSidsDelta_TruncatedData(t *testing.T) {
	// 测试截断数据返回错误
	values := []uint64{1, 2, 3, 4, 5}
	data := EncodeSidsDelta(values)

	// 截断到 2 字节，应该解码失败
	_, err := DecodeSidsDelta(data[:2], len(values))
	if err == nil {
		t.Error("expected error for truncated data")
	}
}
