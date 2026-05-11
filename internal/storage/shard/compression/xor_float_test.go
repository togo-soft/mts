package compression

import (
	"math"
	"testing"
)

func TestXorFloatEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
	}{
		{"empty", nil},
		{"single", []float64{3.14}},
		{"sameValues", []float64{1.5, 1.5, 1.5, 1.5, 1.5}},
		{"increasing", []float64{1.0, 1.1, 1.2, 1.3, 1.4}},
		{"random", []float64{3.14, 2.71, 1.41, 0.0, -1.5, 100.5, 0.001}},
		{"zeros", []float64{0, 0, 0, 0}},
		{"nan", []float64{math.NaN(), math.NaN(), 1.0}},
		{"inf", []float64{math.Inf(1), math.Inf(-1), 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := XorFloatEncode(tt.values)
			decoded, err := XorFloatDecode(data, len(tt.values))
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if len(decoded) != len(tt.values) {
				t.Fatalf("decoded len: want %d, got %d", len(tt.values), len(decoded))
			}
			for i, v := range tt.values {
				got := decoded[i]
				if math.IsNaN(v) {
					if !math.IsNaN(got) {
						t.Errorf("idx %d: want NaN, got %v", i, got)
					}
				} else if got != v {
					t.Errorf("idx %d: want %v, got %v", i, v, got)
				}
			}
		})
	}
}

func TestXorFloatCompressionRatio(t *testing.T) {
	// 相同值应该极小压缩
	values := make([]float64, 1000)
	for i := range values {
		values[i] = 42.0
	}
	data := XorFloatEncode(values)
	// 8B first + 999*1bit ≈ 8 + 125 ≈ 133 bytes
	if len(data) > 200 {
		t.Errorf("same values: expected < 200 bytes, got %d", len(data))
	}

	// 等间隔值应该有良好压缩
	for i := range values {
		values[i] = float64(i) * 0.1
	}
	data = XorFloatEncode(values)
	// 应远小于 8000 bytes (raw)
	rawSize := len(values) * 8
	if len(data) >= rawSize {
		t.Errorf("increasing: expected < %d bytes, got %d", rawSize, len(data))
	}
	t.Logf("1000 increasing floats: %d bytes (raw=%d, ratio=%.1f%%)",
		len(data), rawSize, float64(len(data))/float64(rawSize)*100)
}
