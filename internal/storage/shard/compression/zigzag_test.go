package compression

import (
	"testing"
)

func TestZigZagEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		values []int64
	}{
		{"empty", nil},
		{"zero", []int64{0}},
		{"positive", []int64{1, 2, 100, 1000}},
		{"negative", []int64{-1, -2, -100, -1000}},
		{"mixed", []int64{-1, 0, 1, -100, 100}},
		{"minMax", []int64{0, 1, -1, 9223372036854775807, -9223372036854775808}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := ZigZagEncode(tt.values)
			decoded := ZigZagDecode(encoded)

			if len(encoded) != len(tt.values) {
				t.Errorf("encoded len: want %d, got %d", len(tt.values), len(encoded))
			}
			for i, v := range tt.values {
				if decoded[i] != v {
					t.Errorf("idx %d: want %d, got %d", i, v, decoded[i])
				}
			}
		})
	}
}

func TestZigZagSmallValues(t *testing.T) {
	// ZigZag 编码后小值的 Varint 字节数
	encoded := ZigZagEncode([]int64{-1, 0, 1, -2, 2})
	// -1 -> 1, 0 -> 0, 1 -> 2, -2 -> 3, 2 -> 4
	expected := []uint64{1, 0, 2, 3, 4}
	for i, exp := range expected {
		if encoded[i] != exp {
			t.Errorf("idx %d: want %d, got %d", i, exp, encoded[i])
		}
	}
}
