// internal/storage/shard/compression/varint_test.go
package compression

import (
	"testing"
)

func TestVarintEncode(t *testing.T) {
	tests := []struct {
		input    uint64
		expected []byte
	}{
		{0, []byte{0}},
		{127, []byte{127}},
		{128, []byte{128, 1}},
		{255, []byte{255, 1}},
		{1000, []byte{232, 7}},
	}

	for _, tt := range tests {
		buf := make([]byte, 10)
		n := PutVarint(buf, tt.input)
		if n != len(tt.expected) || string(buf[:n]) != string(tt.expected) {
			t.Errorf("PutVarint(%d): expected %v, got %v", tt.input, tt.expected, buf[:n])
		}
	}
}

func TestVarintDecode(t *testing.T) {
	tests := []uint64{0, 127, 128, 255, 1000, 1 << 63}

	for _, expected := range tests {
		buf := make([]byte, 10)
		n := PutVarint(buf, expected)
		val, n2 := Varint(buf)
		if val != expected || n != n2 {
			t.Errorf("Varint roundtrip: expected %d, got %d", expected, val)
		}
	}
}
