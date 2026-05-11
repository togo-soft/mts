package compression

import (
	"testing"
)

func TestBitmapEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		values []bool
	}{
		{"empty", nil},
		{"singleTrue", []bool{true}},
		{"singleFalse", []bool{false}},
		{"allTrue", []bool{true, true, true, true, true}},
		{"allFalse", []bool{false, false, false, false, false}},
		{"alternating", []bool{true, false, true, false, true, false, true, false}},
		{"oddCount7", []bool{true, false, true, false, true, false, true}},
		{"oddCount9", []bool{false, true, false, true, false, true, false, true, false}},
		{"byte8", make([]bool, 8)},
		{"byte16", make([]bool, 16)},
		{"byte100", make([]bool, 100)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := BitmapEncode(tt.values)
			expectedBytes := (len(tt.values) + 7) / 8
			if len(data) != expectedBytes {
				t.Errorf("encoded len: want %d, got %d", expectedBytes, len(data))
			}

			decoded := BitmapDecode(data, len(tt.values))
			if len(decoded) != len(tt.values) {
				t.Fatalf("decoded len: want %d, got %d", len(tt.values), len(decoded))
			}
			for i, v := range tt.values {
				if decoded[i] != v {
					t.Errorf("idx %d: want %v, got %v", i, v, decoded[i])
				}
			}
		})
	}
}

func TestBitmapSize(t *testing.T) {
	values := make([]bool, 1000)
	data := BitmapEncode(values)
	if len(data) != 125 {
		t.Errorf("1000 bools: want 125 bytes, got %d", len(data))
	}
	// 1000 * 1 byte (raw) = 1000 bytes vs 125 bytes = 8:1
}
