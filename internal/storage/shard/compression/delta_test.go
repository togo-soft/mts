package compression

import (
	"testing"
)

func TestDeltaEncode(t *testing.T) {
	tests := []struct {
		name     string
		values   []int64
		expected []int64
	}{
		{"single value", []int64{100}, []int64{100}},
		{"constant deltas", []int64{100, 105, 110, 115}, []int64{100, 5, 5, 5}},
		{"increasing deltas", []int64{0, 1, 3, 6, 10}, []int64{0, 1, 2, 3, 4}},
		{"decreasing values", []int64{100, 90, 80, 70}, []int64{100, -10, -10, -10}},
		{"mixed", []int64{0, 5, 2, 8, 3}, []int64{0, 5, -3, 6, -5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deltas := DeltaEncode(tt.values)
			if len(deltas) != len(tt.expected) {
				t.Errorf("expected len %d, got %d", len(tt.expected), len(deltas))
			}
			for i, d := range deltas {
				if d != tt.expected[i] {
					t.Errorf("delta[%d]: expected %d, got %d", i, tt.expected[i], d)
				}
			}
		})
	}
}

func TestDeltaDecode(t *testing.T) {
	tests := []struct {
		name     string
		deltas   []int64
		expected []int64
	}{
		{"single value", []int64{100}, []int64{100}},
		{"constant deltas", []int64{100, 5, 5, 5}, []int64{100, 105, 110, 115}},
		{"increasing deltas", []int64{0, 1, 2, 3, 4}, []int64{0, 1, 3, 6, 10}},
		{"decreasing values", []int64{100, -10, -10, -10}, []int64{100, 90, 80, 70}},
		{"mixed", []int64{0, 5, -3, 6, -5}, []int64{0, 5, 2, 8, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := DeltaDecode(tt.deltas)
			if len(values) != len(tt.expected) {
				t.Errorf("expected len %d, got %d", len(tt.expected), len(values))
			}
			for i, v := range values {
				if v != tt.expected[i] {
					t.Errorf("value[%d]: expected %d, got %d", i, tt.expected[i], v)
				}
			}
		})
	}
}

func TestDeltaEncodeDecode_RoundTrip(t *testing.T) {
	original := []int64{100, 105, 110, 115, 120, 125, 130}
	deltas := DeltaEncode(original)
	recovered := DeltaDecode(deltas)

	if len(recovered) != len(original) {
		t.Errorf("round trip length mismatch: expected %d, got %d", len(original), len(recovered))
	}

	for i, v := range recovered {
		if v != original[i] {
			t.Errorf("round trip value[%d]: expected %d, got %d", i, original[i], v)
		}
	}
}

func TestDeltaEncode_Empty(t *testing.T) {
	values := []int64{}
	deltas := DeltaEncode(values)
	if len(deltas) != 0 {
		t.Errorf("expected empty deltas, got %d", len(deltas))
	}
}

func TestDeltaDecode_Empty(t *testing.T) {
	deltas := []int64{}
	values := DeltaDecode(deltas)
	if len(values) != 0 {
		t.Errorf("expected empty values, got %d", len(values))
	}
}
