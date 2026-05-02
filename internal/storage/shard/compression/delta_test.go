// internal/storage/shard/compression/delta_test.go
package compression

import (
	"testing"
)

func TestDeltaEncode(t *testing.T) {
	values := []int64{100, 105, 110, 115}
	deltas := DeltaEncode(values)

	expected := []int64{100, 5, 5, 5}
	for i, d := range deltas {
		if d != expected[i] {
			t.Errorf("delta[%d]: expected %d, got %d", i, expected[i], d)
		}
	}
}
