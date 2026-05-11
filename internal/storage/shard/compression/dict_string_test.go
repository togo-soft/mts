package compression

import (
	"testing"
)

func TestDictEncodeDecode_Success(t *testing.T) {
	tests := []struct {
		name   string
		values []string
	}{
		{"repeated", []string{"a", "b", "a", "b", "a", "b"}},
		{"allSame", []string{"x", "x", "x", "x", "x", "x"}},
		{"manyRepeats", repeatStrings([]string{"foo", "bar", "baz"}, 100)},
		{"manySame", repeatStrings([]string{"hello"}, 200)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := DictEncode(tt.values)
			if data == nil {
				t.Fatal("DictEncode returned nil (unexpected fallback)")
			}
			decoded, err := DictDecode(data, len(tt.values))
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if len(decoded) != len(tt.values) {
				t.Fatalf("decoded len: want %d, got %d", len(tt.values), len(decoded))
			}
			for i, v := range tt.values {
				if decoded[i] != v {
					t.Errorf("idx %d: want %q, got %q", i, v, decoded[i])
				}
			}
		})
	}
}

func TestDictEncodeFallback(t *testing.T) {
	tests := []struct {
		name   string
		values []string
	}{
		{"empty", nil},
		{"single", []string{"hello"}},
		{"allUnique", []string{"unique_a", "unique_b", "unique_c", "unique_d", "unique_e"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := DictEncode(tt.values)
			if data != nil {
				t.Errorf("%s: expected nil (fallback), got %d bytes", tt.name, len(data))
			}
		})
	}
}

func TestShouldUseDict(t *testing.T) {
	if ShouldUseDict(nil) {
		t.Error("nil should not use dict")
	}
	if !ShouldUseDict(repeatStrings([]string{"a", "b"}, 50)) {
		t.Error("many repeats should use dict")
	}
	if ShouldUseDict([]string{"unique_a", "unique_b"}) {
		t.Error("few unique strings should not use dict")
	}
}

func repeatStrings(pattern []string, count int) []string {
	result := make([]string, 0, count)
	for i := 0; i < count; i++ {
		result = append(result, pattern[i%len(pattern)])
	}
	return result
}
