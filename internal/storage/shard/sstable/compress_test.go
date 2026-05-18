package sstable

import (
	"bytes"
	"testing"
)

func TestCompressionAlgorithm_String(t *testing.T) {
	tests := []struct {
		algo CompressionAlgorithm
		want string
	}{
		{CompressionNone, "NONE"},
		{CompressionSnappy, "SNAPPY"},
		{CompressionLZ4, "LZ4"},
		{CompressionAlgorithm(99), "99"},
	}

	for _, tt := range tests {
		got := tt.algo.String()
		if got != tt.want {
			t.Errorf("CompressionAlgorithm(%d).String() = %s, want %s", tt.algo, got, tt.want)
		}
	}
}

func TestCompressDecompressRoundtrip(t *testing.T) {
	original := []byte("hello world, this is test data for compression roundtrip " +
		"with repeated patterns. hello world! hello world! hello world! " +
		"test data test data test data test data")

	algos := []CompressionAlgorithm{CompressionNone, CompressionSnappy, CompressionLZ4}

	for _, algo := range algos {
		t.Run(algo.String(), func(t *testing.T) {
			compressed, err := CompressBlock(original, algo)
			if err != nil {
				t.Fatalf("CompressBlock failed: %v", err)
			}

			decompressed, err := DecompressBlock(compressed, algo)
			if err != nil {
				t.Fatalf("DecompressBlock failed: %v", err)
			}

			if !bytes.Equal(decompressed, original) {
				t.Errorf("roundtrip mismatch for %s", algo.String())
			}
		})
	}
}

func TestCompressBlock_UnknownAlgo(t *testing.T) {
	_, err := CompressBlock([]byte("data"), CompressionAlgorithm(99))
	if err == nil {
		t.Error("expected error for unknown algorithm")
	}
}

func TestDecompressBlock_UnknownAlgo(t *testing.T) {
	_, err := DecompressBlock([]byte("data"), CompressionAlgorithm(99))
	if err == nil {
		t.Error("expected error for unknown algorithm")
	}
}

func TestDecompressBlock_TooShort_Snappy(t *testing.T) {
	_, err := DecompressBlock([]byte{0, 0, 0}, CompressionSnappy)
	if err == nil {
		t.Error("expected error for short snappy data")
	}
}

func TestDecompressBlock_TooShort_LZ4(t *testing.T) {
	_, err := DecompressBlock([]byte{0, 0, 0}, CompressionLZ4)
	if err == nil {
		t.Error("expected error for short lz4 data")
	}
}

func TestDecompressBlock_CorruptData_Snappy(t *testing.T) {
	// 4 bytes header + corrupt data
	corrupt := make([]byte, 100)
	// header says original is 10 bytes
	corrupt[3] = 10
	// remaining bytes are random garbage
	for i := 4; i < 100; i++ {
		corrupt[i] = 0xFF
	}
	_, err := DecompressBlock(corrupt, CompressionSnappy)
	if err == nil {
		t.Error("expected error for corrupt snappy data")
	}
}

func TestDecompressBlock_CorruptData_LZ4(t *testing.T) {
	// 4 bytes header + corrupt data
	corrupt := make([]byte, 100)
	corrupt[3] = 10 // original length = 10
	for i := 4; i < 100; i++ {
		corrupt[i] = 0xFF
	}
	_, err := DecompressBlock(corrupt, CompressionLZ4)
	if err == nil {
		t.Error("expected error for corrupt lz4 data")
	}
}

func TestCompressDecompress_Empty(t *testing.T) {
	algos := []CompressionAlgorithm{CompressionNone, CompressionSnappy, CompressionLZ4}
	for _, algo := range algos {
		t.Run(algo.String(), func(t *testing.T) {
			compressed, err := CompressBlock([]byte{}, algo)
			if err != nil {
				t.Fatalf("CompressBlock empty failed: %v", err)
			}
			decompressed, err := DecompressBlock(compressed, algo)
			if err != nil {
				t.Fatalf("DecompressBlock empty failed: %v", err)
			}
			if len(decompressed) != 0 {
				t.Errorf("expected empty result, got %d bytes", len(decompressed))
			}
		})
	}
}

func TestCompressBlock_SameAsDecompress(t *testing.T) {
	// 验证压缩后数据通过非压缩路径读取结果一致
	data := []byte("consistent data for verification test")
	for _, algo := range []CompressionAlgorithm{CompressionSnappy, CompressionLZ4} {
		compressed, err := CompressBlock(data, algo)
		if err != nil {
			t.Fatalf("CompressBlock failed: %v", err)
		}

		// 压缩后应包含 4 字节 header + 4 字节 CRC
		if len(compressed) < 8 {
			t.Errorf("compressed data too short for %s: %d bytes", algo.String(), len(compressed))
		}

		decompressed, err := DecompressBlock(compressed, algo)
		if err != nil {
			t.Fatalf("DecompressBlock failed: %v", err)
		}
		if !bytes.Equal(decompressed, data) {
			t.Errorf("data mismatch for %s", algo.String())
		}
	}
}

func TestCompressBlock_CompressionRatio(t *testing.T) {
	// 高度可压缩的数据
	var buf bytes.Buffer
	for i := 0; i < 1000; i++ {
		buf.WriteString("hello world ")
	}
	data := buf.Bytes()

	for _, algo := range []CompressionAlgorithm{CompressionSnappy, CompressionLZ4} {
		compressed, err := CompressBlock(data, algo)
		if err != nil {
			t.Fatalf("CompressBlock failed: %v", err)
		}

		// 压缩数据应小于原始数据（repeated patterns compress well）
		if len(compressed) >= len(data) {
			t.Errorf("%s: compressed size %d >= original size %d, expected compression",
				algo.String(), len(compressed), len(data))
		}
	}
}

func TestCompressBlock_NoneReturnsSameSlice(t *testing.T) {
	data := []byte("test-data")
	compressed, err := CompressBlock(data, CompressionNone)
	if err != nil {
		t.Fatalf("CompressBlock failed: %v", err)
	}
	// None 压缩只是追加 4 字节 CRC32C
	if len(compressed) != len(data)+4 {
		t.Errorf("expected len(data)+4 for none, got %d vs %d", len(compressed), len(data)+4)
	}
}

func TestDecompressBlock_CRC32CMismatch_None(t *testing.T) {
	data := []byte("important data")
	compressed, err := CompressBlock(data, CompressionNone)
	if err != nil {
		t.Fatalf("CompressBlock failed: %v", err)
	}
	// 篡改 CRC
	compressed[len(compressed)-1] ^= 0xFF
	_, err = DecompressBlock(compressed, CompressionNone)
	if err == nil {
		t.Error("expected CRC mismatch error for None")
	}
}

func TestDecompressBlock_CRC32CMismatch_Snappy(t *testing.T) {
	data := []byte("important data for snappy compression test")
	compressed, err := CompressBlock(data, CompressionSnappy)
	if err != nil {
		t.Fatalf("CompressBlock failed: %v", err)
	}
	compressed[len(compressed)-1] ^= 0xFF
	_, err = DecompressBlock(compressed, CompressionSnappy)
	if err == nil {
		t.Error("expected CRC mismatch error for Snappy")
	}
}

func TestDecompressBlock_CRC32CMismatch_LZ4(t *testing.T) {
	data := []byte("important data for lz4 compression test")
	compressed, err := CompressBlock(data, CompressionLZ4)
	if err != nil {
		t.Fatalf("CompressBlock failed: %v", err)
	}
	compressed[len(compressed)-1] ^= 0xFF
	_, err = DecompressBlock(compressed, CompressionLZ4)
	if err == nil {
		t.Error("expected CRC mismatch error for LZ4")
	}
}

func TestDecompressBlock_TooShort_None(t *testing.T) {
	_, err := DecompressBlock([]byte{0, 0, 0}, CompressionNone)
	if err == nil {
		t.Error("expected error for short uncompressed data")
	}
}
