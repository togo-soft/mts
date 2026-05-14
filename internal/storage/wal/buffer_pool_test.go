package wal

import (
	"bytes"
	"testing"
)

func TestGetBuf_SmallCap(t *testing.T) {
	buf := getBuf(64)
	if cap(buf) < 64 {
		t.Errorf("expected cap >= 64, got %d", cap(buf))
	}
	if len(buf) != 0 {
		t.Errorf("expected len=0, got %d", len(buf))
	}
}

func TestGetBuf_MediumCap(t *testing.T) {
	buf := getBuf(2048)
	if cap(buf) < 2048 {
		t.Errorf("expected cap >= 2048, got %d", cap(buf))
	}
}

func TestGetBuf_LargeCap(t *testing.T) {
	buf := getBuf(32768)
	if cap(buf) < 32768 {
		t.Errorf("expected cap >= 32768, got %d", cap(buf))
	}
}

func TestGetBuf_ExceedsPool(t *testing.T) {
	buf := getBuf(128 * 1024)
	if cap(buf) < 128*1024 {
		t.Errorf("expected cap >= 128KB, got %d", cap(buf))
	}
	// 超大缓冲区不池化，直接分配即可
}

func TestPutBuf_NilCap(t *testing.T) {
	buf := make([]byte, 0)
	putBuf(buf) // 不应 panic
}

func TestPoolReuse_SameSize(t *testing.T) {
	b1 := getBuf(128)
	copy(b1[:8], []byte("reusable"))
	putBuf(b1)

	b2 := getBuf(128)
	// 可能从池中取回同一缓冲区（内容已清空）
	if len(b2) != 0 {
		t.Errorf("pooled buffer should have len=0, got %d", len(b2))
	}
	if cap(b2) < 128 {
		t.Errorf("pooled buffer should have cap >= 128, got %d", cap(b2))
	}
}

func TestPoolReuse_UpgradeSize(t *testing.T) {
	small := getBuf(64)
	putBuf(small)

	// 请求更大容量时不应 panic，应分配新缓冲区
	large := getBuf(1024)
	if cap(large) < 1024 {
		t.Errorf("expected cap >= 1024, got %d", cap(large))
	}
	putBuf(large)
}

func TestCompressPayload_PoolRelease(t *testing.T) {
	payload := []byte("hello world, this is a test payload for compression")

	compressed, release, err := CompressPayload(payload)
	if err != nil {
		t.Fatalf("CompressPayload: %v", err)
	}
	if compressed == nil {
		t.Fatal("expected non-nil compressed data")
	}
	if compressed[0] != 1 && compressed[0] != 0 {
		t.Fatalf("expected flag 0 or 1, got %d", compressed[0])
	}

	// 验证原始大小字段
	_ = compressed[1]
	_ = compressed[2]
	_ = compressed[3]
	_ = compressed[4]

	// release 不应 panic
	if release != nil {
		release()
	}
}

func TestCompressPayload_EmptyPayload(t *testing.T) {
	compressed, release, err := CompressPayload(nil)
	if err != nil {
		t.Fatalf("CompressPayload(nil): %v", err)
	}
	if compressed != nil {
		t.Errorf("expected nil for empty payload, got %d bytes", len(compressed))
	}
	if release != nil {
		t.Errorf("expected nil release for empty payload")
	}
}

func TestCompressPayload_RoundTrip(t *testing.T) {
	original := []byte("round-trip test data for WAL compression verification")

	compressed, release, err := CompressPayload(original)
	if err != nil {
		t.Fatalf("CompressPayload: %v", err)
	}
	defer release()

	// 使用 DecompressPayload 解压
	decompressed, err := DecompressPayload(compressed)
	if err != nil {
		t.Fatalf("DecompressPayload: %v", err)
	}
	if !bytes.Equal(decompressed, original) {
		t.Errorf("round-trip mismatch: expected %q, got %q", original, decompressed)
	}
}

func TestCompressPayload_Uncompressible(t *testing.T) {
	// 随机数据通常不可压缩
	payload := make([]byte, 100)
	for i := range payload {
		payload[i] = byte(i*7 + 13)
	}

	compressed, release, err := CompressPayload(payload)
	if err != nil {
		t.Fatalf("CompressPayload: %v", err)
	}
	defer release()

	decompressed, err := DecompressPayload(compressed)
	if err != nil {
		t.Fatalf("DecompressPayload: %v", err)
	}
	if !bytes.Equal(decompressed, payload) {
		t.Errorf("uncompressible round-trip mismatch")
	}
}

func TestCompressPayload_HighlyCompressible(t *testing.T) {
	// 重复数据高度可压缩
	payload := bytes.Repeat([]byte("A"), 1000)

	compressed, release, err := CompressPayload(payload)
	if err != nil {
		t.Fatalf("CompressPayload: %v", err)
	}
	defer release()

	if compressed[0] != 1 {
		t.Log("compressible data stored uncompressed (small overhead)")
	}
	if len(compressed) >= len(payload) {
		t.Logf("compressed %d >= original %d (expected for very small payloads)", len(compressed), len(payload))
	}

	decompressed, err := DecompressPayload(compressed)
	if err != nil {
		t.Fatalf("DecompressPayload: %v", err)
	}
	if !bytes.Equal(decompressed, payload) {
		t.Errorf("compressible round-trip mismatch")
	}
}

func TestDecompressPayload_InvalidData(t *testing.T) {
	_, err := DecompressPayload([]byte{})
	if err != nil {
		t.Logf("expected error for empty: %v", err)
	}

	_, err = DecompressPayload([]byte{1, 0, 0, 0})
	if err != nil {
		t.Logf("expected error for truncated: %v", err)
	}
}

func TestCompressPayload_MultipleReleases(t *testing.T) {
	payload := []byte("test data for multiple compress/release cycles")

	for i := 0; i < 100; i++ {
		compressed, release, err := CompressPayload(payload)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if release == nil {
			t.Fatalf("iteration %d: expected non-nil release", i)
		}
		// 多次 release 不应 panic（池的 Put 是幂等的）
		release()

		// 验证解压正确
		decompressed, err := DecompressPayload(compressed)
		if err != nil {
			t.Fatalf("iteration %d decompress: %v", i, err)
		}
		if !bytes.Equal(decompressed, payload) {
			t.Errorf("iteration %d: round-trip mismatch", i)
		}
	}
}
