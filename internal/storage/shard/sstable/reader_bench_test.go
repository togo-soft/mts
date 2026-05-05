// internal/storage/shard/sstable/reader_bench_test.go
package sstable

import (
	"os"
	"path/filepath"
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

// BenchmarkNewReader 测试 SSTable Reader 创建性能
func BenchmarkNewReader(b *testing.B) {
	tmpDir := b.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	// 创建测试数据
	writer, err := NewWriter(dataDir, 1)
	if err != nil {
		b.Fatal(err)
	}

	// 写入 10000 行数据
	points := make([]*types.Point, 10000)
	for i := 0; i < 10000; i++ {
		points[i] = &types.Point{
			Timestamp: int64(1234567890000000000 + i*1e9),
			Tags:      map[string]string{"host": "server1"},
			Fields: map[string]any{
				"usage": float64(i),
			},
		}
	}

	if err := writer.WritePoints(points); err != nil {
		b.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader, err := NewReader(dataDir)
		if err != nil {
			b.Fatal(err)
		}
		_ = reader.Close()
	}

	b.Cleanup(func() {
		_ = os.RemoveAll(tmpDir)
	})
}
