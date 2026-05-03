// internal/storage/shard/wal_test.go
package shard

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"micro-ts/internal/types"
)

func TestWAL_Write(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer func() {
		if closeErr := w.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	}()

	data := []byte("test data")
	n, err := w.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected %d bytes written, got %d", len(data), n)
	}
}

func TestWAL_Sync(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer func() {
		if closeErr := w.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	}()

	data := []byte("test data")
	_, err = w.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	err = w.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}

func TestWAL_Reopen(t *testing.T) {
	tmpDir := t.TempDir()
	w1, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	_, err = w1.Write([]byte("data1"))
	if err != nil {
		_ = w1.Close()
		t.Fatalf("Write failed: %v", err)
	}

	if err := w1.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	w2, err := NewWAL(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer func() {
		if closeErr := w2.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	}()

	if w2.Sequence() != 1 {
		t.Errorf("expected sequence 1, got %d", w2.Sequence())
	}
}

func TestWAL_FilePermissions(t *testing.T) {
	// Permission check skipped on Windows due to ACL differences
	if runtime.GOOS == "windows" {
		t.Skip("skipping permission test on Windows")
	}

	tmpDir := t.TempDir()
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	files, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	for _, f := range files {
		info, _ := os.Stat(f)
		if info.Mode().Perm() != 0600 {
			t.Errorf("expected 0600, got %o", info.Mode().Perm())
		}
	}
}

func TestWAL_Replay(t *testing.T) {
	tmpDir := t.TempDir()

	// 写入 WAL
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// 序列化 point 并写入
	p := &types.Point{
		Timestamp: 1000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]any{"usage": 85.5},
	}

	data, err := serializePoint(p)
	if err != nil {
		t.Fatalf("serializePoint failed: %v", err)
	}

	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 重放 WAL
	points, err := ReplayWAL(tmpDir)
	if err != nil {
		t.Fatalf("ReplayWAL failed: %v", err)
	}

	if len(points) != 1 {
		t.Errorf("expected 1 point, got %d", len(points))
	}

	if points[0].Timestamp != 1000 {
		t.Errorf("expected timestamp 1000, got %d", points[0].Timestamp)
	}

	usage, ok := points[0].Fields["usage"].(float64)
	if !ok {
		t.Errorf("expected usage field to be float64")
	}
	if usage != 85.5 {
		t.Errorf("expected usage 85.5, got %f", usage)
	}
}

func TestWAL_StartPeriodicSync(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	// 确保 WAL 关闭，防止 goroutine 泄露
	defer func() {
		if closeErr := wal.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	}()

	// 写入序列化的 point
	p := &types.Point{
		Timestamp: 1000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]any{"usage": 85.5},
	}

	data, err := serializePoint(p)
	if err != nil {
		t.Fatalf("serializePoint failed: %v", err)
	}

	if _, err := wal.Write(data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 启动 periodic sync，间隔 100ms
	done := make(chan struct{})
	wal.StartPeriodicSync(100*time.Millisecond, done)

	// 等待 250ms，确保至少 sync 了 2 次（100ms 间隔 * 2 + 缓冲时间）
	time.Sleep(250 * time.Millisecond)
	close(done)

	// 验证数据已刷盘（通过重新打开 WAL）
	wal2, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL for verify failed: %v", err)
	}
	defer wal2.Close()

	points, err := ReplayWAL(tmpDir)
	if err != nil {
		t.Fatalf("ReplayWAL failed: %v", err)
	}
	if len(points) != 1 {
		t.Errorf("expected 1 point, got %d", len(points))
	}
}
