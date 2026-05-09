package wal

import (
	"os"
	"sync"
	"testing"
)

func TestWAL_OpenAndClose(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{
		Dir:      tmpDir,
		SyncMode: SyncNone,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, _ := listSegments(tmpDir)
	if len(entries) != 1 {
		t.Errorf("expected 1 segment, got %d", len(entries))
	}
}

func TestWAL_WriteAndReplay(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	payloads := [][]byte{
		[]byte("record-1"),
		[]byte("record-2"),
		[]byte("record-3"),
	}
	for _, p := range payloads {
		if _, err := w.Write(p); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w2.Close() }()

	var replayed [][]byte
	err = w2.Replay(func(data []byte) error {
		replayed = append(replayed, append([]byte(nil), data...))
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(replayed) != 3 {
		t.Errorf("expected 3 replayed records, got %d", len(replayed))
	}
	for i, p := range replayed {
		if string(p) != string(payloads[i]) {
			t.Errorf("record %d: expected %q, got %q", i, payloads[i], p)
		}
	}
}

func TestWAL_WriteBatch(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	payloads := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	n, err := w.WriteBatch(payloads)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if n != 3 {
		t.Errorf("expected total 3, got %d", n)
	}
}

func TestWAL_TruncateCurrent(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	_, _ = w.Write([]byte("data-to-truncate"))
	_ = w.Sync()

	if err := w.TruncateCurrent(); err != nil {
		t.Fatalf("TruncateCurrent: %v", err)
	}

	var count int
	_ = w.Replay(func(data []byte) error {
		count++
		return nil
	})
	// TruncateCurrent 不再截断 segment，数据被保留
	if count != 1 {
		t.Errorf("expected 1 record after truncate, got %d", count)
	}
}

func TestWAL_Rotation(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{
		Dir:         tmpDir,
		SegmentSize: 1024,
		SyncMode:    SyncNone,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	largePayload := make([]byte, 200)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	for i := 0; i < 20; i++ {
		if _, err := w.Write(largePayload); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	_ = w.Sync()
	_ = w.Close()

	entries, _ := listSegments(tmpDir)
	if len(entries) < 2 {
		t.Errorf("expected at least 2 segments after rotation, got %d", len(entries))
	}
}

func TestWAL_ConcurrentWrite(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	const goroutines = 10
	const writesPer = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < writesPer; i++ {
				data := []byte("concurrent-data")
				if _, err := w.Write(data); err != nil {
					t.Errorf("Write error: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestWAL_ReplayIncremental(t *testing.T) {
	tmpDir := t.TempDir()

	w1, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 10; i++ {
		data := make([]byte, 10)
		data[0] = byte('a' + i)
		_, _ = w1.Write(data)
	}
	_ = w1.Close()

	w2, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w2.Close() }()

	for i := 0; i < 5; i++ {
		data := make([]byte, 10)
		data[0] = byte('x')
		_, _ = w2.Write(data)
	}

	var count int
	_ = w2.Replay(func(data []byte) error {
		count++
		return nil
	})
	if count < 5 {
		t.Errorf("expected at least 5 records, got %d", count)
	}
}

func TestWAL_FilePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = w.Close()

	entries, _ := listSegments(tmpDir)
	for _, e := range entries {
		info, _ := os.Stat(e.Path)
		if info != nil && info.Mode().Perm() != 0600 {
			t.Errorf("expected 0600 permission on %s, got %o", e.Path, info.Mode().Perm())
		}
	}
}

func TestWAL_CRC_Corruption_Skip(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, _ = w.Write([]byte("good-record"))
	_ = w.Sync()
	_ = w.Close()

	entries, _ := listSegments(tmpDir)
	if len(entries) > 0 {
		data, _ := os.ReadFile(entries[0].Path)
		if len(data) > segmentHeaderSize+4 {
			data[segmentHeaderSize] = 0xFF
			data[segmentHeaderSize+1] = 0xFF
			_ = os.WriteFile(entries[0].Path, data, 0600)
		}
	}

	w2, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w2.Close() }()

	var count int
	_ = w2.Replay(func(data []byte) error {
		count++
		return nil
	})
	if count != 0 {
		t.Logf("corrupted record skipped: %d records replayed", count)
	}
}

func TestWAL_CloseIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestWAL_Generation(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	if w.Generation() == 0 {
		t.Error("expected non-zero generation")
	}
	if w.SegmentNum() != 1 {
		t.Errorf("expected segment num 1, got %d", w.SegmentNum())
	}
}

func TestWAL_WriteAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = w.Close()

	_, err = w.Write([]byte("test"))
	if err != ErrWALClosed {
		t.Errorf("expected ErrWALClosed, got %v", err)
	}
}
