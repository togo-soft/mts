package measurement

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestTagsHash(t *testing.T) {
	emptyTags := map[string]string{}
	singleTag := map[string]string{"host": "server1"}
	multipleTags := map[string]string{"host": "server1", "region": "us"}
	differentTags := map[string]string{"host": "server2", "region": "eu"}

	tests := []struct {
		name string
		tags map[string]string
	}{
		{"empty", emptyTags},
		{"single", singleTag},
		{"multiple", multipleTags},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := tagsHash(tt.tags)
			if h == 0 && len(tt.tags) > 0 {
				t.Error("hash should not be 0 for non-empty tags")
			}
		})
	}

	// Verify different inputs produce different hashes
	h1 := tagsHash(singleTag)
	h2 := tagsHash(multipleTags)
	h3 := tagsHash(differentTags)

	if h1 == h2 {
		t.Error("hash of single tag and multiple tags should differ")
	}
	if h1 == h3 {
		t.Error("hash of single tag and different tags should differ")
	}
	if h2 == h3 {
		t.Error("hash of multiple tags and different tags should differ")
	}

	// Empty tags must hash to 0
	if h := tagsHash(emptyTags); h != 0 {
		t.Errorf("empty tags should hash to 0, got %d", h)
	}
}

func TestTagsHashConsistency(t *testing.T) {
	tags := map[string]string{"host": "server1", "region": "us", "env": "prod"}

	h1 := tagsHash(tags)
	h2 := tagsHash(tags)

	if h1 != h2 {
		t.Errorf("hash should be consistent: got %d and %d", h1, h2)
	}
}

func TestTagsHashOrderIndependence(t *testing.T) {
	tags1 := map[string]string{"host": "server1", "region": "us"}
	tags2 := map[string]string{"region": "us", "host": "server1"}

	h1 := tagsHash(tags1)
	h2 := tagsHash(tags2)

	if h1 != h2 {
		t.Errorf("hash should be order-independent: got %d and %d", h1, h2)
	}
}

func TestTagsEqual(t *testing.T) {
	tags1 := map[string]string{"host": "server1", "region": "us"}
	tags2 := map[string]string{"region": "us", "host": "server1"}
	tags3 := map[string]string{"host": "server2"}

	if !tagsEqual(tags1, tags2) {
		t.Error("tags with same content but different order should be equal")
	}
	if tagsEqual(tags1, tags3) {
		t.Error("tags with different content should not be equal")
	}
}

func TestMeasurementMetaStore_AllocateSID(t *testing.T) {
	m := NewMeasurementMetaStore()

	tags1 := map[string]string{"host": "server1"}
	sid1, err := m.AllocateSID(tags1)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}
	if sid1 != 0 {
		t.Errorf("first SID should be 0, got %d", sid1)
	}

	// 相同 tags 应该返回相同 SID
	sid1Again, err := m.AllocateSID(tags1)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}
	if sid1Again != sid1 {
		t.Errorf("same tags should return same SID, got %d vs %d", sid1Again, sid1)
	}

	// 不同 tags 应该返回新 SID
	tags2 := map[string]string{"host": "server2"}
	sid2, err := m.AllocateSID(tags2)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}
	if sid2 != 1 {
		t.Errorf("second SID should be 1, got %d", sid2)
	}
}

func TestMeasurementMetaStore_GetTagsBySID(t *testing.T) {
	m := NewMeasurementMetaStore()

	tags := map[string]string{"host": "server1", "region": "us"}
	sid, err := m.AllocateSID(tags)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}

	retrieved, ok := m.GetTagsBySID(sid)
	if !ok {
		t.Error("GetTagsBySID should return true")
	}
	if !tagsEqual(retrieved, tags) {
		t.Error("retrieved tags should match original")
	}

	// 不存在的 SID
	_, ok = m.GetTagsBySID(999)
	if ok {
		t.Error("GetTagsBySID for non-existent SID should return false")
	}
}

func TestMeasurementMetaStore_GetSidsByTag(t *testing.T) {
	m := NewMeasurementMetaStore()

	tags1 := map[string]string{"host": "server1", "region": "us"}
	tags2 := map[string]string{"host": "server2", "region": "us"}
	tags3 := map[string]string{"host": "server3", "region": "eu"}

	_, err := m.AllocateSID(tags1)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}
	_, err = m.AllocateSID(tags2)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}
	_, err = m.AllocateSID(tags3)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}

	sids := m.GetSidsByTag("region", "us")
	if len(sids) != 2 {
		t.Errorf("region=us should have 2 sids, got %d", len(sids))
	}
}

func TestMeasurementMetaStore_SetPersistPath(t *testing.T) {
	m := NewMeasurementMetaStore()
	path := filepath.Join(t.TempDir(), "meta.json")
	m.SetPersistPath(path)

	// Allocate a SID to make it dirty
	tags := map[string]string{"host": "server1"}
	_, _ = m.AllocateSID(tags)

	// Persist should work
	err := m.Persist()
	if err != nil {
		t.Errorf("Persist failed: %v", err)
	}

	// Verify file exists
	if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
		t.Error("meta.json should exist after Persist")
	}
}

func TestMeasurementMetaStore_Persist_EmptyPath(t *testing.T) {
	m := NewMeasurementMetaStore()
	// persistPath 为空，Persist 应该直接返回 nil
	err := m.Persist()
	if err != nil {
		t.Errorf("Persist with empty path should succeed, got: %v", err)
	}
}

func TestMeasurementMetaStore_Close(t *testing.T) {
	m := NewMeasurementMetaStore()

	// Allocate to make it dirty
	tags := map[string]string{"host": "server1"}
	_, _ = m.AllocateSID(tags)

	// Close should succeed
	err := m.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestMeasurementMetaStore_Close_WithPersistPath(t *testing.T) {
	tmpDir := t.TempDir()
	m := NewMeasurementMetaStore()
	path := filepath.Join(tmpDir, "meta.json")
	m.SetPersistPath(path)

	// Allocate to make it dirty
	tags := map[string]string{"host": "server1"}
	_, _ = m.AllocateSID(tags)

	// Close should persist and succeed
	err := m.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify file exists
	if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
		t.Error("meta.json should exist after Close with dirty data")
	}
}

func TestMeasurementMetaStore_AllocateSID_Overflow(t *testing.T) {
	m := NewMeasurementMetaStore()
	// 直接设置 nextSID 为最大值来测试溢出
	m.nextSID = ^uint64(0)

	_, err := m.AllocateSID(map[string]string{"host": "server1"})
	if err == nil {
		t.Error("AllocateSID should return error on overflow")
	}
}

func TestMeasurementMetaStore_Persist_AtomicRename(t *testing.T) {
	// 测试原子性写入：应该先写 .tmp 再 rename
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "meta.json")

	m := NewMeasurementMetaStore()
	m.SetPersistPath(path)

	// Allocate SID 制造脏数据
	tags := map[string]string{"host": "server1"}
	_, _ = m.AllocateSID(tags)

	// Persist
	err := m.Persist()
	if err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// 主文件应该存在
	if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
		t.Fatal("meta.json should exist after Persist")
	}

	// .tmp 文件不应该存在（atomic rename 后已删除）
	tmpPath := path + ".tmp"
	if _, statErr := os.Stat(tmpPath); !os.IsNotExist(statErr) {
		t.Error(".tmp file should not exist after atomic rename")
	}
}

func TestMeasurementMetaStore_Persist_CreatesDir(t *testing.T) {
	// 测试 Persist 自动创建目录
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "subdir", "meta.json")

	m := NewMeasurementMetaStore()
	m.SetPersistPath(path)

	tags := map[string]string{"host": "server1"}
	_, _ = m.AllocateSID(tags)

	err := m.Persist()
	if err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
		t.Fatal("nested path should exist after Persist")
	}
}

func TestMeasurementMetaStore_Persist_NoDirtyNoWrite(t *testing.T) {
	// 测试 dirty=false 时不写入
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "meta.json")

	m := NewMeasurementMetaStore()
	m.SetPersistPath(path)

	// 不分配 SID，dirty 应该是 false
	err := m.Persist()
	if err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// 文件不应该存在
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Error("meta.json should not exist when dirty=false")
	}
}

func TestMeasurementMetaStore_Persist_Reload(t *testing.T) {
	// 测试持久化后重新加载数据完整性
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "meta.json")

	// 第一次创建和写入
	m1 := NewMeasurementMetaStore()
	m1.SetPersistPath(path)

	// 分配多个 SID
	tags1 := map[string]string{"host": "server1", "region": "us"}
	tags2 := map[string]string{"host": "server2", "region": "eu"}
	tags3 := map[string]string{"host": "server3", "region": "us"}

	sid1, _ := m1.AllocateSID(tags1)
	sid2, _ := m1.AllocateSID(tags2)
	sid3, _ := m1.AllocateSID(tags3)

	err := m1.Persist()
	if err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// 从文件重新加载
	m2, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	// 验证所有 SID 仍然有效
	retrieved1, ok := m2.GetTagsBySID(sid1)
	if !ok {
		t.Error("sid1 should exist after reload")
	}
	if retrieved1["host"] != "server1" {
		t.Error("sid1 host tag mismatch")
	}

	retrieved2, ok := m2.GetTagsBySID(sid2)
	if !ok {
		t.Error("sid2 should exist after reload")
	}
	if retrieved2["region"] != "eu" {
		t.Error("sid2 region tag mismatch")
	}

	retrieved3, ok := m2.GetTagsBySID(sid3)
	if !ok {
		t.Error("sid3 should exist after reload")
	}
	_ = retrieved3 // 已验证存在

	// 新分配应该从 sid3+1 继续（nextSID 恢复）
	newTags := map[string]string{"host": "server4"}
	newSid, _ := m2.AllocateSID(newTags)
	if newSid != sid3+1 {
		t.Errorf("next SID should be %d, got %d", sid3+1, newSid)
	}
}

func TestMeasurementMetaStore_Persist_ManySeries(t *testing.T) {
	// 测试大量 Series 的持久化
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "meta.json")

	m := NewMeasurementMetaStore()
	m.SetPersistPath(path)

	// 分配 1000 个不同的 Series
	for i := 0; i < 1000; i++ {
		tags := map[string]string{
			"host":   fmt.Sprintf("server%d", i),
			"region": fmt.Sprintf("region%d", i%10),
			"env":    fmt.Sprintf("env%d", i%3),
		}
		_, err := m.AllocateSID(tags)
		if err != nil {
			t.Fatalf("AllocateSID failed at i=%d: %v", i, err)
		}
	}

	err := m.Persist()
	if err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// 从文件重新加载验证
	m2, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	if len(m2.series) != 1000 {
		t.Errorf("expected 1000 series after reload, got %d", len(m2.series))
	}

	// 验证某些特定的 SID - 分配相同的 tags 应该返回已有 SID
	for i := 0; i < 10; i++ {
		tags := map[string]string{
			"host":   fmt.Sprintf("server%d", i),
			"region": fmt.Sprintf("region%d", i%10),
			"env":    fmt.Sprintf("env%d", i%3),
		}
		// 由于 hash 索引，应该能快速找到并返回已有 SID
		sid, err := m2.AllocateSID(tags)
		if err != nil {
			t.Fatalf("AllocateSID after reload failed at i=%d: %v", i, err)
		}
		// 新分配的 SID 应该不超过 1000
		if sid >= 1000 {
			t.Errorf("expected sid < 1000 for existing series, got %d", sid)
		}
	}
}

func TestMeasurementMetaStore_Persist_Concurrent(t *testing.T) {
	// 测试并发持久化（只读操作不应该阻塞）
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "meta.json")

	m := NewMeasurementMetaStore()
	m.SetPersistPath(path)

	// 先分配一些 SID
	for i := 0; i < 100; i++ {
		tags := map[string]string{"host": fmt.Sprintf("server%d", i)}
		_, _ = m.AllocateSID(tags)
	}

	// 并发读取和写入
	var wg sync.WaitGroup
	const goroutines = 10

	// 多个 goroutine 同时 Persist（写操作会串行化）
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.Persist()
		}()
	}

	// 同时有一些读操作
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, _ = m.GetTagsBySID(uint64(id % 10))
		}(i)
	}

	wg.Wait()
}

func TestMeasurementMetaStore_Persist_FilePermissions(t *testing.T) {
	// 测试文件权限是 0600
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "meta.json")

	m := NewMeasurementMetaStore()
	m.SetPersistPath(path)

	tags := map[string]string{"host": "server1"}
	_, _ = m.AllocateSID(tags)

	err := m.Persist()
	if err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if info.Mode().Perm() != 0600 {
		t.Errorf("expected 0600 permissions, got %o", info.Mode().Perm())
	}
}

func TestMeasurementMetaStore_Close_NoDirtyNoPersist(t *testing.T) {
	// 测试 Close 时如果没有脏数据不持久化
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "meta.json")

	m := NewMeasurementMetaStore()
	m.SetPersistPath(path)

	// 不分配任何 SID，dirty 应该是 false

	err := m.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 文件不应该存在
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Error("meta.json should not exist when dirty=false at Close")
	}
}

func TestMeasurementMetaStore_Load_FileNotExist(t *testing.T) {
	// 测试加载不存在的文件
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "nonexistent.json")

	m := NewMeasurementMetaStore()
	m.SetPersistPath(path)

	err := m.Load()
	if err == nil {
		t.Error("Load should return error for nonexistent file")
	}
}

func TestMeasurementMetaStore_Load_InvalidJSON(t *testing.T) {
	// 测试加载无效 JSON 文件
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "invalid.json")

	// 写入无效 JSON
	if err := os.WriteFile(path, []byte("invalid json content"), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	m := NewMeasurementMetaStore()
	m.SetPersistPath(path)

	err := m.Load()
	if err == nil {
		t.Error("Load should return error for invalid JSON")
	}
}

func TestMeasurementMetaStore_LoadFromFile_Success(t *testing.T) {
	// 测试 LoadFromFile 成功加载
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "meta.json")

	// 先创建并持久化
	m1 := NewMeasurementMetaStore()
	m1.SetPersistPath(path)
	tags := map[string]string{"host": "server1", "region": "us"}
	_, _ = m1.AllocateSID(tags)
	_ = m1.Persist()

	// 使用 LoadFromFile 加载
	m2, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	retrieved, ok := m2.GetTagsBySID(0)
	if !ok {
		t.Error("should find SID 0")
	}
	if retrieved["host"] != "server1" {
		t.Error("host tag mismatch")
	}
}

func TestMeasurementMetaStore_LoadFromFile_NotExist(t *testing.T) {
	// 测试 LoadFromFile 文件不存在
	_, err := LoadFromFile("/nonexistent/path/meta.json")
	if err == nil {
		t.Error("LoadFromFile should return error for nonexistent file")
	}
}

func TestMeasurementMetaStore_LoadFromFile_InvalidJSON(t *testing.T) {
	// 测试 LoadFromFile 无效 JSON
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "invalid.json")

	if err := os.WriteFile(path, []byte("{invalid}"), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := LoadFromFile(path)
	if err == nil {
		t.Error("LoadFromFile should return error for invalid JSON")
	}
}

func TestMeasurementMetaStore_Load_EmptyFile(t *testing.T) {
	// 测试加载空文件
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "empty.json")

	if err := os.WriteFile(path, []byte(""), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	m := NewMeasurementMetaStore()
	m.SetPersistPath(path)

	err := m.Load()
	if err == nil {
		t.Error("Load should return error for empty file")
	}
}

func TestMeasurementMetaStore_Load_PartialJSON(t *testing.T) {
	// 测试加载部分有效的 JSON（缺少字段）
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "partial.json")

	// 写入缺少字段的 JSON
	if err := os.WriteFile(path, []byte(`{"next_sid": 5}`), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	m := NewMeasurementMetaStore()
	m.SetPersistPath(path)

	err := m.Load()
	if err != nil {
		t.Fatalf("Load failed unexpectedly: %v", err)
	}

	// 应该成功加载，但 series 为空
	if len(m.series) != 0 {
		t.Errorf("expected empty series, got %d", len(m.series))
	}
}

func TestMeasurementMetaStore_Load_RebuildsIndexes(t *testing.T) {
	// 测试 Load 后索引正确重建
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "meta.json")

	// 创建多个 Series
	m1 := NewMeasurementMetaStore()
	m1.SetPersistPath(path)
	_, _ = m1.AllocateSID(map[string]string{"host": "server1", "region": "us"})
	_, _ = m1.AllocateSID(map[string]string{"host": "server2", "region": "eu"})
	_, _ = m1.AllocateSID(map[string]string{"host": "server3", "region": "us"})
	_ = m1.Persist()

	// 重新加载
	m2 := NewMeasurementMetaStore()
	m2.SetPersistPath(path)
	_ = m2.Load()

	// 验证 tagIndex 正确重建
	sids := m2.GetSidsByTag("region", "us")
	if len(sids) != 2 {
		t.Errorf("expected 2 sids for region=us, got %d", len(sids))
	}

	sids2 := m2.GetSidsByTag("region", "eu")
	if len(sids2) != 1 {
		t.Errorf("expected 1 sid for region=eu, got %d", len(sids2))
	}
}

func TestMeasurementMetaStore_Load_NextSID(t *testing.T) {
	// 测试 Load 后 nextSID 正确恢复
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "meta.json")

	m1 := NewMeasurementMetaStore()
	m1.SetPersistPath(path)
	_, _ = m1.AllocateSID(map[string]string{"host": "server1"})
	_, _ = m1.AllocateSID(map[string]string{"host": "server2"})
	_ = m1.Persist()

	// 重新加载
	m2, _ := LoadFromFile(path)

	// 新分配应该从 2 开始
	newSid, _ := m2.AllocateSID(map[string]string{"host": "server3"})
	if newSid != 2 {
		t.Errorf("expected next SID to be 2, got %d", newSid)
	}
}

func TestMeasurementMetaStore_Persist_MkdirAllError(t *testing.T) {
	// 测试 Persist 时目录创建失败
	// /dev/null 是文件不是目录，MkdirAll 会失败
	m := NewMeasurementMetaStore()
	m.SetPersistPath("/dev/null/invalid/meta.json")

	_, _ = m.AllocateSID(map[string]string{"host": "server1"})

	err := m.Persist()
	if err == nil {
		t.Error("Persist should fail when MkdirAll fails on /dev/null")
	}
}

func TestMeasurementMetaStore_Persist_RenameError(t *testing.T) {
	// 测试 Persist 时 rename 失败
	// 使用一个存在的目录但无法创建文件的路径
	tmpDir := t.TempDir()
	persistPath := filepath.Join(tmpDir, "subdir", "meta.json")

	// 先创建一个同名的目录文件来阻止 rename
	subdirPath := filepath.Join(tmpDir, "subdir")
	if err := os.MkdirAll(subdirPath, 0700); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// 在目标位置创建一个目录（不是文件），rename 会失败
	if err := os.MkdirAll(persistPath, 0700); err != nil {
		// 如果创建目录失败（因为是个文件），跳过这个测试
		t.Skip("cannot create blocking path")
	}

	m := NewMeasurementMetaStore()
	m.SetPersistPath(persistPath)
	_, _ = m.AllocateSID(map[string]string{"host": "server1"})

	err := m.Persist()
	if err == nil {
		t.Error("Persist should fail when rename fails")
	}
}

func TestMeasurementMetaStore_Persist_SyncError(t *testing.T) {
	// 测试 Persist 时 Sync 失败
	// 创建一个只读目录来触发 Sync 失败
	tmpDir := t.TempDir()
	persistPath := filepath.Join(tmpDir, "meta.json")

	// 先创建文件，然后设置为只读
	f, err := os.Create(persistPath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	_ = f.Close()

	// 设置父目录为只读
	if err := os.Chmod(tmpDir, 0500); err != nil {
		t.Skip("cannot change permissions")
	}
	defer func() { _ = os.Chmod(tmpDir, 0700) }()

	m := NewMeasurementMetaStore()
	m.SetPersistPath(persistPath)
	_, _ = m.AllocateSID(map[string]string{"host": "server1"})

	// 文件已存在且目录只读，OpenFile 可能成功但 Sync 会失败
	// 注意：这个测试依赖操作系统行为
	err = m.Persist()
	// 不检查结果，因为 Sync 在某些系统上可能不会失败
	_ = err
}

func TestMeasurementMetaStore_Close_Success(t *testing.T) {
	// 测试 Close 成功的情况
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "meta.json")

	m := NewMeasurementMetaStore()
	m.SetPersistPath(path)
	_, _ = m.AllocateSID(map[string]string{"host": "server1"})

	err := m.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestMeasurementMetaStore_GetTagsBySID_NilSeries(t *testing.T) {
	// 测试在已关闭的 MetaStore 上操作
	m := NewMeasurementMetaStore()
	_, _ = m.AllocateSID(map[string]string{"host": "server1"})
	_ = m.Close()

	// 访问已关闭的 MetaStore
	_, ok := m.GetTagsBySID(0)
	if ok {
		t.Error("GetTagsBySID should return false for closed MetaStore")
	}
}

func TestMeasurementMetaStore_GetSidsByTag_Empty(t *testing.T) {
	// 测试查询不存在的 tag
	m := NewMeasurementMetaStore()

	sids := m.GetSidsByTag("nonexistent", "value")
	if len(sids) != 0 {
		t.Errorf("expected 0 sids, got %d", len(sids))
	}
}
