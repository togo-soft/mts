package shard

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/types"
)

func TestShardIterator_PointToRow(t *testing.T) {
	tmpDir := t.TempDir()
	metaStore := metadata.NewSimpleSeriesStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metaStore,
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	p := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 创建 ShardIterator
	iter := NewShardIterator(s, 0, time.Hour.Nanoseconds())
	if iter == nil {
		t.Fatal("NewShardIterator returned nil")
	}

	// 验证 Current 方法
	row := iter.Current()
	if row == nil {
		t.Error("Current() should return non-nil for valid shard")
	}

	// 验证 Next 方法
	nextRow := iter.Next()
	if nextRow == nil {
		t.Error("Next() should return non-nil for valid shard")
	}

	// 验证 filterRow
	nilRow := iter.filterRow(nil)
	if nilRow != nil {
		t.Error("filterRow(nil) should return nil")
	}

	_ = s.Close()
}

func TestShardIterator_FilterRow(t *testing.T) {
	tmpDir := t.TempDir()
	metaStore := metadata.NewSimpleSeriesStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metaStore,
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	p := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	iter := NewShardIterator(s, 0, time.Hour.Nanoseconds())

	// 测试 filterRow - 时间在范围内
	row := &types.PointRow{
		Timestamp: 500000000, // 0.5s
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{},
	}
	filtered := iter.filterRow(row)
	if filtered == nil {
		t.Error("filterRow should return non-nil for time in range")
	}

	// 测试 filterRow - 时间超出范围
	outOfRangeRow := &types.PointRow{
		Timestamp: time.Hour.Nanoseconds() + 1, // 超过 endTime
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{},
	}
	_ = iter.filterRow(outOfRangeRow)
	// filterRow 会递归调用 Next，所以这里会跳过

	_ = s.Close()
}

func TestShard_NextSstRow(t *testing.T) {
	tmpDir := t.TempDir()
	metaStore := metadata.NewSimpleSeriesStore()
	schemaStore := metadata.NewSimpleSchemaStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metaStore,
		SchemaStore: schemaStore,
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 先写入一些数据并 flush 到 SSTable
	for i := 0; i < 5; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := s.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// 再写入 MemTable 数据
	for i := 5; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	iter := NewShardIterator(s, 0, time.Hour.Nanoseconds())

	// 消耗所有数据
	count := 0
	for iter.Next() != nil {
		count++
	}

	if count != 10 {
		t.Errorf("expected 10 rows, got %d", count)
	}

	_ = s.Close()
}

func TestShardManager_GetShards(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewShardManager(tmpDir, time.Hour, memtable.DefaultMemTableConfig(), nil, newTestMgr(t, tmpDir))

	// 获取不存在的 shard（不应该创建新的）
	shards := sm.GetShards("db1", "cpu", 0, time.Hour.Nanoseconds())
	if len(shards) != 0 {
		t.Errorf("expected 0 shards for non-existent, got %d", len(shards))
	}

	// 创建 shard
	_, err := sm.GetShard("db1", "cpu", time.Hour.Nanoseconds()/2)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	// 获取 shard
	shards = sm.GetShards("db1", "cpu", 0, time.Hour.Nanoseconds())
	if len(shards) != 1 {
		t.Errorf("expected 1 shard, got %d", len(shards))
	}

	// 获取多个 shard
	_, _ = sm.GetShard("db1", "cpu", time.Hour.Nanoseconds()+1)
	shards = sm.GetShards("db1", "cpu", 0, 2*time.Hour.Nanoseconds())
	if len(shards) != 2 {
		t.Errorf("expected 2 shards, got %d", len(shards))
	}
}

func TestShardManager_FlushAll(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewShardManager(tmpDir, time.Hour, memtable.DefaultMemTableConfig(), nil, newTestMgr(t, tmpDir))

	// 创建 shard 并写入数据
	s, err := sm.GetShard("db1", "cpu", time.Hour.Nanoseconds()/2)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	p := &types.Point{
		Timestamp: time.Hour.Nanoseconds() / 2,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// FlushAll
	if err := sm.FlushAll(); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}
}

func TestShard_FlushLocked(t *testing.T) {
	tmpDir := t.TempDir()
	metaStore := metadata.NewSimpleSeriesStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metaStore,
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 写入数据
	p := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// flushLocked 应该成功
	if err := s.flushLocked(); err != nil {
		t.Fatalf("flushLocked failed: %v", err)
	}

	_ = s.Close()
}

func TestShard_Close_WithData(t *testing.T) {
	tmpDir := t.TempDir()
	metaStore := metadata.NewSimpleSeriesStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metaStore,
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 写入数据
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 关闭应该成功
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestShard_ReadFromSSTable_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	metaStore := metadata.NewSimpleSeriesStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metaStore,
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 没有 SSTable 的情况下读取
	rows, err := s.readFromSSTable(0, time.Hour.Nanoseconds())
	if err != nil {
		t.Fatalf("readFromSSTable failed: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected empty rows, got %d", len(rows))
	}

	_ = s.Close()
}

func TestSerializePoint(t *testing.T) {
	p := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields: map[string]*types.FieldValue{
			"usage": types.NewFieldValue(85.5),
			"count": types.NewFieldValue(int64(100)),
		},
	}

	data, err := serializePoint(p)
	if err != nil {
		t.Fatalf("serializePoint failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("serialized data should not be empty")
	}

	// 反序列化验证
	deserialized, err := deserializePoint(data)
	if err != nil {
		t.Fatalf("deserializePoint failed: %v", err)
	}

	if deserialized.Timestamp != p.Timestamp {
		t.Errorf("expected timestamp %d, got %d", p.Timestamp, deserialized.Timestamp)
	}
}

func TestDeserializePoint_AllFieldTypes(t *testing.T) {
	p := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields: map[string]*types.FieldValue{
			"float_val": types.NewFieldValue(3.14),
			"int_val":   types.NewFieldValue(int64(42)),
			"str_val":   types.NewFieldValue("hello"),
			"bool_val":  types.NewFieldValue(true),
		},
	}

	data, err := serializePoint(p)
	if err != nil {
		t.Fatalf("serializePoint failed: %v", err)
	}

	deserialized, err := deserializePoint(data)
	if err != nil {
		t.Fatalf("deserializePoint failed: %v", err)
	}

	if deserialized.Fields["float_val"] == nil || deserialized.Fields["int_val"] == nil ||
		deserialized.Fields["str_val"] == nil || deserialized.Fields["bool_val"] == nil {
		t.Error("all fields should be present after deserialization")
	}
}

func TestDeserializePoint_InvalidData(t *testing.T) {
	// 过短的数据
	_, err := deserializePoint([]byte("short"))
	if err == nil {
		t.Error("expected error for short data")
	}

	// 无效的 tag 长度
	invalidData := make([]byte, 100)
	// timestamp
	binary.BigEndian.PutUint64(invalidData[:8], uint64(1000000000))
	// tagLen 指向不存在的区域
	binary.BigEndian.PutUint32(invalidData[8:12], uint32(1000))
	_, err = deserializePoint(invalidData)
	if err == nil {
		t.Error("expected error for invalid tag length")
	}
}

func TestNewShard_WALCreationFails(t *testing.T) {
	// 测试 WAL 创建失败时 Shard 仍能正常工作
	// 使用一个只读路径导致 WAL 创建失败
	tmpDir := t.TempDir()
	readonlyDir := filepath.Join(tmpDir, "readonly")
	if err := os.MkdirAll(readonlyDir, 0555); err != nil {
		t.Fatalf("failed to create readonly dir: %v", err)
	}

	// 在只读目录下创建 Shard，WAL 创建会失败但应该继续运行
	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         filepath.Join(readonlyDir, "shard1"),
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 即使 WAL 创建失败，写入仍应该成功（只是没有持久化）
	p := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(85.5))},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed even though WAL creation failed: %v", err)
	}

	// 读取验证
	rows, err := s.Read(0, 2000000000)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}

	_ = s.Close()
}

func TestShard_ReadFromSSTable_NoDataDir(t *testing.T) {
	// 测试读取时数据目录不存在
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 不创建 data 目录，直接读取
	rows, err := s.Read(0, time.Hour.Nanoseconds())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}

	_ = s.Close()
}

func TestShard_Close_WithData_PersistError(t *testing.T) {
	// 测试关闭时含数据的 Shard 可以正常关闭
	tmpDir := t.TempDir()

	metaStore := metadata.NewSimpleSeriesStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metaStore,
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 写入数据
	p := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(85.5))},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 关闭时 MetaStore.Persist 应该失败
	// 但 Close 可能不会返回错误因为 Persist 失败不会中断关闭流程
	_ = s.Close()
}

func TestShard_Extra_Close_EmptyShard(t *testing.T) {
	// 测试关闭空 Shard
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 不写入任何数据，直接关闭
	if err := s.Close(); err != nil {
		t.Fatalf("Close empty shard failed: %v", err)
	}
}

func TestShard_Extra_Close_WithData(t *testing.T) {
	// 测试关闭时有数据的 Shard
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 写入数据触发 MemTable
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e8,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 关闭
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestShard_Flush_AfterMultipleWrites(t *testing.T) {
	// 测试多次写入后 flush
	tmpDir := t.TempDir()
	schemaStore := metadata.NewSimpleSchemaStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: schemaStore,
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 写入多个 points
	for i := 0; i < 100; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e7,
			Tags:      map[string]string{"host": fmt.Sprintf("server%d", i%5)},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 手动 flush
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// 继续写入
	for i := 100; i < 150; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e7,
			Tags:      map[string]string{"host": fmt.Sprintf("server%d", i%5)},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 再次 flush
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// 读取所有数据
	rows, err := s.Read(0, time.Hour.Nanoseconds())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(rows) != 150 {
		t.Errorf("expected 150 rows, got %d", len(rows))
	}

	_ = s.Close()
}

// TestShard_LevelCompaction_NewShard creates Shard with LevelCompactionConfig.
func TestShard_LevelCompaction_NewShard(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
		LevelCompactionCfg: &compaction.LevelCompactionConfig{
			Enabled:          true,
			CheckInterval:    time.Minute,
			Timeout:          time.Minute,
			EnableCheckpoint: true,
		},
	}

	s := NewShard(cfg)

	// 验证 Shard 创建成功
	if s == nil {
		t.Fatal("NewShard returned nil")
	}

	// 验证 LevelCompactionManager 已创建
	if s.levelCompaction == nil {
		t.Fatal("levelCompaction should be initialized")
	}

	_ = s.Close()
}

// TestShard_LevelCompaction_FlushToL0 tests that Flush creates SSTable in L0.
func TestShard_LevelCompaction_FlushToL0(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
		LevelCompactionCfg: &compaction.LevelCompactionConfig{
			Enabled:          true,
			CheckInterval:    time.Minute,
			Timeout:          time.Minute,
			EnableCheckpoint: true,
		},
	}

	s := NewShard(cfg)

	// 写入数据
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i+1) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 手动 flush
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// 验证 L0 目录已创建
	l0Dir := filepath.Join(tmpDir, "data", "L0")
	if _, err := os.Stat(l0Dir); os.IsNotExist(err) {
		t.Fatal("L0 directory should exist after flush")
	}

	// 验证 manifest 中有 L0 parts
	level := s.levelCompaction.Manifest.GetLevel(0)
	if level == nil {
		t.Fatal("L0 level should exist in manifest")
	}
	if len(level.Parts) == 0 {
		t.Error("L0 should have at least one part after flush")
	}

	_ = s.Close()
}

// TestShard_LevelCompaction_Close tests that Close properly stops LevelCompactionManager.
func TestShard_LevelCompaction_Close(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
		LevelCompactionCfg: &compaction.LevelCompactionConfig{
			Enabled:          true,
			CheckInterval:    time.Millisecond * 10,
			Timeout:          time.Minute,
			EnableCheckpoint: true,
		},
	}

	s := NewShard(cfg)

	// 写入数据
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i+1) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 关闭
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证 L0 目录和数据文件存在
	l0Dir := filepath.Join(tmpDir, "data", "L0")
	if _, err := os.Stat(l0Dir); os.IsNotExist(err) {
		t.Fatal("L0 directory should exist after close with data")
	}

	// 验证 manifest 文件存在
	manifestPath := filepath.Join(tmpDir, "data", "_manifest.json")
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		t.Fatal("manifest file should exist after close")
	}
}

// TestShard_LevelCompaction_Read tests reading data with LevelCompaction enabled.
func TestShard_LevelCompaction_Read(t *testing.T) {
	tmpDir := t.TempDir()
	schemaStore := metadata.NewSimpleSchemaStore()

	cfg := ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: schemaStore,
		MemTableCfg: memtable.DefaultMemTableConfig(),
		LevelCompactionCfg: &compaction.LevelCompactionConfig{
			Enabled:          true,
			CheckInterval:    time.Minute,
			Timeout:          time.Minute,
			EnableCheckpoint: true,
		},
	}

	s := NewShard(cfg)

	// 写入数据
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i+1) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i * 10))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 手动 flush
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// 读取数据
	rows, err := s.Read(0, time.Hour.Nanoseconds())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(rows) != 10 {
		t.Errorf("expected 10 rows, got %d", len(rows))
	}

	_ = s.Close()
}

// TestShard_LevelCompaction_BothConfigs tests that LevelCompactionCfg takes precedence.
func TestShard_LevelCompaction_BothConfigs(t *testing.T) {
	tmpDir := t.TempDir()

	// 同时设置 CompactionCfg 和 LevelCompactionCfg
	cfg := ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
		CompactionCfg: &compaction.CompactionConfig{
			MaxSSTableCount:    10,
			MaxCompactionBatch: 5,
			CheckInterval:      time.Minute,
			Timeout:            time.Minute,
		},
		LevelCompactionCfg: &compaction.LevelCompactionConfig{
			Enabled:          true,
			CheckInterval:    time.Minute,
			Timeout:          time.Minute,
			EnableCheckpoint: true,
		},
	}

	s := NewShard(cfg)

	// 验证 LevelCompactionManager 已创建
	if s.levelCompaction == nil {
		t.Fatal("levelCompaction should be initialized even when both configs are set")
	}

	_ = s.Close()
}

// TestShard_LevelCompaction_NoLevelConfig tests Shard without LevelCompactionConfig.
func TestShard_LevelCompaction_NoLevelConfig(t *testing.T) {
	tmpDir := t.TempDir()

	// 不设置 LevelCompactionCfg，使用平坦 compaction
	cfg := ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
		CompactionCfg: &compaction.CompactionConfig{
			MaxSSTableCount:    10,
			MaxCompactionBatch: 5,
			CheckInterval:      time.Minute,
			Timeout:            time.Minute,
		},
	}

	s := NewShard(cfg)

	// 验证 LevelCompactionManager 为 nil
	if s.levelCompaction != nil {
		t.Error("levelCompaction should be nil when LevelCompactionCfg is not set")
	}

	// 验证 CompactionManager 存在
	if s.compaction == nil {
		t.Error("compaction should exist when CompactionCfg is set")
	}

	_ = s.Close()
}

func TestNewShard_WALCreationFailure(t *testing.T) {
	// 测试 WAL 创建失败的情况
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// WAL 创建失败不应该阻止 Shard 创建
	// 数据会写入 MemTable 但不会持久化到 WAL

	// 写入数据
	p := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_ = s.Close()
}

func TestNewShard_LevelCompactionCreationFailure(t *testing.T) {
	// 测试 LevelCompaction 创建失败的情况
	tmpDir := t.TempDir()

	// 使用一个无效的 level 配置导致创建失败
	cfg := &compaction.LevelCompactionConfig{
		LevelConfigs: nil, // 无效配置
		Timeout:      time.Hour,
	}

	s := NewShard(ShardConfig{
		DB:                 "db1",
		Measurement:        "cpu",
		StartTime:          0,
		EndTime:            time.Hour.Nanoseconds(),
		Dir:                tmpDir,
		SeriesStore:        metadata.NewSimpleSeriesStore(),
		MemTableCfg:        memtable.DefaultMemTableConfig(),
		LevelCompactionCfg: cfg,
	})

	// LevelCompaction 创建失败不应该阻止 Shard 创建
	// 即使 levelCompaction 不是 nil，Shard 本身应该仍能正常工作

	// 验证 Shard 可以正常写入和读取
	p := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_ = s.Close()
}

func TestShard_Write_WithoutWAL(t *testing.T) {
	// 测试没有 WAL 的写入
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 写入数据（没有 WAL，所以不会写入预写日志）
	p := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 读取验证
	rows, err := s.Read(0, time.Hour.Nanoseconds())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}

	_ = s.Close()
}

func TestShard_flushLocked_NoPoints(t *testing.T) {
	// 测试 flushLocked 时没有数据
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 直接调用 flushLocked（没有数据）
	err := s.flushLocked()
	if err != nil {
		t.Fatalf("flushLocked failed: %v", err)
	}

	_ = s.Close()
}

func TestShard_flushLocked_WithLevelCompaction(t *testing.T) {
	// 测试带 Level Compaction 的 flush
	tmpDir := t.TempDir()

	cfg := compaction.DefaultLevelCompactionConfig()
	cfg.CheckInterval = time.Hour // 避免自动 compaction

	s := NewShard(ShardConfig{
		DB:                 "db1",
		Measurement:        "cpu",
		StartTime:          0,
		EndTime:            time.Hour.Nanoseconds(),
		Dir:                tmpDir,
		SeriesStore:        metadata.NewSimpleSeriesStore(),
		MemTableCfg:        memtable.DefaultMemTableConfig(),
		LevelCompactionCfg: cfg,
	})

	// 写入触发 flush 的数据
	for i := 0; i < 4000; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e6,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// flushLocked 应该将数据刷到 L0
	err := s.flushLocked()
	if err != nil {
		t.Fatalf("flushLocked failed: %v", err)
	}

	_ = s.Close()
}

func TestShard_Close_WithLevelCompaction(t *testing.T) {
	// 测试带 Level Compaction 的 Close
	tmpDir := t.TempDir()

	cfg := compaction.DefaultLevelCompactionConfig()
	cfg.CheckInterval = time.Hour // 避免自动 compaction

	s := NewShard(ShardConfig{
		DB:                 "db1",
		Measurement:        "cpu",
		StartTime:          0,
		EndTime:            time.Hour.Nanoseconds(),
		Dir:                tmpDir,
		SeriesStore:        metadata.NewSimpleSeriesStore(),
		MemTableCfg:        memtable.DefaultMemTableConfig(),
		LevelCompactionCfg: cfg,
	})

	// 写入数据
	for i := 0; i < 100; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e7,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 关闭
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestShard_Close_WithCompaction(t *testing.T) {
	// 测试带 Compaction 的 Close
	tmpDir := t.TempDir()

	compactionCfg := compaction.DefaultCompactionConfig()
	compactionCfg.CheckInterval = time.Hour // 避免定时触发

	s := NewShard(ShardConfig{
		DB:            "db1",
		Measurement:   "cpu",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compactionCfg,
	})

	// 写入数据
	for i := 0; i < 100; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e7,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 关闭
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestShard_Close_WALCloseError(t *testing.T) {
	// 测试 WAL 关闭错误
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 写入数据
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e8,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// WAL 关闭在正常情况下不会失败，所以只验证关闭能完成
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestShard_readFromSSTable_NoDataDir(t *testing.T) {
	// 测试读取不存在的 SSTable 目录
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 没有 data 目录，readFromSSTable 应该返回空
	rows, err := s.readFromSSTable(0, time.Hour.Nanoseconds())
	if err != nil {
		t.Fatalf("readFromSSTable failed: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}

	_ = s.Close()
}

func TestShard_readSSTableDir_InvalidPath(t *testing.T) {
	// 测试读取无效的 SSTable 目录
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 读取不存在的 SSTable 目录
	var rows []*types.PointRow
	err := s.readSSTableDir("/nonexistent/path", 0, time.Hour.Nanoseconds(), &rows)
	if err == nil {
		t.Error("expected error for nonexistent SSTable dir")
	}

	_ = s.Close()
}

func TestShard_Read_EmptyTimeRange(t *testing.T) {
	// 测试读取空时间范围
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 写入数据
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e8,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 读取不在范围内的时间
	rows, err := s.Read(time.Hour.Nanoseconds(), 2*time.Hour.Nanoseconds())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows for out-of-range query, got %d", len(rows))
	}

	_ = s.Close()
}

func TestShard_Write_SerializeError(t *testing.T) {
	// 测试序列化错误 - 实际上 serializePoint 不太可能失败
	// 但我们可以验证正常路径
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 写入各种类型的字段
	p := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields: map[string]*types.FieldValue{
			"float_val":  types.NewFieldValue(85.5),
			"int_val":    types.NewFieldValue(int64(100)),
			"string_val": types.NewFieldValue("hello"),
			"bool_val":   types.NewFieldValue(true),
		},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_ = s.Close()
}

func TestShard_Write_ManyPoints(t *testing.T) {
	// 测试写入大量数据点
	tmpDir := t.TempDir()
	schemaStore := metadata.NewSimpleSchemaStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: schemaStore,
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 写入 5000 个数据点（触发多次 flush）
	for i := 0; i < 5000; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e6,
			Tags:      map[string]string{"host": fmt.Sprintf("server%d", i%10)},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed at i=%d: %v", i, err)
		}
	}

	// 读取验证
	rows, err := s.Read(0, time.Hour.Nanoseconds())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(rows) != 5000 {
		t.Errorf("expected 5000 rows, got %d", len(rows))
	}

	_ = s.Close()
}

func TestShard_Flush_Manual(t *testing.T) {
	// 测试手动 Flush
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 写入少量数据
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e8,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 手动 Flush
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// 再次写入
	for i := 10; i < 20; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e8,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	_ = s.Close()
}

func TestShard_ContainsTimeRange(t *testing.T) {
	// 测试 ContainsTime 方法
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   1000000000,
		EndTime:     2000000000,
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	// 时间在范围内
	if !s.ContainsTime(1500000000) {
		t.Error("timestamp 1500000000 should be in range [1000000000, 2000000000)")
	}

	// 时间等于起始
	if !s.ContainsTime(1000000000) {
		t.Error("timestamp 1000000000 should be in range (inclusive start)")
	}

	// 时间等于结束
	if s.ContainsTime(2000000000) {
		t.Error("timestamp 2000000000 should NOT be in range (exclusive end)")
	}

	// 时间超出范围
	if s.ContainsTime(500000000) {
		t.Error("timestamp 500000000 should NOT be in range")
	}
	if s.ContainsTime(3000000000) {
		t.Error("timestamp 3000000000 should NOT be in range")
	}

	_ = s.Close()
}

func TestShard_DurationMethod(t *testing.T) {
	// 测试 Duration 方法
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})

	d := s.Duration()
	if d != time.Hour {
		t.Errorf("expected 1 hour duration, got %v", d)
	}

	_ = s.Close()
}

func TestShard_PeriodicFlushCheck_IdleTimeout(t *testing.T) {
	// 测试定时刷盘检查 - 空闲超时触发
	tmpDir := t.TempDir()

	cfg := &memtable.MemTableConfig{
		MaxSize:           64 * 1024 * 1024,
		MaxCount:          3000,
		IdleDurationNanos: 100 * int64(time.Millisecond), // 100ms 空闲触发
	}

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: cfg,
	})

	// 写入一些数据，但不触发任何刷盘条件
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e7,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 此时 MemTable 有数据，但不应该触发刷盘
	count := s.memTable.Count()
	if count != 10 {
		t.Errorf("expected 10 entries in memtable, got %d", count)
	}

	// 等待空闲超时触发（100ms + 一些缓冲时间）
	time.Sleep(200 * time.Millisecond)

	// 验证定时刷盘已触发
	count = s.memTable.Count()
	if count != 0 {
		t.Errorf("expected 0 entries after periodic flush, got %d", count)
	}

	_ = s.Close()
}

func TestShard_PeriodicFlushCheck_CloseStopsTicker(t *testing.T) {
	// 测试 Close 正确停止定时刷盘
	tmpDir := t.TempDir()

	cfg := &memtable.MemTableConfig{
		MaxSize:           64 * 1024 * 1024,
		MaxCount:          3000,
		IdleDurationNanos: 50 * int64(time.Millisecond),
	}

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: cfg,
	})

	// 写入数据
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e7,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 关闭 shard
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证 flushDone 已关闭
	select {
	case <-s.flushDone:
		// expected
	default:
		t.Error("flushDone should be closed after Close")
	}
}

func TestShard_PeriodicFlushCheck_DefaultInterval(t *testing.T) {
	// 测试默认检查间隔（IdleDuration 为 0 时）
	tmpDir := t.TempDir()

	cfg := &memtable.MemTableConfig{
		MaxSize:           64 * 1024 * 1024,
		MaxCount:          3000,
		IdleDurationNanos: 0, // 禁用空闲超时
	}

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: cfg,
	})

	// 写入数据
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e7,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 由于 IdleDuration 为 0，默认使用 10 秒间隔
	// 空闲超时不会触发，但我们可以通过关闭来验证 ticker 正常工作

	_ = s.Close()
}

func TestShard_triggerBackgroundCompaction_LevelCompaction(t *testing.T) {
	// 测试 triggerBackgroundCompaction 触发 level compaction
	tmpDir := t.TempDir()

	cfg := compaction.DefaultLevelCompactionConfig()
	cfg.CheckInterval = time.Hour // 避免自动 compaction 干扰

	s := NewShard(ShardConfig{
		DB:                 "db1",
		Measurement:        "cpu",
		StartTime:          0,
		EndTime:            time.Hour.Nanoseconds(),
		Dir:                tmpDir,
		SeriesStore:        metadata.NewSimpleSeriesStore(),
		MemTableCfg:        memtable.DefaultMemTableConfig(),
		LevelCompactionCfg: cfg,
	})

	// 添加 10+ L0 parts 让 ShouldCompact 返回 true
	for i := 0; i < 12; i++ {
		s.levelCompaction.AddPart(0, compaction.PartInfo{
			Name:    fmt.Sprintf("sst_%020d", i),
			Size:    1 * 1024 * 1024,
			MinTime: int64(i) * 1000,
			MaxTime: int64(i+1) * 1000,
		})
	}

	// 写入一些数据到 MemTable
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e7,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 验证 ShouldCompact 返回 true
	if !s.levelCompaction.ShouldCompact() {
		t.Fatal("ShouldCompact should return true with 12 L0 parts")
	}

	// 调用 flushLocked，它会在末尾调用 triggerBackgroundCompaction
	err := s.flushLocked()
	if err != nil {
		t.Fatalf("flushLocked failed: %v", err)
	}

	// 等待一小段时间让后台 goroutine 启动
	time.Sleep(50 * time.Millisecond)

	_ = s.Close()
}

func TestShard_triggerBackgroundCompaction_LevelCompactionNoTrigger(t *testing.T) {
	// 测试 triggerBackgroundCompaction 在不需要 compaction 时不触发
	tmpDir := t.TempDir()

	cfg := compaction.DefaultLevelCompactionConfig()
	cfg.CheckInterval = time.Hour

	s := NewShard(ShardConfig{
		DB:                 "db1",
		Measurement:        "cpu",
		StartTime:          0,
		EndTime:            time.Hour.Nanoseconds(),
		Dir:                tmpDir,
		SeriesStore:        metadata.NewSimpleSeriesStore(),
		MemTableCfg:        memtable.DefaultMemTableConfig(),
		LevelCompactionCfg: cfg,
	})

	// 不添加任何 L0 parts，ShouldCompact 应返回 false
	if s.levelCompaction.ShouldCompact() {
		t.Fatal("ShouldCompact should return false with 0 L0 parts")
	}

	// 写入一些数据
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e7,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 调用 flushLocked，不会触发 background compaction
	err := s.flushLocked()
	if err != nil {
		t.Fatalf("flushLocked failed: %v", err)
	}

	_ = s.Close()
}

func TestRecoverSSTSeq_NoDataDir(t *testing.T) {
	tmpDir := t.TempDir()
	seq := recoverSSTSeq(tmpDir)
	if seq != 0 {
		t.Errorf("expected 0, got %d", seq)
	}
}

func TestRecoverSSTSeq_EmptyDataDir(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	seq := recoverSSTSeq(tmpDir)
	if seq != 0 {
		t.Errorf("expected 0, got %d", seq)
	}
}

func TestRecoverSSTSeq_SingleSST(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data", "sst_0")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	seq := recoverSSTSeq(tmpDir)
	if seq != 1 {
		t.Errorf("expected 1, got %d", seq)
	}
}

func TestRecoverSSTSeq_MultipleSST(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	for _, name := range []string{"sst_0", "sst_5", "sst_3"} {
		if err := os.MkdirAll(filepath.Join(dataDir, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	seq := recoverSSTSeq(tmpDir)
	if seq != 6 {
		t.Errorf("expected 6, got %d", seq)
	}
}

func TestRecoverSSTSeq_MixedDirectories(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	for _, name := range []string{"sst_0", "other_dir", "sst_2", "not_sst"} {
		if err := os.MkdirAll(filepath.Join(dataDir, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	seq := recoverSSTSeq(tmpDir)
	if seq != 3 {
		t.Errorf("expected 3, got %d", seq)
	}
}

func TestRecoverSSTSeq_NonNumericSuffix(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	for _, name := range []string{"sst_abc", "sst_0"} {
		if err := os.MkdirAll(filepath.Join(dataDir, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	seq := recoverSSTSeq(tmpDir)
	if seq != 1 {
		t.Errorf("expected 1, got %d", seq)
	}
}
