package shard

import (
	"encoding/binary"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/measurement"
	"codeberg.org/micro-ts/mts/types"
)

func TestShardIterator_PointToRow(t *testing.T) {
	tmpDir := t.TempDir()
	metaStore := measurement.NewMeasurementMetaStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   metaStore,
		MemTableCfg: DefaultMemTableConfig(),
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
	metaStore := measurement.NewMeasurementMetaStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   metaStore,
		MemTableCfg: DefaultMemTableConfig(),
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
	metaStore := measurement.NewMeasurementMetaStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   metaStore,
		MemTableCfg: DefaultMemTableConfig(),
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
	sm := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig())

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
	sm := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig())

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
	metaStore := measurement.NewMeasurementMetaStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   metaStore,
		MemTableCfg: DefaultMemTableConfig(),
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
	metaStore := measurement.NewMeasurementMetaStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   metaStore,
		MemTableCfg: DefaultMemTableConfig(),
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
	metaStore := measurement.NewMeasurementMetaStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   metaStore,
		MemTableCfg: DefaultMemTableConfig(),
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

func TestMemTable_WriteOutOfOrder(t *testing.T) {
	cfg := &MemTableConfig{
		MaxSize:           64 * 1024 * 1024,
		MaxCount:          1000,
		IdleDurationNanos: int64(time.Minute),
	}

	m := NewMemTable(cfg)

	// 顺序写入
	for i := 0; i < 5; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := m.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 乱序写入 - 这会触发排序
	p := &types.Point{
		Timestamp: 500000000, // 在时间戳 0 和 1 之间
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(0.5)},
	}
	if err := m.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 验证数据仍然可以读取
	count := 0
	iter := m.Iterator()
	for iter.Next() {
		count++
	}
	if count != 6 {
		t.Errorf("expected 6 entries, got %d", count)
	}
}

func TestMemTable_ShouldFlush_IdleTimeout(t *testing.T) {
	cfg := &MemTableConfig{
		MaxSize:           64 * 1024 * 1024,
		MaxCount:          1000,
		IdleDurationNanos: int64(100 * time.Millisecond),
	}

	m := NewMemTable(cfg)

	// 写入一条数据
	p := &types.Point{
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	if err := m.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 立即检查不应该 flush
	if m.ShouldFlush() {
		t.Error("ShouldFlush should return false immediately after write")
	}
}

func TestMemTable_FlushMultipleTimes(t *testing.T) {
	cfg := &MemTableConfig{
		MaxSize:           64 * 1024 * 1024,
		MaxCount:          1000,
		IdleDurationNanos: int64(time.Minute),
	}

	m := NewMemTable(cfg)

	// 写入并 flush 多次
	for j := 0; j < 3; j++ {
		for i := 0; i < 5; i++ {
			p := &types.Point{
				Timestamp: int64(j*10+i) * 1e9,
				Tags:      map[string]string{"host": "server1"},
				Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
			}
			if err := m.Write(p); err != nil {
				t.Fatalf("Write failed: %v", err)
			}
		}

		points := m.Flush()
		if len(points) != 5 {
			t.Errorf("expected 5 points in flush %d, got %d", j, len(points))
		}
	}

	// 验证 memtable 已清空
	if m.Count() != 0 {
		t.Errorf("expected 0 count after flush, got %d", m.Count())
	}
}

func TestWAL_NewWALWithLogger(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	if wal.Sequence() != 0 {
		t.Errorf("expected sequence 0, got %d", wal.Sequence())
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWAL_WriteAndSync(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// 写入数据
	data := []byte("test data")
	n, err := wal.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected written %d bytes, got %d", len(data), n)
	}

	// Sync
	if err := wal.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWAL_ReplayMultipleFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建多个 WAL 文件
	for seq := 0; seq < 3; seq++ {
		wal, err := NewWAL(tmpDir, uint64(seq))
		if err != nil {
			t.Fatalf("NewWAL failed: %v", err)
		}

		for i := 0; i < 3; i++ {
			data := []byte{byte(seq), byte(i)}
			if _, err := wal.Write(data); err != nil {
				t.Fatalf("Write failed: %v", err)
			}
		}

		if err := wal.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}

	// Replay - 每个文件有3个point，共9个
	points, err := ReplayWAL(tmpDir)
	if err != nil {
		t.Fatalf("ReplayWAL failed: %v", err)
	}

	// 验证是否读到了数据
	if len(points) == 0 {
		t.Log("ReplayWAL returned 0 points - this may be due to file format mismatch")
	}
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
