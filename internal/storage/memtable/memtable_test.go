package memtable

import (
	"sync"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/types"
)

func TestMemTable_Write(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	p := &types.Point{
		Measurement: "cpu",
		Timestamp:   time.Now().UnixNano(),
		Tags:        map[string]string{"host": "server1"},
		Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}

	if err := m.Write(types.PointToInternal(p, 0)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if m.Count() != 1 {
		t.Errorf("expected count 1, got %d", m.Count())
	}
}

func TestMemTable_SortKey(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	now := time.Now().UnixNano()
	p1 := &types.Point{Measurement: "cpu", Timestamp: now + 100}
	p2 := &types.Point{Measurement: "cpu", Timestamp: now}
	p3 := &types.Point{Measurement: "cpu", Timestamp: now + 200}

	if err := m.Write(types.PointToInternal(p2, 0)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := m.Write(types.PointToInternal(p1, 0)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := m.Write(types.PointToInternal(p3, 0)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// entries is unexported, verify via iterator
	it := m.Iterator()
	if !it.Next() {
		t.Fatal("expected first entry")
	}
	if it.Point().Timestamp != now {
		t.Errorf("expected first timestamp %d, got %d", now, it.Point().Timestamp)
	}
	if !it.Next() {
		t.Fatal("expected second entry")
	}
	if it.Point().Timestamp != now+100 {
		t.Errorf("expected second timestamp %d, got %d", now+100, it.Point().Timestamp)
	}
	if !it.Next() {
		t.Fatal("expected third entry")
	}
	if it.Point().Timestamp != now+200 {
		t.Errorf("expected third timestamp %d, got %d", now+200, it.Point().Timestamp)
	}
}

func TestMemTable_ShouldFlush(t *testing.T) {
	cfg := &MemTableConfig{MaxSize: 100, MaxCount: 0, IdleDurationNanos: 0}
	m := NewMemTable(cfg)

	p := &types.Point{
		Measurement: "cpu",
		Timestamp:   time.Now().UnixNano(),
		Tags:        map[string]string{"host": "server1"},
		Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}

	for !m.ShouldFlush() {
		if err := m.Write(types.PointToInternal(p, 0)); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if !m.ShouldFlush() {
		t.Error("expected ShouldFlush to return true")
	}
}

func TestMemTable_WriteOutOfOrder(t *testing.T) {
	cfg := &MemTableConfig{
		MaxSize:           64 * 1024 * 1024,
		MaxCount:          1000,
		IdleDurationNanos: int64(time.Minute),
	}

	m := NewMemTable(cfg)

	for i := 0; i < 5; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := m.Write(types.PointToInternal(p, 0)); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 乱序写入
	p := &types.Point{
		Timestamp: 500000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(0.5)},
	}
	if err := m.Write(types.PointToInternal(p, 0)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

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

	p := &types.Point{
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	if err := m.Write(types.PointToInternal(p, 0)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

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

	for j := 0; j < 3; j++ {
		for i := 0; i < 5; i++ {
			p := &types.Point{
				Timestamp: int64(j*10+i) * 1e9,
				Tags:      map[string]string{"host": "server1"},
				Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
			}
			if err := m.Write(types.PointToInternal(p, 0)); err != nil {
				t.Fatalf("Write failed: %v", err)
			}
		}

		points := m.Flush()
		if len(points) != 5 {
			t.Errorf("expected 5 points in flush %d, got %d", j, len(points))
		}
		// 模拟 flush 成功，清理 passive
		m.ClearPassive()
	}

	if m.Count() != 0 {
		t.Errorf("expected 0 count after flush, got %d", m.Count())
	}
}

func TestMemTable_IdleTimeout(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	idleTimeout := m.IdleTimeout()
	if idleTimeout != time.Minute {
		t.Errorf("expected IdleTimeout %v, got %v", time.Minute, idleTimeout)
	}
}

func TestMemTable_IteratorEmpty(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	iter := m.Iterator()
	if iter.Next() {
		t.Error("empty iterator should return false")
	}
}

// =============================================================================
// Active/Passive 双缓冲测试
// =============================================================================

func TestMemTable_Swap(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	now := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		p := &types.Point{
			Timestamp: now + int64(i)*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	passive := m.Swap()
	if len(passive) != 5 {
		t.Errorf("expected 5 points in passive, got %d", len(passive))
	}
	if m.Count() != 0 {
		t.Errorf("expected active count 0 after swap, got %d", m.Count())
	}
	if !m.IsFlushing() {
		t.Error("expected flushing=true after swap")
	}
}

func TestMemTable_SwapThenWrite(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	now := time.Now().UnixNano()
	for i := 0; i < 3; i++ {
		p := &types.Point{
			Timestamp: now + int64(i)*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	_ = m.Swap()

	// Swap 后继续写入新数据
	for i := 0; i < 3; i++ {
		p := &types.Point{
			Timestamp: now + int64(i+3)*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i + 3))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	if m.Count() != 3 {
		t.Errorf("expected 3 points in new active after swap+write, got %d", m.Count())
	}

	// Iterator 应返回 active + passive 合并结果（6 points）
	iter := m.Iterator()
	count := 0
	for iter.Next() {
		count++
	}
	if count != 6 {
		t.Errorf("expected 6 points from iterator, got %d", count)
	}
}

func TestMemTable_ClearPassive(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	now := time.Now().UnixNano()
	for i := 0; i < 3; i++ {
		p := &types.Point{
			Timestamp: now + int64(i)*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	_ = m.Swap()
	m.ClearPassive()

	if m.IsFlushing() {
		t.Error("expected flushing=false after ClearPassive")
	}

	// Iterator 应只返回 0 条数据（passive 已清空，active 为空）
	iter := m.Iterator()
	if iter.Next() {
		t.Error("expected empty iterator after ClearPassive on empty active")
	}
}

func TestMemTable_IteratorMergesActivePassive(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	now := time.Now().UnixNano()

	// 写入 active: t=0, t=2, t=4
	for _, ts := range []int64{0, 2, 4} {
		p := &types.Point{
			Timestamp: now + ts*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(ts))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	// Swap → passive = {t=0, t=2, t=4}
	_ = m.Swap()

	// 写入新 active: t=1, t=3, t=5
	for _, ts := range []int64{1, 3, 5} {
		p := &types.Point{
			Timestamp: now + ts*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(ts))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	// Iterator 归并: t=0,1,2,3,4,5
	iter := m.Iterator()
	var timestamps []int64
	for iter.Next() {
		timestamps = append(timestamps, iter.Point().Timestamp)
	}

	if len(timestamps) != 6 {
		t.Fatalf("expected 6 merged points, got %d", len(timestamps))
	}
	for i := 0; i < 6; i++ {
		expected := now + int64(i)*1e9
		if timestamps[i] != expected {
			t.Errorf("position %d: expected timestamp %d, got %d", i, expected, timestamps[i])
		}
	}
}

func TestMemTable_MergePassiveBack(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	now := time.Now().UnixNano()

	// 写入 active: t=3, t=1, t=5
	for _, ts := range []int64{3, 1, 5} {
		p := &types.Point{
			Timestamp: now + ts*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(ts))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	// Swap
	passive := m.Swap()
	_ = passive

	// 写入新 active: t=4, t=2
	for _, ts := range []int64{4, 2} {
		p := &types.Point{
			Timestamp: now + ts*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(ts))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	// 模拟 flush 失败：合并 passive 回 active
	m.MergePassiveBack()

	if m.IsFlushing() {
		t.Error("expected flushing=false after MergePassiveBack")
	}
	if m.Count() != 5 {
		t.Errorf("expected 5 points after merge, got %d", m.Count())
	}

	// Iterator 应按时间排序返回所有 5 个点
	iter := m.Iterator()
	var timestamps []int64
	for iter.Next() {
		timestamps = append(timestamps, iter.Point().Timestamp)
	}
	if len(timestamps) != 5 {
		t.Fatalf("expected 5 merged points, got %d", len(timestamps))
	}
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i] < timestamps[i-1] {
			t.Errorf("timestamps not sorted at position %d: %d < %d", i, timestamps[i], timestamps[i-1])
		}
	}
}

func TestMemTable_TrySetFlushing(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	// 第一次 CAS 成功
	if !m.TrySetFlushing() {
		t.Error("first TrySetFlushing should succeed")
	}

	// 第二次 CAS 失败（已 flushing）
	if m.TrySetFlushing() {
		t.Error("second TrySetFlushing should fail")
	}

	// 清除后 CAS 再次成功
	m.ClearFlushing()
	if !m.TrySetFlushing() {
		t.Error("TrySetFlushing after ClearFlushing should succeed")
	}
}

func TestMemTable_ActiveFull(t *testing.T) {
	cfg := &MemTableConfig{
		MaxSize:           64 * 1024 * 1024, // 足够大，仅通过 MaxCount 触发
		MaxCount:          10,
		IdleDurationNanos: 0,
	}
	m := NewMemTable(cfg)

	// 初始不超限
	if m.ActiveFull() {
		t.Error("empty memtable should not be ActiveFull")
	}

	// 写入少量数据，不超过 2x 阈值
	now := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		p := &types.Point{
			Timestamp: now + int64(i)*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}
	if m.ActiveFull() {
		t.Error("5 points should not trigger ActiveFull with MaxCount=10")
	}

	// 写入超过 2x MaxCount
	for i := 0; i < 20; i++ {
		p := &types.Point{
			Timestamp: now + int64(i+5)*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}
	if !m.ActiveFull() {
		t.Error("25 points should trigger ActiveFull with MaxCount=10")
	}
}

func TestMemTable_ConcurrentWriteSwap(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())
	now := time.Now().UnixNano()

	const goroutines = 10
	const writesPerGoroutine = 50
	var wg sync.WaitGroup

	// 并发写入
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				p := &types.Point{
					Timestamp: now + int64(gid*writesPerGoroutine+i)*1e3,
					Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
				}
				_ = m.Write(types.PointToInternal(p, 0))
			}
		}(g)
	}

	// 并发 swap + clear
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			passive := m.Swap()
			_ = passive
			time.Sleep(time.Microsecond)
			m.ClearPassive()
		}()
	}

	wg.Wait()

	// 验证数据完整性：iterator 返回的结果应有序且无重复
	iter := m.Iterator()
	count := 0
	var lastTs int64
	for iter.Next() {
		count++
		ts := iter.Point().Timestamp
		if count > 1 && ts < lastTs {
			t.Errorf("iterator not sorted at position %d: %d < %d", count, ts, lastTs)
		}
		lastTs = ts
	}
	if count == 0 {
		t.Error("expected some data after concurrent writes")
	}
}

func TestMemTable_SwapDoubleSwapDataSafety(t *testing.T) {
	// 测试连续两次 Swap（不调用 ClearPassive）不会丢失数据
	m := NewMemTable(DefaultMemTableConfig())

	now := time.Now().UnixNano()
	for i := 0; i < 3; i++ {
		p := &types.Point{
			Timestamp: now + int64(i)*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	// 第一次 Swap: active(3) → passive(3)
	passive1 := m.Swap()
	if len(passive1) != 3 {
		t.Fatalf("expected 3 in first passive, got %d", len(passive1))
	}

	// 不调用 ClearPassive，再写入新数据到 active
	for i := 0; i < 2; i++ {
		p := &types.Point{
			Timestamp: now + int64(i+3)*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i + 3))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	// 第二次 Swap: 旧 passive(3) 合并回 active → active(5) → passive(5)
	passive2 := m.Swap()
	if len(passive2) != 5 {
		t.Errorf("expected 5 in second passive (3 merged + 2 new), got %d", len(passive2))
	}
}

func TestMemTable_ShouldSwap_WhenFlushing(t *testing.T) {
	m := NewMemTable(&MemTableConfig{
		MaxSize:           100,
		MaxCount:          5,
		IdleDurationNanos: 0,
	})

	// 写入超过阈值的数据
	now := time.Now().UnixNano()
	for i := 0; i < 6; i++ {
		p := &types.Point{
			Timestamp: now + int64(i)*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	// 确认 ShouldSwap 返回 true
	if !m.ShouldSwap() {
		t.Fatal("ShouldSwap should return true when over threshold")
	}

	// Swap 后 flushing=true
	_ = m.Swap()

	// flushing 期间 ShouldSwap 应返回 false
	if m.ShouldSwap() {
		t.Error("ShouldSwap should return false while flushing")
	}

	// ClearPassive 后 ShouldSwap 返回 false（active 为空）
	m.ClearPassive()
	if m.ShouldSwap() {
		t.Error("ShouldSwap should return false with empty active")
	}
}

func TestMemTable_ActiveCount(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	if m.ActiveCount() != 0 {
		t.Errorf("expected ActiveCount 0, got %d", m.ActiveCount())
	}

	now := time.Now().UnixNano()
	for i := 0; i < 3; i++ {
		p := &types.Point{
			Timestamp: now + int64(i)*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	if m.ActiveCount() != 3 {
		t.Errorf("expected ActiveCount 3, got %d", m.ActiveCount())
	}
}

func TestMemTable_MergePassiveBack_EmptyPassive(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	// MergePassiveBack with no passive should not panic
	m.MergePassiveBack()

	if m.IsFlushing() {
		t.Error("expected flushing=false after MergePassiveBack with empty passive")
	}
}

func TestMemTable_Iterator_OnlyPassive(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	now := time.Now().UnixNano()
	for i := 0; i < 3; i++ {
		p := &types.Point{
			Timestamp: now + int64(i)*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	_ = m.Swap()

	iter := m.Iterator()
	count := 0
	for iter.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 from iterator (only passive), got %d", count)
	}
}

func TestMemTable_Iterator_PointWithoutCurrent(t *testing.T) {
	iter := &MemTableIterator{}
	pt := iter.Point()
	if pt.Timestamp != 0 {
		t.Error("Point() without Next() should return zero InternalPoint")
	}
}

// =============================================================================
// sorted 标志与有序性加固测试
// =============================================================================

func TestMemTable_SortMethod(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	now := time.Now().UnixNano()

	// 乱序写入：t=5, t=1, t=3, t=2, t=4
	for _, ts := range []int64{5, 1, 3, 2, 4} {
		p := &types.Point{
			Timestamp: now + ts*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(ts))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	// Write 内部应已排序，但显式调用 Sort 应幂等
	m.Sort()

	iter := m.Iterator()
	var timestamps []int64
	for iter.Next() {
		timestamps = append(timestamps, iter.Point().Timestamp)
	}
	if len(timestamps) != 5 {
		t.Fatalf("expected 5 points, got %d", len(timestamps))
	}
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i] < timestamps[i-1] {
			t.Errorf("timestamps not sorted at position %d: %d < %d", i, timestamps[i], timestamps[i-1])
		}
	}
}

func TestMemTable_SortMethod_Empty(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	// 空 MemTable 调用 Sort 不应 panic
	m.Sort()

	if m.Count() != 0 {
		t.Errorf("expected count 0 after Sort on empty, got %d", m.Count())
	}
}

func TestMemTable_SortMethod_SingleElement(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	now := time.Now().UnixNano()
	p := &types.Point{
		Timestamp: now,
		Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)},
	}
	_ = m.Write(types.PointToInternal(p, 0))

	// 单元素 Sort 不应 panic
	m.Sort()

	if m.Count() != 1 {
		t.Errorf("expected count 1, got %d", m.Count())
	}
}

func TestMemTable_SortAfterSwap(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	now := time.Now().UnixNano()

	// 写入第一批数据并 Swap + ClearPassive
	for i := 0; i < 3; i++ {
		p := &types.Point{
			Timestamp: now + int64(i)*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}
	_ = m.Swap()
	m.ClearPassive()

	// Swap 后 sorted=false，写入乱序数据应触发排序
	for _, ts := range []int64{5, 2, 8} {
		p := &types.Point{
			Timestamp: now + ts*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(ts))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}

	// 验证 active 有序（passive 已清空，仅 active 有数据）
	iter := m.Iterator()
	var timestamps []int64
	for iter.Next() {
		timestamps = append(timestamps, iter.Point().Timestamp)
	}
	if len(timestamps) != 3 {
		t.Fatalf("expected 3 points, got %d", len(timestamps))
	}
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i] < timestamps[i-1] {
			t.Errorf("timestamps not sorted at position %d: %d < %d", i, timestamps[i], timestamps[i-1])
		}
	}
}

func TestMemTable_WriteUnsortedActive(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	now := time.Now().UnixNano()

	// 正常写入后 Swap + ClearPassive，sorted 被重置为 false
	for i := 0; i < 3; i++ {
		p := &types.Point{
			Timestamp: now + int64(i)*1e9,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
		_ = m.Write(types.PointToInternal(p, 0))
	}
	_ = m.Swap()
	m.ClearPassive()

	// 此时 sorted=false，写入有序数据也应触发 !m.sorted 分支并排序
	p := &types.Point{
		Timestamp: now + 100*1e9,
		Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(100.0)},
	}
	_ = m.Write(types.PointToInternal(p, 0))

	// sorted 应已被 Write 设置为 true（sortActive 内部设置）
	if !m.sorted {
		t.Error("expected sorted=true after Write when previously unsorted")
	}

	// 再写入一个更早时间戳的数据，应触发 last-two 检查并排序
	p2 := &types.Point{
		Timestamp: now + 50*1e9,
		Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(50.0)},
	}
	_ = m.Write(types.PointToInternal(p2, 0))

	// 验证 active 有序
	iter := m.Iterator()
	var timestamps []int64
	for iter.Next() {
		timestamps = append(timestamps, iter.Point().Timestamp)
	}
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i] < timestamps[i-1] {
			t.Errorf("timestamps not sorted at position %d: %d < %d", i, timestamps[i], timestamps[i-1])
		}
	}
}

func TestMemTable_ShouldSwap_IdleTimeout_ZeroMaxCount(t *testing.T) {
	cfg := &MemTableConfig{
		MaxSize:           64 * 1024 * 1024,
		MaxCount:          0, // disable count check
		IdleDurationNanos: int64(50 * time.Millisecond),
	}
	m := NewMemTable(cfg)

	if m.ShouldSwap() {
		t.Error("empty memtable should not trigger idle timeout")
	}

	now := time.Now().UnixNano()
	p := &types.Point{
		Timestamp: now,
		Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)},
	}
	_ = m.Write(types.PointToInternal(p, 0))

	// 刚写入不应触发
	if m.ShouldSwap() {
		t.Error("should not swap immediately after write")
	}

	// 等待 idle 超时后应触发
	time.Sleep(100 * time.Millisecond)
	if !m.ShouldSwap() {
		t.Error("should swap after idle timeout with MaxCount=0")
	}
}
