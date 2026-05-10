package memtable

import (
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/types"
)

func BenchmarkMemTableWrite(b *testing.B) {
	mt := NewMemTable(DefaultMemTableConfig())
	p := &types.Point{
		Measurement: "cpu",
		Timestamp:   time.Now().UnixNano(),
		Tags:        map[string]string{"host": "server1"},
		Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Timestamp = int64(i)
		_ = mt.Write(p, 0)
	}
}

func BenchmarkMemTableWriteParallel(b *testing.B) {
	mt := NewMemTable(DefaultMemTableConfig())

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			i++
			p := &types.Point{
				Measurement: "cpu",
				Timestamp:   i,
				Tags:        map[string]string{"host": "server1"},
				Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
			}
			_ = mt.Write(p, 0)
		}
	})
}

func BenchmarkMemTableFlush(b *testing.B) {
	numTables := 4
	mts := make([]*MemTable, numTables)
	for i := 0; i < numTables; i++ {
		mts[i] = NewMemTable(DefaultMemTableConfig())
		for j := 0; j < 1000; j++ {
			p := &types.Point{
				Measurement: "cpu",
				Timestamp:   int64(j) * 1e9,
				Tags:        map[string]string{"host": "server1"},
				Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(j))},
			}
			_ = mts[i].Write(p, 0)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % numTables
		mts[idx].Flush()
	}
}

func BenchmarkMemTableShouldFlush(b *testing.B) {
	mt := NewMemTable(DefaultMemTableConfig())
	for i := 0; i < 100; i++ {
		p := &types.Point{
			Measurement: "cpu",
			Timestamp:   int64(i) * 1e9,
			Tags:        map[string]string{"host": "server1"},
			Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		_ = mt.Write(p, 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mt.ShouldFlush()
	}
}
