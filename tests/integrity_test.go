// tests/integrity_test.go
package tests

import (
	"context"
	"testing"
	"time"

	"micro-ts"
	"micro-ts/internal/types"
)

func TestIntegrity_100K_Points(t *testing.T) {
	// 使用固定种子保证可复现
	gen := NewDataGenerator(42)

	// 创建数据库
	db, err := microts.Open(microts.Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	const count = 100000
	baseTime := time.Now().UnixNano()
	expectedPoints := make([]*types.Point, count)

	// 生成并写入数据
	t.Log("Generating and writing 100K points...")
	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint("db1", "cpu", ts)
		expectedPoints[i] = p

		if err := db.Write(context.TODO(), p); err != nil {
			t.Fatalf("Write failed at %d: %v", i, err)
		}
	}

	// 查询所有数据
	t.Log("Reading back data...")
	resp, err := db.QueryRange(context.TODO(), &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   baseTime,
		EndTime:     baseTime + int64(count)*int64(time.Second),
		Limit:       0, // 不限制
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// 验证数量
	if int(resp.TotalCount) != count {
		t.Errorf("expected %d points, got %d", count, resp.TotalCount)
	}

	// 逐字段验证
	t.Log("Verifying data integrity...")
	for i, row := range resp.Rows {
		expected := expectedPoints[i]

		// 验证 timestamp
		if row.Timestamp != expected.Timestamp {
			t.Errorf("row %d: timestamp mismatch: expected %d, got %d", i, expected.Timestamp, row.Timestamp)
		}

		// 验证 fields
		for name, expectedVal := range expected.Fields {
			actualVal, ok := row.Fields[name]
			if !ok {
				t.Errorf("row %d: missing field %s", i, name)
				continue
			}
			if actualVal != expectedVal {
				t.Errorf("row %d: field %s mismatch: expected %v, got %v", i, name, expectedVal, actualVal)
			}
		}
	}

	t.Log("Data integrity verified successfully")
}
