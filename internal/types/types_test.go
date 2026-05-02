// internal/types/types_test.go
package types

import (
	"testing"
	"time"
)

func TestPointFieldTypes(t *testing.T) {
	p := &Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   time.Now().UnixNano(),
		Fields: map[string]any{
			"usage":  85.5,
			"count":  int64(100),
			"active": true,
			"name":   "test",
		},
	}

	if p.Database != "db1" {
		t.Errorf("expected database db1, got %s", p.Database)
	}

	if p.Measurement != "cpu" {
		t.Errorf("expected measurement cpu, got %s", p.Measurement)
	}

	if len(p.Tags) != 1 || p.Tags["host"] != "server1" {
		t.Errorf("unexpected tags: %v", p.Tags)
	}

	if len(p.Fields) != 4 {
		t.Errorf("expected 4 fields, got %d", len(p.Fields))
	}
}

func TestQueryRangeRequest(t *testing.T) {
	req := &QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   1000,
		EndTime:     2000,
		Fields:      []string{"usage"},
		Tags:        map[string]string{"host": "server1"},
		Offset:      0,
		Limit:       100,
	}

	if req.Database != "db1" {
		t.Errorf("expected database db1")
	}
	if req.StartTime >= req.EndTime {
		t.Errorf("start_time should be less than end_time")
	}
	if req.Limit != 100 {
		t.Errorf("expected limit 100, got %d", req.Limit)
	}
}

func TestPointRow(t *testing.T) {
	row := &PointRow{
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]any{"usage": 85.5},
	}

	if row.Timestamp == 0 {
		t.Errorf("timestamp should not be zero")
	}
	if len(row.Tags) != 1 {
		t.Errorf("expected 1 tag, got %d", len(row.Tags))
	}
}
