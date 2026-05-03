// internal/engine/engine_test.go
package engine

import (
	"testing"
	"time"

	"micro-ts/internal/types"
)

func TestEngine_Open(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	if engine == nil {
		t.Errorf("expected non-nil engine")
	}
}

func TestEngine_Close(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, _ := NewEngine(cfg)
	err := engine.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestEngine_Write(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	point := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   time.Now().UnixNano(),
		Fields:      map[string]any{"usage": 85.5},
	}

	err = engine.Write(point)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestEngine_WriteBatch(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	points := []*types.Point{
		{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   time.Now().UnixNano(),
			Fields:      map[string]any{"usage": 85.5},
		},
		{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server2"},
			Timestamp:   time.Now().UnixNano() + 1e9,
			Fields:      map[string]any{"usage": 90.0},
		},
	}

	err = engine.WriteBatch(points)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
}
