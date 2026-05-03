// internal/engine/engine_test.go
package engine

import (
	"testing"
	"time"
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
