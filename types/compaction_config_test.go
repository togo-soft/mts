package types

import (
	"testing"
	"time"
)

func TestDefaultCompactionConfig(t *testing.T) {
	cfg := DefaultCompactionConfig()
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.MaxSSTableCount != 4 {
		t.Errorf("expected MaxSSTableCount 4, got %d", cfg.MaxSSTableCount)
	}
	if cfg.MaxCompactionBatch != 0 {
		t.Errorf("expected MaxCompactionBatch 0, got %d", cfg.MaxCompactionBatch)
	}
	if cfg.ShardSizeLimit != 1*1024*1024*1024 {
		t.Errorf("expected ShardSizeLimit 1GB, got %d", cfg.ShardSizeLimit)
	}
	if cfg.CheckInterval != 1*time.Hour {
		t.Errorf("expected CheckInterval 1h, got %v", cfg.CheckInterval)
	}
	if cfg.Timeout != 30*time.Minute {
		t.Errorf("expected Timeout 30min, got %v", cfg.Timeout)
	}
}

func TestCompactionConfig_ZeroValues(t *testing.T) {
	cfg := &CompactionConfig{}
	if cfg.MaxSSTableCount != 0 {
		t.Errorf("expected zero MaxSSTableCount, got %d", cfg.MaxSSTableCount)
	}
	if cfg.MaxCompactionBatch != 0 {
		t.Errorf("expected zero MaxCompactionBatch, got %d", cfg.MaxCompactionBatch)
	}
	if cfg.ShardSizeLimit != 0 {
		t.Errorf("expected zero ShardSizeLimit, got %d", cfg.ShardSizeLimit)
	}
}
