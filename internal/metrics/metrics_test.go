package metrics

import (
	"expvar"
	"testing"
)

func TestMetrics_WriteCounters(t *testing.T) {
	resetInts()
	WriteTotal.Add(1)
	WriteBytes.Add(100)

	if got := WriteTotal.String(); got != "1" {
		t.Errorf("WriteTotal = %s, want 1", got)
	}
	if got := WriteBytes.String(); got != "100" {
		t.Errorf("WriteBytes = %s, want 100", got)
	}
}

func TestMetrics_FlushCounters(t *testing.T) {
	resetInts()
	FlushTotal.Add(1)
	FlushPoints.Add(500)

	if got := FlushTotal.String(); got != "1" {
		t.Errorf("FlushTotal = %s, want 1", got)
	}
}

func TestMetrics_CompactionCounters(t *testing.T) {
	resetInts()
	CompactionTotal.Add(1)
	CompactionOutputCount.Add(1000)
	CompactionDupCount.Add(50)

	if got := CompactionTotal.String(); got != "1" {
		t.Errorf("CompactionTotal = %s, want 1", got)
	}
}

func TestMetrics_QueryCounters(t *testing.T) {
	resetInts()
	QueryTotal.Add(1)
	QueryPoints.Add(200)

	if got := QueryTotal.String(); got != "1" {
		t.Errorf("QueryTotal = %s, want 1", got)
	}
}

func TestMetrics_MemTableCounters(t *testing.T) {
	resetInts()
	MemTableActiveCount.Add(1500)
	MemTableSwapTotal.Add(10)

	if got := MemTableActiveCount.String(); got != "1500" {
		t.Errorf("MemTableActiveCount = %s, want 1500", got)
	}
}

func TestMetrics_WALCounters(t *testing.T) {
	resetInts()
	WALWriteTotal.Add(1)
	WALWriteBytes.Add(500)

	if got := WALWriteTotal.String(); got != "1" {
		t.Errorf("WALWriteTotal = %s, want 1", got)
	}
}

func TestMetrics_GaugeMaps(t *testing.T) {
	resetInts()
	MemTableGauge.Add("active_count", 1000)
	WALGauge.Add("gen", 42)
	ShardGauge.Add("sst_count", 5)

	if got := MemTableGauge.Get("active_count").String(); got != "1000" {
		t.Errorf("expected 1000, got %s", got)
	}
}

func TestIncr_NilSafe(t *testing.T) {
	// 不应 panic
	Incr(nil, 10)
}

// resetInts resets all expvar.Int values to 0 for test isolation.
// expvar.Int values persist across tests, so we need to reset them.
func resetInts() {
	for _, v := range []*expvar.Int{
		WriteTotal, WriteBytes, WriteErrors, WriteBatchTotal,
		FlushTotal, FlushPoints, FlushErrors, FlushDuration,
		CompactionTotal, CompactionInputFiles, CompactionOutputCount, CompactionDupCount, CompactionErrors,
		QueryTotal, QueryPoints, QueryErrors, QueryDuration,
		MemTableActiveCount, MemTableSwapTotal,
		WALWriteTotal, WALWriteBytes, WALReplayTotal, WALRotateTotal,
	} {
		v.Set(0)
	}
	// reset maps
	MemTableGauge.Init()
	WALGauge.Init()
	ShardGauge.Init()
}
