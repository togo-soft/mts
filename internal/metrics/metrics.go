// Package metrics 提供 expvar 指标定义与埋点。
// 不暴露 HTTP 端口，仅通过 expvar 标准库注册指标变量。
// 外部可通过 expvar.Handler 或自定义 HTTP handler 按需暴露（需显式启动）。
package metrics

import "expvar"

// Write 子系统指标
var (
	WriteTotal      = expvar.NewInt("write_total")
	WriteBytes      = expvar.NewInt("write_bytes")
	WriteErrors     = expvar.NewInt("write_errors")
	WriteBatchTotal = expvar.NewInt("write_batch_total")
)

// Flush 子系统指标
var (
	FlushTotal    = expvar.NewInt("flush_total")
	FlushPoints   = expvar.NewInt("flush_points")
	FlushErrors   = expvar.NewInt("flush_errors")
	FlushDuration = expvar.NewInt("flush_duration_ms")
)

// Compaction 子系统指标
var (
	CompactionTotal       = expvar.NewInt("compaction_total")
	CompactionInputFiles  = expvar.NewInt("compaction_input_files")
	CompactionOutputCount = expvar.NewInt("compaction_output_count")
	CompactionDupCount    = expvar.NewInt("compaction_dup_count")
	CompactionErrors      = expvar.NewInt("compaction_errors")
)

// Query 子系统指标
var (
	QueryTotal    = expvar.NewInt("query_total")
	QueryPoints   = expvar.NewInt("query_points")
	QueryErrors   = expvar.NewInt("query_errors")
	QueryDuration = expvar.NewInt("query_duration_ms")
)

// MemTable 子系统指标
var (
	MemTableActiveCount = expvar.NewInt("memtable_active_count")
	MemTableSwapTotal   = expvar.NewInt("memtable_swap_total")
)

// WAL 子系统指标
var (
	WALWriteTotal  = expvar.NewInt("wal_write_total")
	WALWriteBytes  = expvar.NewInt("wal_write_bytes")
	WALReplayTotal = expvar.NewInt("wal_replay_total")
	WALRotateTotal = expvar.NewInt("wal_rotate_total")
)

// Gauge 类型的 Map 指标
var (
	MemTableGauge = expvar.NewMap("memtable")
	WALGauge      = expvar.NewMap("wal")
	ShardGauge    = expvar.NewMap("shard")
)

// Incr 安全递增 expvar.Int，nil 安全。
func Incr(v *expvar.Int, delta int64) {
	if v != nil {
		v.Add(delta)
	}
}
