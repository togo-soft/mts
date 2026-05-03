// tests/e2e/pkg/metrics/metrics.go
package metrics

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"time"
)

// MemStats 内存统计
type MemStats struct {
	Alloc      uint64 // bytes allocated
	TotalAlloc uint64 // total bytes allocated
	Sys        uint64 // total bytes obtained from OS
	NumGC      uint32 // number of completed GC cycles
}

// ReadMemStats 读取当前内存统计
func ReadMemStats() MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return MemStats{
		Alloc:      m.Alloc,
		TotalAlloc: m.TotalAlloc,
		Sys:        m.Sys,
		NumGC:      m.NumGC,
	}
}

// FormatMemStats 格式化内存统计为可读字符串
func FormatMemStats(m MemStats) string {
	return fmt.Sprintf("Alloc: %s, TotalAlloc: %s, Sys: %s, NumGC: %d",
		formatBytes(m.Alloc),
		formatBytes(m.TotalAlloc),
		formatBytes(m.Sys),
		m.NumGC)
}

// formatBytes 格式化字节数
func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// MemDelta 内存变化
type MemDelta struct {
	Before MemStats
	After  MemStats
}

// CalcDelta 计算内存变化
func CalcDelta(before, after MemStats) MemDelta {
	return MemDelta{Before: before, After: after}
}

// Format 格式化内存变化
func (d MemDelta) Format() string {
	allocDiff := int64(d.After.Alloc) - int64(d.Before.Alloc)
	return fmt.Sprintf("Alloc Δ: %s (%.2f%%), GC cycles: %d → %d",
		formatBytesWithSign(uint64(abs(allocDiff))),
		float64(allocDiff)/float64(d.Before.Alloc)*100,
		d.Before.NumGC,
		d.After.NumGC)
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func formatBytesWithSign(bytes uint64) string {
	return formatBytes(bytes)
}

// GC 运行垃圾回收以获得准确的内存统计
func GC() {
	debug.FreeOSMemory()
	runtime.GC()
}

// Timer 时间统计
type Timer struct {
	start time.Time
}

// NewTimer 创建计时器
func NewTimer() *Timer {
	return &Timer{start: time.Now()}
}

// Elapsed 返回经过的时间
func (t *Timer) Elapsed() time.Duration {
	return time.Since(t.start)
}

// TPS 计算 TPS
func TPS(count int, elapsed time.Duration) float64 {
	if elapsed.Seconds() == 0 {
		return 0
	}
	return float64(count) / elapsed.Seconds()
}

// FormatTPS 格式化 TPS
func FormatTPS(op string, count int, elapsed time.Duration) string {
	tps := TPS(count, elapsed)
	return fmt.Sprintf("%s: %d ops in %v, TPS: %.2f", op, count, elapsed, tps)
}