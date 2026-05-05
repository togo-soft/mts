// tests/e2e/pkg/metrics/metrics.go
package metrics

import (
	"fmt"
	"io/fs"
	"path/filepath"
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

// ===================================
// 存储统计
// ===================================

// StorageStats 存储统计
type StorageStats struct {
	Dir       string
	TotalSize int64
	FileCount int
}

// CalcDirSize 计算目录总大小
func CalcDirSize(dir string) (*StorageStats, error) {
	stats := &StorageStats{Dir: dir}

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // 跳过错误继续
		}
		if !d.IsDir() {
			info, err := d.Info()
			if err == nil {
				stats.TotalSize += info.Size()
				stats.FileCount++
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return stats, nil
}

// Format 格式化存储统计
func (s *StorageStats) Format() string {
	return fmt.Sprintf("Dir: %s, Size: %s, Files: %d", s.Dir, FormatBytes(uint64(s.TotalSize)), s.FileCount)
}

// BytesPerPoint 计算每点平均字节数
func (s *StorageStats) BytesPerPoint(pointCount int) float64 {
	if pointCount == 0 {
		return 0
	}
	return float64(s.TotalSize) / float64(pointCount)
}

// CompressionRatio 计算压缩率 (原始数据大小 / 存储大小)
// rawBytesPerPoint: 每个点的原始字节数估计值
func (s *StorageStats) CompressionRatio(pointCount int, rawBytesPerPoint int) float64 {
	if s.TotalSize == 0 {
		return 1.0
	}
	rawSize := int64(pointCount * rawBytesPerPoint)
	return float64(rawSize) / float64(s.TotalSize)
}

// FormatStorageReport 格式化存储报告
func FormatStorageReport(dir string, pointCount int, rawBytesPerPoint int) string {
	stats, err := CalcDirSize(dir)
	if err != nil {
		return fmt.Sprintf("Storage: failed to calculate: %v", err)
	}

	bytesPerPoint := stats.BytesPerPoint(pointCount)
	compressionRatio := stats.CompressionRatio(pointCount, rawBytesPerPoint)

	return fmt.Sprintf(
		"Storage Report:\n"+
			"  Directory: %s\n"+
			"  Total Disk Usage: %s (%d files)\n"+
			"  Points: %d\n"+
			"  Bytes/Point: %.2f\n"+
			"  Raw Data Est: %s\n"+
			"  Compression Ratio: %.2fx\n"+
			"  === Total Disk Occupied: %s ===",
		dir,
		FormatBytes(uint64(stats.TotalSize)),
		stats.FileCount,
		pointCount,
		bytesPerPoint,
		FormatBytes(uint64(pointCount*rawBytesPerPoint)),
		compressionRatio,
		FormatBytes(uint64(stats.TotalSize)),
	)
}

// FormatBytes 格式化字节数
func FormatBytes(bytes uint64) string {
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
