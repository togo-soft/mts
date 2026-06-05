// tests/e2e/query_op_benchmark/main.go
//
// 算子 Pipeline 性能基准测试：1M 数据点下对比新旧查询路径的 TPS、内存、延迟。
//
// 测试场景：
//  1. 原始扫描（Iterator vs Execute Scan+Project）
//  2. 行级过滤（Iterator+手动过滤 vs Execute Filter）
//  3. 分组聚合（Iterator+手动聚合 vs Execute GroupBy+Aggregate）
//  4. 全 Pipeline（Execute Filter→GroupBy→Aggregate→Sort→Limit）
//  5. Top-N 排序（Execute Sort+Limit）
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"codeberg.org/micro-ts/mts/internal/query"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/framework"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/metrics"
	"codeberg.org/micro-ts/mts/types"
)

const (
	benchCount      = 1000000
	pointInterval   = int64(100 * time.Microsecond)
	queryLimit      = 100
	filterThreshold = 50.0
	dbName          = "benchdb"
	measName        = "cpu"
)

type benchResult struct {
	name     string
	rows     int
	latency  time.Duration
	tps      float64
	memDelta metrics.MemDelta
}

func formatMem(m metrics.MemStats) string {
	return fmt.Sprintf("Alloc=%.1fMB Sys=%.1fMB",
		float64(m.Alloc)/(1024*1024), float64(m.Sys)/(1024*1024))
}

func formatDelta(d metrics.MemDelta) string {
	allocDiff := int64(d.After.Alloc) - int64(d.Before.Alloc)
	return fmt.Sprintf("AllocΔ=%+.1fMB",
		float64(allocDiff)/(1024*1024))
}

func runBench(name string, fn func() (int, error)) benchResult {
	metrics.GC()
	memBefore := metrics.ReadMemStats()
	timer := metrics.NewTimer()

	rows, err := fn()
	if err != nil {
		fmt.Printf("  FAIL %s: %v\n", name, err)
		return benchResult{name: name, rows: 0}
	}

	elapsed := timer.Elapsed()
	metrics.GC()
	memAfter := metrics.ReadMemStats()

	result := benchResult{
		name:     name,
		rows:     rows,
		latency:  elapsed,
		tps:      metrics.TPS(rows, elapsed),
		memDelta: metrics.CalcDelta(memBefore, memAfter),
	}

	fmt.Printf("  %-35s rows=%-8d latency=%-10v TPS=%-12.0f mem=[%s] delta=[%s]\n",
		name, rows, elapsed.Round(time.Microsecond), result.tps,
		formatMem(memAfter), formatDelta(result.memDelta))

	return result
}

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║     算子 Pipeline 性能基准 — 1M 数据点新旧路径对比           ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// ── 阶段 1: 写入 ──
	fmt.Println("── 阶段 1: 写入 1M 数据点 ──")

	h, err := framework.NewTestHarness("op_bench",
		framework.WithDBName(dbName),
		framework.WithMeasurementName(measName),
		framework.WithFlushCount(50000),
		framework.WithFlushIdle(5*time.Second),
		framework.WithCompaction(4, 30*time.Second),
		framework.WithShardDuration(24*time.Hour),
	)
	if err != nil {
		fmt.Printf("FAIL: setup: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = h.Close() }()

	baseTime := h.StartTime()
	endTime := baseTime + int64(benchCount)*pointInterval
	hosts := []string{"h1", "h2", "h3"}
	regions := []string{"us-east", "us-west", "eu-west"}

	metrics.GC()
	memBeforeWrite := metrics.ReadMemStats()
	writeTimer := metrics.NewTimer()

	batchSize := 50000 // 每 50K 写入后触发 flush
	for i := 0; i < benchCount; i++ {
		ts := baseTime + int64(i)*pointInterval
		host := hosts[i%len(hosts)]
		region := regions[i%len(regions)]
		p := &types.Point{
			Database:    dbName,
			Measurement: measName,
			Tags:        map[string]string{"host": host, "region": region},
			Timestamp:   ts,
			Fields: map[string]*types.FieldValue{
				"cpu":     types.NewFieldValue(float64(10 + i%100)),
				"mem":     types.NewFieldValue(float64(100 + i%200)),
				"counter": types.NewFieldValue(int64(i * 10)),
			},
		}
		if err := h.DB().Write(context.Background(), p); err != nil {
			fmt.Printf("FAIL: write at %d: %v\n", i, err)
			os.Exit(1)
		}
		if i > 0 && i%200000 == 0 {
			fmt.Printf("  wrote %d / %d points...\n", i, benchCount)
		}
		// 每 batchSize 点主动 flush 一次，给 compaction 时间转换
		if i > 0 && i%batchSize == 0 {
			_ = h.DB().FlushAll()
		}
	}

	writeElapsed := writeTimer.Elapsed()
	writeTPS := metrics.TPS(benchCount, writeElapsed)
	metrics.GC()
	memAfterWrite := metrics.ReadMemStats()
	writeDelta := metrics.CalcDelta(memBeforeWrite, memAfterWrite)

	fmt.Printf("  Write complete: %d points in %v, TPS=%.0f\n", benchCount, writeElapsed, writeTPS)
	fmt.Printf("  Write memory: %s, delta=[%s]\n\n", formatMem(memAfterWrite), formatDelta(writeDelta))

	// ── 阶段 2: Flush + 等待 Compaction ──
	fmt.Println("── 阶段 2: Flush + Compaction ──")
	fmt.Println("  触发 FlushAll...")

	_ = h.DB().FlushAll()

	// 等待 unordered compaction 将数据转为 SSTable（每 500ms 一轮）
	fmt.Println("  等待 unordered→L0 compaction...")
	cStart := time.Now()
	for i := 0; i < 20; i++ {
		time.Sleep(2 * time.Second)
		n := h.SSTableCount()
		if n > 0 {
			fmt.Printf("  SSTables found: %d (after %v)\n", n, time.Since(cStart).Round(time.Second))
			break
		}
	}

	// 再等 compaction 合并
	time.Sleep(5 * time.Second)
	_ = h.WaitForCompaction(8, 60*time.Second)
	time.Sleep(2 * time.Second)

	finalSST := h.SSTableCount()
	diskBytes := h.DiskUsage()
	fmt.Printf("  Final SSTables: %d (elapsed %v)\n", finalSST, time.Since(cStart).Round(time.Second))
	if diskBytes > 0 {
		fmt.Printf("  Disk: %.2f MB (%.2f bytes/point)\n",
			float64(diskBytes)/(1024*1024), float64(diskBytes)/float64(benchCount))
	} else if finalSST > 0 {
		fmt.Printf("  Disk: %.2f MB\n", float64(diskBytes)/(1024*1024))
	} else {
		fmt.Println("  (数据仍在 MemTable 中，性能反映内存读取速度)")
	}

	// ── 阶段 3: 基准测试 ──
	fmt.Println("── 阶段 3: 查询基准测试 ──")
	fmt.Println()
	fmt.Printf("  %-35s %-8s %-10s %-12s %s\n", "Scenario", "Rows", "Latency", "TPS", "Memory(Alloc/Sys) Delta")
	fmt.Println("  " + strings.Repeat("-", 95))

	var results []benchResult

	// ─── 3a. Iterator 原始扫描（基线）───
	res := runBench("[Iterator] 原始扫描", func() (int, error) {
		it, err := h.DB().Iterator(context.Background(), &types.QueryRangeRequest{
			Database:    dbName,
			Measurement: measName,
			StartTime:   baseTime,
			EndTime:     endTime,
			Limit:       queryLimit,
		})
		if err != nil {
			return 0, err
		}
		defer func() { _ = it.Close() }()
		count := 0
		for it.Next(context.Background()) {
			_ = it.Points()
			count++
		}
		return count, nil
	})
	results = append(results, res)

	// ─── 3b. Execute Scan+Project（等效）───
	res = runBench("[Execute] Scan+Project", func() (int, error) {
		plan := query.NewBuilder().
			Select("cpu", "mem").
			From(dbName, measName).
			TimeRange(baseTime, endTime).
			Limit(queryLimit).
			Build()
		iter, err := h.DB().Execute(context.Background(), plan)
		if err != nil {
			return 0, err
		}
		defer func() { _ = iter.Close() }()
		count := 0
		for iter.Next(context.Background()) {
			_ = iter.Points()
			count++
		}
		return count, nil
	})
	results = append(results, res)

	fmt.Println()

	// ─── 3c. Iterator + 手动过滤 ───
	res = runBench("[Iterator] 手动过滤 cpu>50", func() (int, error) {
		it, err := h.DB().Iterator(context.Background(), &types.QueryRangeRequest{
			Database:    dbName,
			Measurement: measName,
			StartTime:   baseTime,
			EndTime:     endTime,
		})
		if err != nil {
			return 0, err
		}
		defer func() { _ = it.Close() }()
		count := 0
		for it.Next(context.Background()) {
			row := it.Points()
			for _, f := range row.Fields {
				if f.Key == "cpu" && f.Value.GetFloatValue() > filterThreshold {
					count++
					break
				}
			}
		}
		return count, nil
	})
	results = append(results, res)

	// ─── 3d. Execute Filter ───
	res = runBench("[Execute] Filter cpu>50", func() (int, error) {
		plan := query.NewBuilder().
			Select("cpu", "mem", "counter").
			From(dbName, measName).
			TimeRange(baseTime, endTime).
			Where("cpu", query.GT, types.NewFieldValue(filterThreshold)).
			Build()
		iter, err := h.DB().Execute(context.Background(), plan)
		if err != nil {
			return 0, err
		}
		defer func() { _ = iter.Close() }()
		count := 0
		for iter.Next(context.Background()) {
			_ = iter.Points()
			count++
		}
		return count, nil
	})
	results = append(results, res)

	fmt.Println()

	// ─── 3e. Iterator + 手动分组聚合 ───
	res = runBench("[Iterator] 手动 GroupBy+Agg", func() (int, error) {
		it, err := h.DB().Iterator(context.Background(), &types.QueryRangeRequest{
			Database:    dbName,
			Measurement: measName,
			StartTime:   baseTime,
			EndTime:     endTime,
		})
		if err != nil {
			return 0, err
		}
		defer func() { _ = it.Close() }()

		type acc struct {
			sum, min, max float64
			count         int64
		}
		groups := make(map[string]*acc)
		for it.Next(context.Background()) {
			row := it.Points()
			host := row.Tags["host"]
			a, ok := groups[host]
			if !ok {
				a = &acc{min: 1e18, max: -1e18}
				groups[host] = a
			}
			for _, f := range row.Fields {
				if f.Key == "cpu" {
					v := f.Value.GetFloatValue()
					a.sum += v
					a.count++
					if v < a.min {
						a.min = v
					}
					if v > a.max {
						a.max = v
					}
					break
				}
			}
		}
		return len(groups), nil
	})
	results = append(results, res)

	// ─── 3f. Execute GroupBy+Aggregate ───
	res = runBench("[Execute] GroupBy+Agg(avg/max/min/count)", func() (int, error) {
		plan := query.NewBuilder().
			Select("avg(cpu)", "max(cpu)", "min(cpu)", "count(cpu)").
			From(dbName, measName).
			TimeRange(baseTime, endTime).
			GroupBy("host").
			Build()
		iter, err := h.DB().Execute(context.Background(), plan)
		if err != nil {
			return 0, err
		}
		defer func() { _ = iter.Close() }()
		count := 0
		for iter.Next(context.Background()) {
			_ = iter.Points()
			count++
		}
		return count, nil
	})
	results = append(results, res)

	fmt.Println()

	// ─── 3g. Execute 全 Pipeline ───
	res = runBench("[Execute] Filter→GroupBy→Agg→Sort→Limit", func() (int, error) {
		plan := query.NewBuilder().
			Select("avg(cpu)", "max(cpu)", "min(cpu)", "sum(mem)").
			From(dbName, measName).
			TimeRange(baseTime, endTime).
			Where("cpu", query.GT, types.NewFieldValue(30.0)).
			GroupBy("host").
			OrderBy("avg_cpu", query.DESC).
			Limit(10).
			Build()
		iter, err := h.DB().Execute(context.Background(), plan)
		if err != nil {
			return 0, err
		}
		defer func() { _ = iter.Close() }()
		count := 0
		for iter.Next(context.Background()) {
			_ = iter.Points()
			count++
		}
		return count, nil
	})
	results = append(results, res)

	// ─── 3h. Execute Sort+Limit Top-N ───
	res = runBench("[Execute] Sort+Limit Top-100", func() (int, error) {
		plan := query.NewBuilder().
			Select("cpu", "mem", "counter").
			From(dbName, measName).
			TimeRange(baseTime, endTime).
			OrderBy("cpu", query.DESC).
			Limit(queryLimit).
			Build()
		iter, err := h.DB().Execute(context.Background(), plan)
		if err != nil {
			return 0, err
		}
		defer func() { _ = iter.Close() }()
		count := 0
		for iter.Next(context.Background()) {
			_ = iter.Points()
			count++
		}
		return count, nil
	})
	results = append(results, res)

	fmt.Println()

	// ── 阶段 4: 对比分析 ──
	fmt.Println("── 阶段 4: 对比分析 ──")
	fmt.Println()

	// 对比 1: 原始扫描
	if r1 := findResult(results, "[Iterator] 原始扫描"); r1 != nil {
		if r2 := findResult(results, "[Execute] Scan+Project"); r2 != nil {
			fmt.Println("┌─ 对比 1: 原始扫描 ─────────────────────────────────────────────┐")
			compareResults(*r1, *r2, "Iterator 扫描", "Execute Scan+Project")
			fmt.Println("└──────────────────────────────────────────────────────────────────┘")
			fmt.Println()
		}
	}

	// 对比 2: 过滤
	if r1 := findResult(results, "[Iterator] 手动过滤"); r1 != nil {
		if r2 := findResult(results, "[Execute] Filter cpu>50"); r2 != nil {
			fmt.Println("┌─ 对比 2: 行级过滤 (cpu > 50) ──────────────────────────────────┐")
			compareResults(*r1, *r2, "Iterator+手动过滤", "Execute Filter")
			fmt.Println("└──────────────────────────────────────────────────────────────────┘")
			fmt.Println()
		}
	}

	// 对比 3: 分组聚合
	if r1 := findResult(results, "[Iterator] 手动 GroupBy+Agg"); r1 != nil {
		if r2 := findResult(results, "[Execute] GroupBy+Agg"); r2 != nil {
			fmt.Println("┌─ 对比 3: 分组聚合 (GroupBy host) ──────────────────────────────┐")
			compareResults(*r1, *r2, "Iterator+手动聚合", "Execute GroupBy+Aggregate")
			fmt.Println("└──────────────────────────────────────────────────────────────────┘")
			fmt.Println()
		}
	}

	// 汇总
	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                        汇总报告                                  ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")
	for _, r := range results {
		fmt.Printf("║ %-35s %8d rows  %10v  %10.0f TPS ║\n",
			r.name, r.rows, r.latency.Round(time.Microsecond), r.tps)
	}
	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")
}

func compareResults(oldRes, newRes benchResult, oldLabel, newLabel string) {
	speedup := oldRes.latency.Seconds() / newRes.latency.Seconds()
	fmt.Printf("│ %-25s rows=%-8d latency=%-10v TPS=%-10.0f │\n",
		oldLabel, oldRes.rows, oldRes.latency.Round(time.Microsecond), oldRes.tps)
	fmt.Printf("│ %-25s rows=%-8d latency=%-10v TPS=%-10.0f │\n",
		newLabel, newRes.rows, newRes.latency.Round(time.Microsecond), newRes.tps)
	fmt.Printf("│ 加速比: %.2fx  (新旧延迟比)                                      │\n", speedup)

	oldAllocDelta := int64(oldRes.memDelta.After.Alloc) - int64(oldRes.memDelta.Before.Alloc)
	newAllocDelta := int64(newRes.memDelta.After.Alloc) - int64(newRes.memDelta.Before.Alloc)
	memRatio := float64(newAllocDelta) / float64(oldAllocDelta)
	if oldAllocDelta == 0 {
		memRatio = 0
	}
	fmt.Printf("│ 内存(old): AllocΔ=%+.1fMB, 内存(new): AllocΔ=%+.1fMB, 比=%.2fx │\n",
		float64(oldAllocDelta)/(1024*1024), float64(newAllocDelta)/(1024*1024), memRatio)
}

func findResult(results []benchResult, prefix string) *benchResult {
	for i := range results {
		if results[i].name == prefix {
			return &results[i]
		}
	}
	return nil
}

