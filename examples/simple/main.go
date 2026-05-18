// examples/simple/main.go
//
// 本示例展示 microts 完整数据流：写入 → MemTable → flush → unordered → compaction → ordered SSTable。
//
// 配置:
//   - FlushPointCount=25, FlushIdle=10s: 控制刷盘阈值
//   - 写入 70 条数据 (< ActiveFull=125)，手动触发刷盘
//   - 刷盘后数据进入 unordered/（未排序 SSTable）
//   - UnorderedCompactor 每 500ms 分拣排序到 stable Shard 的 L0 目录
//
// 运行:
//
//	go run examples/simple/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/types"
)

func main() {
	tmpDir := filepath.Join(os.TempDir(), "microts_simple_example")
	if os.Getenv("CLEANUP") == "1" {
		_ = os.RemoveAll(tmpDir)
	}

	const maxCount int32 = 25
	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDurationNanos: int64(time.Hour),
		MemTableCfg: &microts.MemTableConfig{
			FlushMemorySize:           64 * 1024 * 1024,
			FlushPointCount:          maxCount,
			FlushIdleNanos: int64(10 * time.Second),
		},
	}

	db, err := microts.Open(dbCfg)
	if err != nil {
		log.Fatalf("打开数据库失败: %v", err)
	}
	defer func() { _ = db.Close() }()

	fmt.Println("╔══════════════════════════════════════════════╗")
	fmt.Println("║   MicroTS 完整数据流 Demo                     ║")
	fmt.Println("╚══════════════════════════════════════════════╝")
	fmt.Printf("\n数据目录: %s\n", tmpDir)
	fmt.Printf("MemTable: FlushPointCount=%d, NearFull=%d, ActiveFull=%d\n",
		maxCount, 2*maxCount, 5*maxCount)

	// ── Step 1: 写入数据 ──
	dbName := "testdb"
	meas := "cpu"
	baseTime := time.Now().UnixNano()
	const totalPoints = 70 // < ActiveFull(125)，不会在写入过程中自动刷盘

	fmt.Printf("\n═══ Step 1: 写入 %d 条数据 ═══\n", totalPoints)
	for i := 0; i < totalPoints; i++ {
		p := &types.Point{
			Database:    dbName,
			Measurement: meas,
			Tags:        map[string]string{"host": fmt.Sprintf("server%d", i%3+1)},
			Timestamp:   baseTime + int64(i)*int64(time.Millisecond),
			Fields: map[string]*types.FieldValue{
				"usage": types.NewFieldValue(float64(50.0 + float64(i%30))),
				"count": types.NewFieldValue(int64(i * 10)),
			},
		}
		if err := db.Write(context.Background(), p); err != nil {
			log.Fatalf("写入失败 %d: %v", i, err)
		}
	}
	fmt.Printf("写入完成: %d 条数据 (均在 MemTable 内存中)\n", totalPoints)

	// ── Step 2: 写入后状态 —— 只有 WAL + metadata ──
	fmt.Println("\n═══ Step 2: 写入后（数据仅在 MemTable）═══")
	showSSTableInventory(tmpDir)

	// ── Step 3: 手动触发刷盘 ──
	fmt.Println("\n═══ Step 3: 手动触发 FlushAll ═══")
	if err := db.FlushAll(); err != nil {
		log.Printf("FlushAll: %v", err)
	}
	time.Sleep(300 * time.Millisecond) // 等待异步写入完成

	// ── Step 4: 刷盘后 —— 应看到 unordered/ 下的 SSTable ──
	fmt.Println("\n═══ Step 4: 刷盘后（数据应已进入 unordered/）═══")
	fmt.Println("预期: unordered/{db}/{meas}/sst_N.bin")
	showSSTableInventory(tmpDir)

	// ── Step 5: 等待 UnorderedCompactor 分拣排序 ──
	fmt.Println("\n═══ Step 5: 等待 UnorderedCompactor ═══")
	fmt.Println("Compactor 每 500ms 扫描 unordered/，分拣排序后写入 stable L0")
	fmt.Println("如 unordered 文件已消失，说明已被 Compactor 处理")

	var lastInventory []sstEntry
	for attempt := 1; attempt <= 8; attempt++ {
		time.Sleep(800 * time.Millisecond)
		current := collectSSTables(tmpDir)
		if len(current) != len(lastInventory) || !sameInventory(current, lastInventory) {
			fmt.Printf("[%d] SSTable 文件快照:\n", attempt)
			for _, e := range current {
				fmt.Printf("    %s\n", e.path)
			}
			lastInventory = current
		}

		// 检查是否所有文件都在 ordered 区域
		allOrdered := true
		for _, e := range current {
			if !strings.Contains(e.path, "data/") {
				allOrdered = false
				break
			}
		}
		if allOrdered && len(current) > 0 {
			fmt.Println("\n✓ 所有 SSTable 已进入 ordered 区域")
			break
		}
	}

	// ── Step 6: 最终状态 ──
	fmt.Println("\n═══ Step 6: 最终目录结构 ═══")
	printDirTree(tmpDir, 5)

	// ── Step 7: 查询验证 ──
	fmt.Println("\n═══ Step 7: 查询验证 ═══")
	oneMonthLater := time.Now().Add(30 * 24 * time.Hour).UnixNano()
	it, err := db.Iterator(context.Background(), &types.QueryRangeRequest{
		Database:    dbName,
		Measurement: meas,
		StartTime:   0,
		EndTime:     oneMonthLater,
	})
	if err != nil {
		log.Fatalf("查询失败: %v", err)
	}
	defer func() { _ = it.Close() }()

	var rows []*types.PointRow
	for it.Next(context.Background()) {
		rows = append(rows, it.Points())
	}
	fmt.Printf("预期: %d 条, 实际: %d 条", totalPoints, len(rows))
	if len(rows) == totalPoints {
		fmt.Println(" ✓")
	} else {
		fmt.Printf(" ✗ 缺失 %d 条\n", totalPoints-len(rows))
	}

	fmt.Println("\n前 5 条:")
	for i := 0; i < 5 && i < len(rows); i++ {
		r := rows[i]
		host := ""
		if r.Tags != nil {
			host = r.Tags["host"]
		}
		fmt.Printf("  ts=%d host=%s usage=%.1f\n", r.Timestamp, host,
			r.GetFieldValue("usage").GetFloatValue())
	}

	// ── 总结 ──
	fmt.Println("\n═══ 数据流总结 ═══")
	fmt.Println("  Write → WAL → MemTable → FlushCoordinator → unordered/")
	fmt.Println("                                                │")
	fmt.Println("                                  UnorderedCompactor (500ms)")
	fmt.Println("                                                │")
	fmt.Println("                                          stable L0/")
	fmt.Println("                                                │")
	fmt.Println("                                      Level Compaction")
	fmt.Println("                                                │")
	fmt.Println("                                          data/sst_N.bin")
	fmt.Println()
	fmt.Println("查询时合并三层: MemTable + unordered + stable Shard")
	fmt.Println("\n=== Demo 完成 ===")
}

// ── 辅助类型和函数 ──

type sstEntry struct {
	path string
}

func collectSSTables(dataDir string) []sstEntry {
	var entries []sstEntry
	_ = filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(info.Name(), ".bin") {
			rel, _ := filepath.Rel(dataDir, path)
			entries = append(entries, sstEntry{path: rel})
		}
		return nil
	})
	sort.Slice(entries, func(i, j int) bool { return entries[i].path < entries[j].path })
	return entries
}

func sameInventory(a, b []sstEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].path != b[i].path {
			return false
		}
	}
	return true
}

func showSSTableInventory(dataDir string) {
	entries := collectSSTables(dataDir)
	if len(entries) == 0 {
		fmt.Println("  (无 SSTable .bin 文件)")
		return
	}
	for _, e := range entries {
		// 标注文件位置类型
		loc := ""
		switch {
		case strings.Contains(e.path, "unordered/"):
			loc = " [unordered - 未排序]"
		case strings.Contains(e.path, "/data/L0/"):
			loc = " [L0 - 刚排序]"
		case strings.Contains(e.path, "/data/"):
			loc = " [ordered - 有序]"
		}
		fmt.Printf("  %s%s\n", e.path, loc)
	}
}

func printDirTree(root string, maxDepth int) {
	type entry struct {
		path  string
		isDir bool
	}
	var entries []entry
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if rel == "." {
			return nil
		}
		depth := strings.Count(rel, string(filepath.Separator))
		if depth >= maxDepth {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		entries = append(entries, entry{path: rel, isDir: info.IsDir()})
		return nil
	})

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].path < entries[j].path
	})

	if len(entries) == 0 {
		fmt.Println("  (空)")
		return
	}
	for _, e := range entries {
		depth := strings.Count(e.path, string(filepath.Separator))
		indent := strings.Repeat("  ", depth+1)
		name := filepath.Base(e.path)
		if e.isDir {
			fmt.Printf("%s%s/\n", indent, name)
		} else {
			fmt.Printf("%s%s\n", indent, name)
		}
	}
}
