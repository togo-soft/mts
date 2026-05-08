// tests/e2e/wal_test/debug_test6.go
//
// Debug test for Test6 WAL replay issue.
// This test traces the WAL replay process to understand why session 1 data
// is not being recovered in session 2.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/types"
)

// DebugInfo holds debug information collected during the test
type DebugInfo struct {
	session     string
	shardKey    string
	WALDir      string
	WALSegments []string
	PointCount  int
}

// countWALSegments counts WAL segment files in a directory
func countWALSegments(walDir string) (int, error) {
	if _, err := os.Stat(walDir); os.IsNotExist(err) {
		return 0, nil
	}
	pattern := filepath.Join(walDir, "*.wal")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return 0, err
	}
	return len(matches), nil
}

// listWALSegments lists WAL segment files in a directory
func listWALSegments(walDir string) ([]string, error) {
	if _, err := os.Stat(walDir); os.IsNotExist(err) {
		return nil, nil
	}
	pattern := filepath.Join(walDir, "*.wal")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(matches)-1; i++ {
		for j := i + 1; j < len(matches); j++ {
			if filepath.Base(matches[i]) > filepath.Base(matches[j]) {
				matches[i], matches[j] = matches[j], matches[i]
			}
		}
	}
	return matches, nil
}

// getShardWALDir returns the WAL directory path for a shard
func getShardWALDir(dataDir, dbName, measurement string, timestamp int64) string {
	shardTime := (timestamp / int64(time.Hour)) * int64(time.Hour)
	shardDir := filepath.Join(dataDir, dbName, measurement, fmt.Sprintf("%d_%d", shardTime, shardTime+int64(time.Hour)))
	return filepath.Join(shardDir, "wal")
}

// collectDebugInfo collects debug information about the shard and WAL state
func collectDebugInfo(db *microts.DB, dataDir, dbName, measurement string, timestamp int64, session string) DebugInfo {
	info := DebugInfo{
		session:  session,
		PointCount: 0,
	}

	// Calculate expected shard key
	shardTime := (timestamp / int64(time.Hour)) * int64(time.Hour)
	info.shardKey = fmt.Sprintf("%s/%s/%d", dbName, measurement, shardTime)

	// Get WAL directory
	info.WALDir = getShardWALDir(dataDir, dbName, measurement, timestamp)

	// List WAL segments
	segments, _ := listWALSegments(info.WALDir)
	info.WALSegments = segments

	// Try to list shards from shardIndex
	// Note: We need to access internal metadata.Manager which is not exposed,
	// so we'll use file system inspection instead

	// Count point data by querying
	ctx := context.Background()
	resp, err := db.QueryRange(ctx, &types.QueryRangeRequest{
		Database:    dbName,
		Measurement: measurement,
		StartTime:   timestamp,
		EndTime:    timestamp + 100*int64(time.Millisecond),
		Offset:      0,
		Limit:       0,
	})
	if err == nil {
		info.PointCount = len(resp.Rows)
	}

	return info
}

// printDebugInfo prints debug information in a formatted way
func printDebugInfo(info DebugInfo) {
	fmt.Printf("\n--- Debug Info [%s] ---\n", info.session)
	fmt.Printf("  Shard Key:     %s\n", info.shardKey)
	fmt.Printf("  WAL Dir:       %s\n", info.WALDir)
	fmt.Printf("  WAL Dir Exists: %v\n", func() bool { _, err := os.Stat(info.WALDir); return err == nil }())
	fmt.Printf("  WAL Segments:  %d files\n", len(info.WALSegments))
	for _, seg := range info.WALSegments {
		fmt.Printf("    - %s\n", filepath.Base(seg))
	}
	fmt.Printf("  Point Count:   %d\n", info.PointCount)
}

// writeTestPoints writes test data points (copied from main.go for standalone execution)
func writeTestPoints(db *microts.DB, dbName, measurement string, startTime int64, count int, interval time.Duration) error {
	for i := 0; i < count; i++ {
		p := &types.Point{
			Database:    dbName,
			Measurement: measurement,
			Tags: map[string]string{
				"host": fmt.Sprintf("server%d", i%3+1),
			},
			Timestamp: startTime + int64(i)*int64(interval),
			Fields: map[string]*types.FieldValue{
				"usage": types.NewFieldValue(float64(50.0 + float64(i%50))),
				"count": types.NewFieldValue(int64(i * 10)),
			},
		}
		if err := db.Write(context.Background(), p); err != nil {
			return fmt.Errorf("write point %d: %w", i, err)
		}
	}
	return nil
}

// queryAndCount queries data and returns row count (copied from main.go for standalone execution)
func queryAndCount(db *microts.DB, dbName, measurement string, startTime, endTime int64) (int, error) {
	resp, err := db.QueryRange(context.Background(), &types.QueryRangeRequest{
		Database:    dbName,
		Measurement: measurement,
		StartTime:   startTime,
		EndTime:     endTime,
		Offset:      0,
		Limit:       0,
	})
	if err != nil {
		return 0, err
	}
	return len(resp.Rows), nil
}

// TestDebug6_WALReplayDebug is the main debug test
func TestDebug6_WALReplayDebug() error {
	fmt.Println("\n=== DEBUG Test 6: WAL Replay Debug ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_wal_debug_test6")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Same config as Test6
	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           64 * 1024 * 1024,
			MaxCount:          100,                           // 最大 100 条
			IdleDurationNanos: int64(5 * time.Second),        // 5 秒空闲触发刷盘
		},
	}

	dbName := "testdb"
	measurement := "cpu"

	// ============ Session 1 ============
	fmt.Printf("\n>>> SESSION 1: Opening database\n")
	db1, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db1: %w", err)
	}

	session1BaseTime := time.Now().UnixNano()
	fmt.Printf("\n>>> SESSION 1: Writing 100 points\n")
	if err := writeTestPoints(db1, dbName, measurement, session1BaseTime, 100, time.Millisecond); err != nil {
		_ = db1.Close()
		return fmt.Errorf("write session1 points: %w", err)
	}
	fmt.Printf("      Written 100 points, time range: [%d, %d]\n", session1BaseTime, session1BaseTime+100*int64(time.Millisecond))

	// Collect debug info before close
	infoBeforeClose := collectDebugInfo(db1, tmpDir, dbName, measurement, session1BaseTime, "Session1-BeforeClose")
	printDebugInfo(infoBeforeClose)

	// Check WAL segments exist
	walDir := getShardWALDir(tmpDir, dbName, measurement, session1BaseTime)
	walSegCount, _ := countWALSegments(walDir)
	fmt.Printf("      WAL segment count before close: %d\n", walSegCount)

	fmt.Printf("\n>>> SESSION 1: Closing database\n")
	if err := db1.Close(); err != nil {
		return fmt.Errorf("close db1: %w", err)
	}

	// After close, check if WAL segments still exist
	walSegCountAfter, _ := countWALSegments(walDir)
	fmt.Printf("      WAL segment count after close: %d\n", walSegCountAfter)

	// List files in WAL dir after close
	segmentsAfter, _ := listWALSegments(walDir)
	fmt.Printf("      WAL segments after close:\n")
	for _, seg := range segmentsAfter {
		info, _ := os.Stat(seg)
		if info != nil {
			fmt.Printf("        %s (%d bytes)\n", filepath.Base(seg), info.Size())
		}
	}

	// Check metadata.db for shard registration
	metaDBPath := filepath.Join(tmpDir, "metadata.db")
	if _, err := os.Stat(metaDBPath); err == nil {
		fmt.Printf("      metadata.db exists after close\n")
	} else {
		fmt.Printf("      metadata.db NOT FOUND after close\n")
	}

	// ============ Session 2 ============
	fmt.Printf("\n>>> SESSION 2: Opening database\n")
	db2, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db2: %w", err)
	}
	defer func() { _ = db2.Close() }()

	// Wait for background discovery to complete
	fmt.Printf("      Waiting for background WAL discovery...\n")
	time.Sleep(1 * time.Second)

	// Collect debug info after reopen
	infoAfterOpen := collectDebugInfo(db2, tmpDir, dbName, measurement, session1BaseTime, "Session2-AfterOpen")
	printDebugInfo(infoAfterOpen)

	// Check WAL segments in session 2
	walSegCount2, _ := countWALSegments(walDir)
	fmt.Printf("      WAL segment count in session 2: %d\n", walSegCount2)

	session2BaseTime := time.Now().UnixNano()
	fmt.Printf("\n>>> SESSION 2: Writing 100 new points\n")
	if err := writeTestPoints(db2, dbName, measurement, session2BaseTime, 100, time.Millisecond); err != nil {
		return fmt.Errorf("write session2 points: %w", err)
	}
	fmt.Printf("      Written 100 new points, time range: [%d, %d]\n", session2BaseTime, session2BaseTime+100*int64(time.Millisecond))

	// Collect debug info after write
	infoAfterWrite := collectDebugInfo(db2, tmpDir, dbName, measurement, session2BaseTime, "Session2-AfterWrite")
	printDebugInfo(infoAfterWrite)

	// ============ Query Results ============
	fmt.Printf("\n>>> QUERY RESULTS:\n")

	// Query session 1's data
	oldCount, err := queryAndCount(db2, dbName, measurement, session1BaseTime, session1BaseTime+100*int64(time.Millisecond))
	if err != nil {
		return fmt.Errorf("query old data failed: %w", err)
	}
	fmt.Printf("      Session 1 data (old): %d points (expected: 100)\n", oldCount)

	// Query session 2's data
	newCount, err := queryAndCount(db2, dbName, measurement, session2BaseTime, session2BaseTime+100*int64(time.Millisecond))
	if err != nil {
		return fmt.Errorf("query new data failed: %w", err)
	}
	fmt.Printf("      Session 2 data (new): %d points (expected: 100)\n", newCount)

	// Total query
	totalCount, err := queryAndCount(db2, dbName, measurement, session1BaseTime, session2BaseTime+100*int64(time.Millisecond))
	if err != nil {
		return fmt.Errorf("query total data failed: %w", err)
	}
	fmt.Printf("      Total data: %d points (expected: 200)\n", totalCount)

	// ============ Analysis ============
	fmt.Printf("\n>>> ANALYSIS:\n")
	if oldCount == 0 {
		fmt.Printf("      ISSUE DETECTED: Session 1 data was NOT recovered via WAL replay!\n")
		fmt.Printf("      This indicates WAL replay is not working correctly.\n")
	} else {
		fmt.Printf("      Session 1 data was recovered correctly.\n")
	}

	// Check if WAL segments exist after session 2 opened
	if walSegCountAfter > 0 && walSegCount2 == 0 {
		fmt.Printf("      NOTE: WAL segments existed after close but disappeared in session 2.\n")
		fmt.Printf("      This may indicate WAL segments were cleaned up or not properly persisted.\n")
	}

	// Expected result
	expectedTotal := 200
	actualTotal := oldCount + newCount
	fmt.Printf("\n>>> FINAL RESULT: old=%d, new=%d, total=%d (expected: %d)\n", oldCount, newCount, actualTotal, expectedTotal)

	if actualTotal != expectedTotal {
		return fmt.Errorf("data count mismatch: expected %d, got %d (old=%d, new=%d)", expectedTotal, actualTotal, oldCount, newCount)
	}

	fmt.Printf("\n=== DEBUG Test 6 completed ===\n")
	return nil
}

func main() {
	fmt.Println("========================================")
	fmt.Println("MTS WAL Replay Debug Test")
	fmt.Println("========================================")

	if err := TestDebug6_WALReplayDebug(); err != nil {
		fmt.Printf("\nDebug test failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\nDebug test passed!")
}
