// Package downsample 实现时序数据降采样服务。
//
// DownsampleService 定期扫描各 database 的 Shard，对已过原始数据保留期的数据
// 按配置的窗口和聚合函数生成降采样 SSTable 文件。
//
// 目录结构：
//
//	{shard}/downsampled/{window_nanos}/sst_N.bin
//
// 幂等性：
//
//	处理完成后写入 _downsample_done 标记，重启后跳过已完成的窗口。
package downsample

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// Catalog 是 DownsampleService 需要的 catalog 接口。
type Catalog interface {
	ListDatabases() []string
	ListMeasurements(database string) ([]string, error)
	GetDatabaseRetention(database string) (time.Duration, error)
	GetDownsampleConfig(database string) (*types.DownsampleConfig, error)
	GetSchema(database, measurement string) (sstable.Schema, error)
}

// Service 是降采样后台服务。
type Service struct {
	dataDir         string
	catalog         Catalog
	compressionAlgo types.CompressionAlgorithm
	defaultInterval time.Duration
	mu              sync.Mutex
	running         bool
	stopCh          chan struct{}
	wg              sync.WaitGroup
}

// NewService 创建降采样服务。
func NewService(dataDir string, catalog Catalog, compressionAlgo types.CompressionAlgorithm) *Service {
	return &Service{
		dataDir:         dataDir,
		catalog:         catalog,
		compressionAlgo: compressionAlgo,
		defaultInterval: 5 * time.Minute,
		stopCh:          make(chan struct{}),
	}
}

// ForceRun 手动触发一次降采样处理（用于测试和运维）。
func (s *Service) ForceRun() {
	s.processAll()
}

// Start 启动降采样服务。
func (s *Service) Start() {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.mu.Unlock()

	slog.Info("downsample service started")
	s.wg.Add(1)
	go s.run()
}

// Stop 停止降采样服务并等待 goroutine 退出。
func (s *Service) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	close(s.stopCh)
	s.mu.Unlock()

	s.wg.Wait()
	slog.Info("downsample service stopped")
}

// run 是主循环。
func (s *Service) run() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.defaultInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.processAll()
		case <-s.stopCh:
			return
		}
	}
}

// processAll 遍历所有 database 并执行降采样。
func (s *Service) processAll() {
	for _, db := range s.catalog.ListDatabases() {
		cfg, err := s.catalog.GetDownsampleConfig(db)
		if err != nil || cfg == nil || !cfg.Enabled {
			continue
		}

		retention, err := s.catalog.GetDatabaseRetention(db)
		if err != nil || retention <= 0 {
			continue
		}

		measurements, err := s.catalog.ListMeasurements(db)
		if err != nil {
			slog.Warn("failed to list measurements for downsample", "db", db, "error", err)
			continue
		}

		for _, meas := range measurements {
			s.processMeasurement(db, meas, retention, cfg)
		}
	}
}

// processMeasurement 处理单个 measurement。
func (s *Service) processMeasurement(db, meas string, retention time.Duration, cfg *types.DownsampleConfig) {
	measDir := filepath.Join(s.dataDir, db, meas)
	entries, err := os.ReadDir(measDir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		shardDir := filepath.Join(measDir, entry.Name())
		startTime, endTime, ok := parseShardDir(entry.Name())
		if !ok {
			continue
		}

		s.processShard(db, meas, shardDir, startTime, endTime, retention, cfg)
	}
}

// processShard 对单个 Shard 执行降采样。
func (s *Service) processShard(db, meas, shardDir string, startTime, endTime int64, retention time.Duration, cfg *types.DownsampleConfig) {
	cutoff := time.Now().Add(-retention).UnixNano()

	if endTime > cutoff {
		return
	}

	dataDir := filepath.Join(shardDir, "data")
	sstFiles, err := listSSTFiles(dataDir)
	if err != nil || len(sstFiles) == 0 {
		return
	}

	schema, err := s.catalog.GetSchema(db, meas)
	if err != nil {
		slog.Warn("failed to get schema for downsample", "db", db, "meas", meas, "error", err)
		return
	}

	for _, rule := range cfg.Rules {
		windowNanos := rule.WindowNanos
		windowDir := filepath.Join(shardDir, "downsampled", fmt.Sprintf("%d", windowNanos))

		if isDownsampleDone(windowDir) {
			continue
		}

		slog.Info("downsampling shard",
			"db", db, "meas", meas,
			"shard", filepath.Base(shardDir),
			"window", time.Duration(windowNanos))

		if err := s.downsampleShard(db, meas, sstFiles, windowDir, windowNanos, startTime, rule, schema); err != nil {
			slog.Error("downsample shard failed",
				"db", db, "meas", meas,
				"shard", filepath.Base(shardDir),
				"window", time.Duration(windowNanos),
				"error", err)
			continue
		}

		_ = markDownsampleDone(windowDir)
	}
}

// downsampleShard 读取原始 SSTable 并生成降采样数据。
func (s *Service) downsampleShard(db, meas string, sstFiles []string, windowDir string, windowNanos, shardStart int64, rule *types.DownsampleRule, schema sstable.Schema) error {
	buckets, err := aggregateSSTFiles(sstFiles, windowNanos, shardStart, rule.Functions, schema)
	if err != nil {
		return err
	}

	if len(buckets) == 0 {
		return nil
	}

	if err := os.MkdirAll(windowDir, 0700); err != nil {
		return fmt.Errorf("create window dir: %w", err)
	}

	seq, err := nextSSTSeq(windowDir)
	if err != nil {
		return fmt.Errorf("next seq: %w", err)
	}

	writer, err := sstable.NewWriter(windowDir, uint64(seq), sstable.BlockSize, s.compressionAlgo, sstable.FlagSorted)
	if err != nil {
		return fmt.Errorf("create writer: %w", err)
	}

	tags := map[string]string{"_downsampled": db + "/" + meas}

	windowSeconds := float64(windowNanos) / 1e9
	var batch []*types.PointRow
	const batchSize = 1000

	for _, bucket := range buckets {
		fields := buildDownsampledFields(bucket, rule.Functions, windowSeconds)

		row := &types.PointRow{
			Timestamp: bucket.windowStart,
			Tags:      tags,
			Fields:    fields,
		}
		batch = append(batch, row)

		if len(batch) >= batchSize {
			if err := writer.WritePointRows(batch); err != nil {
				_ = writer.Close()
				return fmt.Errorf("write batch: %w", err)
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := writer.WritePointRows(batch); err != nil {
			_ = writer.Close()
			return fmt.Errorf("write final batch: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}

	// NewWriter 将文件写入 {shardDir}/data/sst_N.bin，移动到 windowDir
	dataFile := filepath.Join(windowDir, "data", fmt.Sprintf("sst_%d.bin", seq))
	finalFile := filepath.Join(windowDir, fmt.Sprintf("sst_%d.bin", seq))
	if err := os.Rename(dataFile, finalFile); err != nil {
		return fmt.Errorf("move sst file: %w", err)
	}
	_ = os.Remove(filepath.Join(windowDir, "data"))

	slog.Info("downsample shard completed",
		"window", time.Duration(windowNanos),
		"buckets", len(buckets),
		"file", finalFile)
	return nil
}

// buildDownsampledFields 为聚合桶构建字段列表。
func buildDownsampledFields(b *bucket, functions []string, windowSeconds float64) []*types.FieldEntry {
	var entries []*types.FieldEntry
	for _, fn := range functions {
		switch fn {
		case "avg":
			for field, acc := range b.accumulators {
				entries = append(entries, &types.FieldEntry{
					Key:   "avg_" + field,
					Value: types.NewFieldValue(acc.avg()),
				})
			}
		case "max":
			for field, acc := range b.accumulators {
				entries = append(entries, &types.FieldEntry{
					Key:   "max_" + field,
					Value: types.NewFieldValue(acc.max),
				})
			}
		case "min":
			for field, acc := range b.accumulators {
				entries = append(entries, &types.FieldEntry{
					Key:   "min_" + field,
					Value: types.NewFieldValue(acc.min),
				})
			}
		case "sum":
			for field, acc := range b.accumulators {
				entries = append(entries, &types.FieldEntry{
					Key:   "sum_" + field,
					Value: types.NewFieldValue(acc.sum),
				})
			}
		case "count":
			for field, acc := range b.accumulators {
				entries = append(entries, &types.FieldEntry{
					Key:   "count_" + field,
					Value: types.NewFieldValue(int64(acc.count)),
				})
			}
		case "first":
			for field, acc := range b.accumulators {
				entries = append(entries, &types.FieldEntry{
					Key:   "first_" + field,
					Value: types.NewFieldValue(acc.first),
				})
			}
		case "last":
			for field, acc := range b.accumulators {
				entries = append(entries, &types.FieldEntry{
					Key:   "last_" + field,
					Value: types.NewFieldValue(acc.last),
				})
			}
		case "diff":
			for field, acc := range b.accumulators {
				entries = append(entries, &types.FieldEntry{
					Key:   "diff_" + field,
					Value: types.NewFieldValue(acc.diff()),
				})
			}
		case "rate":
			for field, acc := range b.accumulators {
				entries = append(entries, &types.FieldEntry{
					Key:   "rate_" + field,
					Value: types.NewFieldValue(acc.rate(windowSeconds)),
				})
			}
		case "irate":
			for field, acc := range b.accumulators {
				entries = append(entries, &types.FieldEntry{
					Key:   "irate_" + field,
					Value: types.NewFieldValue(acc.irate()),
				})
			}
		case "derivative":
			for field, acc := range b.accumulators {
				entries = append(entries, &types.FieldEntry{
					Key:   "derivative_" + field,
					Value: types.NewFieldValue(acc.derivative(windowSeconds)),
				})
			}
		}
	}
	return entries
}

// ===================================
// 文件系统辅助函数
// ===================================

// parseShardDir 解析 shard 目录名 "{start}_{end}"。
func parseShardDir(name string) (start, end int64, ok bool) {
	parts := strings.SplitN(name, "_", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	s, err1 := strconv.ParseInt(parts[0], 10, 64)
	e, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return s, e, true
}

// listSSTFiles 列出目录中的 .bin 文件。
func listSSTFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".bin") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

// isDownsampleDone 检查窗口是否已完成降采样。
func isDownsampleDone(windowDir string) bool {
	_, err := os.Stat(filepath.Join(windowDir, "_downsample_done"))
	return err == nil
}

// markDownsampleDone 标记窗口降采样完成。
func markDownsampleDone(windowDir string) error {
	f, err := os.Create(filepath.Join(windowDir, "_downsample_done"))
	if err != nil {
		return err
	}
	return f.Close()
}

// nextSSTSeq 获取下一个 SSTable 序列号。
func nextSSTSeq(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 1, nil
		}
		return 0, err
	}
	maxSeq := 0
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, "sst_") && strings.HasSuffix(name, ".bin") {
			numStr := name[4 : len(name)-4]
			if n, err := strconv.Atoi(numStr); err == nil && n > maxSeq {
				maxSeq = n
			}
		}
	}
	return maxSeq + 1, nil
}
