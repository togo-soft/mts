package engine

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"codeberg.org/micro-ts/mts/internal/query"
	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/internal/storage/unordered"
	"codeberg.org/micro-ts/mts/types"
)

type scopedSeriesStore struct {
	inner SeriesStore
	db    string
	meas  string
}

func (s *scopedSeriesStore) AllocateSID(database, measurement string, tags map[string]string) (uint64, error) {
	return s.inner.AllocateSID(database, measurement, tags)
}

func (s *scopedSeriesStore) GetTags(database, measurement string, sid uint64) (map[string]string, bool) {
	// 在 nil shard 场景下，shard iterator 传入空 db/meas
	if database == "" && measurement == "" {
		return s.inner.GetTags(s.db, s.meas, sid)
	}
	return s.inner.GetTags(database, measurement, sid)
}

// Iterator 返回流式查询迭代器。
// 合并全局 MemTable（未刷盘数据）、Shard SSTable（已刷盘数据）和 unordered 文件（未 compaction 数据）。
// 当 req.DownsampleWindowNanos > 0 时，读取降采样数据而非原始数据。
func (e *Engine) Iterator(ctx context.Context, req *types.QueryRangeRequest) (*query.Iterator, error) {
	e.shutdownMu.Lock()
	if e.closed {
		e.shutdownMu.Unlock()
		return nil, fmt.Errorf("engine is closed")
	}
	e.queryWg.Add(1)
	e.shutdownMu.Unlock()
	defer e.queryWg.Done()

	// 等待启动恢复完成
	select {
	case <-e.recoveryDone:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if req.DownsampleWindowNanos > 0 {
		return e.downsampleIterator(ctx, req)
	}

	// 全局 MemTable 包含所有未刷盘数据
	writerMT := e.memTable

	shards := e.flusher.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)

	// 创建 scoped SeriesStore，确保 nil shard 场景下能正确解析 SID→Tags
	scoped := &scopedSeriesStore{
		inner: e.seriesStore,
		db:    req.Database,
		meas:  req.Measurement,
	}

	// 收集 unordered 目录下匹配 db/measurement 的文件数据
	unorderedData := e.collectUnorderedData(req)

	return query.NewIteratorWithMemTable(ctx, shards, writerMT, scoped, req, req.Fields, nil, unorderedData...), nil
}

// downsampleIterator 创建降采样查询迭代器。
func (e *Engine) downsampleIterator(ctx context.Context, req *types.QueryRangeRequest) (*query.Iterator, error) {
	downsampledData := e.collectDownsampledData(req)
	if len(downsampledData) == 0 {
		return nil, fmt.Errorf("no downsampled data found for %s/%s window=%d",
			req.Database, req.Measurement, req.DownsampleWindowNanos)
	}

	scoped := &scopedSeriesStore{
		inner: e.seriesStore,
		db:    req.Database,
		meas:  req.Measurement,
	}

	return query.NewIteratorWithMemTable(ctx, nil, nil, scoped, req, nil, nil, downsampledData...), nil
}

// collectDownsampledData 收集降采样数据。
func (e *Engine) collectDownsampledData(req *types.QueryRangeRequest) [][]*types.PointRow {
	windowNanos := req.DownsampleWindowNanos
	if windowNanos <= 0 {
		return nil
	}

	// 使用空 schema；降采样 SSTable 的解码依赖 section table 中的编码类型
	emptySchema := sstable.Schema{Fields: make(map[string]sstable.FieldType)}

	measDir := filepath.Join(e.dataDir, req.Database, req.Measurement)
	entries, err := os.ReadDir(measDir)
	if err != nil {
		return nil
	}

	var result [][]*types.PointRow

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// 解析 shard 目录名 {start}_{end}
		parts := strings.SplitN(entry.Name(), "_", 2)
		if len(parts) != 2 {
			continue
		}
		shardStart, err1 := strconv.ParseInt(parts[0], 10, 64)
		shardEnd, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil {
			slog.Debug("failed to parse shard dir name", "dir", entry.Name(), "err1", err1, "err2", err2)
			continue
		}

		// 检查 shard 是否与查询时间范围重叠
		if shardEnd <= req.StartTime || (req.EndTime > 0 && shardStart >= req.EndTime) {
			continue
		}

		shardDir := filepath.Join(measDir, entry.Name())
		windowDir := filepath.Join(shardDir, "downsampled", fmt.Sprintf("%d", windowNanos))

		sstFiles, listErr := listSSTFilesInDir(windowDir)
		if listErr != nil {
			slog.Debug("failed to list SST files in downsample dir", "dir", windowDir, "error", listErr)
		}
		if len(sstFiles) == 0 {
			continue
		}

		for _, f := range sstFiles {
			reader, rErr := sstable.NewReader(f, emptySchema)
			if rErr != nil {
				continue
			}

			rows, rdErr := reader.ReadAll(nil)
			_ = reader.Close()
			if rdErr != nil {
				continue
			}

			filtered := make([]*types.PointRow, 0, len(rows))
			for _, row := range rows {
				if row.Timestamp >= req.StartTime && (req.EndTime <= 0 || row.Timestamp < req.EndTime) {
					filtered = append(filtered, row)
				}
			}

			if len(filtered) == 0 {
				continue
			}

			sort.Slice(filtered, func(i, j int) bool {
				return filtered[i].Timestamp < filtered[j].Timestamp
			})

			result = append(result, filtered)
		}
	}

	return result
}

// listSSTFilesInDir 列出目录中的 SSTable 文件。
func listSSTFilesInDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "sst_") && strings.HasSuffix(e.Name(), ".bin") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

// Execute 执行查询计划，返回算子 Pipeline 迭代器。
func (e *Engine) Execute(ctx context.Context, plan *types.QueryPlan) (*query.RowIterator, error) {
	e.shutdownMu.Lock()
	if e.closed {
		e.shutdownMu.Unlock()
		return nil, fmt.Errorf("engine is closed")
	}
	e.queryWg.Add(1)
	e.shutdownMu.Unlock()
	defer e.queryWg.Done()

	// 等待启动恢复完成
	select {
	case <-e.recoveryDone:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	var projFields []string
	for _, op := range plan.Ops {
		if p := op.GetProject(); p != nil {
			projFields = p.Fields
			break
		}
	}

	// 从 FilterSpec 提取条件用于 ZoneMap 谓词下推
	var filterConds []sstable.FilterCondition
	for _, op := range plan.Ops {
		if f := op.GetFilter(); f != nil {
			for _, c := range f.Conditions {
				if c.Tag != "" {
					continue
				}
				var val float64
				if c.Value != nil {
					val = c.Value.GetFloatValue()
				}
				filterConds = append(filterConds, sstable.FilterCondition{
					Field: c.Field,
					Op:    int32(c.Op),
					Value: val,
				})
			}
			break
		}
	}

	dataIter, err := e.createDataIterator(ctx, plan.Database, plan.Measurement, plan.StartTime, plan.EndTime, projFields, filterConds)
	if err != nil {
		return nil, err
	}

	head, err := query.BuildPipeline(dataIter, plan.Ops)
	if err != nil {
		_ = dataIter.Close()
		return nil, fmt.Errorf("build pipeline: %w", err)
	}

	rowIter := query.NewRowIterator(head)
	if err := rowIter.Open(ctx); err != nil {
		_ = rowIter.Close()
		return nil, fmt.Errorf("open pipeline: %w", err)
	}

	return rowIter, nil
}

// createDataIterator 创建数据源 Iterator（共享逻辑，供 Iterator 和 Execute 使用）。
func (e *Engine) createDataIterator(ctx context.Context, database, measurement string, startTime, endTime int64, fields []string, filterConds []sstable.FilterCondition) (*query.Iterator, error) {
	writerMT := e.memTable

	req := &types.QueryRangeRequest{
		Database:    database,
		Measurement: measurement,
		StartTime:   startTime,
		EndTime:     endTime,
	}

	shards := e.flusher.GetShards(database, measurement, startTime, endTime)

	scoped := &scopedSeriesStore{
		inner: e.seriesStore,
		db:    database,
		meas:  measurement,
	}

	unorderedData := e.collectUnorderedData(req)

	return query.NewIteratorWithMemTable(ctx, shards, writerMT, scoped, req, fields, filterConds, unorderedData...), nil
}

// IteratorWithMemTable 是包内使用的辅助函数（供测试等场景使用）。
func IteratorWithMemTable(ctx context.Context, shards []*shard.Shard, wmt *memtable.MemTable, extSeriesStore shard.SeriesStore, req *types.QueryRangeRequest) *query.Iterator {
	return query.NewIteratorWithMemTable(ctx, shards, wmt, extSeriesStore, req, req.Fields, nil)
}


// collectUnorderedData 收集 unordered 目录下匹配 db/measurement 的数据，
// 使用 Iterator 逐行过滤，避免 ReadAll 全量加载后再二次分配。
func (e *Engine) collectUnorderedData(req *types.QueryRangeRequest) [][]*types.PointRow {
	unorderedFiles, err := unordered.ListFiles(e.dataDir)
	if err != nil || len(unorderedFiles) == 0 {
		return nil
	}

	metaSchema, schemaErr := e.catalog.GetSchema(req.Database, req.Measurement)
	if schemaErr != nil {
		return nil
	}
	sstSchema := shard.MetadataSchemaToSSTableSchema(metaSchema)

	result := make([][]*types.PointRow, 0, len(unorderedFiles))

	for _, f := range unorderedFiles {
		db, meas, _, ok := unordered.ParseFilePath(e.dataDir, f)
		if !ok || db != req.Database || meas != req.Measurement {
			continue
		}

		reader, err := sstable.NewReader(f, sstSchema)
		if err != nil {
			continue
		}

		it, err := reader.NewIterator(req.Fields, nil)
		if err != nil {
			_ = reader.Close()
			continue
		}

		var filtered []*types.PointRow
		for it.Next() {
			row := it.Point()
			if row.Timestamp >= req.StartTime && (req.EndTime <= 0 || row.Timestamp < req.EndTime) {
				tags, _ := e.seriesStore.GetTags(req.Database, req.Measurement, row.Sid)
				row.Tags = tags
				filtered = append(filtered, row)
			}
		}

		_ = reader.Close()

		if len(filtered) == 0 {
			continue
		}

		sort.Slice(filtered, func(i, j int) bool {
			return filtered[i].Timestamp < filtered[j].Timestamp
		})

		result = append(result, filtered)
	}

	return result
}
