// Package microts 提供高性能时序数据库功能。
//
// microts（micro time-series）是一个专为高性能时间序列数据存储设计的数据库。
// 它支持高效的点写入、批量写入和范围查询操作。
//
// 基本使用示例：
//
//	db, err := microts.Open(microts.Config{
//	    DataDir:       "/var/lib/microts",
//	    ShardDuration: 7 * 24 * time.Hour,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	// 写入数据点
//	point := &microts.Point{
//	    Database:    "metrics",
//	    Measurement: "cpu",
//	    Tags:        map[string]string{"host": "server1"},
//	    Timestamp:   time.Now().UnixNano(),
//	    Fields:      map[string]any{"usage": 45.2},
//	}
//	if err := db.Write(ctx, point); err != nil {
//	    log.Fatal(err)
//	}
package microts

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"codeberg.org/micro-ts/mts/internal/engine"
	"codeberg.org/micro-ts/mts/internal/query"
	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/types"
)

// 重导出公共类型，方便用户使用
type (
	// Point 是写入时序数据库的基本数据单元。
	//
	// 每个 Point 属于特定的 Database 和 Measurement，包含时间戳、标签集合和字段值。
	// 标签用于标识时间序列，字段存储实际的数据值。
	// Timestamp 使用纳秒级 Unix 时间戳。
	//
	// 使用示例：
	//
	//	point := &microts.Point{
	//	    Database:    "metrics",
	//	    Measurement: "cpu_usage",
	//	    Tags: map[string]string{
	//	        "host":   "server1",
	//	        "region": "us-east-1",
	//	    },
	//	    Timestamp: time.Now().UnixNano(),
	//	    Fields: map[string]any{
	//	        "value": 45.2,
	//	    },
	//	}
	Point = types.Point

	// PointRow 是查询结果的单行数据。
	//
	// 包含 Series ID (SID)、时间戳、标签和字段值。
	// SID 是内部生成的序列标识符，用户通常不需要直接使用。
	PointRow = types.PointRow

	// QueryRangeRequest 定义范围查询的请求参数。
	//
	// 支持按时间范围 (StartTime, EndTime)、字段列表和标签过滤器查询数据。
	// Offset 和 Limit 用于分页查询，Limit = 0 表示不限制。
	// 时间戳使用纳秒级 Unix 时间戳。
	//
	// 使用示例：
	//
	//	req := &microts.QueryRangeRequest{
	//	    Database:    "metrics",
	//	    Measurement: "cpu_usage",
	//	    StartTime:   startTime.UnixNano(),
	//	    EndTime:     endTime.UnixNano(),
	//	    Fields:      []string{"value"},           // 只返回指定字段
	//	    Tags:        map[string]string{"host": "server1"}, // 过滤标签
	//	    Offset:      0,
	//	    Limit:       1000,
	//	}
	QueryRangeRequest = types.QueryRangeRequest

	// QueryRangeResponse 返回范围查询的结果。
	//
	// 包含查询元数据和实际的行数据。
	// HasMore 表示是否还有更多数据，用于分页处理。
	QueryRangeResponse = types.QueryRangeResponse

	// MemTableConfig 配置内存表的行为。
	//
	// 控制 MemTable 的大小、条目数和空闲时间，当达到任一阈值时会触发刷盘。
	//
	// 使用示例：
	//
	//	cfg := microts.MemTableConfig{
	//	    MaxSize:      64 * 1024 * 1024, // 64MB
	//	    MaxCount:     10000,
	//	    IdleDuration: 5 * time.Minute,
	//	}
	MemTableConfig = types.MemTableConfig

	// CompactionConfig 配置 Compaction 行为。
	//
	// 控制 Compaction 的触发策略、批处理大小和资源限制。
	//
	// 使用示例：
	//
	//	cfg := microts.CompactionConfig{
	//	    MaxSSTableCount: 4,
	//	    CheckInterval:   1 * time.Hour,
	//	}
	CompactionConfig = types.CompactionConfig
)

// Config 是数据库的配置选项。
//
// DataDir 指定数据存储目录，必须可写。
// ShardDuration 定义每个 shard 的时间窗口，最小 1 小时，默认 7 天。
// MemTableCfg 配置内存表行为，使用 nil 时将采用 DefaultMemTableConfig() 的默认值。
// CompactionCfg 配置 compaction 行为，使用 nil 时将采用默认配置。
// RetentionPeriod 配置数据保留期，0 表示不自动删除过期数据。
// RetentionCheckInterval 配置 retention 检查间隔，默认 1 小时。
type Config struct {
	DataDir                string
	ShardDuration          time.Duration
	MemTableCfg            *types.MemTableConfig
	CompactionCfg          *types.CompactionConfig
	RetentionPeriod        time.Duration
	RetentionCheckInterval time.Duration
}

// DefaultMemTableConfig 返回默认的 MemTable 配置。
//
// 默认配置：
//   - MaxSize: 64MB，内存表最大内存占用
//   - MaxCount: 3000，最大条目数
//   - IdleDuration: 1分钟，空闲时间阈值
//
// 返回：
//
//	MemTableConfig: 包含默认值的配置结构
//
// 使用示例：
//
//	cfg := microts.Config{
//	    DataDir:       "/data",
//	    MemTableCfg:   microts.DefaultMemTableConfig(),
//	}
func DefaultMemTableConfig() *types.MemTableConfig {
	return &types.MemTableConfig{
		MaxSize:           64 * 1024 * 1024,
		MaxCount:          3000,
		IdleDurationNanos: int64(time.Minute),
	}
}

// DB 是微时序数据库的主结构。
//
// 提供完整的时序数据存储和查询功能。
// DB 实例是并发安全的，可以从多个 goroutine 同时访问。
//
// 使用 Open 函数创建 DB 实例，使用 Close 方法关闭。
// 关闭后不可再使用，否则会导致未定义行为。
type DB struct {
	engine *engine.Engine
}

// Open 打开或创建一个时序数据库实例。
//
// 参数：
//   - cfg: 数据库配置，包含数据目录、分片时长和 MemTable 配置
//
// 返回：
//   - *DB: 数据库实例
//   - error: 如果目录创建失败或引擎初始化失败则返回错误
//
// 配置文件默认值：
//   - ShardDuration: 默认为 7 天
//   - MemTableCfg: 使用 DefaultMemTableConfig()
//
// 使用示例：
//
//	db, err := microts.Open(microts.Config{
//	    DataDir:       "/var/lib/microts",
//	    ShardDuration: 7 * 24 * time.Hour,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
// 注意：必须调用 Close 释放资源。
func Open(cfg Config) (*DB, error) {
	// 默认ShardDuration为7天
	shardDuration := cfg.ShardDuration
	if shardDuration == 0 {
		shardDuration = 7 * 24 * time.Hour
	}

	// 默认 MemTable 配置
	memTableCfg := cfg.MemTableCfg
	if memTableCfg == nil || memTableCfg.MaxSize == 0 {
		memTableCfg = DefaultMemTableConfig()
	}

	// 默认 Compaction 配置
	var compactionCfg *shard.CompactionConfig
	if cfg.CompactionCfg != nil {
		compactionCfg = &shard.CompactionConfig{
			MaxSSTableCount:    cfg.CompactionCfg.MaxSSTableCount,
			MaxCompactionBatch: cfg.CompactionCfg.MaxCompactionBatch,
			ShardSizeLimit:     cfg.CompactionCfg.ShardSizeLimit,
			CheckInterval:      cfg.CompactionCfg.CheckInterval,
			Timeout:            cfg.CompactionCfg.Timeout,
		}
	}

	// 确保数据目录存在
	if cfg.DataDir != "" {
		if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
			return nil, fmt.Errorf("create data directory: %w", err)
		}
	}

	eng, err := engine.New(&engine.Config{
		DataDir:                cfg.DataDir,
		ShardDuration:          shardDuration,
		MemTableCfg:            memTableCfg,
		CompactionCfg:          compactionCfg,
		RetentionPeriod:        cfg.RetentionPeriod,
		RetentionCheckInterval: cfg.RetentionCheckInterval,
	})
	if err != nil {
		return nil, err
	}
	return &DB{engine: eng}, nil
}

// Write 写入单个数据点到数据库。
//
// 参数：
//   - ctx: 上下文，可用于取消操作（会透传到引擎层）
//   - point: 要写入的数据点，包含时间戳、标签和字段
//
// 返回：
//   - error: 写入失败时返回错误，错误类型包括 IO 错误和序列化错误
//
// 时间戳排序：
//
//	MemTable 会自动维护按时间戳排序的条目。
//	当写入乱序数据时，会触发内部排序操作。
//
// 持久化保证：
//
//	数据首先写入 WAL (Write-Ahead Log)，然后写入 MemTable。
//	只有 WAL 写入成功后，写入才被认为是成功的。
//
// 使用示例：
//
//	point := &microts.Point{
//	    Database:    "metrics",
//	    Measurement: "cpu",
//	    Tags:        map[string]string{"host": "server1"},
//	    Timestamp:   time.Now().UnixNano(),
//	    Fields:      map[string]any{"usage": 45.2},
//	}
//	if err := db.Write(ctx, point); err != nil {
//	    log.Printf("写入失败: %v", err)
//	}
func (db *DB) Write(ctx context.Context, point *types.Point) error {
	return db.engine.Write(ctx, point)
}

// WriteBatch 批量写入多个数据点。
//
// 相比多次调用 Write，批量写入可以减少系统调用开销，提高吞吐量。
// 批量写入是原子失败的：如果任何一个点写入失败，不会回滚已写入的点。
//
// 参数：
//   - ctx: 上下文
//   - points: 要写入的数据点切片
//
// 返回：
//   - error: 写入失败时返回错误，包含失败点的时间戳信息
//
// 性能建议：
//
//	建议每个批次包含 100-1000 个点，具体取决于数据点大小。
//	批次过大可能导致内存占用过高。
//
// 使用示例：
//
//	points := []*microts.Point{p1, p2, p3}
//	if err := db.WriteBatch(ctx, points); err != nil {
//	    log.Printf("批量写入失败: %v", err)
//	}
func (db *DB) WriteBatch(ctx context.Context, points []*types.Point) error {
	return db.engine.WriteBatch(ctx, points)
}

// QueryRange 执行范围查询，返回指定时间范围内的数据点。
//
// 查询会自动合并 MemTable（内存数据）和 SSTable（磁盘数据）的结果。
// 数据按时间戳升序返回。
//
// 参数：
//   - ctx: 上下文，可用于取消查询
//   - req: 查询请求，包含时间范围、字段列表、标签过滤和分页参数
//
// 返回：
//   - *QueryRangeResponse: 包含查询结果行、总数和是否有更多数据
//   - error: 查询失败时返回错误
//
// 分页处理：
//
//	使用 Offset 和 Limit 进行分页。当 HasMore 为 true 时，
//	可以通过设置 Offset = 已返回行数 来获取下一页。
//
// 字段过滤：
//
//	如果 Fields 为空，返回所有字段。
//	如果指定字段，只返回这些字段的值。
//
// 使用示例：
//
//	resp, err := db.QueryRange(ctx, &microts.QueryRangeRequest{
//	    Database:    "metrics",
//	    Measurement: "cpu",
//	    StartTime:   start.UnixNano(),
//	    EndTime:     end.UnixNano(),
//	    Fields:      []string{"usage", "temperature"},
//	    Tags:        map[string]string{"host": "server1"},
//	    Offset:      0,
//	    Limit:       1000,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for _, row := range resp.Rows {
//	    fmt.Printf("时间: %d, 字段: %v\n", row.Timestamp, row.Fields)
//	}
//	if resp.HasMore {
//	    // 获取下一页
//	}
func (db *DB) QueryRange(ctx context.Context, req *types.QueryRangeRequest) (*types.QueryRangeResponse, error) {
	return db.engine.Query(ctx, req)
}

// QueryIterator 创建流式查询迭代器，用于处理大量数据而不占用大量内存。
//
// 相比 QueryRange，流式迭代器只在需要时加载数据，适合处理海量数据查询。
//
// 参数：
//   - ctx: 上下文，可用于取消迭代
//   - req: 查询请求，支持时间范围、字段过滤和标签过滤
//
// 返回：
//   - *query.QueryIterator: 查询迭代器，使用完后必须调用 Close()
//   - error: 创建失败时返回错误
//
// 使用示例：
//
//	it, err := db.QueryIterator(ctx, &microts.QueryRangeRequest{
//	    Database:    "metrics",
//	    Measurement: "cpu",
//	    StartTime:   start.UnixNano(),
//	    EndTime:     end.UnixNano(),
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer it.Close()
//
//	for it.Next(ctx) {
//	    row := it.Points()
//	    process(row)
//	}
func (db *DB) QueryIterator(ctx context.Context, req *types.QueryRangeRequest) (*query.QueryIterator, error) {
	return db.engine.QueryIterator(ctx, req)
}

// ListMeasurements 列出指定数据库中的所有 Measurement 名称。
//
// 参数：
//   - ctx: 上下文
//   - database: 数据库名称
//
// 返回：
//   - []string: Measurement 名称列表（按字母序排序）
//   - error: 如果数据库不存在返回错误
//
// 实现说明：
//
//	从 Engine 的 DatabaseMetaStore 获取元数据，遍历 measurement keys。
//	这反映了当前已知的所有 measurement（有元数据的）。
//	返回的列表按字母序排序。
//
// 使用示例：
//
//	measurements, err := db.ListMeasurements(ctx, "metrics")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for _, m := range measurements {
//	    fmt.Println(m)
//	}
func (db *DB) ListMeasurements(ctx context.Context, database string) ([]string, error) {
	_ = ctx // 保留参数以符合接口约定
	measurements, found := db.engine.ListMeasurements(database)
	if !found {
		// 数据库不存在时返回空列表（与旧行为兼容）
		return []string{}, nil
	}
	// 按字母序排序
	sort.Strings(measurements)
	return measurements, nil
}

// CreateMeasurement 在指定数据库中创建一个新的 Measurement。
//
// 参数：
//   - ctx: 上下文
//   - database: 数据库名称
//   - measurement: Measurement 名称
//
// 返回：
//   - error: 创建失败时返回错误
//
// 说明：
//
//	创建 DatabaseMetaStore（如果不存在）并创建 MeasurementMetaStore。
//	实际的数据目录在第一次写入时才会创建。
//	如果 measurement 已存在，不会返回错误。
//
// 使用示例：
//
//	if err := db.CreateMeasurement(ctx, "metrics", "cpu_usage"); err != nil {
//	    log.Fatal(err)
//	}
func (db *DB) CreateMeasurement(ctx context.Context, database, measurement string) error {
	_ = ctx // 保留参数以符合接口约定
	_, _ = db.engine.CreateMeasurement(database, measurement)
	return nil
}

// DropMeasurement 删除指定的 Measurement。
//
// 参数：
//   - ctx: 上下文
//   - database: 数据库名称
//   - measurement: Measurement 名称
//
// 返回：
//   - error: 删除失败时返回错误
//
// 警告：
//
//	此操作会永久删除该 Measurement 的元数据（Schema、Series、Tag 索引）。
//	磁盘上的数据文件不会被立即删除，需要单独清理。
//	如果数据库或 Measurement 不存在，返回错误。
//
// 使用示例：
//
//	if err := db.DropMeasurement(ctx, "metrics", "old_metric"); err != nil {
//	    log.Fatal(err)
//	}
func (db *DB) DropMeasurement(ctx context.Context, database, measurement string) error {
	_ = ctx // 保留参数以符合接口约定
	found, err := db.engine.DropMeasurement(database, measurement)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("measurement not found: %s/%s", database, measurement)
	}
	return nil
}

// ListDatabases 列出所有数据库名称。
//
// 参数：
//   - ctx: 上下文
//
// 返回：
//   - []string: 数据库名称列表（按字母序排序）
//   - error: 查询失败时返回错误（当前不会返回错误）
//
// 实现说明：
//
//	从 Engine 的 dbMetaStores 获取所有数据库名称。
//	这反映了当前已知的所有数据库（有元数据的）。
//	返回的列表按字母序排序。
//
// 使用示例：
//
//	databases, err := db.ListDatabases(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for _, dbName := range databases {
//	    fmt.Println(dbName)
//	}
func (db *DB) ListDatabases(ctx context.Context) ([]string, error) {
	_ = ctx // 保留参数以符合接口约定
	databases := db.engine.ListDatabases()
	// 按字母序排序
	sort.Strings(databases)
	return databases, nil
}

// CreateDatabase 创建一个新的数据库。
//
// 参数：
//   - ctx: 上下文
//   - database: 数据库名称
//
// 返回：
//   - error: 创建失败时返回错误（当前不会返回错误）
//
// 说明：
//
//	创建 DatabaseMetaStore 并注册到 Engine。
//	实际的数据目录在第一次写入时才会创建。
//	如果数据库已存在，不会返回错误。
//
// 使用示例：
//
//	if err := db.CreateDatabase(ctx, "new_metrics"); err != nil {
//	    log.Fatal(err)
//	}
func (db *DB) CreateDatabase(ctx context.Context, database string) error {
	_ = ctx // 保留参数以符合接口约定
	_ = db.engine.CreateDatabase(database)
	return nil
}

// DropDatabase 删除指定的数据库。
//
// 参数：
//   - ctx: 上下文
//   - database: 数据库名称
//
// 返回：
//   - error: 删除失败时返回错误
//
// 警告：
//
//	此操作会永久删除该数据库的所有元数据（包括所有 Measurement）。
//	磁盘上的数据文件不会被立即删除，需要单独清理。
//	如果数据库不存在，返回错误。
//
// 使用示例：
//
//	if err := db.DropDatabase(ctx, "old_metrics"); err != nil {
//	    log.Fatal(err)
//	}
func (db *DB) DropDatabase(ctx context.Context, database string) error {
	_ = ctx // 保留参数以符合接口约定
	found := db.engine.DropDatabase(database)
	if !found {
		return fmt.Errorf("database not found: %s", database)
	}
	return nil
}

// Close 关闭数据库，释放所有资源。
//
// 返回：
//   - error: 关闭失败时返回错误
//
// 注意：
//
//	关闭前会尝试将所有 MemTable 数据刷写到 SSTable。
//	关闭后 DB 实例不可再使用。
//	多个 Close() 调用是安全的，但只有第一个会生效。
//	建议配合 defer 使用：
//
//		db, err := microts.Open(cfg)
//		if err != nil {
//		    log.Fatal(err)
//		}
//		defer db.Close()
func (db *DB) Close() error {
	if db.engine == nil {
		return nil
	}

	// 先刷盘，确保所有内存数据持久化
	_ = db.engine.Flush()

	return db.engine.Close()
}

// DataDir 返回数据库的数据目录路径。
//
// 返回：
//   - string: 数据目录路径
func (db *DB) DataDir() string {
	return db.engine.DataDir()
}

// CreateEmptyMeasurement 创建一个空目录结构的 Measurement（用于兼容旧 API）。
// 此函数在首次写入数据时自动调用，通常不需要手动调用。
//
// 参数：
//   - database: 数据库名称
//   - measurement: Measurement 名称
//
// 返回：
//   - error: 创建失败时返回错误
func (db *DB) CreateEmptyMeasurement(database, measurement string) error {
	measurementPath := filepath.Join(db.engine.DataDir(), database, measurement)
	if err := os.MkdirAll(measurementPath, 0700); err != nil {
		return fmt.Errorf("create measurement directory: %w", err)
	}
	return nil
}

// FlushAll 刷新所有 Shard 的 MemTable 到 SSTable。
//
// 返回：
//   - error: 如果任一 Shard 刷盘失败则返回错误
//
// 说明：
//
//	遍历所有已创建的 Shard，调用其 Flush() 方法。
//	用于确保所有内存数据持久化，或在关闭前确保数据安全。
func (db *DB) FlushAll() error {
	return db.engine.Flush()
}
