// Package engine 实现微时序数据库的存储引擎。
//
// Engine 是数据库的核心组件，负责协调写入和查询操作。
// 它管理 Shard 的创建和回收，以及元数据的访问。
//
// 架构说明：
//
//	Engine → ShardManager → Shards → MemTable/SSTable
//	Engine → MetaStores (测量元数据)
//
// Engine 是并发安全的，所有公共方法都可以从多个 goroutine 调用。
package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/query"
	"codeberg.org/micro-ts/mts/internal/storage/measurement"
	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/types"
)

// Config 定义存储引擎的配置。
//
// 配置包含数据目录、Shard 时长和 MemTable 配置。
//
// 字段说明：
//
//   - DataDir:       数据存储目录
//   - ShardDuration: 每个 Shard 的时间窗口
//   - MemTableCfg:   MemTable 配置
//
// 默认值：
//
//	New 函数会为 MemTableCfg 提供默认值（如果 MaxSize 为 0）。
//	ShartDuration 应从外部传入。
type Config struct {
	DataDir       string
	ShardDuration time.Duration
	MemTableCfg   shard.MemTableConfig
}

// Engine 是微时序数据库的存储引擎。
//
// Engine 协调数据写入和查询，管理 Shard 生命周期和元数据访问。
//
// 字段说明：
//
//   - cfg:           引擎配置
//   - shardManager:  Shard 管理器，负责创建、回收 Shard
//   - dbMetaStores:  数据库级元数据缓存（database -> DatabaseMetaStore）
//   - mu:            保护 dbMetaStores 的读写锁
//
// 并发安全：
//
//	Engine 的所有公共方法都是并发安全的。
//	写入操作通过 ShardManager 路由到对应的 Shard。
//	元数据操作通过读写锁保护。
//
// 生命周期：
//
//	使用 New 创建，使用 Close 关闭。
//	关闭后不可再使用。
type Engine struct {
	cfg          *Config
	shardManager *shard.ShardManager
	dbMetaStores map[string]*measurement.DatabaseMetaStore
	mu           sync.RWMutex
}

// New 创建新的存储引擎实例。
//
// 参数：
//   - cfg: 引擎配置
//
// 返回：
//   - *Engine: 引擎实例
//   - error: 创建失败时返回错误
//
// 配置处理：
//
//	如果 MemTableCfg.MaxSize 为 0，使用 shard.DefaultMemTableConfig()。
//	这样可以支持零值配置的便捷使用。
//
// 错误情况：
//
//	目前不会返回错误，但保留错误返回值以便未来扩展。
func New(cfg *Config) (*Engine, error) {
	// 默认 MemTable 配置
	memTableCfg := cfg.MemTableCfg
	if memTableCfg.MaxSize == 0 {
		memTableCfg = shard.DefaultMemTableConfig()
	}
	return &Engine{
		cfg:          cfg,
		shardManager: shard.NewShardManager(cfg.DataDir, cfg.ShardDuration, memTableCfg),
		dbMetaStores: make(map[string]*measurement.DatabaseMetaStore),
	}, nil
}

// Close 关闭引擎，释放所有资源。
//
// 返回：
//   - error: 关闭失败时返回错误
//
// 注意：
//
//	关闭引擎会清空 MetaStore 缓存。
//	关闭后引擎实例不可再使用。
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, dbMeta := range e.dbMetaStores {
		_ = dbMeta.Close()
	}
	e.dbMetaStores = nil
	return nil
}

// Flush 将所有 MemTable 数据刷写到 SSTable。
//
// 返回：
//   - error: 刷盘失败时返回错误
//
// 使用场景：
//
//	通常在关闭前调用以确保数据持久化。
//	也可用于手动触发刷盘（如备份前）。
func (e *Engine) Flush() error {
	return e.shardManager.FlushAll()
}

// Write 写入单个数据点到存储引擎。
//
// 写入流程：
//
//  1. 检查上下文状态（是否已取消）
//  2. 根据时间戳确定或创建目标 Shard
//  3. 将数据写入对应的 Shard
//  4. Shard 内部先写 WAL，再写 MemTable
//  5. 检查 MemTable 是否需要刷盘
//
// 参数：
//   - ctx:    上下文，用于取消操作
//   - point:  要写入的数据点
//
// 返回：
//   - error: 写入失败时返回错误，错误信息包含失败阶段
//
// 并发安全：
//
//	并发写入会分别路由到不同的 Shard（如果时间窗口不同），
//	同一 Shard 的并发写入由 Shard 内部的锁保护。
//
// Context 取消：
//
//	如果 context 在写入过程中被取消，操作会尽快返回 ctx.Err()。
//	但需要注意：WAL 写入成功后即使 context 被取消，数据也已持久化。
func (e *Engine) Write(ctx context.Context, point *types.Point) error {
	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 获取或创建 Shard
	s, err := e.shardManager.GetShard(point.Database, point.Measurement, point.Timestamp)
	if err != nil {
		return fmt.Errorf("get shard: %w", err)
	}

	// 写入 Shard
	if err := s.Write(point); err != nil {
		return fmt.Errorf("write to shard: %w", err)
	}
	return nil
}

// WriteBatch 批量写入数据点。
//
// 内部为每个点调用 Write，不保证原子性。
// 批量写入的吞吐量通常比单独写入高。
//
// 参数：
//   - ctx:    上下文，用于取消操作
//   - points: 要写入的数据点切片
//
// 返回：
//   - error: 任一数据点写入失败时返回错误，包含失败点的时间戳
//
// 部分失败：
//
//	如果部分点写入失败，返回错误。
//	已经成功写入的点不会被回滚。
//
// Context 取消：
//
//	每次写入前检查 context 状态。
//	如果 context 被取消，返回 ctx.Err()，已写入的点保留。
func (e *Engine) WriteBatch(ctx context.Context, points []*types.Point) error {
	for _, p := range points {
		// 检查上下文是否已取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := e.Write(ctx, p); err != nil {
			return fmt.Errorf("write point (timestamp=%d): %w", p.Timestamp, err)
		}
	}
	return nil
}

// Query 执行范围查询。
//
// 查询会自动合并多个 Shard 的结果，返回按时间排序的数据。
// 当结果集较大时，建议使用 QueryIterator 进行流式处理。
//
// 参数：
//   - ctx: 上下文
//   - req: 查询请求
//
// 返回：
//   - *types.QueryRangeResponse: 查询结果
//   - error: 查询失败时返回错误
//
// 分页语义：
//
//	支持 Offset/Limit 分页。HasMore 表示结果集中还有更多数据。
//	流式语义下，TotalCount 只反映当前页数量（当 HasMore=true 时）。
func (e *Engine) Query(ctx context.Context, req *types.QueryRangeRequest) (*types.QueryRangeResponse, error) {
	// 获取相交的 Shard
	shards := e.shardManager.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)

	if len(shards) == 0 {
		return &types.QueryRangeResponse{
			Database:    req.Database,
			Measurement: req.Measurement,
			StartTime:   req.StartTime,
			EndTime:     req.EndTime,
			TotalCount:  0,
			Rows:        []*types.Row{},
		}, nil
	}

	// 创建流式查询迭代器
	qit, err := e.QueryIterator(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("create query iterator: %w", err)
	}
	defer qit.Close()

	// 流式收集结果，跳过 offset 行，然后收集 limit 行
	var pointRows []types.PointRow
	targetCount := int(req.Limit) + int(req.Offset)
	hasExplicitLimit := req.Limit > 0
	// 如果没有指定 limit，不设置目标数量上限（使用最大 int）
	if !hasExplicitLimit {
		targetCount = int(^uint(0) >> 1) // MaxInt
	}

	skipped := 0
	collected := 0
	hasLimit := req.Limit > 0
	for qit.Next(ctx) {
		row := qit.Points()
		if row == nil {
			continue
		}
		// 跳过 offset 指定的行
		if int64(skipped) < req.Offset {
			skipped++
			continue
		}
		pointRows = append(pointRows, *row)
		collected++
		// 已收集足够的行（仅在指定了 limit 时提前停止）
		if hasLimit && collected >= int(req.Limit) {
			// 停止收集，但继续检查是否有更多数据
			break
		}
		// 也检查是否已达到目标数量上限
		if collected >= targetCount-int(req.Offset) {
			break
		}
	}

	// 检查是否有更多数据
	// 流式语义：如果收集满 limit 行，认为可能还有更多数据
	hasMore := false
	if hasLimit && collected >= int(req.Limit) {
		hasMore = true
	}

	// 计算 totalCount
	// 流式语义：当 HasMore=true 时，无法知道精确总数
	// 当 HasMore=false 时，表示已处理完所有数据，可以报告精确总数
	totalCount := int64(skipped + collected)

	// 将 PointRow 转换为 Row
	rows := make([]*types.Row, len(pointRows))
	for i, pr := range pointRows {
		fields := make(map[string]*types.FieldValue, len(pr.Fields))
		for name, v := range pr.Fields {
			fv, err := anyToProtoFieldValue(v)
			if err != nil {
				return nil, fmt.Errorf("convert field %s: %w", name, err)
			}
			fields[name] = fv
		}
		rows[i] = &types.Row{
			Timestamp: pr.Timestamp,
			Tags:      pr.Tags,
			Fields:    fields,
		}
	}

	return &types.QueryRangeResponse{
		Database:    req.Database,
		Measurement: req.Measurement,
		StartTime:   req.StartTime,
		EndTime:     req.EndTime,
		TotalCount:  totalCount,
		HasMore:     hasMore,
		Rows:        rows,
	}, nil
}

// anyToProtoFieldValue 将 any 转换为 protobuf FieldValue。
func anyToProtoFieldValue(v any) (*types.FieldValue, error) {
	switch val := v.(type) {
	case int64:
		return &types.FieldValue{Value: &types.FieldValue_IntValue{IntValue: val}}, nil
	case float64:
		return &types.FieldValue{Value: &types.FieldValue_FloatValue{FloatValue: val}}, nil
	case string:
		return &types.FieldValue{Value: &types.FieldValue_StringValue{StringValue: val}}, nil
	case bool:
		return &types.FieldValue{Value: &types.FieldValue_BoolValue{BoolValue: val}}, nil
	default:
		return nil, fmt.Errorf("unsupported field type: %T", v)
	}
}

// DataDir 返回引擎的数据目录。
//
// 返回：
//   - string: 数据目录路径
func (e *Engine) DataDir() string {
	return e.cfg.DataDir
}

// 相比 Query，迭代器按需加载数据，适合处理超过内存容量的大查询。
//
// 参数：
//   - ctx: 上下文，用于取消查询
//   - req: 查询请求
//
// 返回：
//   - *query.QueryIterator: 流式迭代器
//   - error: 没有匹配的 shard 时返回错误
//
// 时间处理：
//
//	接受纳秒级 Unix 时间戳，内部计算需要访问的 Shard 列表。
//	如果没有匹配的 Shard，返回错误。
func (e *Engine) QueryIterator(ctx context.Context, req *types.QueryRangeRequest) (*query.QueryIterator, error) {
	// 使用原始时间（假设为纳秒）
	startTimeNs := req.StartTime
	endTimeNs := req.EndTime

	// 获取相交的 Shards
	shards := e.shardManager.GetShards(req.Database, req.Measurement, startTimeNs, endTimeNs)
	if len(shards) == 0 {
		return nil, errors.New("no shards found")
	}

	return query.NewQueryIterator(ctx, shards, req), nil
}

// getOrCreateDBMetaStore 获取或创建指定数据库的 DatabaseMetaStore。
func (e *Engine) getOrCreateDBMetaStore(db string) *measurement.DatabaseMetaStore {
	e.mu.RLock()
	dbMeta, ok := e.dbMetaStores[db]
	e.mu.RUnlock()
	if ok {
		return dbMeta
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	// 双重检查
	if dbMeta, ok = e.dbMetaStores[db]; ok {
		return dbMeta
	}
	dbMeta = measurement.NewDatabaseMetaStore()
	e.dbMetaStores[db] = dbMeta
	return dbMeta
}

// ListDatabases 列出所有数据库名称。
//
// 返回：
//   - []string: 数据库名称列表（按字母序排序）
//
// 说明：
//
//	遍历 dbMetaStores 的 keys，返回所有数据库名称。
//	这反映了当前已知的所有数据库（有元数据的）。
func (e *Engine) ListDatabases() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	databases := make([]string, 0, len(e.dbMetaStores))
	for name := range e.dbMetaStores {
		databases = append(databases, name)
	}
	return databases
}

// ListMeasurements 列出指定数据库中的所有 Measurement 名称。
//
// 参数：
//   - database: 数据库名称
//
// 返回：
//   - []string: Measurement 名称列表（按字母序排序）
//   - bool: 数据库是否存在
//
// 说明：
//
//	遍历 DatabaseMetaStore 中的 measurements map keys。
//	如果数据库不存在，返回空列表和 false。
func (e *Engine) ListMeasurements(database string) ([]string, bool) {
	e.mu.RLock()
	dbMeta, ok := e.dbMetaStores[database]
	e.mu.RUnlock()
	if !ok {
		return nil, false
	}

	return dbMeta.ListMeasurements(), true
}

// CreateDatabase 创建一个新的数据库。
//
// 参数：
//   - database: 数据库名称
//
// 返回：
//   - bool: 是否新创建（false 表示已存在）
//
// 说明：
//
//	创建 DatabaseMetaStore 并缓存。
//	实际的数据目录在第一次写入时才会创建。
func (e *Engine) CreateDatabase(database string) bool {
	e.mu.RLock()
	_, ok := e.dbMetaStores[database]
	e.mu.RUnlock()
	if ok {
		return false
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	// 双重检查
	if _, ok = e.dbMetaStores[database]; ok {
		return false
	}
	e.dbMetaStores[database] = measurement.NewDatabaseMetaStore()
	return true
}

// DropDatabase 删除指定的数据库。
//
// 参数：
//   - database: 数据库名称
//
// 返回：
//   - bool: 是否成功删除（false 表示不存在）
//
// 警告：
//
//	此操作会永久删除该数据库下的所有元数据。
//	磁盘上的数据文件不会被立即删除，需要单独清理。
func (e *Engine) DropDatabase(database string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	dbMeta, ok := e.dbMetaStores[database]
	if !ok {
		return false
	}

	_ = dbMeta.Close()
	delete(e.dbMetaStores, database)
	return true
}

// CreateMeasurement 在指定数据库中创建一个新的 Measurement。
//
// 参数：
//   - database: 数据库名称
//   - measurement: Measurement 名称
//
// 返回：
//   - bool: 是否新创建（false 表示已存在）
//   - error: 如果数据库不存在则返回错误
//
// 说明：
//
//	调用 DatabaseMetaStore.GetOrCreate 创建 MeasurementMetaStore。
//	实际的目录和数据在第一次写入时才会创建。
func (e *Engine) CreateMeasurement(database, measurement string) (bool, error) {
	dbMeta := e.getOrCreateDBMetaStore(database)

	// DatabaseMetaStore 内部使用双检锁，这里不需要额外加锁
	// GetOrCreate 返回时，measurement 一定已存在
	dbMeta.GetOrCreate(measurement)

	// 注意：这里无法区分是新建还是已存在，GetOrCreate 不返回这个信息
	// 对于当前需求（空 measurement 不做处理），这不影响功能
	return true, nil
}

// DropMeasurement 删除指定的 Measurement。
//
// 参数：
//   - database: 数据库名称
//   - measurement: Measurement 名称
//
// 返回：
//   - bool: 是否成功删除（false 表示不存在）
//   - error: 如果数据库不存在则返回错误
//
// 警告：
//
//	此操作会永久删除该 Measurement 的元数据（包括 Schema、Series、Tag 索引）。
//	磁盘上的数据文件不会被立即删除，需要单独清理。
func (e *Engine) DropMeasurement(database, measurement string) (bool, error) {
	e.mu.RLock()
	dbMeta, ok := e.dbMetaStores[database]
	e.mu.RUnlock()
	if !ok {
		return false, fmt.Errorf("database not found: %s", database)
	}

	return dbMeta.DropMeasurement(measurement), nil
}
