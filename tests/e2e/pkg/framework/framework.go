// tests/e2e/pkg/framework/framework.go
package framework

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/types"
)

// Config 数据库配置选项
type Config struct {
	DBName                 string
	MeasurementName        string
	ShardDuration          time.Duration
	MaxSize                int64
	MaxCount               int32
	IdleDurationNanos      int64
	RetentionPeriod        time.Duration
	RetentionCheckInterval time.Duration
}

// DefaultConfig 返回默认配置
func DefaultConfig(name string) *Config {
	return &Config{
		DBName:                 "db1",
		MeasurementName:        "cpu",
		ShardDuration:          time.Hour,
		MaxSize:                64 * 1024 * 1024,
		MaxCount:               3000,
		IdleDurationNanos:      int64(10 * time.Second),
		RetentionPeriod:        0,         // 默认不启用 retention
		RetentionCheckInterval: time.Hour, // 默认检查间隔 1 小时
	}
}

// WithShardDuration 设置 shard duration
func WithShardDuration(d time.Duration) func(*Config) {
	return func(c *Config) {
		c.ShardDuration = d
	}
}

// TestHarness 测试工具，管理数据库生命周期
type TestHarness struct {
	tmpDir    string
	db        *microts.DB
	cfg       *Config
	startTime int64
}

// NewTestHarness 创建测试工具
func NewTestHarness(name string, opts ...func(*Config)) (*TestHarness, error) {
	cfg := DefaultConfig(name)

	for _, opt := range opts {
		opt(cfg)
	}

	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("microts_%s", name))
	_ = os.RemoveAll(tmpDir)

	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: cfg.ShardDuration,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           cfg.MaxSize,
			MaxCount:          cfg.MaxCount,
			IdleDurationNanos: cfg.IdleDurationNanos,
		},
		RetentionPeriod:        cfg.RetentionPeriod,
		RetentionCheckInterval: cfg.RetentionCheckInterval,
	}

	db, err := microts.Open(dbCfg)
	if err != nil {
		return nil, fmt.Errorf("open db failed: %w", err)
	}

	return &TestHarness{
		tmpDir:    tmpDir,
		db:        db,
		cfg:       cfg,
		startTime: time.Now().UnixNano(),
	}, nil
}

// DB 返回数据库实例
func (h *TestHarness) DB() *microts.DB {
	return h.db
}

// Config 返回配置
func (h *TestHarness) Config() *Config {
	return h.cfg
}

// StartTime 返回测试开始时间
func (h *TestHarness) StartTime() int64 {
	return h.startTime
}

// TempDir 返回临时目录
func (h *TestHarness) TempDir() string {
	return h.tmpDir
}

// DataDir 返回数据目录
func (h *TestHarness) DataDir() string {
	return filepath.Join(h.tmpDir, h.cfg.DBName, h.cfg.MeasurementName)
}

// Close 关闭数据库并清理临时目录
func (h *TestHarness) Close() error {
	if h.db != nil {
		if err := h.db.Close(); err != nil {
			return err
		}
	}
	_ = os.RemoveAll(h.tmpDir)
	return nil
}

// WritePoints写入多个数据点
func (h *TestHarness) WritePoints(ctx context.Context, count int, interval time.Duration) error {
	for i := 0; i < count; i++ {
		ts := h.startTime + int64(i)*int64(interval)
		p := &types.Point{
			Database:    h.cfg.DBName,
			Measurement: h.cfg.MeasurementName,
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   ts,
			Fields: map[string]*types.FieldValue{
				"usage": types.NewFieldValue(float64(i) * 1.5),
				"count": types.NewFieldValue(int64(i * 10)),
			},
		}
		if err := h.db.Write(ctx, p); err != nil {
			return fmt.Errorf("write failed at %d: %w", i, err)
		}
	}
	return nil
}

// QueryRange 查询指定时间范围的数据
func (h *TestHarness) QueryRange(ctx context.Context, start, end int64) (*types.QueryRangeResponse, error) {
	return h.db.QueryRange(ctx, &types.QueryRangeRequest{
		Database:    h.cfg.DBName,
		Measurement: h.cfg.MeasurementName,
		StartTime:   start,
		EndTime:     end,
		Offset:      0,
		Limit:       0,
	})
}

// VerifyDataIntegrity 验证数据完整性
func (h *TestHarness) VerifyDataIntegrity(count int, interval time.Duration) error {
	resp, err := h.QueryRange(context.Background(), h.startTime, h.startTime+int64(count)*int64(interval))
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	if len(resp.Rows) != count {
		return fmt.Errorf("expected %d rows, got %d", count, len(resp.Rows))
	}

	errors := 0
	for i, row := range resp.Rows {
		expectedUsage := float64(i) * 1.5
		expectedCount := int64(i * 10)
		usage := row.Fields["usage"]
		countVal := row.Fields["count"]
		if usage == nil || countVal == nil {
			fmt.Printf("Row %d: nil fields!\n", i)
			errors++
			continue
		}
		if usage.GetFloatValue() != expectedUsage {
			fmt.Printf("Row %d: usage mismatch: expected %v, got %v\n", i, expectedUsage, usage.GetFloatValue())
			errors++
		}
		if countVal.GetIntValue() != expectedCount {
			fmt.Printf("Row %d: count mismatch: expected %v, got %v\n", i, expectedCount, countVal.GetIntValue())
			errors++
		}
	}

	if errors > 0 {
		return fmt.Errorf("data integrity check failed: %d errors", errors)
	}
	return nil
}

// WithConfig 自定义配置选项
func WithConfig(cfg *Config) func(*Config) {
	return func(c *Config) {
		*c = *cfg
	}
}

// WithIdleDuration 设置空闲刷盘时间
func WithIdleDuration(d time.Duration) func(*Config) {
	return func(c *Config) {
		c.IdleDurationNanos = int64(d)
	}
}

// WithMaxSize 设置最大内存大小
func WithMaxSize(size int64) func(*Config) {
	return func(c *Config) {
		c.MaxSize = size
	}
}

// WithMaxCount 设置最大条目数
func WithMaxCount(count int32) func(*Config) {
	return func(c *Config) {
		c.MaxCount = count
	}
}

// WithRetentionPeriod 设置数据保留期
func WithRetentionPeriod(d time.Duration) func(*Config) {
	return func(c *Config) {
		c.RetentionPeriod = d
	}
}

// WithRetentionCheckInterval 设置 retention 检查间隔
func WithRetentionCheckInterval(d time.Duration) func(*Config) {
	return func(c *Config) {
		c.RetentionCheckInterval = d
	}
}
