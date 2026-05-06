// Package shard 实现分片存储管理。
package shard

import (
	"log/slog"
	"time"
)

// RetentionService 负责定期清理过期的 Shard。
//
// 工作原理：
//
//	定期检查所有 Shard 的时间范围
//	如果 Shard 的结束时间早于 (now - retentionPeriod)，则删除该 Shard
//
// 删除粒度：
//
//	以 Shard 为单位删除，而不是单个数据点
//	最小保留期 = ShardDuration
//
// 线程安全：
//
//	Start/Stop 可以安全地在多个 goroutine 中调用
type RetentionService struct {
	manager      *ShardManager
	retention    time.Duration
	checkInterval time.Duration
	done         chan struct{}
}

// NewRetentionService 创建 RetentionService。
//
// 参数：
//   - manager:       ShardManager 实例
//   - retention:     数据保留期
//   - checkInterval: 检查间隔
//
// 返回：
//   - *RetentionService: 初始化后的服务
func NewRetentionService(manager *ShardManager, retention, checkInterval time.Duration) *RetentionService {
	return &RetentionService{
		manager:      manager,
		retention:    retention,
		checkInterval: checkInterval,
		done:         make(chan struct{}),
	}
}

// Start 启动 RetentionService 后台清理 goroutine。
//
// 启动后，服务会定期检查并删除过期的 Shard。
// 直到调用 Stop 才停止。
func (s *RetentionService) Start() {
	go s.run()
}

// Stop 停止 RetentionService。
//
// 发送停止信号给后台 goroutine 并等待其退出。
// 多次调用是安全的。
func (s *RetentionService) Stop() {
	select {
	case <-s.done:
		// 已经停止
		return
	default:
		close(s.done)
	}
}

// run 是后台清理循环。
func (s *RetentionService) run() {
	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()

	slog.Info("retention service started",
		"retention", s.retention,
		"checkInterval", s.checkInterval)

	for {
		select {
		case <-ticker.C:
			s.cleanup()
		case <-s.done:
			slog.Info("retention service stopped")
			return
		}
	}
}

// cleanup 执行一次清理操作。
func (s *RetentionService) cleanup() {
	cutoff := time.Now().Add(-s.retention).UnixNano()

	shards := s.manager.GetAllShards()

	for _, shard := range shards {
		// 如果 Shard 的结束时间早于 cutoff，则删除
		if shard.EndTime() < cutoff {
			key := shard.DB() + "/" + shard.Measurement() + "/" + formatInt64(shard.StartTime())
			slog.Info("deleting expired shard",
				"key", key,
				"endTime", shard.EndTime(),
				"cutoff", cutoff)

			if err := s.manager.DeleteShard(key); err != nil {
				slog.Error("failed to delete expired shard",
					"key", key,
					"error", err)
			}
		}
	}
}
