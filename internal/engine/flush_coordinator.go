package engine

import "time"

// FlushCoordinator 编排全局 MemTable → unordered 的异步刷盘流程。
// 暂为桩实现，完整实现在 Task 6 中完成。
type FlushCoordinator struct{}

// NewFlushCoordinator 创建新的 FlushCoordinator 桩实例。
func NewFlushCoordinator(_ Flusher) *FlushCoordinator {
	return &FlushCoordinator{}
}

// StartPeriodicCheck 启动周期性自动检查（桩实现，空操作）。
func (fc *FlushCoordinator) StartPeriodicCheck(_ time.Duration) {}

// FlushAll 同步刷写所有数据（桩实现，空操作）。
func (fc *FlushCoordinator) FlushAll() error {
	return nil
}
