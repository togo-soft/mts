package shard

import (
	"sync"
	"sync/atomic"
)

// SSTableRef 跟踪 SSTable 的引用计数，防止删除正在被查询使用的文件。
type SSTableRef struct {
	path   string
	refCnt atomic.Int32
}

// Acquire 增加引用计数。
func (r *SSTableRef) Acquire() { r.refCnt.Add(1) }

// Release 释放引用计数。
func (r *SSTableRef) Release() { r.refCnt.Add(-1) }

// IsUnused 无引用。
func (r *SSTableRef) IsUnused() bool { return r.refCnt.Load() == 0 }

// sstRefs 是 SSTable 引用计数的容器，内嵌在 Shard 中使用。
type sstRefs struct {
	mu   sync.Mutex
	refs map[string]*SSTableRef
}

func newSSTRefs() *sstRefs {
	return &sstRefs{refs: make(map[string]*SSTableRef)}
}

// acquire 增加对指定路径 SSTable 的引用，必要时创建新的引用对象。
func (sr *sstRefs) acquire(path string) *SSTableRef {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if ref, ok := sr.refs[path]; ok {
		ref.Acquire()
		return ref
	}

	ref := &SSTableRef{path: path}
	ref.Acquire()
	sr.refs[path] = ref
	return ref
}

// release 释放对指定路径的引用。
func (sr *sstRefs) release(path string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if ref, ok := sr.refs[path]; ok {
		ref.Release()
	}
}

// AcquireSSTRef 增加对指定 SSTable 的引用（Shard 公开方法）。
func (s *Shard) AcquireSSTRef(path string) *SSTableRef {
	return s.sstRefs.acquire(path)
}

// ReleaseSSTRef 释放对指定 SSTable 的引用（Shard 公开方法）。
func (s *Shard) ReleaseSSTRef(path string) {
	s.sstRefs.release(path)
}

// IsSSTUnused 检查 SSTable 路径是否无引用，可安全删除。
func (s *Shard) IsSSTUnused(path string) bool {
	s.sstRefs.mu.Lock()
	defer s.sstRefs.mu.Unlock()

	ref, ok := s.sstRefs.refs[path]
	if !ok {
		return true
	}
	return ref.IsUnused()
}
