package wal

import "sync"

// 缓冲池容量分级
const (
	bufSmallCap  = 256
	bufMediumCap = 4096
	bufLargeCap  = 65536
)

// 分级缓冲池，减少频繁分配带来的 GC 压力。
// 存储 *[]byte 而非 []byte，避免 Put 时 slice header 堆逃逸。
var (
	smallPool  = sync.Pool{New: func() any { buf := make([]byte, 0, bufSmallCap); return &buf }}
	mediumPool = sync.Pool{New: func() any { buf := make([]byte, 0, bufMediumCap); return &buf }}
	largePool  = sync.Pool{New: func() any { buf := make([]byte, 0, bufLargeCap); return &buf }}
)

// getBuf 从分级池中获取 cap >= minCap 的缓冲区，len=0。
func getBuf(minCap int) []byte {
	switch {
	case minCap <= bufSmallCap:
		return getFromPool(&smallPool, minCap)
	case minCap <= bufMediumCap:
		return getFromPool(&mediumPool, minCap)
	case minCap <= bufLargeCap:
		return getFromPool(&largePool, minCap)
	default:
		return make([]byte, 0, minCap)
	}
}

func getFromPool(pool *sync.Pool, minCap int) []byte {
	bp := pool.Get().(*[]byte)
	buf := *bp
	if cap(buf) < minCap {
		return make([]byte, 0, minCap)
	}
	return buf[:0]
}

// putBuf 归还缓冲区到对应容量分级的池中。
func putBuf(buf []byte) {
	if cap(buf) == 0 {
		return
	}
	switch {
	case cap(buf) <= bufSmallCap:
		smallPool.Put(&buf)
	case cap(buf) <= bufMediumCap:
		mediumPool.Put(&buf)
	case cap(buf) <= bufLargeCap:
		largePool.Put(&buf)
	}
}
