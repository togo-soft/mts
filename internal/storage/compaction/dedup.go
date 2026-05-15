package compaction

import "math/bits"

const (
	// bloomBits 位数组大小：1.2 MB = 10,485,760 bits ≈ 10 Mbits
	bloomBits = 1 << 20 * 10 // 10,485,760
	// bloomHashes 哈希函数数量
	bloomHashes = 4
	// defaultWindowSize 滑动窗口默认大小
	defaultWindowSize = 50000
	// strictThreshold 严格模式阈值：预估行数少于此值使用 map
	strictThreshold = 50000
)

// DedupFilter 去重过滤器，用 Bloom Filter + 滑动窗口替代 map[uint64]bool。
type DedupFilter struct {
	bloom      []uint64 // 位数组，64位一组
	window     map[uint64]struct{}
	ring       []uint64 // 环形缓冲区
	ringIdx    int
	windowSize int
	strict     bool
}

// NewDedupFilter 创建去重过滤器。
// estimatedRows <= 0 时默认使用松弛模式，否则根据阈值自动选择。
func NewDedupFilter(estimatedRows int) *DedupFilter {
	if estimatedRows > 0 && estimatedRows < strictThreshold {
		return &DedupFilter{strict: true}
	}
	return &DedupFilter{
		bloom:      make([]uint64, bloomBits/64),
		window:     make(map[uint64]struct{}, defaultWindowSize),
		ring:       make([]uint64, defaultWindowSize),
		windowSize: defaultWindowSize,
	}
}

// Seen 判断 key 是否已见过。返回 true 表示已见过（重复）。
func (f *DedupFilter) Seen(key uint64) bool {
	if f.strict {
		return f.seenStrict(key)
	}
	return f.seenRelaxed(key)
}

// seenStrict 严格模式：直接用 window map（等同于原始 map[uint64]bool）。
func (f *DedupFilter) seenStrict(key uint64) bool {
	if f.window == nil {
		f.window = make(map[uint64]struct{})
	}
	if _, ok := f.window[key]; ok {
		return true
	}
	f.window[key] = struct{}{}
	return false
}

// seenRelaxed 松弛模式：Bloom Filter 快速排除 + 滑动窗口精确判断。
func (f *DedupFilter) seenRelaxed(key uint64) bool {
	// 第一步：Bloom Filter 快速判断
	if !f.bloomMayContain(key) {
		f.bloomAdd(key)
		f.addToWindow(key)
		return false
	}

	// 第二步：窗口内精确检查
	if _, ok := f.window[key]; ok {
		return true
	}

	// Bloom 假阳性 → 视为不重复，加入窗口
	f.addToWindow(key)
	return false
}

// addToWindow 将 key 加入滑动窗口，FIFO 淘汰旧 key。
func (f *DedupFilter) addToWindow(key uint64) {
	if f.windowSize <= 0 {
		return
	}
	if f.ringIdx >= f.windowSize {
		old := f.ring[0]
		delete(f.window, old)
		copy(f.ring, f.ring[1:])
		f.ringIdx = f.windowSize - 1
	}
	if f.ringIdx < len(f.ring) {
		f.ring[f.ringIdx] = key
		f.ringIdx++
	}
	f.window[key] = struct{}{}
}

// bloomMayContain 检查 key 是否可能存在于 Bloom Filter 中。
func (f *DedupFilter) bloomMayContain(key uint64) bool {
	h1, h2 := f.hashKey(key)
	for i := uint64(0); i < bloomHashes; i++ {
		bit := (h1 + i*h2) % bloomBits
		word := bit / 64
		offset := bit % 64
		if f.bloom[word]&(1<<offset) == 0 {
			return false
		}
	}
	return true
}

// bloomAdd 将 key 加入 Bloom Filter。
func (f *DedupFilter) bloomAdd(key uint64) {
	h1, h2 := f.hashKey(key)
	for i := uint64(0); i < bloomHashes; i++ {
		bit := (h1 + i*h2) % bloomBits
		word := bit / 64
		offset := bit % 64
		f.bloom[word] |= 1 << offset
	}
}

// hashKey 使用双哈希生成两个独立的哈希值（Kirsch-Mitzenmacher 方法）。
func (f *DedupFilter) hashKey(key uint64) (uint64, uint64) {
	h1 := bits.RotateLeft64(key, 21) ^ 0x9e3779b97f4a7c15
	h2 := bits.RotateLeft64(key, 37) ^ 0x85ebca6b1225c1a3
	return h1, h2
}

// DedupStats 去重统计信息。
type DedupStats struct {
	ProcessedCount int // 处理的总行数（不含重复跳过）
	DuplicateCount int // 检测到的重复数
}
