// internal/storage/measurement/store_test.go
package measurement

import (
	"testing"
)

func TestMetaStoreInterface(t *testing.T) {
	// 验证 MemoryMetaStore 实现了 MetaStore 接口
	var _ MetaStore = (*MemoryMetaStore)(nil)
}
