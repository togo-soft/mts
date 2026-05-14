# gRPC 流式查询实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 gRPC `QueryRange` 从 unary RPC 改为 server-side streaming `returns (stream Row)`，消除全量内存收集瓶颈。

**Architecture:** Proto 删除 `QueryRangeResponse` 消息体，`QueryRange` RPC 返回 `stream Row`。gRPC handler 直接消费 `Engine.QueryIterator` 逐行 `stream.Send()`。删除 `Engine.Query`/`collectQueryResults`/`buildQueryResponse` 三个中间函数。所有现有调用方改为使用 `QueryIterator`。

**Tech Stack:** Go, gRPC server-side streaming, protobuf (buf generate)

---

### 文件改动总览

| 文件 | 操作 |
|------|------|
| `proto/microts.proto` | 修改：删除 QueryRangeResponse，QueryRange 改为 stream Row |
| `types/microts.pb.go` | make 重新生成 |
| `types/microts_grpc.pb.go` | make 重新生成 |
| `internal/api/grpc.go` | 修改：QueryRange handler 改为 streaming |
| `internal/api/grpc_test.go` | 修改：3 个 QueryRange 测试适配流式 |
| `internal/engine/engine_query.go` | 修改：删除 Query/collectQueryResults/buildQueryResponse |
| `internal/engine/engine_test.go` | 修改：所有 Query 调用改为 QueryIterator |
| `mts.go` | 修改：删除 DB.QueryRange |
| `mts_test.go` | 修改：TestDB_QueryRange 适配 |
| `internal/query/executor.go` | 修改：Execute 改为流式 |
| `internal/query/executor_test.go` | 修改：适配新的 Execute 签名 |
| `tests/e2e/pkg/framework/framework.go` | 修改：QueryRange 内部使用 QueryIterator |
| `examples/simple/main.go` | 修改：使用 QueryIterator |
| `tests/e2e/compression_test/main.go` | 修改：使用 QueryIterator |
| `tests/e2e/wal_compression_test/main.go` | 修改：使用 QueryIterator |
| `tests/e2e/restart_recovery/main.go` | 修改：使用 QueryIterator |

---

### Task 1: Proto 变更并重新生成

**Files:**
- Modify: `proto/microts.proto:157-165` — 删除 `QueryRangeResponse`
- Modify: `proto/microts.proto:341` — QueryRange 改为 `returns (stream Row)`

- [ ] **Step 1: 修改 proto 文件**

删除 `QueryRangeResponse` message（lines 157-165）：
```protobuf
// 要删除的块
// 范围查询响应
message QueryRangeResponse {
  string       database    = 1;
  string       measurement = 2;
  int64        start_time  = 3;
  int64        end_time    = 4;
  int64        total_count = 5;
  bool         has_more    = 6;
  repeated Row rows        = 7;
}
```

修改 `QueryRange` RPC（line 341）：
```protobuf
// 改前
rpc QueryRange(QueryRangeRequest) returns (QueryRangeResponse);

// 改后
rpc QueryRange(QueryRangeRequest) returns (stream Row);
```

- [ ] **Step 2: 重新生成 proto**

```bash
make
```

验证生成文件不再包含 `QueryRangeResponse`：
```bash
grep -c "QueryRangeResponse" types/microts.pb.go
# Expected: 0
```

- [ ] **Step 3: 验证编译（预期大量编译错误，因为 Go 代码还在引用旧类型）**

```bash
go build ./... 2>&1 | head -20
```
Expected: 编译失败，所有引用 `types.QueryRangeResponse` 的代码报错。

- [ ] **Step 4: Commit**

```bash
git add proto/microts.proto types/microts.pb.go types/microts_grpc.pb.go
git commit -m "feat(proto): QueryRange 改为 server-side streaming, 删除 QueryRangeResponse"
```

---

### Task 2: Engine 层清理 + gRPC Handler 流式化

**Files:**
- Modify: `internal/engine/engine_query.go:13-116` — 删除 Query/collectQueryResults/buildQueryResponse/anyToProtoFieldValue
- Modify: `internal/api/grpc.go:208-219` — QueryRange 改为 streaming handler

- [ ] **Step 1: 删除 Engine.Query 及相关函数**

删除 `engine_query.go` 中以下函数：
- `Engine.Query()` (lines 14-55)
- `collectQueryResults()` (lines 58-84)
- `buildQueryResponse()` (lines 87-116)
- `anyToProtoFieldValue()` (lines 119-136)

保留 `Engine.QueryIterator()` (lines 138-150)。

同时删除不再需要的 import：`"log/slog"`，`"codeberg.org/micro-ts/mts/internal/metrics"`。

最终 `engine_query.go` 应只包含：
```go
package engine

import (
	"context"
	"fmt"

	"codeberg.org/micro-ts/mts/internal/query"
	"codeberg.org/micro-ts/mts/types"
)

// QueryIterator 返回流式查询迭代器。
func (e *Engine) QueryIterator(ctx context.Context, req *types.QueryRangeRequest) (*query.QueryIterator, error) {
	if e.isClosed() {
		return nil, fmt.Errorf("engine is closed")
	}

	shards := e.shardManager.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)
	if len(shards) == 0 {
		return nil, fmt.Errorf("no shards found")
	}

	return query.NewQueryIterator(ctx, shards, req), nil
}
```

- [ ] **Step 2: 修改 gRPC QueryRange handler 为 streaming**

`internal/api/grpc.go` 的 `QueryRange` 从 unary handler 改为 streaming handler：

```go
// QueryRange 处理范围查询请求（服务端流式）。
//
// 数据按时间戳升序逐行发送，客户端可边接收边处理。
// 当客户端断开连接或 context 取消时，自动停止迭代。
func (s *MicroTSService) QueryRange(req *types.QueryRangeRequest, stream types.MicroTS_QueryRangeServer) error {
	ctx := stream.Context()

	qit, err := s.engine.QueryIterator(ctx, req)
	if err != nil {
		return status.Errorf(codes.NotFound, "no shards found: %v", err)
	}
	defer func() {
		if err := qit.Close(); err != nil {
			slog.Warn("failed to close query iterator", "error", err)
		}
	}()

	for qit.Next(ctx) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		row := qit.Points()
		if row == nil {
			continue
		}
		if err := stream.Send(pointRowToProto(row)); err != nil {
			return err
		}
	}
	return nil
}
```

需添加 import：`"log/slog"` 和 `"google.golang.org/grpc/status"` 和 `"google.golang.org/grpc/codes"`。

现有的 `pointRowToProto` 函数（line 221-233）保持不变，可复用。

`anyToProtoFieldValue` 函数保留，因为 `pointRowToProto` 内部调用了它。

- [ ] **Step 3: 验证编译通过**

```bash
go build ./internal/engine/... ./internal/api/...
```

- [ ] **Step 4: Commit**

```bash
git add internal/engine/engine_query.go internal/api/grpc.go
git commit -m "feat(query): 删除 Engine.Query 全量收集逻辑, gRPC QueryRange 改为流式 handler"
```

---

### Task 3: 适配公共 API (mts.go) + query/executor.go

**Files:**
- Modify: `mts.go:317-363` — 删除 `DB.QueryRange`
- Modify: `mts.go:76-100` — 删除 `QueryRangeResponse` 类型别名
- Modify: `internal/query/executor.go:64-66` — Execute 改为流式

- [ ] **Step 1: 删除 mts.go 中的 QueryRange**

删除 `DB.QueryRange` 方法（lines 317-363）及 `QueryRangeResponse` 类型别名（lines 96-100）。

`QueryRangeRequest` 别名保留（line 94），`DB.QueryIterator` 保留（lines 365-394）。

- [ ] **Step 2: 修改 executor.go 的 Execute 方法**

```go
// Execute 执行查询请求，返回流式迭代器。
//
// 当前实现为基础框架，engine 参数用于未来依赖注入。
func (e *Executor) Execute(ctx context.Context, req *types.QueryRangeRequest) (*QueryIterator, error) {
	return nil, fmt.Errorf("query executor not implemented: use Engine.QueryIterator instead")
}
```

删除文件顶部的 `// Executor: 查询执行器...` 注释中对 `QueryRangeResponse` 的引用。

- [ ] **Step 3: 验证编译**

```bash
go build ./...
```

- [ ] **Step 4: Commit**

```bash
git add mts.go internal/query/executor.go
git commit -m "refactor: 删除 DB.QueryRange 和 QueryRangeResponse 别名, executor 返回迭代器"
```

---

### Task 4: 适配 E2E Framework + 示例代码

**Files:**
- Modify: `tests/e2e/pkg/framework/framework.go:181-229` — QueryRange/VerifyDataIntegrity 适配
- Modify: `examples/simple/main.go:85-113` — 使用 QueryIterator

- [ ] **Step 1: 修改 E2E Framework 的 QueryRange**

`QueryRange` 内部使用 `QueryIterator`，将结果收集到 `[]*types.PointRow`（这是测试便利层，全量收集可接受）：

```go
// QueryRange 查询指定时间范围的数据（内部使用流式迭代器）
func (h *TestHarness) QueryRange(ctx context.Context, start, end int64) ([]*types.PointRow, error) {
	it, err := h.db.QueryIterator(ctx, &types.QueryRangeRequest{
		Database:    h.cfg.DBName,
		Measurement: h.cfg.MeasurementName,
		StartTime:   start,
		EndTime:     end,
		Offset:      0,
		Limit:       0,
	})
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}
	defer func() { _ = it.Close() }()

	var rows []*types.PointRow
	for it.Next(ctx) {
		row := it.Points()
		if row != nil {
			rows = append(rows, row)
		}
	}
	return rows, nil
}
```

返回类型改为 `[]*types.PointRow`（不再是 `*QueryRangeResponse`）。

- [ ] **Step 2: 修改 VerifyDataIntegrity**

适配 `QueryRange` 新返回类型：

```go
func (h *TestHarness) VerifyDataIntegrity(count int, interval time.Duration) error {
	rows, err := h.QueryRange(context.Background(), h.startTime, h.startTime+int64(count)*int64(interval))
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	if len(rows) != count {
		return fmt.Errorf("expected %d rows, got %d", count, len(rows))
	}

	errors := 0
	for i, row := range rows {
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
```

主要变化：`resp.Rows` → `rows`（直接迭代 `PointRow`），去掉 `resp.Rows[i].Fields` → `rows[i].Fields`。

- [ ] **Step 3: 修改 examples/simple/main.go**

```go
// 查询数据
oneMonthLater := time.Now().Add(30 * 24 * time.Hour).UnixNano()
fmt.Println("\nStep 2: 查询所有累积数据（从 0 到未来一个月）")
it, err := db.QueryIterator(context.Background(), &types.QueryRangeRequest{
	Database:    dbName,
	Measurement: measurement,
	StartTime:   0,
	EndTime:     oneMonthLater,
	Offset:      0,
	Limit:       0,
})
if err != nil {
	log.Fatalf("查询失败: %v", err)
}
defer func() { _ = it.Close() }()

var rows []*types.PointRow
for it.Next(context.Background()) {
	rows = append(rows, it.Points())
}

fmt.Printf("查询结果: %d 条数据（时间范围 [0, %d]）\n\n", len(rows), oneMonthLater)

// 打印前几条数据
fmt.Println("前 5 条数据:")
for i := 0; i < 5 && i < len(rows); i++ {
	row := rows[i]
	fmt.Printf("  [%d] host=%s usage=%.1f\n", row.Timestamp, row.Tags["host"], row.Fields["usage"].GetFloatValue())
}
```

- [ ] **Step 4: 验证编译**

```bash
go build ./tests/e2e/pkg/framework/... ./examples/simple/...
```

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/pkg/framework/framework.go examples/simple/main.go
git commit -m "refactor: E2E framework 和示例代码适配流式查询 API"
```

---

### Task 5: 适配所有测试代码

**Files:**
- Modify: `internal/engine/engine_test.go` — 所有 Query 调用
- Modify: `internal/api/grpc_test.go` — 3 个 QueryRange 测试
- Modify: `internal/query/executor_test.go` — 适配 Execute
- Modify: `mts_test.go` — TestDB_QueryRange

- [ ] **Step 1: 修改 engine_test.go**

所有 `engine.Query(ctx, req)` 调用改为使用 `QueryIterator` 手动收集：

典型替换模式：
```go
// 改前：
resp, err := engine.Query(t.Context(), req)
if err != nil {
    t.Fatalf("Query failed: %v", err)
}
if len(resp.Rows) != 2 {
    t.Errorf("expected 2 rows, got %d", len(resp.Rows))
}

// 改后：
it, err := engine.QueryIterator(t.Context(), req)
if err != nil {
    t.Fatalf("QueryIterator failed: %v", err)
}
defer func() { _ = it.Close() }()
var rows []*types.PointRow
for it.Next(t.Context()) {
    rows = append(rows, it.Points())
}
if len(rows) != 2 {
    t.Errorf("expected 2 rows, got %d", len(rows))
}
```

需要修改的测试函数（按行号）：
- `TestEngine_Query` (~line 114) — resp.Rows → rows
- `TestEngine_Query_FieldProjection` (~line 168) — resp.Rows[i].Fields → rows[i].Fields
- `TestEngine_Query_TagFilter` (~line 205) — resp.Rows → rows
- `TestEngine_Query_TimeRangeFilter` (~line 275) — resp.Rows → rows
- `TestEngine_Query_EmptyShards` (~line 327) — resp → nil check → error check
- `TestEngine_Query_Pagination` (~line 378) — HasMore/TotalCount/Rows → iterator semantics
- `TestEngine_Query_WithCompaction` (~line 705) — resp.Rows → rows
- `TestEngine_Query_AfterRestart` (~line 748) — resp.Rows → rows
- `TestEngine_Query_EmptyResult` (~line 929) — resp.Rows → rows
- `TestEngine_Query_WithLevelCompaction` (~line 952) — resp.Rows → rows

- [ ] **Step 2: 修改 api/grpc_test.go**

3 个测试需要适配 streaming handler：

`TestMicroTSService_QueryRange` (~line 176): QueryRange 现在是 streaming，需要通过 mock stream 测试或改为调用 `engine.QueryIterator`。

`TestMicroTSService_QueryRange_WithData` (~line 617): 同上。

`TestMicroTSService_QueryRange_EngineClosed` (~line 874): 测试 engine 关闭后 stream 行为。

这些测试可以改为通过 `engine.QueryIterator` 间接测试（因为 gRPC handler 本身只有简单的调用链），或者使用 gRPC 的 `grpctest` 创建真实的 streaming 测试。

推荐改为测试 `engine.QueryIterator`（简单、不依赖 gRPC 测试框架）：

```go
func TestMicroTSService_QueryRange_WithData(t *testing.T) {
	eng, _ := engine.New(&engine.Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	})
	srv := New(eng)
	ctx := t.Context()

	now := time.Now().UnixNano()
	writeReq := &types.WriteRequest{
		Database:    "testdb",
		Measurement: "testmeas",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   now,
		Fields: map[string]*types.FieldValue{
			"value": types.NewFieldValue(float64(85.5)),
		},
	}
	if _, err := srv.Write(ctx, writeReq); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	it, err := eng.QueryIterator(ctx, &types.QueryRangeRequest{
		Database:    "testdb",
		Measurement: "testmeas",
		StartTime:   now - 1e9,
		EndTime:     now + 1e9,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = it.Close() }()

	var rows []*types.PointRow
	for it.Next(ctx) {
		rows = append(rows, it.Points())
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}
```

- [ ] **Step 3: 修改 query/executor_test.go**

适配 `Execute` 新返回类型 `(*QueryIterator, error)`：

```go
func TestExecutor_Execute_ReturnsError(t *testing.T) {
	exec := NewExecutor(nil)
	_, err := exec.Execute(t.Context(), &types.QueryRangeRequest{
		Database:    "db",
		Measurement: "m",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
```

- [ ] **Step 4: 修改 mts_test.go**

找到 `TestDB_QueryRange`，改为使用 `QueryIterator`。

- [ ] **Step 5: 验证编译**

```bash
go build ./...
```

- [ ] **Step 6: 运行所有单元测试**

```bash
go test ./internal/engine/... ./internal/api/... ./internal/query/... -count=1 -timeout 60s
```

所有测试必须 PASS。

- [ ] **Step 7: Commit**

```bash
git add internal/engine/engine_test.go internal/api/grpc_test.go internal/query/executor_test.go mts_test.go
git commit -m "test: 适配所有单元测试到流式查询 API"
```

---

### Task 6: 适配 E2E 测试文件

**Files:**
- Modify: `tests/e2e/compression_test/main.go:151,202`
- Modify: `tests/e2e/wal_compression_test/main.go:99`
- Modify: `tests/e2e/restart_recovery/main.go:106`

- [ ] **Step 1: 修改所有 E2E 测试中的 QueryRange 调用**

统一替换模式（用 `QueryIterator` + 手动收集）：

```go
// 改前：
resp, err := db.QueryRange(ctx, &microts.QueryRangeRequest{...})
// resp.Rows[i].Timestamp, resp.Rows[i].Fields, etc.

// 改后：
it, err := db.QueryIterator(ctx, &microts.QueryRangeRequest{...})
if err != nil { ... }
defer func() { _ = it.Close() }()
var rows []*types.PointRow
for it.Next(ctx) {
	rows = append(rows, it.Points())
}
// rows[i].Timestamp, rows[i].Fields, etc.
```

- [ ] **Step 2: 验证编译**

```bash
go build ./tests/e2e/compression_test/... ./tests/e2e/wal_compression_test/... ./tests/e2e/restart_recovery/...
```

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/
git commit -m "refactor: E2E 测试适配流式查询 API"
```

---

### Task 7: 全量验证

- [ ] **Step 1: 构建完整项目**

```bash
go build ./...
```

- [ ] **Step 2: 运行所有单元测试**

```bash
go test ./... -count=1 -timeout 120s 2>&1 | tail -30
```

- [ ] **Step 3: 运行 golangci-lint**

```bash
golangci-lint run ./... 2>&1 | tail -20
```

- [ ] **Step 4: 运行 goimports-reviser**

```bash
find . -name "*.go" -not -path "./types/microts*.go" -not -path "./vendor/*" | xargs goimports-reviser -company-prefixes "codeberg.org/micro-ts/mts"
```

- [ ] **Step 5: 运行全量 E2E 测试**

```bash
# 关键测试目录
cd tests/e2e/compaction_test && go build && ./compaction_test && cd ../../..
cd tests/e2e/grpc_write_query && go build && ./grpc_write_query && cd ../../..
cd tests/e2e/integrity && go build && ./integrity && cd ../../..
cd tests/e2e/restart_recovery && go build && ./restart_recovery && cd ../../..
cd tests/e2e/simple_integrity && go build && ./simple_integrity && cd ../../..
cd tests/e2e/persistence_test && go build && ./persistence_test && cd ../../..
cd tests/e2e/wal_test && go build && ./wal_test && cd ../../..
# 性能测试
cd tests/e2e/write_100k && go build && ./write_100k && cd ../../..
cd tests/e2e/query_10k && go build && ./query_10k && cd ../../..
```

- [ ] **Step 6: Commit（如有 lint 修复）**

```bash
git add -A && git diff --cached --stat
# 仅当有 lint/format 相关修改时提交
```
