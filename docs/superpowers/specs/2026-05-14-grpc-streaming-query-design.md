# gRPC 流式查询设计

> **目标：** 将 gRPC `QueryRange` 从 unary RPC 改为 server-side streaming，消除全量内存收集瓶颈。

## 架构

```
gRPC stream → MicroTSService.QueryRange (stream.Send 循环) → Engine.QueryIterator → QueryIterator.Next
```

存储层迭代器（SSTable → MergeIterator → ShardIterator → QueryIterator）已全部流式化，无需改动。只需拆掉 Engine 层的 `collectQueryResults` + `buildQueryResponse` 全量收集逻辑。

## Proto 变更

```protobuf
// 删除 QueryRangeResponse message
// 修改：
rpc QueryRange(QueryRangeRequest) returns (stream Row);
```

`QueryRangeRequest`、`Row`、`FieldValue` 保持不变。`QueryRangeResponse` 删除。

## gRPC Handler

```go
func (s *MicroTSService) QueryRange(req *types.QueryRangeRequest, stream types.MicroTS_QueryRangeServer) error {
    qit, err := s.engine.QueryIterator(stream.Context(), req)
    if err != nil {
        return status.Errorf(codes.NotFound, "no shards found: %v", err)
    }
    for qit.Next(stream.Context()) {
        if err := stream.Send(rowToProto(qit.Points())); err != nil {
            return err
        }
    }
    return nil
}
```

## Engine 清理

删除 3 个函数：
- `Engine.Query()` — unary 入口
- `collectQueryResults()` — 全量内存收集
- `buildQueryResponse()` — 批量 proto 转换

保留 `Engine.QueryIterator()` 作为新的公共 API。

## 公共 API

`mts.go` 中 `DB.QueryRange()` 移除。`DB.QueryIterator()` 保留（已存在）。

`tests/e2e/pkg/framework/framework.go` 的 `QueryRange` 改为内部使用 `QueryIterator` 收集结果（测试便利层）。

## 影响范围

- Proto: `QueryRange` RPC 签名变更，`QueryRangeResponse` 删除
- 生成代码: `make` 重新生成
- Handler: 1 文件（grpc.go）
- Engine: 1 文件（engine_query.go）
- 公共 API: 1 文件（mts.go）
- E2E harness: 1 文件（framework.go）
- 单元测试: ~5 文件
- E2E 测试: ~4 文件
- 示例: 1 文件（examples/simple）
