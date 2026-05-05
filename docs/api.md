# MicroTS gRPC API 文档

## 概述

MicroTS 是一个轻量级时间序列数据库服务，提供 gRPC 接口进行数据写入、查询和管理。

- **服务名称**: `microts.v1.MicroTS`
- **协议**: gRPC
- **导入路径**: `microts/internal/api/pb`

---

## 目录

- [服务方法](#服务方法)
  - [Write](#write) - 写入单个数据点
  - [WriteBatch](#writebatch) - 批量写入
  - [QueryRange](#queryrange) - 范围查询
  - [ListMeasurements](#listmeasurements) - 列出测量项
  - [Health](#health) - 健康检查
- [消息定义](#消息定义)
- [错误码](#错误码)
- [使用示例](#使用示例)

---

## 服务方法

### Write

写入单个时间序列数据点。

#### 方法签名

```protobuf
rpc Write(WriteRequest) returns (WriteResponse);
```

#### 功能说明

将单个数据点写入指定数据库的测量项中。数据点包含时间戳、标签和字段值，用于存储时间序列数据。

#### 请求参数 (WriteRequest)

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `database` | `string` | 是 | 数据库名称 |
| `measurement` | `string` | 是 | 测量项名称（类似表名） |
| `tags` | `map<string, string>` | 否 | 标签集合，用于索引和分组 |
| `timestamp` | `int64` | 是 | 数据点的时间戳（Unix 纳秒） |
| `fields` | `map<string, FieldValue>` | 是 | 字段值集合 |

#### 响应结构 (WriteResponse)

| 字段 | 类型 | 说明 |
|------|------|------|
| `success` | `bool` | 写入是否成功 |
| `error` | `string` | 错误信息（成功时为空） |

#### 使用示例

```go
package main

import (
    "context"
    "log"
    "time"

    "google.golang.org/grpc"
    pb "micro-ts/internal/api/pb"
)

func main() {
    conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    client := pb.NewMicroTSClient(conn)

    resp, err := client.Write(context.Background(), &pb.WriteRequest{
        Database:    "metrics",
        Measurement: "cpu_usage",
        Tags:        map[string]string{"host": "server-01", "region": "us-east"},
        Timestamp:   time.Now().UnixNano(),
        Fields: map[string]*pb.FieldValue{
            "value": {Value: &pb.FieldValue_FloatValue{FloatValue: 75.5}},
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Write success: %v", resp.Success)
}
```

---

### WriteBatch

批量写入多个时间序列数据点。

#### 方法签名

```protobuf
rpc WriteBatch(WriteBatchRequest) returns (WriteBatchResponse);
```

#### 功能说明

高效地批量写入多个数据点，减少网络往返开销。适用于需要同时写入大量数据点的场景。

#### 请求参数 (WriteBatchRequest)

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `points` | `repeated WriteRequest` | 是 | 数据点数组，每个元素同 WriteRequest |

#### 响应结构 (WriteBatchResponse)

| 字段 | 类型 | 说明 |
|------|------|------|
| `success` | `bool` | 批量写入是否成功 |
| `error` | `string` | 错误信息（成功时为空） |
| `count` | `int32` | 成功写入的数据点数量 |

#### 使用示例

```go
package main

import (
    "context"
    "log"
    "time"

    "google.golang.org/grpc"
    pb "micro-ts/internal/api/pb"
)

func main() {
    conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    client := pb.NewMicroTSClient(conn)

    points := []*pb.WriteRequest{
        {
            Database:    "metrics",
            Measurement: "cpu_usage",
            Tags:        map[string]string{"host": "server-01"},
            Timestamp:   time.Now().UnixNano(),
            Fields: map[string]*pb.FieldValue{
                "usage": {Value: &pb.FieldValue_FloatValue{FloatValue: 75.5}},
            },
        },
        {
            Database:    "metrics",
            Measurement: "memory_usage",
            Tags:        map[string]string{"host": "server-01"},
            Timestamp:   time.Now().UnixNano(),
            Fields: map[string]*pb.FieldValue{
                "used": {Value: &pb.FieldValue_IntValue{IntValue: 8589934592}},
            },
        },
    }

    resp, err := client.WriteBatch(context.Background(), &pb.WriteBatchRequest{
        Points: points,
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Batch write: success=%v, count=%d", resp.Success, resp.Count)
}
```

---

### QueryRange

按时间范围查询数据。

#### 方法签名

```protobuf
rpc QueryRange(QueryRangeRequest) returns (QueryRangeResponse);
```

#### 功能说明

查询指定数据库和测量项在时间段内的数据。支持按标签过滤、字段选择和分页。

#### 请求参数 (QueryRangeRequest)

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `database` | `string` | 是 | 数据库名称 |
| `measurement` | `string` | 是 | 测量项名称 |
| `start_time` | `int64` | 是 | 起始时间戳（Unix 纳秒，包含） |
| `end_time` | `int64` | 是 | 结束时间戳（Unix 纳秒，包含） |
| `fields` | `repeated string` | 否 | 指定返回的字段列表，为空则返回所有字段 |
| `tags` | `map<string, string>` | 否 | 标签过滤条件，键值对必须全部匹配 |
| `offset` | `int64` | 否 | 分页偏移量，默认为 0 |
| `limit` | `int64` | 否 | 返回结果数量限制，默认为 0（无限制） |

#### 响应结构 (QueryRangeResponse)

| 字段 | 类型 | 说明 |
|------|------|------|
| `database` | `string` | 查询的数据库名称 |
| `measurement` | `string` | 查询的测量项名称 |
| `start_time` | `int64` | 实际查询起始时间 |
| `end_time` | `int64` | 实际查询结束时间 |
| `total_count` | `int64` | 符合条件的总记录数 |
| `has_more` | `bool` | 是否还有更多数据 |
| `rows` | `repeated Row` | 查询结果数据行 |

#### 数据行结构 (Row)

| 字段 | 类型 | 说明 |
|------|------|------|
| `timestamp` | `int64` | 数据点的时间戳 |
| `tags` | `map<string, string>` | 标签集合 |
| `fields` | `map<string, FieldValue>` | 字段值集合 |

#### 使用示例

```go
package main

import (
    "context"
    "log"
    "time"

    "google.golang.org/grpc"
    pb "micro-ts/internal/api/pb"
)

func main() {
    conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    client := pb.NewMicroTSClient(conn)

    now := time.Now()
    resp, err := client.QueryRange(context.Background(), &pb.QueryRangeRequest{
        Database:    "metrics",
        Measurement: "cpu_usage",
        StartTime:   now.Add(-1 * time.Hour).UnixNano(),
        EndTime:     now.UnixNano(),
        Tags:        map[string]string{"host": "server-01"},
        Limit:       100,
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Query returned %d rows, total: %d", len(resp.Rows), resp.TotalCount)
    for _, row := range resp.Rows {
        log.Printf("Timestamp: %d, Tags: %v, Fields: %v",
            row.Timestamp, row.Tags, row.Fields)
    }
}
```

---

### ListMeasurements

列出指定数据库中的所有测量项。

#### 方法签名

```protobuf
rpc ListMeasurements(ListMeasurementsRequest) returns (ListMeasurementsResponse);
```

#### 功能说明

获取指定数据库中所有已存在的测量项名称列表。

#### 请求参数 (ListMeasurementsRequest)

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `database` | `string` | 是 | 数据库名称 |

#### 响应结构 (ListMeasurementsResponse)

| 字段 | 类型 | 说明 |
|------|------|------|
| `measurements` | `repeated string` | 测量项名称列表 |

#### 使用示例

```go
package main

import (
    "context"
    "log"

    "google.golang.org/grpc"
    pb "micro-ts/internal/api/pb"
)

func main() {
    conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    client := pb.NewMicroTSClient(conn)

    resp, err := client.ListMeasurements(context.Background(), &pb.ListMeasurementsRequest{
        Database: "metrics",
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Database 'metrics' has %d measurements:", len(resp.Measurements))
    for _, name := range resp.Measurements {
        log.Printf("  - %s", name)
    }
}
```

---

### Health

健康检查接口。

#### 方法签名

```protobuf
rpc Health(HealthRequest) returns (HealthResponse);
```

#### 功能说明

用于服务健康检查和存活探测。返回服务当前状态和服务版本信息。

#### 请求参数 (HealthRequest)

无参数。

#### 响应结构 (HealthResponse)

| 字段 | 类型 | 说明 |
|------|------|------|
| `healthy` | `bool` | 服务是否健康 |
| `version` | `string` | 服务版本号 |

#### 使用示例

```go
package main

import (
    "context"
    "log"

    "google.golang.org/grpc"
    pb "micro-ts/internal/api/pb"
)

func main() {
    conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    client := pb.NewMicroTSClient(conn)

    resp, err := client.Health(context.Background(), &pb.HealthRequest{})
    if err != nil {
        log.Fatal(err)
    }

    if resp.Healthy {
        log.Printf("Service is healthy, version: %s", resp.Version)
    } else {
        log.Printf("Service is unhealthy")
    }
}
```

---

## 消息定义

### FieldValue

字段值类型，支持多种数据类型的联合类型。

```protobuf
message FieldValue {
    oneof value {
        int64 int_value = 1;
        double float_value = 2;
        string string_value = 3;
        bool bool_value = 4;
    }
}
```

**字段说明**:

| 字段 | 类型 | 说明 |
|------|------|------|
| `int_value` | `int64` | 整型值 |
| `float_value` | `double` | 浮点型值 |
| `string_value` | `string` | 字符串值 |
| `bool_value` | `bool` | 布尔值 |

**注意**: `oneof` 类型意味着一次只能设置一个字段值。

---

### WriteRequest

单个数据点写入请求。

```protobuf
message WriteRequest {
    string database = 1;
    string measurement = 2;
    map<string, string> tags = 3;
    int64 timestamp = 4;
    map<string, FieldValue> fields = 5;
}
```

---

### WriteBatchRequest

批量写入请求。

```protobuf
message WriteBatchRequest {
    repeated WriteRequest points = 1;
}
```

---

### WriteResponse

单个写入响应。

```protobuf
message WriteResponse {
    bool success = 1;
    string error = 2;
}
```

---

### WriteBatchResponse

批量写入响应。

```protobuf
message WriteBatchResponse {
    bool success = 1;
    string error = 2;
    int32 count = 3;
}
```

---

### QueryRangeRequest

范围查询请求。

```protobuf
message QueryRangeRequest {
    string database = 1;
    string measurement = 2;
    int64 start_time = 3;
    int64 end_time = 4;
    repeated string fields = 5;
    map<string, string> tags = 6;
    int64 offset = 7;
    int64 limit = 8;
}
```

---

### QueryRangeResponse

范围查询响应。

```protobuf
message QueryRangeResponse {
    string database = 1;
    string measurement = 2;
    int64 start_time = 3;
    int64 end_time = 4;
    int64 total_count = 5;
    bool has_more = 6;
    repeated Row rows = 7;
}
```

---

### Row

数据行结构。

```protobuf
message Row {
    int64 timestamp = 1;
    map<string, string> tags = 2;
    map<string, FieldValue> fields = 3;
}
```

---

### ListMeasurementsRequest

列出测量项请求。

```protobuf
message ListMeasurementsRequest {
    string database = 1;
}
```

---

### ListMeasurementsResponse

列出测量项响应。

```protobuf
message ListMeasurementsResponse {
    repeated string measurements = 1;
}
```

---

### HealthRequest

健康检查请求。

```protobuf
message HealthRequest {
}
```

---

### HealthResponse

健康检查响应。

```protobuf
message HealthResponse {
    bool healthy = 1;
    string version = 2;
}
```

---

## 错误码

gRPC 状态码用于表示错误类型：

| 状态码 | 说明 | 可能触发场景 |
|--------|------|-------------|
| `OK` (0) | 请求成功 | - |
| `CANCELLED` (1) | 操作被取消 | 客户端取消请求 |
| `UNKNOWN` (2) | 未知错误 | 未预期的内部错误 |
| `INVALID_ARGUMENT` (3) | 无效参数 | 缺少必填字段、参数格式错误 |
| `DEADLINE_EXCEEDED` (4) | 超时 | 请求处理超时 |
| `NOT_FOUND` (5) | 资源不存在 | 数据库或测量项不存在 |
| `ALREADY_EXISTS` (6) | 资源已存在 | - |
| `RESOURCE_EXHAUSTED` (8) | 资源耗尽 | 存储空间不足 |
| `UNAVAILABLE` (14) | 服务不可用 | 服务未启动或维护中 |
| `INTERNAL` (13) | 内部错误 | 服务器内部错误 |

---

## 使用示例

### 完整客户端示例

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    pb "micro-ts/internal/api/pb"
)

func main() {
    // 建立连接
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    conn, err := grpc.DialContext(ctx, "localhost:50051",
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    )
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer conn.Close()

    client := pb.NewMicroTSClient(conn)

    // 1. 健康检查
    health, err := client.Health(context.Background(), &pb.HealthRequest{})
    if err != nil {
        log.Fatalf("Health check failed: %v", err)
    }
    fmt.Printf("Service healthy: %v, version: %s\n", health.Healthy, health.Version)

    // 2. 写入数据
    writeResp, err := client.Write(context.Background(), &pb.WriteRequest{
        Database:    "demo",
        Measurement: "temperature",
        Tags: map[string]string{
            "location": "room-a",
            "device":   "sensor-01",
        },
        Timestamp: time.Now().UnixNano(),
        Fields: map[string]*pb.FieldValue{
            "value": {
                Value: &pb.FieldValue_FloatValue{FloatValue: 23.5},
            },
        },
    })
    if err != nil {
        log.Fatalf("Write failed: %v", err)
    }
    fmt.Printf("Write success: %v\n", writeResp.Success)

    // 3. 范围查询
    queryResp, err := client.QueryRange(context.Background(), &pb.QueryRangeRequest{
        Database:    "demo",
        Measurement: "temperature",
        StartTime:   time.Now().Add(-24 * time.Hour).UnixNano(),
        EndTime:     time.Now().UnixNano(),
        Limit:       100,
    })
    if err != nil {
        log.Fatalf("Query failed: %v", err)
    }
    fmt.Printf("Query returned %d rows\n", len(queryResp.Rows))

    // 4. 列出测量项
    listResp, err := client.ListMeasurements(context.Background(), &pb.ListMeasurementsRequest{
        Database: "demo",
    })
    if err != nil {
        log.Fatalf("List measurements failed: %v", err)
    }
    fmt.Printf("Measurements: %v\n", listResp.Measurements)
}
```

---

## 附录

### Proto 文件位置

- **源文件**: `proto/microts.proto`
- **生成代码**: `internal/api/pb/`

### 版本信息

- **当前版本**: `1.0.0`
- **Protocol Buffers 版本**: `proto3`
