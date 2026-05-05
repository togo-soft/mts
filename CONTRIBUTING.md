# 贡献者指南

感谢您对 micro-ts 项目的关注！本指南将帮助您快速搭建开发环境并参与项目贡献。

## 目录

- [开发环境准备](#开发环境准备)
- [克隆和构建](#克隆和构建)
- [运行测试](#运行测试)
- [代码规范](#代码规范)
- [提交 PR 流程](#提交-pr-流程)
- [代码审查清单](#代码审查清单)
- [性能测试要求](#性能测试要求)

## 开发环境准备

### 必需依赖

| 工具 | 版本要求 | 用途 |
|------|----------|------|
| Go | 1.24+ | 主要开发语言 |
| make | 任意版本 | 构建自动化（可选） |
| protobuf | 3.x+ | Protocol Buffers 编译 |
| protoc-gen-go | 最新版 | Go 代码生成 |
| protoc-gen-go-grpc | 最新版 | gRPC 代码生成 |

### 安装依赖

**Ubuntu/Debian:**
```bash
# 安装 Go（参考 https://golang.org/doc/install）
# 安装 protobuf 编译器
sudo apt-get install -y protobuf-compiler

# 安装 Go 插件
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

**macOS:**
```bash
# 使用 Homebrew
brew install go protobuf protoc-gen-go protoc-gen-go-grpc
```

**验证安装:**
```bash
go version
protoc --version
```

## 克隆和构建

### 克隆仓库

```bash
git clone https://codeberg.org/micro-ts/mts.git
cd micro-ts
```

### 构建项目

```bash
# 下载依赖
go mod download

# 编译所有组件
go build ./...

# 编译服务器
export CGO_ENABLED=0
go build -o bin/microts-server ./cmd/server
```

### 生成 protobuf 代码

修改 `proto/microts.proto` 后需要重新生成 Go 代码：

```bash
cd proto
protoc --go_out=../internal/api/pb --go_opt=paths=source_relative \
       --go-grpc_out=../internal/api/pb --go-grpc_opt=paths=source_relative \
       microts.proto
```

## 运行测试

### 单元测试

```bash
# 运行所有测试
go test ./...

# 运行特定包测试
go test ./internal/storage/...
go test ./internal/engine/...

# 带覆盖率报告
go test -cover ./...

# 生成详细覆盖率报告
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

### E2E 测试

```bash
# 写入测试
go run tests/e2e/write_1k/main.go

# 查询测试
go run tests/e2e/query_1k/main.go
```

### 覆盖率要求

- **所有代码的行覆盖率必须 >= 90%**
- 新增功能必须包含完整的测试用例
- 使用 `go test -cover` 验证覆盖率达标

## 代码规范

### 格式化

项目使用以下工具进行代码格式化：

```bash
# 安装工具
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
go install github.com/incu6us/goimports-reviser/v3@latest

# 运行 goimports-reviser 格式化导入
goimports-reviser -rm-unused -set-alias -format ./...

# 运行 golangci-lint 检查
golangci-lint run ./...
```

### 代码风格规范

#### 错误处理

**所有错误必须显式处理**，禁止忽略错误：

```go
// 正确：显式处理错误
if err != nil {
    return fmt.Errorf("operation failed: %w", err)
}

// 正确：使用 _ 明确表示忽略
_ = file.Close()

// 错误：完全忽略错误
file.Close()
```

#### 文件权限

创建文件和目录时必须使用安全权限：

```go
// 目录权限 0700（仅所有者可读写执行）
err := os.MkdirAll(dataDir, 0700)

// 文件权限 0600（仅所有者可读写）
err := os.WriteFile(filename, data, 0600)

// 可执行文件使用 0700
err := os.WriteFile(scriptPath, data, 0700)
```

#### 禁用 defer 在循环中

```go
// 错误：在 for 循环中使用 defer
for _, item := range items {
    f, _ := os.Open(item)
    defer f.Close()  // 禁用！
}

// 正确：使用函数封装
func processItem(item string) error {
    f, err := os.Open(item)
    if err != nil {
        return err
    }
    defer f.Close()
    // 处理逻辑
}

for _, item := range items {
    if err := processItem(item); err != nil {
        return err
    }
}
```

#### 其他规范

- 函数长度不超过 50 行
- 文件长度不超过 300 行
- 嵌套层级不超过 3 层
- 位置参数不超过 3 个
- 圈复杂度不超过 10
- 禁止魔法数字

## 提交 PR 流程

### 1. 创建分支

```bash
git checkout -b feature/your-feature-name
# 或
git checkout -b fix/issue-description
```

### 2. 开发并提交

```bash
# 修改代码...

# 格式化代码
goimports-reviser -rm-unused -set-alias -format ./...

# 运行 lint
golangci-lint run ./...

# 运行测试
go test ./...

# 提交（遵循提交规范）
git add .
git commit -m "feat(storage): 添加 WAL 压缩功能"
```

### 3. 推送并创建 PR

```bash
git push origin feature/your-feature-name
```

在 GitHub 上创建 Pull Request，填写 PR 模板。

### 提交规范

格式：`<type>(<scope>): <description>`

| Type | 说明 |
|------|------|
| `feat` | 新功能 |
| `fix` | Bug 修复 |
| `refactor` | 重构 |
| `docs` | 文档修改 |
| `test` | 测试相关 |
| `chore` | 构建/工具相关 |

示例：
```
feat(storage): 添加 SSTable 合并压缩
fix(query): 修复时间范围查询边界问题
docs(api): 更新 gRPC 接口文档
```

## 代码审查清单

提交 PR 前请自检以下项目：

### 功能实现
- [ ] 代码实现符合需求描述
- [ ] 处理了所有错误路径
- [ ] 没有并发安全问题
- [ ] 资源（文件、连接等）正确释放

### 测试
- [ ] 新增代码有对应的单元测试
- [ ] 测试覆盖率达到 90% 以上
- [ ] 所有测试通过 `go test ./...`
- [ ] 边界条件和错误路径有测试覆盖

### 代码质量
- [ ] 通过 `golangci-lint run ./...`
- [ ] 通过 `goimports-reviser` 格式化
- [ ] 没有 `for` 循环中使用 `defer`
- [ ] 文件权限使用 0600/0700
- [ ] 没有魔法数字
- [ ] 函数/文件/嵌套符合长度限制

### 文档
- [ ] 公共 API 有 GoDoc 注释
- [ ] 复杂的逻辑有 inline 注释
- [ ] 修改了相关的 README/文档（如需要）

## 性能测试要求

### 运行基准测试

```bash
# 运行所有基准测试
go test -bench=. -benchmem ./internal/storage/...

# 特定组件测试
# MemTable 性能
go test -bench=. -benchmem ./internal/storage/shard

# WAL 序列化性能
go test -bench=. -benchmem ./internal/storage/shard -run=BenchmarkEncode

# SSTable 读取性能
go test -bench=. -benchmem ./internal/storage/shard/sstable
```

### 基线对比

性能敏感代码修改前需要保存基线：

```bash
# 保存修改前基线
go test -bench=. -benchmem ./internal/storage/shard > benchmark/baseline.txt

# 修改代码后对比
go test -bench=. -benchmem ./internal/storage/shard > benchmark/current.txt

# 使用 benchcmp 或 benchstat 对比
go install golang.org/x/perf/cmd/benchstat@latest
benchstat benchmark/baseline.txt benchmark/current.txt
```

### 关键性能指标

| 组件 | 关键指标 | 基线要求 |
|------|----------|----------|
| MemTable 写入 | ns/op, B/op, allocs/op | 参考现有基准 |
| WAL 序列化 | ns/op, B/op, allocs/op | 参考现有基准 |
| SSTable 读取 | ns/op, B/op, allocs/op | 参考现有基准 |
| 查询执行 | ns/op | 参考现有基准 |

### 性能回归

- 性能优化 PR 需附上基准测试对比结果
- 禁止无说明的性能下降 > 10%
- 内存分配增加需有明确理由

---

如有疑问，请通过 Issue 或邮件联系维护团队。感谢贡献！
