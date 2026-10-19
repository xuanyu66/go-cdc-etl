# Go CDC + Lua 本地 MVP

真实 MySQL binlog → 用户 Lua 转换 → 本地幂等 JSONL + 原子提交 manifest。

支持固定表、ROW/FULL、只做增量、复杂无状态 Lua、按业务键 upsert/delete、多输出、持久化 checkpoint、暂停恢复和进程重启恢复。

本次交付是单用户本地 MVP。实际 Sink 是本地磁盘，**不是 Iceberg、Parquet 或 S3**；Iceberg v2 + Parquet 仍是后续目标。未实现多租户隔离、分布式调度、自动扩缩容和云缓冲，不用于运行外部不可信代码。

## 启动

需要 Go 1.26+、Docker Compose、curl、Python 3。命令均在项目根目录执行。

```bash
make mysql-up
make build
ETL_MYSQL_PASSWORD=etl-demo-password ./bin/etl serve
```

HTTP 默认绑定 `127.0.0.1:8080`，MySQL 为独立 Compose 容器 `127.0.0.1:13306`。初始化账号及口令仅供本机演示。

另一终端创建函数、发布管道并启动实例：

```bash
./scripts/demo.sh
```

然后向 `shop.orders` 写入数据，按返回的 `instance_id` 查询输出。完整步骤、API 语义及本次功能范围见 [用户文档](docs/user-guide.md)。

## 验证

```bash
make test
make e2e
```

`make e2e` 使用真实 MySQL、HTTP API、Lua 子进程和临时磁盘；不会自动启动容器，需要先执行 `make mysql-up`。默认 `go test ./...` 不会运行需要数据库的 E2E。[E2E 说明](e2e/README.md)

## 代码结构

| 目录 | 内容 |
| --- | --- |
| `cmd/etl` | serve、validate、隔离进程 Lua worker 命令 |
| `internal/mysqlcdc` | 真实复制流、事务组装、schema 检查、无损事件编码 |
| `internal/luaruntime` | Lua 函数、精确小数 SDK、子进程执行 |
| `internal/localstore` | schema/键校验、幂等写入、manifest/checkpoint、当前结果读取 |
| `internal/server` | 已实现的 HTTP API、版本与实例生命周期、持久化控制面 |
| `pkg/pipeline` | 已实现 MVP 图定义、严格 JSON 解析及图编译 |
| `api` | 当前 MVP 的 OpenAPI 和 JSON Schema |
| `e2e` | Go integration 测试 |
| `pkg` 其余包 | 早期扩展接口草案，尚未接入运行链路，不是额外已支持后端 |

[总体设计](docs/design.md) 与 [调研](docs/research/) 保留后续架构讨论；本次已实现范围以 [用户文档](docs/user-guide.md) 和 OpenAPI 为准。已知缺陷与后续规划见 [Roadmap](docs/roadmap.md)。
