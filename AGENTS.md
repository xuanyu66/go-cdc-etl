# AGENTS.md

面向 AI 编码助手与协作者的项目指南。改动代码前请通读本文件；改动了本文档描述的内容时，请同步更新本文档。

## 项目一句话

MySQL binlog CDC → 用户 Lua 转换 → 本地幂等 JSONL 落盘 + 原子提交 manifest 的单用户本地 ETL 工具（Go 1.26+）。当前是 MVP，sink 不是 Iceberg/Parquet/S3。

## 构建与验证

```bash
make mysql-up    # 启动 MySQL 8.4 容器（127.0.0.1:13306，e2e 前置）
make build       # 产出 bin/etl
make test        # go test ./...（不含需数据库的 e2e）
make e2e         # 真实 MySQL + HTTP API + Lua 子进程的集成测试，需先 mysql-up
```

提交前必须满足：`gofmt`/`go vet` 干净、`make test` 全绿；改了 CDC/幂等/生命周期逻辑时跑 `make e2e`。

## 目录速查

| 路径 | 内容 |
| --- | --- |
| `cmd/etl` | serve / validate / 隔离进程 Lua worker 命令 |
| `internal/mysqlcdc` | 真实复制流、事务组装、schema 检查 |
| `internal/luaruntime` | Lua 函数、精确小数 SDK、子进程执行（时间盒隔离，非资源沙箱） |
| `internal/localstore` | 幂等写入、manifest/checkpoint、当前结果读取 |
| `internal/server` | HTTP API、版本与实例生命周期、持久化控制面 |
| `internal/model`、`pkg/cdc` | 两套 CDC 模型并存（字段类型不统一，见 docs/roadmap.md K20），改这里前先确认走哪套 |
| `pkg/` 其余包 | 未接入运行链路的接口草案，修改运行链路时不要假设它们生效 |
| `api/` | OpenAPI 与 JSON Schema，由 `scripts/generate_api.py` 单一事实源生成，改 schema 只改脚本再重新生成 |
| `docs/` | 设计、用户文档、调研、roadmap |
| `e2e/` | Go 集成测试 |

## 硬性约定

1. **文档诚实优先**：新增功能必须同步更新 `docs/user-guide.md` 与 OpenAPI；未实现的能力在文档中显式标注"未实现"，禁止过度承诺。这是本项目的核心纪律。
2. **契约三方同步**：HTTP 行为 ↔ `internal/server` ↔ `api/openapi.json` 必须一致；改 API 后运行 `scripts/generate_api.py` 并 diff `api/` 确认无漂移。
3. **幂等链不可破坏**：transaction ID 内容寻址 + manifest 原子发布 + 文件 checksum 是核心正确性保证。任何改动不得引入"写一半即可见"的中间态；checkpoint 只能在成功提交后推进。
4. **Lua 隔离边界**：worker 是独立子进程 + 2s 时间盒 + 空环境变量，**不是安全沙箱**（不限内存）。不要把需要资源隔离的不可信代码执行需求往里塞，除非同步实现 RLIMIT/cgroup（见 roadmap K4）。
5. **锁粒度**：`internal/server` 的全局锁只应保护 catalog 短临界区，禁止在持锁期间新增网络 I/O 或子进程调用（现有问题见 roadmap K1，修复中不要回退）。
6. **中文文档**：docs 与 README 用中文；代码、commit message、API 错误文本用英文。

## 常见坑

- e2e 不会自动起容器：`make e2e` 前先 `make mysql-up`，否则相关测试被跳过且不易察觉。
- MySQL 前置条件：源需 ROW/FULL binlog、binlog 未过期、未开 `binlog_transaction_compression`（遇 `TRANSACTION_PAYLOAD_EVENT` 会停止，见 roadmap K7）。
- 错误处理：当前内部错误统一返回 422 并透出 detail（已知缺陷 K5），新增代码不要扩大这一模式——内部错误用 500/503。
- 幂等冲突（`IDEMPOTENT_CONFLICT` / `SEQUENCE_GAP` / `CONTENT_CONFLICT`）是终态语义，不要私自添加自动重试掩盖。
- `catalog.json`（控制面）与 store manifest（数据面）是两套持久化真相，崩溃恢复靠各自幂等拼回；改动任一侧的写入路径时对照 roadmap K18 评估。

## 规划参照

已知缺陷与能力缺口集中维护在 [docs/roadmap.md](docs/roadmap.md)。领新任务前先查该文档：修 bug 时在对应条目标注进展，引入新限制（如容量上限）时新增条目。
