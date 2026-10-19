# Roadmap 与已知限制

本文档记录当前 MVP 的**已知缺陷**（代码中已存在、待修复的问题）和**缺乏的能力**（相对完整 CDC/ETL 产品的差距），以及后续规划优先级。已实现范围以 [用户文档](user-guide.md) 和 OpenAPI 为准；本文档随代码演进更新，最近核验日期见文末。

## 已知缺陷（Known Issues）

### 严重

| # | 问题 | 位置 | 说明与计划 |
| --- | --- | --- | --- |
| K1 | 全局互斥锁内执行重 I/O | `internal/server/http.go:76` | `mutate()` 持锁期间执行 MySQL Probe（最长 10s）和 Lua worker 子进程（最长 2s），期间所有读 API 排队。计划：锁内只做"校验 + 登记"短临界区，重操作移出锁外、结果二次校验后登记。 |
| K2 | CDC 零重连，错误会把实例永久钉死 | `internal/mysqlcdc/mysql.go:187`、`internal/server/server.go:241` | `DisableRetrySync` + 出错即暂停且改写 DesiredState 并持久化，进程重启也不恢复。计划：增加指数退避重连（区分可重试网络错误与不可重试 schema/权限错误），暂停不再改写 DesiredState。 |
| K3 | SEQUENCE_GAP / CONTENT_CONFLICT 无恢复出口 | `internal/localstore/store.go:214` | 一旦出现 catalog 与 manifest 不一致，实例永久卡死，只能删数据目录。计划：提供"截断到指定 sequence 重新提交"的 repair/resync 运维接口。 |
| K4 | Lua 隔离是时间盒而非资源沙箱 | `internal/luaruntime/runtime.go:24` | 2s 超时只限时间不限内存，恶意/失控脚本可在超时前耗尽宿主内存。空环境变量只挡了凭证继承。计划：worker 加 `RLIMIT_AS` 或独立 cgroup；文档已声明不构成安全边界。 |
| K5 | 内部错误一律 422 且外泄内部细节 | `internal/server/http.go:31` | MySQL 断连、磁盘 I/O 等内部错误被包装成"客户端请求无效"，客户端无法据此重试，detail 原文透出。计划：增加 500/503 分支并做错误脱敏。 |
| K6 | catalog 内存态靠 JSON 往返"回滚"，吞错且副作用不可回滚 | `internal/server/http.go:107` | `json.Marshal` 错误被忽略；fn 已执行的副作用（子进程跑过、目录已建）无法随内存态回滚。计划：校验与变更分离，先算完副作用再一次性落锁提交。 |

### 中等

| # | 问题 | 位置 | 说明与计划 |
| --- | --- | --- | --- |
| K7 | 不支持 `binlog_transaction_compression` 源 | `internal/mysqlcdc/mysql.go:213` | MySQL 8.0.20+ 开启事务压缩的源遇 `TRANSACTION_PAYLOAD_EVENT` 直接停止，Probe 阶段也未检测。计划：Probe 增加该开关检测并在文档列出前置条件。 |
| K8 | replication server ID 分配隐式假设"实例永不删除" | `internal/server/http.go:353` | 按 `ServerID + len(Instances)` 分配，catalog 回滚或手工编辑后可能分配出已占用 ID，两个 syncer 互相顶替。计划：改为从 catalog 实际占用值取 max + 1。 |
| K9 | 输出视图全历史重放且与提交路径共用一把锁 | `internal/localstore/store.go:336` | `Snapshot` 持锁重读全部数据文件并重放 upsert/delete，阻塞 worker `Commit`，历史变大后一次 GET 即可卡住数据面。计划：分页/增量快照/读写锁。 |
| K10 | manifest 64 MiB 硬上限 = 实例寿命上限 | `internal/localstore/store.go:25` | 每次提交全量重写，到顶后实例永久失败，无压缩/归档/截断。计划：manifest 分段或转追加日志，并在 capabilities 暴露水位。 |
| K11 | TIMESTAMP 语义无端到端测试 | `e2e/mysql_lua_test.go:52` | 建了 `timestampTable` 但从未 INSERT 断言 CDC 后的字符串格式，用户文档 §6 的 UTC 语义声明零测试背书。计划：补 e2e 断言。 |
| K12 | pause/resume 状态机靠字符串 + 互斥锁串行化硬撑 | `internal/server/server.go:216` | 旧 worker goroutine 的收尾可能覆盖 resume 后的状态，目前无测试守护该竞争窗口。计划：引入 generation/epoch 校验。 |
| K13 | `Validate` 硬编码 `context.Background()` + 1s | `internal/luaruntime/runtime.go:99` | HTTP 层传入的可取消 ctx 无效，无法随客户端断开取消；初始化复杂度上限不可配置。 |

### 低优先

| # | 问题 | 位置 | 说明 |
| --- | --- | --- | --- |
| K14 | 批处理循环 O(n²) marshal 且吞错 | `internal/server/server.go:292` | 每迭代全量重编 candidate，应增量估算或先 marshal 再切片。 |
| K15 | 每事件重建 VM 并重新编译整个 Lua 源码 | `internal/luaruntime/runtime.go:131` | 10k 事件事务 = 1 万次编译同一源码，`Parse` 一次复用 chunk 可提升一个数量级（保持每事件新全局环境语义）。 |
| K16 | `Server.Close` 超时路径泄漏 store/lock | `internal/server/server.go:159` | ctx 超时直接返回，跳过 `store.Close()` 和文件锁释放。 |
| K17 | `string.dump` 未从 Lua string 库剔除 | `internal/luaruntime/runtime.go:45` | 白名单完备性漏洞，本场景危害极小。 |
| K18 | catalog 与 store manifest 是两套持久化真相 | `internal/server/server.go:205`、`internal/localstore/store.go:267` | 崩溃恢复靠各自动作幂等拼回，K6/K8 的脆弱性源于此分裂。计划：design 文档补崩溃矩阵及对应测试。 |
| K19 | main 退出路径错误处理不严 | `cmd/etl/main.go:106` | `Shutdown` 错误被忽略，首个错误来源丢失。 |
| K20 | 两套 CDC 模型字段类型不一致 | `internal/model/model.go:8`、`pkg/cdc/types.go:13` | Position 的 JSON tag 与位宽（uint32 vs uint64）不统一，接 GTID/切主时是类型地雷，应现在统一。 |

## 缺乏的能力（Missing Capabilities）

相对一个可生产的 CDC/ETL 产品，MVP 明确未做、按优先级规划如下：

### P0 — 面试作业到"可演示给别人"的近期补齐

| 能力 | 现状 | 计划 |
| --- | --- | --- |
| CI | 无 `.github/workflows`，push 后测试不自动跑 | GitHub Actions：gofmt + `go vet` + `go test ./...` |
| 故障注入 e2e | 只有优雅 `Close()` 重启，无 SIGKILL/杀 worker/断连场景 | kill -9 中途崩溃恢复、Lua 执行中杀 worker、源断连重连 |
| `pkg/` 草案包归宿 | `buffer`/`cdc`/`checkpoint`/`listener`/`orchestrator`/`sink`/`transform` 七个包零测试且未接入运行链路 | 接入实现或降级为 docs 附件，避免"死代码"印象 |
| README 架构图 | 只有未实现愿景的 mermaid（`docs/design.md:26`） | 补一张 MVP 实际链路图（binlog → listener → Lua 子进程 → manifest） |
| LICENSE | 缺失 | 补 MIT/Apache-2.0 |

### P1 — 从 MVP 到可信的单机工具

| 能力 | 现状 | 计划 |
| --- | --- | --- |
| CDC 断线重连 | 零重连（见 K2） | 指数退避 + 心跳 + 错误分类 |
| 运维恢复出口 | 一致性错误只能删目录（见 K3） | repair/resync 接口 |
| API 错误语义 | 内部错误一律 422（见 K5） | 500/503 分支 + 脱敏 |
| 输出读取扩展性 | 全历史重放（见 K9） | 分页、增量快照 |
| manifest 规模 | 64 MiB 到顶即死（见 K10） | 分段 manifest / 追加日志 + 水位暴露 |
| 资源隔离 | 时间盒沙箱（见 K4） | RLIMIT/cgroup |
| Lua 性能 | 每事件重编译（见 K15） | chunk 复用 |
| 大事务处理 | 100 事件 / 900KB 批切分阈值固定 | 可配置 + 大事务跨批次拆分测试 |

### P2 — 调研中、方向性目标（详见 [design.md](design.md) 与 [research/](research/)）

- **Iceberg v2 + Parquet / S3 Sink**：当前 sink 仅为本地 JSONL，文档已明确声明
- **GTID 支持**：当前仅 binlog file + position（`internal/model`），切主场景依赖 GTID
- **多租户隔离、分布式调度、自动扩缩容、云缓冲**
- **WASM 替代 Lua 子进程**：design 中有讨论，用于更强隔离与更低进程开销
- **吞吐/延迟基准测试**：无任何 benchmark 数据

## 状态

- 文档初版：2026-10-19，基于全量代码评审（含 `go test ./...` 实测通过）
- 下次核验：每轮功能交付后更新对应条目状态
