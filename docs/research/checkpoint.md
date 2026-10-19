# S3 Sink 与 Checkpoint 语义调研

状态：设计调研及接口说明；不表示生产适配器已经实现。核验日期：2026-10-19。本文补充 [总体设计](../design.md)，代码契约以 [checkpoint](../../pkg/checkpoint/checkpoint.go) 与 [sink](../../pkg/sink/sink.go) 的实际文件为准。

## 1. 首版结论与边界

首版采用 **至少一次执行 + 独立尝试 S3 对象 + 元数据提交账本 + 完整事务 checkpoint**。这里的持久化元数据 Store 是生产要求；当前内存参考实现只验证协议，重启丢失所有状态，不能用于真实 CDC。

- Listener 读取位置只表示接收进度；checkpoint 表示此前完整事务的全部必需分支均已可靠完成。
- `Receipt` 表示某个 Sink/batch 输出已持久化；它不等于整个 DAG 已提交，即 **persisted != globally committed**。
- Coordinator 通过 `Store.Commit` 原子检查所有 required writes、当前租约、连续事务序号和前驱 revision，再同时写 commit record 与推进 checkpoint。
- S3 与元数据库没有本设计可用的跨系统原子事务。上传后、元数据提交前崩溃时，文件可能已经可见，恢复也可能新增重复文件。
- 首版不提供 committed manifest 查询或事务原子可见性；直接扫描 S3 的消费者必须接受未提交的部分输出，依据稳定事件标识识别重复。

S3 为成功写入后的对象读取提供强一致性，单 key 更新原子；它没有跨 key 原子更新。这支持核验上传结果，却不能替代 DAG 提交协议。[S3 一致性模型](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)

## 2. 提交单位、稳定身份与执行顺序

**提交单位是完整源事务，执行单位是事务内批次，物理输出单位是有界对象。** 第一版每个对象只属于一个事务，不混写多个事务。大事务可拆成多个批次，但每条 pipeline 按源事务和批次顺序执行，同一时刻只处理一个批次。不同来源或独立实例可并行；事务内批次、分支和上传并行属于后续扩展。

事务计划必须固定来源代次、事务身份、事务序号、恢复位置、批次划分、pipeline/转换版本和 required writes。重试、扩缩容和重新部署不能更改这些逻辑身份。每个 `Write` 是 `(SinkID, BatchID)`；全部过滤或没有数据路由到的分支也要有显式 `Empty` receipt，不能因为没有 S3 对象就永远阻止 checkpoint。

GTID 标识源事务，不是可在不同 source UUID 间直接比较的全局递增整数。提交顺序使用接收并规划时分配的源序号；恢复位置保存已消费集合或完整事务后的 file/position，不用单条事件位置代替。[MySQL GTID 定义](https://dev.mysql.com/doc/refman/8.4/en/replication-gtids-concepts.html)

事件及输出身份沿用总体设计；输出携带 pipeline、事件 ID、转换版本和 output index。DAG 的多个 Sink 输出还应保留 Sink 身份，配置切换通过不可变 pipeline version 和实例 generation 隔离。

首版对象 key 包含 `attempt_id`，与总体设计一致。一次新的上传尝试或恢复重放可以生成新 key；因此 receipt 能记录已知对象，但并不保证 S3 中只有一份输出。超时或崩溃后的未知成功对象可能成为未引用文件。不能将文件命名或事件 ID 当作 exactly-once 证明。

## 3. 首版提交协议与 fencing

```text
读取完整事务
  → Prepare 不可变事务计划与 required sink/batch writes
  → 顺序转换并校验一个批次
  → Sink.Persist 上传本批次的全部对象
  → RecordReceipt 记录持久化确认
  → 继续后续批次，直到全部 required writes 完成
  → Store.Commit 原子写 commit record + checkpoint
```

所有声明的 Sink 在首版都是 required。某个 Sink 已成功、另一个失败时，checkpoint 保持不变；已知成功 receipt 可以保留并避免重复写，未知结果重试仍可能生成重复对象。确定性转换错误暂停流水线，临时存储错误采用有界退避。

生产 PostgreSQL Store 的 `Commit` 在一个事务中锁定同一实例的 lease/checkpoint 行，检查 owner、递增 fence、数据库时间下的有效期、expected revision、连续 sequence，以及 required writes 的全部 receipts。然后插入唯一 commit record、更新 checkpoint/revision 并提交。接管、续租和提交遵循同一行锁协议；S3 网络调用放在数据库事务外。数据库行锁能力参见 [PostgreSQL 锁文档](https://www.postgresql.org/docs/current/explicit-locking.html)。

数据库提交是 checkpoint 的线性化点。旧 worker 可能仍将 S3 请求完成，但它无法提交元数据库记录。fencing 保护元数据，**不能撤销已发出的 S3 上传**。丧失租约立即取消调度；迟到对象可能成为未引用文件，至少一次语义允许这一结果。若同一提交成功但客户端响应丢失，以同一 expected revision/sequence 重试应返回已提交结果，不重复推进。

## 4. 与代码框架一致的接口

以下列出当前接口职责，实际签名以 `pkg/checkpoint` 和 `pkg/sink` 为准；不维护第二套兼容接口。

| 类型 | 含义 |
| --- | --- |
| `Scope` | 从认证所有权派生的 tenant、instance、generation |
| `Lease` | owner、递增 fence 和有效期 |
| `Cursor` | revision、连续 sequence、事务 ID 和 ResumeAfter |
| `Write` | 一个必需 Sink/batch 写入身份 |
| `Prepared` | 不可变事务计划：sequence、事务 ID、PlanDigest、恢复位置、全部 required writes |
| `Receipt` | Write 的对象列表，或显式 Empty；只表示持久化 |
| `CommitRecord` | 原子提交后的 Cursor、PlanDigest 与 receipts |
| `Store` | 租约管理、Prepare、RecordReceipt 和 Commit |

```go
// checkpoint.Store
Acquire(context.Context, Scope, string, time.Duration) (Lease, error)
Renew(context.Context, Lease, time.Duration) (Lease, error)
Load(context.Context, Scope) (Cursor, error)
Prepare(context.Context, Lease, uint64, Prepared) error
RecordReceipt(context.Context, Lease, uint64, Receipt) error
Commit(context.Context, Lease, uint64, uint64) (CommitRecord, error)

// sink.Writer
Persist(context.Context, Request) (checkpoint.Receipt, error)
```

`Prepare` 和 `RecordReceipt` 对相同内容幂等，相同逻辑身份不同内容拒绝。`Commit` 在 Store 内部原子验证，调用方预检查不能替代存储约束。当前错误分类为 `ErrFenced`、`ErrConflict`、`ErrIncomplete` 和 `ErrInvalid`。Sink 不提交 checkpoint，不声称全 DAG 成功。

首版 `Prepared` 固定逻辑 writes，并未持久化每个物理对象的预期 key、大小和摘要。`Receipt` 在上传成功后记录这些信息。后续若实现严格对象复用，需要新增上传前的对象计划与恢复查询，不能假定当前接口已具备该能力。

## 5. 状态机、重试与恢复

```text
PREPARED → PERSISTING → PERSISTED → COMMITTED
                ↘ retry / PAUSED
```

`PERSISTED` 表示所有 required receipts 已记录，仍未提交；`COMMITTED` 与 checkpoint 更新处于同一原子操作。`PAUSED` 是运行状态，不销毁计划。生产 Store 需持久化计划和 receipts，接管后恢复未完成事务；内存参考 Store 无法提供进程崩溃恢复。

| 故障时点 | 恢复动作 | Checkpoint |
| --- | --- | --- |
| 计划持久化前崩溃 | 从旧位置重读并建计划 | 不变 |
| 部分批次成功 | 复用已知 receipts，继续缺失 writes | 不变 |
| 上传成功、receipt 未入库 | 未知成功可能产生重复文件，按原逻辑 write 重试 | 不变 |
| Sink A 成功、Sink B 失败 | 保留 A 的已知 receipt，补 B | 不变 |
| 全部 receipt 成功、提交前崩溃 | 当前 owner 验证完整性后原子提交 | 提交后前进 |
| Commit 成功、响应丢失 | 重试同一提交，返回原结果 | 不重复推进 |
| 旧 worker 上传迟到 | 不能更新 checkpoint；文件可能成为孤儿 | 新 owner 决定 |
| binlog 已过期 | 暂停，需要恢复方案或新来源代次 | 不伪造前进 |

临时网络错误、S3 5xx/限流采用 AWS SDK standard retry，并显式配置尝试次数和截止时间；外层使用带抖动退避及总重试预算。不能在 SDK 与 Coordinator 两层无限重试。权限、桶配置错误应分类暂停。客户端超时并不证明远端请求未完成。[AWS SDK for Go v2 重试与超时](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-retries-timeouts.html)

SHA-256 记录实际压缩字节，`ETag` 不当作通用 MD5。上传校验算法应与完整对象/分片 checksum 的语义匹配。首版具体 S3 上传适配器仍待实现。[S3 上传完整性校验](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html)

对象保留期应覆盖消费与恢复需求，不删除仍被账本引用的对象。孤儿清理需检查活动计划、commit 引用和保护窗口；首版可先提供人工审计，不自动删除未知文件。

## 6. 后续候选：稳定对象 key、条件写与 manifest

本节是改进方向，**不是首版协议或已实现 API**。

### 稳定对象 key 与条件写

如需减少重放重复，可新增上传前的持久化对象计划，固定 key、字节数、SHA-256、记录顺序、编码器版本和 gzip header。稳定 key 不包含 fence 或 attempt ID；跨接管复用同一对象。重放产生不同字节时报内容冲突，不能静默覆盖。这要求变更当前 Prepared/对象计划及恢复协议，并验证确定性编码。

在此方案下使用 `PutObject` 的 `If-None-Match: *`。`412` 不能直接当成功，须核对已有对象身份及真实内容摘要；需要时读取 checksum 或下载计算。PUT 的 `409` 可重试；multipart 完成时的 `409` 需要重新发起 multipart。条件写防覆盖，不验证平台 fencing token，也不提供跨对象事务。[S3 条件写](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)

### Committed manifest

后续可提供从元数据 commit records 查询的只读 API，让消费者只读取已提交对象集合；当前 OpenAPI 未实现这些端点。若导出 S3 manifest，上传 manifest 后崩溃仍不能证明元数据已提交，需定义单一提交真相及数据库 outbox 发布协议。S3 LIST、文件存在或对象通知均不能作为提交依据。

即便实现上述优化，也不能自动宣称任意下游 exactly-once。通过 manifest 读取的语义与直接枚举 S3 文件不同，必须分别说明。

## 7. 验证清单

- 在每个状态迁移前后注入崩溃，验证不丢事务、不越过未完成分支、重复提交幂等。
- 双 worker 竞争、租约到期和延迟上传，验证旧 fence 无法更新 checkpoint。
- 大事务多批、多 Sink、完全过滤与无数据路由分支，都必须有全部 required receipts。
- PUT 成功后丢失响应，验证重试允许重复文件，同时逻辑事件身份稳定。
- 提交前切换版本，验证 pending 事务固定原版本，新版本在完整事务边界生效。
- 内存参考测试只能证明单进程原子协议；生产 PostgreSQL 故障恢复与真实 S3 错误行为必须分别集成验证。
- 后续条件写候选单独测试 412 内容匹配/冲突、409、multipart 重建及确定性编码。

以上提交机制是本项目工程设计，不是 AWS 或 PostgreSQL 自动提供的跨系统事务。
