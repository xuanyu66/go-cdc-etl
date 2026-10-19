# MySQL Listener 的扩缩容与内部 API

状态：调研与接口草案；核验日期：2026-10-19。本文不是生产实现，首版范围沿用 [项目设计](../design.md)。下文扩缩容策略与接口为本项目建议，引用资料只证明第三方能力。

## 1. 结论：可以弹性扩展，但要区分读取与执行

单一 MySQL 实例的一条 binlog 不天然提供 Kafka partition 一样的独立消费分区。首版采用每个 `pipeline instance + source` 一个 active reader，顺序解析事务；为同一 source 增加多个 reader 并按表过滤，通常会重复接收/扫描日志，不能宣称读取吞吐按副本数线性增长。

MySQL 描述的复制链路是 dump thread、receiver 与后续 applier。多线程复制主要并行化应用阶段，不意味着多个 receiver 会自动分担同一条日志。[MySQL replication threads](https://dev.mysql.com/doc/refman/8.4/en/replication-threads.html)。Debezium 的 MySQL connector 也固定使用一个 task；这是一项成熟实现的边界参考，不是对所有自研读取算法的理论禁止。[Debezium MySQL connector](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-property-tasks-max)。

| 扩容层 | 扩容单位 | 能解决的问题 | 边界 |
| --- | --- | --- | --- |
| Reader worker 池 | 不同来源/管道的 reader assignment | 更多租户、MySQL 实例或物理分片 | 同一个 assignment 只能有一个有效所有者 |
| 单 Reader | CPU、网络、解码与缓存优化 | 单源读取/解析瓶颈 | 不能无界扩内存；应单独压测单流上限 |
| Transform worker 池 | 已形成的独立无状态调用 | 用户转换 CPU 成本与突发 | 首版每管道在途事务仍为 1；并发来自不同管道 |
| Sink worker 池 | 独立对象上传任务 | 压缩/上传等待、跨管道并发 | 全部分支持久化后才能提交事务 |
| 来源物理分片 | 独立 MySQL 日志 | 提升总体源吞吐 | 需要业务分片或来源拓扑变化，不是 ETL 自动切表即可实现 |

从多个副本各读一遍通常用于可用性或隔离源负载，不能据此声称切开了同一 binlog。切换副本还必须验证 GTID 覆盖、源逻辑身份、schema 历史与所需 binlog 的可用性。

首版不做跨租户共享 reader：同一数据库的多条管道可以各自读取，有明确连接/源压力配额。未来共享 reader 时，必须先引入可靠 fan-out 和每订阅者独立进度，否则慢订阅者和日志过期会相互影响。

## 2. 第一版与后续扩展路径

### 第一版：保持事务顺序，先支持多来源弹性

```text
Scheduler → Reader assignment + lease
                          ↓
                    one ordered reader
                          ↓
                    committed transaction
                          ↓
             bounded transform / sink execution
                          ↓
               durable transaction completion
                          ↓
                    fenced checkpoint
```

每个管道只持有一个未提交事务，多个管道分配到 worker 池并行运行。已读事务可能因大小拆成多个转换批次，但提交单元仍是完整事务。该策略简单且与现有设计一致；代价是单条管道无法仅靠增加 executor 副本无限提速。单个大事务超预算时明确暂停，不为了扩容突破资源上限。

### 后续：单 Reader + 有界并行执行

如果原型证实转换或 S3 是单管道瓶颈，可增加多个在途事务：

1. Reader 顺序建立稳定事务身份、源位置与批次计划；批次计划先持久化。
2. 无状态事务/批次允许并行执行；关联 DAG 全部分支的完成状态。
3. Checkpoint coordinator 只推进**源顺序中连续完成的事务前缀**：`T1` 未完成时，即使 `T2`、`T3` 完成，也不能提交到 `T3`。
4. 未完成窗口受事务数、字节数和最早事务年龄限制；慢事务达到窗口上限就停止继续分发，避免完成状态和输出无限增长。

这里的顺序是已观察到的源事务顺序。GTID set 不是单调可比较的整数，不能用字符串排序或取“最大 GTID”实现前缀推进；必须维护源遍历顺序与对应恢复位置。

这种扩展保持完整事务 checkpoint，但不提供 S3 对象物理出现顺序。需要下游事务可见性时应使用 commit manifest 或湖表协议；不能把上传完成顺序当成提交顺序。

### 分片和顺序

首版只有 `source_order`，不提供用户自定义 key 分区。后续若支持按主键分片，要显式规定保证范围：相同 key 有序、不同 key 无全局顺序。主键更新应映射旧 key 和新 key 的依赖；一个事务跨多个 key/分区时仍要合并确认。无主键表需要稳定键策略，否则不能承诺按业务记录有序。扩容后直接改变 `hash(key) % worker_count` 会改变路由，应使用稳定虚拟分区，并在分区排空后迁移所有权。

## 3. Lease、Fencing 与迁移

Assignment key 为 `(tenant_id, pipeline_id, instance_id, source_id)`；源代次与输出世代另作为持久身份，不混同于短期 worker lease。

- 元数据库以条件更新获取 assignment lease，返回严格递增的 `fencing_token`。续约和提交均匹配 owner 与 token，并检查数据库时间下的有效期。
- 原型建议 lease TTL 30 秒、每 10 秒续约；失去续约能力时尽早停止拉取和取消执行。这些是故障注入前的起点，不是生产可用性承诺。
- 所有 checkpoint 和事务完成写入都在元数据库事务中校验 fencing token；旧 worker 即使网络恢复也不能推进任何提交状态。
- 进程内取消、唯一 `server_id` 和 Kubernetes leader election 都不能代替元数据写入时的 fencing。
- S3 不自动理解数据库 token。旧 worker 可能留下完整或未完成的额外对象；对象需要 attempt 身份，已发布 manifest 或已提交元数据只能引用有效所有者确认的对象。
- 正常缩容：标记 draining、停止获取新事务、完成当前事务并提交、关闭复制连接、释放 lease。超过排空期限则终止并由新 owner 从已提交 checkpoint 重放，允许重复，不能跳到读取位置。
- Worker 崩溃：新 worker 等 lease 过期并获得新 token，读取持久 checkpoint，验证源连续性后恢复。未完成本地暂存丢弃。

## 4. 背压、指标和扩缩容控制

MySQL binlog 本身仍是首版重读来源，内存 channel、本地 spool 与复制库缓存都不作为可靠队列。对应用队列、库内部队列与本地暂存分别设字节上限；应用停止拉取后，库可能仍短暂读取，最终应通过有界缓冲和 TCP 对上游施加背压。Debezium 的有界阻塞队列是可参考的成熟背压机制。[Debezium queue 配置](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-property-max-queue-size)。

| 指标 | 用途 |
| --- | --- |
| input bytes/s、events/s、transactions/s | 区分大行、大事务与高事件数负载 |
| reader CPU、解码耗时、网络吞吐、重连次数 | 定位读取侧瓶颈 |
| waiting assignments、active assignments | 决定 reader 池是否需要更多承载容量 |
| queue bytes / limit、spool bytes / limit | 实施背压和内存/磁盘保护 |
| oldest uncommitted transaction age、checkpoint age | 识别实际交付落后与阻塞 |
| read position 与 committed position | 区分读落后和下游落后；跨文件不得直接减位置 |
| transform/sink queue age、latency、errors、throttles | 决定扩执行侧还是修复目标瓶颈 |
| lease loss、fence reject、drain duration | 验证缩容与故障切换 |
| oldest available binlog / required checkpoint | 判断实际重放缺口与保留风险 |

源事件时间只用于近似延迟，需要区分空闲来源与真实积压，并考虑源时钟偏差。不能只靠“最后一条事件时间距现在多久”把空闲数据库扩到上限。

建议控制策略：

1. Reader 池根据待分配来源、加权 assignment 负载和 CPU 扩容；若仅一个热 source 的读取侧饱和，标记 `SOURCE_READER_SATURATED`，不循环创建无用副本。
2. Executor 池根据可调度 backlog 和服务时间估算并发，再受租户并发、最大副本和下游配额约束；S3 被节流时先退避，不持续加上传并发。
3. 原型以 30 秒平滑窗口评估一次，连续两个窗口高负载才扩容；缩容观察 5 分钟低负载并逐个排空。具体阈值由基准确定，用户 API 不暴露未经验证的固定容量保证。
4. Kubernetes 部署可将这些队列/assignment 指标提供给 `autoscaling/v2` HPA；HPA 自身支持自定义指标和稳定窗口，但它不负责 source 分配、事务排空与 fencing，这些仍由本服务实现。[Kubernetes HPA](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/)。
5. Binlog 保留需覆盖最长故障和追赶窗口；配置的过期时间不是实际保留保证，还需检测手工清理等情况。恢复点已被清理就进入 `NEEDS_RESEED`，不切到 latest。[MySQL binlog 保留](https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html#sysvar_binlog_expire_logs_seconds)。

## 5. Go 内部 API 草案

下列为职责说明，统一代码框架以项目 Go 类型为准。`CDCEvent` 使用主设计的无损类型；来源连接参数以凭证引用而非明文在外部 API 暴露。

```go
type SourcePosition struct {
    Mode         string // "gtid" or "file_position"; validate as a tagged union
    GTIDSet      string // consumed transaction set, not the current transaction GTID
    File         string
    NextPosition uint64 // resume after a committed transaction
}

type Transaction struct {
    ID       string
    Events   []CDCEvent // bounded; larger transaction may use an owned spool handle
    Resume   SourcePosition
    Bytes    int64
}

type OpenRequest struct {
    SourceID    string
    Generation  string
    Position    SourcePosition
    Credential  string // resolved secret reference, never passed to a transform
    ServerID    uint32 // allocated uniquely for concurrent MySQL connections
    MaxTxnBytes int64
}

type Listener interface {
    Open(ctx context.Context, req OpenRequest) (Stream, error)
}

type Stream interface {
    Next(ctx context.Context) (Transaction, error)
    Close() error
}

type Lease struct {
    Assignment string
    Owner      string
    Token      uint64
    ExpiresAt  time.Time
}

type LeaseStore interface {
    Acquire(ctx context.Context, assignment, owner string, ttl time.Duration) (Lease, error)
    Renew(ctx context.Context, lease Lease, ttl time.Duration) (Lease, error)
    Release(ctx context.Context, lease Lease) error
}
```

契约：

- `Next` 仅返回完整已提交事务；不订阅的行在标准化前过滤，但仍识别边界。ROLLBACK 不生成已提交业务事件；DDL 走安全暂停错误，不能伪装成普通事务。心跳/轮转由内部处理，可观测但不产生用户行记录。
- 调用 `Next` 不写 checkpoint；不把复制协议 ACK 当作最终 S3 交付确认。Checkpoint coordinator 是唯一进度提交者。
- 每个 stream 仅单 goroutine 调用 `Next`；`Close` 必须可与阻塞读取配合取消并幂等关闭。`Next` 的取消语义、连接中断后的重连策略在适配器中统一测试。
- `Open` 失败需区分 `SOURCE_UNAVAILABLE`、`INVALID_POSITION`、`BINLOG_GAP`、`SCHEMA_REQUIRED` 与 `INVALID_CONFIGURATION`；运行期另有 `SCHEMA_CHANGED`、`TRANSACTION_TOO_LARGE` 和 `LEASE_LOST`。暂时性断网可重试，缺口和结构变化需要显式处理。
- 事件身份不依赖 worker、token 和本次重试；重启后相同源事件生成相同 ID。`Lease.Token` 仅控制写入权限。

候选 Go 库的复制适配器以 `StartSync(mysql.Position)` / `StartSyncGTID(mysql.GTIDSet)` 打开，`BinlogStreamer.GetEvent(ctx)` 拉取事件，`BinlogSyncer.Close()` 关闭。库提供原始复制能力，事务组装、schema 历史、fencing 和 checkpoint 协调由本项目实现。已通过 `gh api` 查阅官方源码，正式接入仍需固定版本。[go-mysql replication API](https://pkg.go.dev/github.com/go-mysql-org/go-mysql/replication)。

## 6. 验收与下一步

- 多来源扩容后 assignment 能分散；同一 assignment 不出现两个有效提交者。
- 热单源读取饱和能正确诊断；增加空闲 reader 副本不被误报为吞吐提升。
- 故障注入覆盖续约中断、旧 owner 恢复、新 owner 接管、缩容超时；旧 token 提交始终拒绝。
- 下游停顿时全部队列/暂存有界；超大事务暂停；数据库仍能保留足够 binlog 时恢复无静默遗漏。
- 对事务序列 `T1/T2/T3`，即使后续扩展允许乱序完成，`T1` 未完成时 checkpoint 不越过它。
- 无输出、全部过滤及非订阅表事务能推进安全位置；DDL、轮转、重连和 binlog 缺口有明确状态。
- GTID 主库切换与 file-position 恢复分开测试，不声称 file-position 可直接迁移到另一台服务器。

建议先完成单 active reader 的恢复与 fencing 原型，再测单源瓶颈。只有数据证明转换或上传阻塞单管道时才引入有界并行和重排完成账本；需要更长故障缓冲时可采用下一节的 S3 durable buffer，而不是直接引入完整 Kafka 集群。

## 7. 下游积压时是否把 binlog 转存云存储

可以，并且适合下游经常出现长时间积压、但希望尽快把数据从 MySQL 保留窗口内安全接走的场景。建议提供可选的 **S3 durable buffer**，归档已经解码和标准化的完整 CDC 事务及 schema 快照或不可变引用，而不是只上传裸 binlog 文件。裸 binlog 仍可作为额外备份，但恢复需要匹配的 schema、解码器、来源身份和事务边界，不适合作为执行器唯一输入协议。

```text
direct:
  MySQL → Listener → bounded execution → output S3 → delivered checkpoint

buffered:
  MySQL → Listener → archive S3 + PostgreSQL catalog → archive watermark
                                           ↓
                             ordered catalog reader
                                           ↓
                          execution → output S3 → delivered checkpoint
```

### 7.1 两个水位不能混用

| 水位 | 表示什么 | 谁推进 | 重启用途 |
| --- | --- | --- | --- |
| archive ingestion watermark | 到此位置的完整事务已经可从归档连续恢复 | Archive publisher | Listener 可以从此继续读 MySQL |
| delivered checkpoint | 到此位置的转换与所有必需输出已经持久化并提交 | Checkpoint coordinator | Executor 从此位置之后继续读 catalog |

归档成功只代表“接走了源数据”，绝不代表“用户输出已经完成”。Buffered 模式下 MySQL 故障恢复依赖前者，端到端交付状态依赖后者。告警分别呈现读取滞后、归档滞后和交付滞后。Direct 模式没有可靠归档水位，Listener 仍从 delivered checkpoint 恢复。

### 7.2 归档对象和目录如何发布

归档 segment 可以包含多个完整事务，受压缩后/压缩前字节、事务数与最大封口时间限制；不能把未完整提交的事务宣布为已归档。事务过大时仍遵守显式大小限制，不能以对象存储容量大为理由无界缓存。

每个 segment 持有协议版本、来源及代次、稳定 segment ID、前驱 segment ID、起止恢复位置、事务索引、事件 ID、schema 摘要、压缩方式、长度与校验摘要。Schema 若单独存储，必须先可靠写入不可变 schema 对象，再发布引用；只记当前数据库表结构不足以重放历史事务。

发布顺序：

1. 以不可变对象键上传完整 segment，校验上传结果；可使用条件写入防止同名覆盖。归档凭证与用户输出凭证分开，用户函数无法读归档。
2. 在 PostgreSQL 事务中校验 ingestion lease/fencing、预期旧 archive watermark 和前驱目录项，插入 segment catalog，再原子推进 archive watermark。任一条件不符则不发布该 segment。
3. S3 对象与 PostgreSQL 不能跨系统原子提交；所谓“原子发布”指目录项和水位在 PostgreSQL 中一起提交，且只引用已确认存在的 S3 对象。上传后数据库提交前崩溃会留下孤儿对象，但不会产生已发布目录缺口。
4. 发布失败重试按稳定 segment 身份与摘要核对；已有相同身份却内容不同则报错。旧 owner 可留下孤儿，但无权更新有效目录。

S3 支持对象级强一致读写，条件写入也能防止意外覆盖，但这些不提供 S3 与 PostgreSQL 之间的分布式事务。[S3 consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)、[S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)。目录竞争控制可采用行锁或条件更新，始终在同一数据库事务里维护前驱关系和水位。[PostgreSQL transaction isolation](https://www.postgresql.org/docs/current/transaction-iso.html)。

Executor 根据 PostgreSQL catalog 的前驱关系/已发布序号顺序取 segment，校验来源代次、事务索引和校验摘要，再从 delivered checkpoint 恢复。**禁止用 S3 LIST 排序拼接数据流**：即使 LIST 强一致，也无法辨别孤儿对象、旧 owner 尝试与已发布连续前缀；对象名排序也不能替代 GTID 语义。

### 7.3 持续 buffered 优于临时溢写

推荐在管道启动时明确选择 `direct` 或 `buffered`。Buffered 始终先归档再执行，这样路径和恢复规则固定；不在队列超过阈值时临时把后半段切到 S3，又在恢复后直接追实时数据。这种自动溢写需要额外保证切换边界、在途数据和归档流之间没有洞、重复或超越，收益通常不足以抵消复杂度。

模式切换仅允许显式暂停、排空并记录完整事务边界后完成；第一版可以直接拒绝运行中切换。此轮只增加 durable buffer 的职责、接口边界与文档，不实现归档/重放后端；direct 仍为现有首版参考路径。

### 7.4 清理、额度和成本

- 清理依据所有有效订阅者 delivered checkpoint 的最小连续位置，同时满足最短回放保留期；不能按 archive watermark 删除尚未交付的数据。
- 初期每管道独立归档；未来共享来源归档时，订阅者注册、退出和最慢进度必须纳入 GC 协议。单独故障订阅者达到额度只能暂停、告警或由用户显式放弃历史，不能静默跳过。
- GC 在元数据库先标记候选并与新订阅/回放 pin 协调，再删除对象并记录结果；schema 在无任何活跃 segment/回放引用后才回收。孤儿对象需经过恢复安全期并核对目录，不能立即删新上传对象。
- 设置每租户 buffer 字节、segment 数量、最长交付滞后和费用预算告警。达到硬额度时暂停 ingestion，依靠 MySQL 剩余保留期报警；如果所需日志已过期，进入 `NEEDS_RESEED`。
- Lifecycle 不能无条件按固定天数删除未交付 buffer；应由目录 GC 控制业务可删除性。归档本身还需备份目录、保护 schema 与数据对象；只备份 S3 数据而丢失 catalog 不等于可恢复。
- 成本包括额外 S3 PUT/GET/存储、PostgreSQL catalog、压缩与加解密、网络以及重放计算。较大 segment 降低请求成本但增加封口延迟、重放读放大与单次重试代价；小事务不宜默认每事务一个对象。单价按部署区域核算。[S3 pricing](https://aws.amazon.com/s3/pricing/)。

Durable buffer 解决的是源读取与下游执行的时间解耦，既不能突破单源 reader 吞吐，也不能使长期低于输入速率的执行器自动追上。积压持续增长时仍要扩 executor、提高转换效率、控制租户输入或扩容目标存储。

补充验收包括上传后 catalog 提交前崩溃、catalog 提交结果未知、旧 ingestion token 发布、对象缺失/摘要错误、schema 丢失、最慢订阅者阻止 GC、达到额度停止归档，以及从归档重放后 delivered checkpoint 正确前进。
