# 数据管道编排与 API 调研

核验日期：2026-10-19。本文提出接口设计，不代表服务已经实现；项目总览见 [design.md](../design.md)。本轮交付 OpenAPI、JSON Schema 和 Go 代码骨架；CLI 实现 Go 结构与图校验，尚未集成通用 JSON Schema 校验器，也未实现 HTTP handler、资源鉴权或连接测试。

## 1. 推荐方案与标准边界

采用 **OpenAPI 3.1.1 + JSON Schema 2020-12 + 小型声明式数据流定义**。HTTP 接口使用 JSON；本次代码框架 CLI 使用 JSON，JSON 文档本身也符合 YAML 1.2。一般 YAML 输入适配器留待后续添加，不宣称当前支持任意 YAML 文件。这样用户只需认识来源、转换、目标及连线，不需要先学习通用工作流引擎。

未发现能直接统一 MySQL CDC、持续数据流、事务边界、分支确认及 S3 checkpoint 的通用 DAG 标准。以下协议需要本项目明确，不能通过选一种 YAML 外观自动获得。

| 候选 | 实际标准化内容 | 本项目取舍 |
| --- | --- | --- |
| OpenAPI | HTTP 路径、请求响应、鉴权、接口文档和客户端生成 | 用来描述控制面，不负责 DAG 执行或 checkpoint。[官方规范](https://spec.openapis.org/oas/v3.1.1.html) |
| JSON Schema | JSON 文档结构、字段类型和局部约束 | 用于 IDE 提示和结构校验；环路、可达性、资源访问权限仍由服务校验。[官方规范](https://json-schema.org/draft/2020-12) |
| CNCF Serverless Workflow / Open Workflow Specification | 服务调用、事件驱动及控制流程的声明式工作流 | 官网目前以 Open Workflow Specification 展示，并标注 DSL 1.0.3。可借鉴定义与实例分离，但不直接采用完整 DSL；需要额外自定义 CDC 的持续运行和确认语义。[官方主页](https://open-workflow-specification.org/) |
| Argo Workflows | Kubernetes 上任务完成依赖的 DAG | `dependencies` 表示任务完成后再运行，不等于数据沿边持续传递；不按每条 CDC 事件创建 Pod 或 Workflow。[官方 DAG 文档](https://argo-workflows.readthedocs.io/en/latest/walk-through/dag/) |
| Redpanda Connect（Benthos 系谱） | YAML 描述 input、processors、output 及其产品执行行为 | 最接近流式数据集成体验，借鉴组件配置和输入输出模型；它是具体产品配置，并非跨引擎 CDC 标准。[配置](https://docs.redpanda.com/connect/configuration/about/)、[处理管道](https://docs.redpanda.com/connect/configuration/processing_pipelines/) |

配置语义选择 nodes/edges 而不是同时提供 processors 简写与完整图两套 DSL，避免用户面对两种等价写法。图形编辑器后续直接读写同一份定义。v1 不引入表达式语言；过滤和字段映射使用转换函数，避免同时学习 Lua/WASM SDK 和另一种表达式 DSL。

## 2. 定义、版本和运行实例

- **Pipeline Definition**：可编辑的配置，具有 `pipeline_id` 和单调递增 `revision`。
- **Pipeline Version**：发布时产生的不可变快照，具有 `version_id`、规范化配置摘要和固定转换制品摘要；配置默认值在发布时展开。
- **Pipeline Instance**：某个版本的长期运行实例，具有 `instance_id`、输出 `generation`、状态、checkpoint 和执行租约。进程重启或迁移不创建新输出世代。

编辑草稿不影响正在运行的实例。v1 实例版本固定；暂停和恢复不会修改版本、来源起点或输出世代。本轮不提供 upgrade/stop API。后续升级、失败事务修复和历史重算需要独立流程，必须明确未提交事务、来源与输出世代的处理，不能通过覆盖已发布版本实现。

本轮生命周期围绕 `STARTING → RUNNING → PAUSING → PAUSED → RUNNING` 定义。可恢复错误进入 `PAUSED` 并附原因；源 binlog 缺口进入 `NEEDS_RESEED`，不得通过普通 resume 自动跳过缺口。暂停仅在事务安全边界完成，崩溃则从最后提交 checkpoint 恢复。

## 3. 最小数据流配置

以下字段是本项目拟定格式，不声称兼容 Kubernetes、Argo 或 Serverless Workflow。

```yaml
api_version: v1
kind: Pipeline
metadata:
  name: orders-export
spec:
  nodes:
    - id: source
      type: source.mysql
      mysql:
        connection_ref: mysql-production
        tables: ["shop.orders"]
        start:
          mode: gtid
          gtid_set: "24bc7850-2c16-11ef-9f31-0242ac120002:1-41"
    - id: project
      type: transform.wasm
      wasm:
        artifact_ref: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
        timeout_ms: 5000
        memory_mib: 64
    - id: archive
      type: sink.s3
      s3:
        connection_ref: s3-archive
        bucket: company-archive
        prefix: orders
        format: jsonl
        gzip: true
  edges:
    - from: source
      to: project
    - from: project
      to: archive
```

`connection_ref` 是预先注册、按租户鉴权的资源标识；`artifact_ref` 为不可变 `sha256:<64位小写十六进制>` 标识；定义内不能填写密码、访问密钥或可变远端代码地址。完整服务发布时需要验证制品摘要存在、归属与连接类型及目标授权范围；这些资源校验尚未实现。连接凭证可轮换，但不能借凭证轮换更改来源身份或目标 bucket。

字段规范：

| 字段 | 约束及语义 |
| --- | --- |
| `api_version`、`kind` | 固定为 `v1`、`Pipeline`；不为未上线格式添加兼容分支 |
| `metadata.name` | 正则 `^[a-z][a-z0-9_-]{0,62}$`；租户内唯一性由后续控制面保证 |
| `spec.nodes[].id` | 同样的字符规则，图内唯一；语义标识，不按数组下标识别节点 |
| `type` | v1 实现 `source.mysql`、`transform.wasm`、`sink.s3`；Lua/Lambda 为候选，不接受尚未实现的节点类型 |
| `mysql.tables` | 至少一项精确 `database.table` 字符串，例如 `["shop.orders"]`；首版不支持通配符或动态匹配新表 |
| `s3.prefix` | 目标连接授权范围内的相对业务前缀；平台另加 tenant/pipeline/generation 身份，不能覆盖其他租户路径 |
| `spec.edges[]` | `from` 与 `to` 指向节点；边传递记录流，不是“上游任务结束后启动下游” |

v1 图约束：

1. 恰好一个 source，至少一个 sink，无自环、重复边和环路。
2. source 入度为零，sink 出度为零；所有非 source 节点入度恰为一。
3. 每个节点从 source 可达，并可到达某个 sink；所有分支都是交付必需分支。
4. 允许分叉，向各分支复制相同输入；禁止汇合、Join、窗口聚合及有状态节点。并发分支不会改变源事务的统一提交边界。
5. 转换允许零到多条输出；过滤为零条时仍传递完成确认，不能让后续 sink 永远等待。
6. 原型限制每条路径最多一个转换节点。现有 `design.md` 的转换协议以源事件 ID 为输入关联标识；多级拆分会使该标识不再唯一，因此多级转换须先引入平台生成的逐级记录 ID 和 lineage 协议，再放开，不能直接复用源事件 ID 作为每级唯一键。

第一版不要求用户配置 checkpoint 算法、内部缓冲、worker 数或确认模式。运行资源由平台配额管理；后续性能参数须与业务图版本分开，防止扩缩容意外改变处理逻辑。

## 4. HTTP 控制面规范建议

统一前缀 `/v1`，请求和成功响应为 `application/json`。Bearer token 决定租户身份，body 不接受可覆盖身份的 `tenant_id`。资源跨租户不可见。下面是目标契约，不表示端点已实现。

| 方法与路径 | 目的 | 成功响应 |
| --- | --- | --- |
| `POST /v1/pipelines/validate` | 校验并编译完整 Pipeline 文档，不保存，不连接用户 MySQL；资源鉴权另待实现 | `200`，编译后的 `Plan` |
| `POST /v1/pipelines` | 创建草稿，body 为完整 Pipeline 文档 | `201`，资源 body、`Location`、`ETag` |
| `GET /v1/pipelines?limit=20&cursor=...` | 游标分页，返回 `items`、`next_cursor` | `200` |
| `GET /v1/pipelines/{pipeline_id}` | 读取草稿及 revision | `200`，`ETag` |
| `PUT /v1/pipelines/{pipeline_id}` | 完整替换草稿，要求 `If-Match` | `200`，新 revision 与 `ETag` |
| `POST /v1/pipelines/{pipeline_id}/versions` | body 为 `{"draft_revision":"3"}`（十进制字符串），校验并发布不可变快照 | `201`，version 资源 |
| `GET /v1/pipelines/{pipeline_id}/versions/{version_id}` | 读取固定配置和解析后的依赖摘要 | `200` |
| `POST /v1/pipelines/{pipeline_id}/instances` | 使用固定版本和已验证起点创建实例 | `201`，初始 `STARTING` 状态及 `Location` |
| `GET /v1/instances/{instance_id}` | 返回实际/期望状态、错误摘要、当前版本 | `200` |
| `POST /v1/instances/{instance_id}/pause` | 排空到事务边界，body 为 `{}` | `202`，返回实例供轮询 |
| `POST /v1/instances/{instance_id}/resume` | 从当前 checkpoint 恢复，body 为 `{}` | `202`，返回实例供轮询 |
| `GET /v1/instances/{instance_id}/checkpoint` | 只读消费进度及来源身份，不暴露租约 token | `200` |

暂停和恢复是异步状态转换；`202` 仅表示请求已持久化接受，用户通过实例 GET 确认最终状态。重复请求相同目标状态返回当前实例；与正在进行的转换冲突则 `409`。暂停失败可保留故障原因，不能把 HTTP 接受当作已排空。

创建实例请求示意：

```json
{
  "version_id": "ver-001",
  "schema_baseline_ref": "schema-001"
}
```

来源定义的 `mysql.start.mode` 为 `gtid` 或 `file_position`；前者要求 `gtid_set`，后者要求 `file` 和十进制字符串 `position`，两组字段互斥。GTID set 表示已经消费的集合，从未消费事务继续；file/position 表示已经完成事务之后的下一事件边界。起点属于固定版本；实例创建解析来源身份并验证 schema 基线，历史起点缺少匹配 schema 历史则拒绝启动。普通 resume 不接受新起点；现有实例重启始终使用其 checkpoint，不重新套用定义中的初始起点。

写请求使用项目定义的 `Idempotency-Key`，按租户、方法、规范化路径及 key 存储请求摘要与结果；相同请求重试返回相同结果，不同 body 复用 key 返回 `409`。至少覆盖创建、发布和生命周期命令；建议保存期固定为 24 小时并写入正式 OpenAPI；客户端不得把过期 key 当永久去重机制。`If-Match` 用于草稿并发编辑，缺少条件返回 `428`，revision 不符返回 `412`。两者分别解决请求重试和并发更新，不互相替代。

### 错误响应

采用 [RFC 9457 Problem Details](https://www.rfc-editor.org/rfc/rfc9457.html)，媒体类型 `application/problem+json`；`errors`、`code` 和 `request_id` 是项目扩展字段。`pointer` 为请求 JSON 的 JSON Pointer，不返回密码、行数据或内部堆栈。

```json
{
  "type": "urn:etl:problem:invalid-pipeline",
  "title": "Invalid pipeline definition",
  "status": 422,
  "detail": "Pipeline validation failed.",
  "code": "INVALID_PIPELINE",
  "request_id": "req-001",
  "errors": [
    {
      "pointer": "/spec/edges/1/to",
      "code": "UNKNOWN_NODE",
      "detail": "Referenced node does not exist."
    }
  ]
}
```

状态码约定：非法 JSON 为 `400`；认证失败 `401`；权限不足 `403`；不可见资源 `404`；状态/幂等冲突 `409`；并发版本冲突 `412`；请求体超限 `413`；格式正确但结构或图不合法 `422`；限流 `429`；临时基础设施故障 `503`。重试建议通过 `Retry-After` 返回，不能让客户端无条件重试确定性配置错误。

## 5. 编译器与执行接口边界

大致代码模块：`pipeline` 存放定义和编译器；`control` 管理草稿、发布与实例；`runtime` 执行编译结果；`checkpoint` 协调所有必需分支确认。Go 首版可在一个服务内实现，接口边界不要求微服务边界。

```go
type Compiler interface {
    // Validate schema, graph, references and supported capabilities.
    Compile(ctx context.Context, definition Definition) (Plan, error)
}

type InstanceManager interface {
    Create(ctx context.Context, req CreateInstanceRequest) (Instance, error)
    SetDesiredState(ctx context.Context, id string, state DesiredState) (Instance, error)
}

type Runtime interface {
    // Run a long-lived plan under an acquired fenced execution lease.
    Run(ctx context.Context, plan Plan, lease ExecutionLease) error
}
```

这些是接口草图，实际类型在代码框架中统一。`Plan` 至少固定版本、拓扑顺序、节点配置、依赖摘要和必需 sink 集合。运行时处理数据，控制面不逐条调度；一次 binlog 事务对应多个批次和多个 sink 的完成记录，编排图只是其交付路径。

目标编译顺序：严格解析 JSON → JSON Schema 校验 → 节点/边引用和拓扑校验 → 配额与资源鉴权 → 固定依赖 → 生成规范化计划。当前 CLI 仅以 Go 类型、严格字段解析及结构/图检查覆盖其中一部分；JSON Schema 文件用于契约与工具消费，不代表 CLI 已通过通用 Schema 引擎校验。禁用任意远程 `$ref` 获取和重复 key；未来 YAML 适配器还需禁用自定义 tag，限制文档大小与嵌套，防止编辑器和服务器解释不同配置。连接测试单独执行，不能让 validate 悄悄建立复制连接。

## 6. 验证清单

- 后续 YAML 适配器须与对应 JSON 编译为相同计划；当前 JSON 的未知字段、重复 key、错误节点配置明确拒绝。
- 环路、多来源、汇合、悬挂节点、重复边、未知节点和多级转换按 v1 能力拒绝。
- 发布后修改草稿不影响旧版本；实例重新调度不改变版本和来源起点。
- 过滤为空仍完成全部必需分支；一个 sink 失败不能推进事务 checkpoint。
- 相同幂等 key 不创建重复实例；不同请求复用 key 报错；并发更新草稿返回 `412`。
- pause 的 `202` 与最终 `PAUSED` 区分；暂停恢复保持实例版本固定。
- 资源引用、错误日志和实例查询不能跨租户泄露；用户不得通过请求 body 选择其他租户身份。

本文各候选标准与官方页面按 2026-10-19 核验；版本号仅说明本次观察，不代表项目依赖会浮动追随最新版本。
