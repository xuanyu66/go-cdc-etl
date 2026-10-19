# 本地 CDC + Lua MVP 用户文档

本文描述本次**实际实现**，不是未来产品承诺。API 版本为 `v1`；源为真实 MySQL，目标为本地 JSONL/manifest，Lua 使用 GopherLua 的 Lua 5.1 语言模型。

## 1. 本次功能范围

| 能力 | 本次支持与限制 |
| --- | --- |
| 来源 | 一个预配置的 `mysql-local` 连接；每条管道显式订阅 1–16 张 InnoDB 固定表 |
| binlog | 必须开启 binlog，`ROW` + `FULL`；仅普通提交事务，file/position 恢复，不支持 GTID 切主 |
| 初始数据 | 只做增量；创建实例时捕获当前位置，不扫描已有数据 |
| 管道图 | 恰好一个 MySQL source → 一个 Lua transform → 1–16 个 local sink；不接受任意串联、Join、环路 |
| 用户逻辑 | Lua 条件、循环、函数、字符串处理、同事件计算；可过滤、一对多、分流到已声明输出 |
| 输出模型 | `upsert` / `delete`，显式业务键和固定 schema；一次返回多个变更按数组顺序应用 |
| 存储 | 本地未压缩 JSONL 变更文件及原子 manifest；HTTP 提供按键物化的当前结果 |
| 可靠性 | 全部逻辑输出及 checkpoint 同一次 manifest 提交；相同事务相同内容重复写不产生重复数据文件 |
| 生命周期 | 不可变函数、可编辑草稿、不可变版本、每 pipeline 一个不可变版本实例；pause/resume、同目录服务重启恢复 |
| DDL | 订阅表/schema 的 DDL 暂停；可安全解析的无关表/库 DDL 作为零输出操作推进；未知语句仍暂停 |
| 未实现 | Iceberg/Parquet/S3、云端积压归档、初始全量、历史重算、GTID failover、自动 schema 演进、窗口状态、外部 API、分布式/多租户与自动扩缩容 |

本地服务只允许数值 loopback 地址监听，没有账号认证、生产级租户隔离和硬内存沙箱。Lua 子进程不继承平台凭证，库能力受限且有执行超时，但这**不构成运行外部不可信代码的生产安全边界**。不要通过代理把服务暴露到公网或共享网络。数据目录应为当前用户独占，不能由其他用户或程序修改。

## 2. 启动真实 MySQL 和服务

依赖：Go 1.26+、Docker Desktop/Engine、Docker Compose、curl、Python 3；支持 macOS/Linux（本地文件锁使用 flock）。

```bash
make mysql-up
make build
ETL_MYSQL_PASSWORD=etl-demo-password ./bin/etl serve \
  -listen 127.0.0.1:8080 \
  -data .local/etl \
  -mysql-addr 127.0.0.1:13306 \
  -mysql-user etl
```

Compose 创建独立 MySQL 8.4 容器、`shop.orders` 表，以及只允许 SELECT/复制的 `etl` 账号。MySQL 绑定本机 13306，root 演示口令是 `etl-demo-root`，复制账号口令是 `etl-demo-password`。凭证是本地开发配置，不写入 Pipeline 或 Lua。

通过服务查询运行能力以及来源检查：

```bash
curl -sS http://127.0.0.1:8080/healthz
curl -sS http://127.0.0.1:8080/v1/capabilities
curl -sS 'http://127.0.0.1:8080/v1/connections/mysql-local/inspect?tables=shop.orders'
```

源列当前支持整数、DECIMAL、UTF-8/ASCII 字符串、binary/blob、JSON、date/time/datetime/timestamp/year；拒绝 FLOAT/DOUBLE、BIT、ENUM/SET、空间类型、生成列、不可见列及其他字符集，避免隐式损坏。目标 float64 类型可由 Lua 计算产生，不表示来源浮点列已支持。

inspect 返回当前 file/position、server UUID 和列信息，同时检查 ROW/FULL、InnoDB、列类型。它不是创建消费任务，也不会推进 checkpoint。连接只在服务参数中配置；MVP 没有连接管理或密钥管理 API。

## 3. 函数、草稿、版本与实例

- **Function**：Lua 源码的不可变资源；内容变化需要注册新函数，旧函数不会被覆盖。
- **Pipeline Draft**：可修改的图定义，使用 revision 和 ETag 处理并发编辑。
- **Pipeline Version**：发布草稿的固定快照，含函数引用、输出 schema 和图配置。
- **Instance**：持续消费的运行实例，固定一个版本、源身份、schema 基线和首次起点。重启不创建新实例、不重置进度。

MVP 每 pipeline 只创建一个实例，不支持停止删除或在线升级。修改草稿、发布新版本不会更新旧实例。代码/schema 修复和保留原消费位置的新版本切换尚未提供；遇到此类错误先保留数据目录和 binlog，不通过重建实例伪装无损恢复。新 pipeline 从当前起点开始会遗漏之前未处理的历史，必须明确接受该边界。

## 4. 端到端快速体验

创建函数、草稿、发布版本并启动实例：

```bash
./scripts/demo.sh
```

脚本读取 `examples/order-transform.lua` 与 `examples/pipeline.json`，替换函数引用；请求及响应保存在 `.local/demo/`，实例响应包含 `instance_id`。

```bash
INSTANCE_ID=$(python3 -c 'import json; print(json.load(open(".local/demo/instance.json"))["instance_id"])')
curl -sS "http://127.0.0.1:8080/v1/instances/$INSTANCE_ID"
```

等待实际状态 `RUNNING`（仅代表 worker 已启动；最终成功以 checkpoint 和输出为准），向专用容器写入真实事务：

```bash
docker compose exec -T mysql mysql -uroot -petl-demo-root -e \
  "INSERT INTO shop.orders VALUES (1001, 7, 12.34, 'paid');"
curl -sS "http://127.0.0.1:8080/v1/instances/$INSTANCE_ID/outputs/orders"
```

异步处理完成后，结果包含：

```json
{"rows":[{"order_id":"1001","customer_id":"7","total":"24.68"}]}
```

示例只输出 paid 订单，金额通过精确小数乘以 2。然后分别验证更新和删除（每条执行后轮询输出）：

```bash
docker compose exec -T mysql mysql -uroot -petl-demo-root -e \
  "UPDATE shop.orders SET amount=15.55 WHERE id=1001;"
docker compose exec -T mysql mysql -uroot -petl-demo-root -e \
  "DELETE FROM shop.orders WHERE id=1001;"
```

更新后的 total 为字符串 `"31.1"`，删除后该业务键消失。精确小数 SDK 返回规范化字符串，不保证末尾补零；`31.1` 与 decimal(18,2) 兼容。

脚本幂等键固定，重新执行相同脚本返回原资源，不创建第二个任务，也不自动恢复暂停的实例。不要修改请求内容后复用旧 key。演示 ID 已存在时可改用其他 SQL 主键，不要为了演示自动清空数据。

## 5. Pipeline 定义

完整样例为 `examples/pipeline.json`，结构如下：

```json
{
  "api_version":"v1",
  "kind":"Pipeline",
  "metadata":{"name":"orders-export"},
  "spec":{
    "nodes":[
      {"id":"mysql","type":"source.mysql","mysql":{
        "connection_ref":"mysql-local","tables":["shop.orders"],"start":{"mode":"current"}
      }},
      {"id":"normalize","type":"transform.lua","lua":{
        "function_ref":"fn_RETURNED_BY_API","timeout_ms":2000
      }},
      {"id":"archive","type":"sink.local","local":{
        "output":"orders","key_fields":["order_id"],
        "fields":[
          {"name":"order_id","type":"string","required":true},
          {"name":"customer_id","type":"string","required":true},
          {"name":"total","type":"decimal(18,2)","required":true}
        ]
      }}
    ],
    "edges":[{"from":"mysql","to":"normalize"},{"from":"normalize","to":"archive"}]
  }
}
```

- `start.mode=current` 只在创建实例时解析一次。暂停/重启均恢复已提交位置，不能切到 latest。
- 节点、字段和逻辑 output 名以小写字母开始，最多 63 字符，允许数字、`_` 和 `-`。固定表为简单 `database.table` 标识，不支持转义名称、通配符和动态新增。
- 每个 sink 的 `output` 唯一；Lua 只可向这些名称输出，不可选择任意文件路径或创建表。
- 所有 sink 均为必需分支。即使某输出本事务无变更，也会写显式空输出并参与统一提交。
- `timeout_ms` 为 1–5000，每个调用批次受该期限约束；Lua 模块另有 2 秒执行上限，实际较小者生效。不是每条事件可独占全部时间。
- `key_fields` 必须是 schema 中 required 的非浮点字段；业务键改变时由 Lua 产生旧键 delete 和新键 upsert。
- fields 只支持 `string`、`bool`、`int64`、`uint64`、`float64`、`decimal(p,s)`（p≤38，0≤s≤p）。未知字段、类型错误和不完整 required 字段会使事务失败。可选字段允许缺失或 null；upsert 是整行替换，未提供可选字段不会保留旧值。

CLI 可校验图：`./bin/etl validate -file .local/demo/pipeline.json`。CLI 只做结构和图校验，不检查函数存在或建立数据库连接；HTTP validate 还检查函数引用。API 只接受 JSON；未实现一般 YAML 解析器。JSON Schema 用于编辑器和客户端，服务使用同一语义的 Go 校验器，不执行任意远程 schema 引用。

## 6. Lua 编程协议

### 输入

`function transform(event)` 接收一条完整 CDC 行变更。每事件新建 VM，因此全局变量无法跨事件积累状态。

```json
{
  "event_id":"stable-source-event-id",
  "source_id":"mysql-server-uuid",
  "transaction_id":"stable-source-transaction-id",
  "row_index":0,
  "database":"shop",
  "table":"orders",
  "op":"update",
  "before":{"id":"1001","customer_id":"7","amount":"12.34","status":"paid"},
  "after":{"id":"1001","customer_id":"7","amount":"15.55","status":"paid"},
  "columns":[{"name":"id","type":"bigint unsigned","required":true}],
  "source_position":{"file":"mysql-bin.000001","position":1234}
}
```

columns 实际包含整张表的列。insert 的 before、delete 的 after 为 `etl.null`，行内 SQL NULL 也使用该哨兵，不是 Lua nil。`etl.null` 在 Lua 条件中为真，不能用 `event.after or event.before` 选择镜像，应按 `event.op` 显式分支；判断字段空值用 `field == etl.null`。`event_id` 与 binlog 位置绑定，同一源重放保持稳定；不承诺跨源重建或 binlog reset 仍可复用。

整数/DECIMAL 作为字符串传入，避免 Lua number 丢失大整数精度。二进制为 Base64；MySQL JSON 为原始 JSON 文本字符串，不自动变成 table；时间为源语义字符串，TIMESTAMP 按 UTC，DATETIME 不添加时区。当前没有内置 JSON 解码 SDK。详情可查 inspect 返回的源列类型。

### 输出

返回连续 Lua 数组，每项是：

```lua
{output="orders", op="upsert", key={order_id="1001"},
 value={order_id="1001", customer_id="7", total="24.68"}}
```

或删除：

```lua
{output="orders", op="delete", key={order_id="1001"}}
```

`return {}` 表示过滤；`return nil`、稀疏数组、混合数组、循环引用、非有限数和函数值等均报错。delete 必须省略 value，upsert 必须包含完整 value；key 必须精确包含 key_fields，并与 value 对应字段一致。输出 op 不必与源 op 相同。

一对多通过返回多个 mutation；多输出通过不同 output 名路由。稳定子键、旧子项删除、状态过滤退出后的撤回，均由用户根据 before/after 实现。可参考 `examples/order-transform.lua` 的 paid→cancelled 删除和主键变更处理。平台不会自动从返回空数组推断应删除旧数据。

平台给每条 mutation 增加 `mutation_id`、`source_event_id`、`output_index`。用户不要自行填写这些字段。用户函数需对同一输入返回相同顺序和内容；遍历对象的 `pairs` 顺序不作为稳定协议，生成数组前应显式排序；重放内容冲突会停止，不能覆盖已经写出的不同内容。

### SDK 与限制

可用基础函数白名单、string/table/math（无 random/randomseed），以及：

| API | 行为 |
| --- | --- |
| `etl.null` | JSON/SQL null 哨兵，保留对象中的空值 |
| `etl.decimal_add(a,b)` | 两个十进制字符串精确相加，返回字符串 |
| `etl.decimal_mul(a,b)` | 两个十进制字符串精确相乘，返回字符串 |

小数操作数最多 128 字节，只接受普通十进制格式。金额和业务 ID 不应使用 `tonumber`。没有 os/io/package/debug/print/dofile/loadfile/loadstring、网络或外部访问。函数注册仅编译、初始化并验证 transform 入口；真实输入、输出 schema 在运行时验证。

源码上限 64 KiB，worker 输入 1 MiB、输出 4 MiB。执行器按最多 100 事件及约 900 KB 输入拆批；单事件无法放入时暂停。一个事务的全部批次成功后才提交。子进程没有继承 MySQL 密码，超时被终止；尚无可靠硬内存限制，不接收恶意代码。

## 7. HTTP API

成功响应为 `application/json`，错误为 RFC 9457 风格 `application/problem+json`。写请求必须 `Content-Type: application/json`；请求上限 1 MiB，拒绝未知字段、重复 JSON key 和多文档。浏览器带 Origin 的写请求拒绝，MVP 使用 curl/SDK。

除 validate 外，所有写请求要求 `Idempotency-Key`（1–128 字符）。按方法、路径、key 保存原请求及响应：相同 body 和 If-Match 重试返回原结果，不重新执行；不同 body 或 precondition 复用 key 返回 409。记录持久化在本地 catalog，MVP 不自动过期，因此需要关注目录增长。字节内容不同的等价 JSON 也视为不同请求。使用新 key 才会发起新的操作。

| 方法与路径 | 请求/行为 |
| --- | --- |
| `GET /healthz` | 进程存活；不代表 MySQL 可用或管道健康 |
| `GET /v1/capabilities` | 本次实现能力 |
| `GET /v1/connections/mysql-local/inspect?tables=shop.orders` | 来源配置和类型检查、当前位置，不创建任务 |
| `POST /v1/functions` | `{ "name":"orders", "source":"function transform(event) ... end" }` → 201 Function |
| `GET /v1/functions/{function_id}` | 不可变源码与标识 |
| `POST /v1/pipelines/validate` | 完整 Pipeline 文档 → 200 编译计划，不保存 |
| `POST /v1/pipelines` | 完整 Pipeline 文档 → 201 草稿及 ETag |
| `GET /v1/pipelines` | `{ "items": [...] }`；MVP 无分页 |
| `GET /v1/pipelines/{pipeline_id}` | 草稿及 ETag |
| `PUT /v1/pipelines/{pipeline_id}` | 完整替换草稿，必须 If-Match，返回新 revision |
| `POST /v1/pipelines/{pipeline_id}/versions` | `{ "draft_revision":"1" }` → 201 不可变 Version |
| `GET /v1/pipelines/{pipeline_id}/versions/{version_id}` | 已发布版本及编译计划 |
| `POST /v1/pipelines/{pipeline_id}/instances` | `{ "version_id":"ver_..." }` → 201 STARTING，捕获当前起点及 schema |
| `GET /v1/instances/{instance_id}` | actual/desired 状态、起点、schema、错误摘要 |
| `POST /v1/instances/{instance_id}/pause` | `{}` → 202，异步取消在途工作并保留安全 checkpoint |
| `POST /v1/instances/{instance_id}/resume` | `{}` → 202，从已有 checkpoint 启动 |
| `GET /v1/instances/{instance_id}/checkpoint` | 已提交 sequence、事务 ID、version、file/position；初始 sequence=0 |
| `GET /v1/instances/{instance_id}/outputs/{output}` | `{ "rows": [...] }`，只读取已提交结果，无分页 |

首次实例请求无需用户上传 schema_baseline_ref：服务从当前来源采集并固定基线。后续恢复拒绝源 server UUID 变化和 schema 变化；不自动重新取起点或补全已有表数据。

创建函数示例：

```bash
python3 - <<'PY' > .local/function.json
import json
from pathlib import Path
print(json.dumps({"name":"order-transform","source":Path("examples/order-transform.lua").read_text()}))
PY
curl --fail-with-body -sS http://127.0.0.1:8080/v1/functions \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: my-function-1' \
  --data-binary @.local/function.json
```

错误示例：

```json
{
  "type":"urn:etl:problem:revision_conflict",
  "title":"Precondition Failed",
  "status":412,
  "code":"REVISION_CONFLICT",
  "detail":"draft changed"
}
```

常见状态码：400 请求/幂等键错误，404 资源不存在，409 名称/状态/幂等冲突，412 草稿版本不符，413 超大请求，415 Content-Type 错误，422 结构/能力/函数/schema 错误，428 缺少 If-Match，503 服务关闭。来源不可用等问题当前可能以 422 返回，MVP 尚未精细分类为可重试网络错误；客户端应读取 detail，不无条件重试。

## 8. 暂停、恢复、错误与 DDL

```bash
curl -sS -X POST "http://127.0.0.1:8080/v1/instances/$INSTANCE_ID/pause" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: pause-1' -d '{}'
# 轮询 GET instance，等实际 state=PAUSED 再做其他操作
curl -sS -X POST "http://127.0.0.1:8080/v1/instances/$INSTANCE_ID/resume" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: resume-1' -d '{}'
```

状态为 STARTING、RUNNING、PAUSING、PAUSED。202 仅表示命令已记录；暂停可能取消未完成事务，恢复后会从最后提交点重放。PAUSING 时 resume 返回 409。函数、输出校验、写盘或 listener 错误会让实例进入 PAUSED，保留 last_error；不静默跳过坏事件，不自动无限重试。resume 清除上次错误并重试固定代码，若根因未消除仍会暂停。

正常退出服务后，desired_state=RUNNING 的实例下次以同一数据目录自动恢复；用户主动 pause 的实例保持暂停。源网络错误会暂停而非自动重连。binlog 被清理、源 UUID 改变、schema 不符都拒绝继续，不自动切到 current。

**受控 DDL**：启动/恢复比较完整 pinned schema；运行期用 SQL AST 检查 DDL 引用的表/库。已确认不涉及订阅表的建表、改表、删表、重命名及索引操作可继续，并沿正常提交链路记录零输出 checkpoint。涉及订阅表/schema、语句无法解析或无法安全分类时暂停，不跳过。订阅表 DDL 前应暂停对应实例；MVP 无 schema 版本迁移和消费点接续操作，结构改变后普通 resume 不会自动恢复。初始化期间仍须保持无 DDL 窗口。

## 9. 文件布局与 checkpoint 语义

```text
.local/etl/
  .server.lock
  catalog.json                     # 函数、草稿、版本、实例、幂等响应
  outputs/
    .localstore.lock
    inst_.../
      00000000000000000001-<digest>-orders.jsonl
      manifest.json                # 唯一提交真相及当前 checkpoint
```

一次完整源事务的处理顺序：读取 COMMIT → Lua 全部批次 → 校验所有变更 → 写各 output 文件（包括空输出）并 fsync → 原子替换 manifest 并同步父目录。checkpoint 和文件引用同在该 manifest，避免二者分开提交形成进度漏洞。

重放同一 transaction、sequence、版本和完全相同内容返回原提交结果；内容改变报 CONTENT_CONFLICT；序号不连续报 SEQUENCE_GAP。读取只沿 manifest 引用且校验 checksum，不扫描目录推断提交。上传/写盘后、manifest 前中断可能留下无引用文件；它们不会进入 rows，不提供自动 GC。禁止用户修改文件或从任意 JSONL 文件出现就推断事务已经提交。

本地保证限于单进程写入、独占目录和支持相应 fsync/atomic rename 的本机文件系统。复制流可能重读，但本地提交幂等；不能据此宣称跨系统 exactly-once。数据目录丢失将同时丢失结果和恢复状态。

Rows 查询从完整已提交历史应用 upsert/delete，未建索引；全量 manifest 每次重写，单事务/单文件/累计 manifest 上限为 64 MiB，内存和读取成本随历史增长，适用于验证而非长期生产。listener 每事务上限为 10,000 条订阅事件或约 16 MiB 标准事件；超过即暂停。没有磁盘容量自动扩缩容或留存管理。

## 10. 验证与停止

```bash
make test                     # Go race 单测，默认不访问数据库
make e2e                      # 真实 Compose MySQL + HTTP + Lua 子进程 + 临时磁盘
```

E2E 的唯一测试表在 `shop` 下创建、结束时删除；运行前检查本机 13306 是此项目 Compose MySQL。覆盖增量无全量、精确大整数与小数、复杂转换、更新删除、过滤、暂停追赶、重启恢复、幂等请求、Lua/schema 错误不推进进度、订阅表 DDL 暂停、无关表 DDL 不阻塞。具体见 `e2e/README.md`。

停止服务使用 Ctrl-C；停止 MySQL 使用 `make mysql-down`，保留 Docker volume。不要直接删除 `.local/etl` 或 MySQL volume 后沿用旧任务。需要完全重置时先停止服务并备份数据，由用户显式删除本地目录和 Compose volume；本项目启动脚本不会自动做破坏性重置。

## 11. 后续 Iceberg 接入边界

目标仍可采用 Iceberg v2 + Parquet + S3/Catalog，但本次不模拟该能力。后续替换 Sink 时，成功条件必须是 Iceberg 快照提交成功，而不是文件出现；还要实现提交身份核对、catalog 并发、类型映射、upsert/delete 与维护策略。S3 积压缓冲则是另外一个持久化组件，有归档水位与交付 checkpoint，当前仅有接口草案。

历史设计和研究文档中未实现的 PostgreSQL、WASM/Lambda、归档、升级、自动重试、manifest API 等内容，不应被解释为此次 API 已支持功能。
