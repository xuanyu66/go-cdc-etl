# MySQL → Lua → 本地磁盘端到端测试

从仓库根目录运行：

```sh
docker compose up -d --wait mysql
ETL_E2E=1 go test -tags integration ./e2e -count=1 -v
```

测试通过 Go `httptest` 调用真实控制面 HTTP handler，构建 `cmd/etl` 并启动真实 Lua worker 子进程，从专用 MySQL 8.4 容器消费 binlog，最终验证临时目录中的提交文件和 checkpoint。它不模拟 MySQL、转换器或本地 Sink。

测试会在 Compose 的 `shop` 库中创建唯一名称的测试表，结束后删除，不修改演示用的 `shop.orders`。订阅表 DDL 会暂停对应 reader；可安全分类的无关表 DDL 不再阻塞其他固定表管道。测试本身仍串行执行，避免同一测试表被并发修改。HTTP 服务和输出目录均由测试创建并清理；MySQL 容器继续保留。

覆盖范围：

- 从当前位点开始，只导出增量，不导出建实例前的记录。
- Lua 条件、字符串操作和精确小数计算；超过 JavaScript 安全整数范围的业务键无损保留。
- 事务内多行写入、更新、删除、状态过滤和旧输出撤回。
- 全部过滤时仍可提交事务 checkpoint。
- 暂停期间进度不变，恢复后补处理积压。
- 同一数据目录重启，追上停机期间的数据，已提交文件不重复、不改写。
- 重放相同幂等创建请求，返回相同资源响应。
- Lua 执行错误、输出 schema 错误和订阅表 DDL 导致暂停，checkpoint 不越过失败事务。
- 无关表 CREATE/ALTER/RENAME/DROP 不暂停订阅，并可持久化推进 checkpoint。
- 控制面校验、函数读取、草稿查询与修改、乐观并发检查、发布版本和实例管理。
- 两个必需输出的插入、更新和删除，以及幂等冲突、未知资源和非法定义的 HTTP 错误语义。

未设置 `ETL_E2E=1` 时测试跳过；未指定 `integration` build tag 时常规 `go test ./...` 不编译该测试。测试固定使用本项目 Compose 的 `127.0.0.1:13306`，不接受任意远端数据库，避免误操作其他环境。
