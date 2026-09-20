# 统一采集执行器与版本化 CollectionOutcome 契约 - 产品需求文档

## Overview
- **Summary**: 为 pgmetrics 引入统一的采集执行器（collection executor）与版本化的 `CollectionOutcome` 契约：每个稳定采集域的执行结果（成功 / 主动省略 / 不支持 / 权限不足 / 超时 / 失败）、耗时、稳定错误码与脱敏摘要被结构化记录，并作为 JSON、human 文本、CSV 输出与 CLI 退出码共享的唯一状态源。查询函数返回类型化错误，策略层区分严格模式与尽力模式。
- **Purpose**: 现状下采集路径混合使用 `log.Fatalf`（立即终止，collect.go 约 130 处）、`log.Printf("warning...")` 后返回（约 30 处）与静默忽略（零值 / nil / -1）三种失败策略；`Model` 只保留最终值，快照离开进程后消费者无法区分真实空值、版本不支持、权限不足、超时与采集器缺陷；一个非关键查询可能终止整个长流程，另一个同类失败却只留在 stderr，自动化可能把不完整快照误判为健康。
- **Target Users**: 解析 pgmetrics JSON/CSV 的自动化监控系统与运维工程师；CLI 交互用户；以库方式调用 `collector.Collect*` 的 Go 消费者。

## Goals
- 每个稳定采集域产生一条结构化 outcome：域名、目标（数据库等）、六态状态、开始/结束时间、稳定错误码、可公开摘要、成功行数。
- 查询函数返回类型化错误；错误分类（权限、超时、版本/对象缺失、连接/认证、其他）由统一层完成。
- 策略层显式支持**严格模式**与**尽力模式**：默认尽力（单域失败不中断，输出带可信度标记的完整快照，退出码反映结果）；`--strict` 下必需域失败立即中止。
- JSON 元数据、human 摘要、CSV、CLI 退出码共享同一份状态源；错误文本在落盘/显示前经唯一脱敏入口移除连接串与凭证。
- 旧 Model 字段全部保留、语义不变；新字段附加（additive），旧 JSON 快照可继续被读取与渲染。

## Non-Goals
- 不重写 SQL 查询内容、不改变采集指标定义、不调整版本门控的技术含义（仅把门控结果显式化）。
- 不引入并发采集（仍按当前顺序执行）。
- 不改造 AWS/Azure SDK 的内部错误体系，仅在域边界分类。
- 不把每个逐行 size 查询（`pg_database_size`/`pg_table_size`/`pg_tablespace_size`，失败填 -1 的既有约定）升级为独立 outcome；保留 -1 约定。
- 不新增需要真实 PostgreSQL 实例的集成测试设施（测试以纯逻辑单测为主；真实/伪造数据库驱动集成留待后续）。
- 不改变 `--input` 旧文件的渲染结果（旧文件无 outcome 块时不显示状态段）。

## Background & Context
- 关键现状位置：
  - 入口 `Collect(o CollectConfig, dbnames []string) *pgmetrics.Model`（[collect.go L177](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L177-L271)），只返回 Model，内部 `log.Fatal*`；源码注释已预告签名未来会破坏式变更。
  - 集群域调度 `collectCluster`（L435-L565）、单库域调度 `collectDatabase`（L568-L619）；版本门控（`c.version >= pgvXX`）、`--omit` 列表、Aurora 跳过、`local` 判定散布于调度与函数内部。
  - 失败策略示例：`getSettings`/`getActivity*`/`pg_stat_database` 等 fatal；`getReplication*`/`getProgress*`/`getStatIOs`/`getStatLocks`/`getStatRecovery` warning+返回；`getLocal`（L747）、`getLogInfo` 的 `_ = Scan`（L3534）、size 填 -1（L1059/L1069）静默忽略。
  - pgbouncer.go / pgpool.go 几乎全部 fatal；citus.go 全部 warning+返回；RDS/Azure warning（L3655/L3692）。
  - 每条 SQL 各自 `context.WithTimeout`；超时与权限错误当前不区分（仅 `isLockTimeoutError` 识别 55P03 做 tables/indexes 降级重试）。
  - 输出：JSON `json.Encoder`（[main.go L341](file:///Users/nancy/swe-project/pgmetrics_fork1/cmd/pgmetrics/main.go#L341-L347)）；CSV 反射式 `model2csv`（[csv.go](file:///Users/nancy/swe-project/pgmetrics_fork1/cmd/pgmetrics/csv.go)）；human `postgresWriteHumanTo`（[report.go L59](file:///Users/nancy/swe-project/pgmetrics_fork1/cmd/pgmetrics/csv.go)）；运行时错误退出码 1（log.Fatal 默认），用法错误退出码 2。
  - 契约版本：`ModelSchemaVersion = "1.21"`（[model.go L45](file:///Users/nancy/swe-project/pgmetrics_fork1/model.go#L45)），Metadata 结构 L287-L295。
- 模块 `github.com/rapidloop/pgmetrics`，Go 1.26，pgx v5.10.0；仓库当前无任何 `_test.go`。

## Functional Requirements

### 契约模型
- **FR-1**: 在 `pgmetrics` 包新增版本化契约类型：`CollectionReport`（契约版本、策略、总体状态、开始/结束时间、outcome 列表）与 `DomainOutcome`（domain、target、status、started_at、ended_at、duration_millis、code、summary、rows）。契约自身带版本号常量（初版 `"1.0"`）。
- **FR-2**: 状态枚举固定为六态：`success`、`omitted`（主动省略：`--omit`/`--no-sizes` 等用户选项、非 local、服务端功能配置关闭）、`unsupported`（服务器版本不支持、平台不支持、Aurora 不支持、所需扩展/对象不存在）、`permission_denied`、`timeout`、`failed`（其余：连接/认证失败、IO、扫描错误、采集器缺陷等）。
- **FR-3**: `Model` 以新附加 JSON 字段（建议键 `"collection"`，`omitempty`）携带报告；`ModelSchemaVersion` 升至 `1.22` 并补版本注释。旧字段一个不改、不删、不改 JSON 键。
- **FR-4**: 总体状态聚合：`aborted`（严格模式下必需域失败而中止）、`failed`（≥1 个必需域为 timeout/permission_denied/failed）、`degraded`（无必需域失败但有可选域失败类状态）、`success`（其余；任意数量 omitted/unsupported 不降低可信度）。
- **FR-5**: success 类 outcome 必须携带 `rows`（标量域为 1，列表域为落表行数），使“真实空集合”（success + rows=0）与“未采到”（其余五态）可区分。

### 采集域目录
- **FR-6**: postgres 模式至少覆盖以下稳定域（cluster 级 target=""，单库级 target=库名）：
  - 连接/基础（每目标）：`connection`、`current_user`、`settings`、`system_info`、`local_probe`；`--all-dbs` 时的 `database_list`。
  - cluster 级：`control_system`、`control_checkpoint`、`last_xact`、`bg_writer`、`replication_outgoing`、`replication_incoming`、`recovery`、`wal_archiver`、`activity`、`backend_type_counts`、`databases`、`tablespaces`、`roles`、`replication_slots`、`wal_counts`、`notification`、`locks`、`wal`、`vacuum_progress`、`checkpointer`、`stat_io`、`stat_locks`、`stat_recovery`、`progress_cluster`、`progress_create_index`、`progress_analyze`、`progress_basebackup`、`progress_copy`、`progress_repack`、`system`、`logs`、`rds`、`azure`。
  - 单库级（target=库名）：`current_database`、`tables`（含 partition/parent 信息）、`indexes`（含 index defs 子项）、`sequences`、`functions`、`extensions`、`triggers`、`statements`、`bloat`、`publications`、`subscriptions`、`citus`。
  - 每条 outcome 以 `(domain, target)` 唯一；一次运行内同键只允许一条终态记录。
- **FR-7**: pgbouncer 模式域：`pb_pools`、`pb_servers`、`pb_clients`、`pb_stats`、`pb_databases`；pgpool 模式域：`pp_version`、`pp_nodes`、`pp_health_stats`、`pp_backend_stats`、`pp_cache`（外加两种模式共用的 `connection`，pgpool 另含 `current_user`）。
- **FR-8**: 版本门控（如 `<pgv13` 不采 wal receiver）、平台门控（非 Linux 的 system）、Aurora 跳过、`--omit`、非 local、`track_commit_timestamp` 关闭等“未执行”路径，必须显式落 `unsupported`/`omitted` outcome（含 code 与一句话 summary），而不是无声缺席；判定优先级：用户省略 > 版本/平台不支持 > 配置/环境省略。与当前模式无关的域不产生记录（如 postgres 模式不产生 pb_* 记录）。

### 类型化错误与执行器
- **FR-9**: 查询函数返回类型化错误（携带稳定错误码与原因），不再自行 `log.Fatal*`；collector 包内运行时路径不得残留 `log.Fatal`/`log.Fatalf`（CLI 用法/输出层不受此约束）。
- **FR-10**: 统一执行器包装每个域：记录开始/结束时间与 duration_millis；调用域函数；按分类规则把错误映射为状态与稳定错误码；成功时记录 rows。分类至少覆盖：
  - SQLSTATE `42501`（及 `28000`/`28P01` 认证类）→ permission_denied；
  - `57014`（statement_timeout）、`55P03`（lock_timeout）与 `context.DeadlineExceeded` → timeout；
  - `42P01`/`42P02`/`42883`（relation/function/param 不存在）→ unsupported（扩展缺失/版本错配类）；
  - `sql.ErrNoRows` 在语义为空的域（如 wal receiver、replication 无 standby）视为 success（rows=0），保持现行语义；
  - 连接/网络错误 → failed，code `conn_failed`/`auth_failed`；其余 → failed。
- **FR-11**: 策略：`CollectConfig` 增加严格开关（建议 `Strict bool`）。尽力模式（默认）：任何域失败都记录并继续后续域；严格模式：必需域失败立即中止采集（总体状态 aborted）。可选域失败在两种模式下都不中止。
- **FR-12**: 必需域最小核心集（用户已确认）：
  - postgres：`connection`（每个目标）、`database_list`（仅 --all-dbs）、`current_user`、`settings`、`system_info`、`activity`、`databases`，以及每个被采库的 `current_database`；
  - pgbouncer：`connection`、`pb_pools`、`pb_servers`；
  - pgpool：`connection`、`current_user`、`pp_version`、`pp_nodes`。
  - 其余皆为可选。
- **FR-13**: 保留 tables/indexes 的 lock_timeout 降级重试（去掉尺寸列重试一次）；重试成功则该域 success，summary 标注尺寸因 lock timeout 跳过；重试仍失败按 timeout/failed 落态。
- **FR-14**: 保留现有交互式 stderr 反馈，但文本来自同一脱敏后的 outcome 摘要（如 `warning: domain "replication_slots" target "db1": permission denied ...`）；严格中止时向 stderr 打印导致中止的必需域摘要。

### API
- **FR-15**: 保留 `Collect(o, dbnames) *pgmetrics.Model`（尽力模式包装器，报告同时嵌入 Model）；新增 `CollectWithReport(o, dbnames) (*pgmetrics.Model, *pgmetrics.CollectionReport)` 供 CLI 与库消费者取得状态；`getConn`、`getDBNames` 等连接层函数改为返回 error，由上层统一记录 `connection` 域。
- **FR-16**: 多库运行中单库连接失败：尽力模式记录该 target 的 `connection` 失败并继续其余库；严格模式立即中止。

### 输出与退出码
- **FR-17**: JSON 输出自动包含 `collection` 对象；human 输出在结尾前增加“Collection Status”段（总体状态 + 非 success 域的逐条列表，含 target/code/摘要；全 success 时仅一行汇总）；CSV 增加 `pgmetrics.collection.*` 键值行（总体字段 + 每条 outcome 展开）。三种输出与内存 report 同源、数值一致。
- **FR-18**: 脱敏：所有 outcome.summary 在写入 report 前经过唯一 sanitizer，移除 KV 连接串与 URI 中的密码/密钥（如 `password='x'`、`postgres://user:pass@host`），替换为固定掩码；human/CSV/JSON/strict stderr 全部只能见到脱敏后文本。
- **FR-19**: CLI 新增 `--strict` 长选项（进入严格模式），用法文本与 omit 校验一并更新。
- **FR-20**: 退出码（尽力模式默认）：全部 success，或仅存在 omitted/unsupported/可选域失败类状态 → 0；存在必需域 timeout/permission_denied/failed → 1，且快照（含 collection 块）照常写出；严格模式中止 → 1 且不写快照文件（保持致命错误语义）；用法错误保持 2；`--input` 回放旧文件（无 collection 块）→ 0。

## Non-Functional Requirements
- **NFR-1（兼容性）**: `go build ./...` 与 `go vet ./...` 通过；旧 JSON 字段与键名零变化；旧快照文件可被新版 `--input` 解析且 human 输出不新增状态段。
- **NFR-2（可测性）**: 分类器、sanitizer、聚合与退出码决策为不依赖数据库的纯函数，配单元测试；仓库从 0 测试增加到覆盖上述纯逻辑。
- **NFR-3（稳定性）**: 单次运行 outcome 条数有界（= 域目录规模 × 目标数），不随表/库对象行数膨胀；逐行 size 失败不产生 outcome。
- **NFR-4（可维护性）**: 域定义（名称、必需性、适用模式）集中在一处登记，新增域只需登记 + 经执行器调用；错误分类与脱敏逻辑单点实现。
- **NFR-5（安全）**: 凭证不以任何形式（summary、stderr、JSON、CSV、human）出现在落盘/输出文本中。

## Constraints
- **Technical**: Go 1.26；pgx v5（`*pgconn.PgError.Code` 为 SQLSTATE）；不能假设运行环境有 PostgreSQL；CSV 依赖反射读 json tag，新类型需兼容该机制或显式展开。
- **Business**: 旧字段必须保留、不得破坏现有 JSON 消费者；Model schema 以 minor 版本（1.22） additive 演进。
- **Dependencies**: 不新增第三方依赖；仅用标准库与现有 pgx。

## Assumptions
- 监控账号常见权限失败（42501）应映射 permission_denied；`42P01/42883` 在 pgmetrics 的受控 SQL 语境下视为“版本/扩展不支持”而非采集器缺陷。
- `sql.ErrNoRows` 对单行列域（wal receiver 等）属正常空态，维持现行“非错误”语义。
- 逐行尺寸查询失败保留 -1 约定，现有以 -1 判“无尺寸”的消费者不受影响。
- outcome 时间戳沿用 Model 既有的 Unix 秒精度，另加 duration_millis 弥补短查询精度。

## Acceptance Criteria

### AC-1: 契约类型与版本化
- **Type**: `rule`
- **Given**: 一次成功的 postgres 采集
- **When**: 序列化为 JSON
- **Then**: 顶层出现 `collection` 对象，含 `contract_version`（"1.0"）、`policy`、`status`、`started_at`、`ended_at`、`outcomes[]`；每条 outcome 含 domain/target/status/时间戳/duration_millis；`meta.version == "1.22"`；旧字段与旧 JSON 键保持不变
- **Pass Condition**: 代码检查 + 序列化快照单测断言上述字段存在且旧字段名未变
- **Evidence**: model.go 类型定义、单测输出

### AC-2: 域目录全覆盖且键唯一
- **Type**: `rule`
- **Given**: postgres/pgbouncer/pgpool 三种模式的调度代码
- **When**: 静态核对调度路径
- **Then**: FR-6/FR-7 列出的每个域都经执行器登记并产生 outcome；版本/omit/平台等未执行路径显式落 omitted/unsupported；同 `(domain,target)` 无重复终态
- **Pass Condition**: 每个域在代码中有且仅有一个执行器登记点；单测验证登记表完整性与键唯一
- **Evidence**: collector 源码、域登记表、单测

### AC-3: 错误分类正确性
- **Type**: `rule`
- **Given**: 构造的 `*pgconn.PgError`（42501/57014/55P03/42P01/42883/28P01 等）、`context.DeadlineExceeded`、连接错误、`sql.ErrNoRows`
- **When**: 经分类器映射
- **Then**: 状态与 code 符合 FR-10；ErrNoRows 在空态域映射 success
- **Pass Condition**: 表驱动单测全部通过
- **Evidence**: 分类器单测

### AC-4: 无进程内致命退出
- **Type**: `rule`
- **Given**: collector 包
- **When**: 检索运行时失败路径
- **Then**: 不存在 `log.Fatal`/`log.Fatalf`/`os.Exit`；查询函数返回 error；warning 经由执行器/报告通道
- **Pass Condition**: `grep -nE 'log\.(Fatal|Fatalf)|os\.Exit' collector/` 无命中（注释除外）
- **Evidence**: grep 输出 + `go vet`

### AC-5: 执行器记录完整时间与行语义
- **Type**: `rule`
- **Given**: 任一域执行（成功/失败各一）
- **When**: 检查 outcome
- **Then**: started_at ≤ ended_at、duration_millis ≥ 0 且与差值一致；成功时 rows 被填充，空集合域 rows=0 且 status=success
- **Pass Condition**: 执行器单测（含人工可控时钟或误差容忍）
- **Evidence**: 单测

### AC-6: 严格/尽力策略行为
- **Type**: `rule`
- **Given**: 必需域与可选域各注入一个失败
- **When**: 分别以默认模式与 `--strict` 运行采集编排（可用假域函数单测）
- **Then**: 尽力模式两域皆有记录、流程走完、总体 failed；严格模式在必需域失败处中止、总体 aborted、可选域失败不中止
- **Pass Condition**: 编排层单测覆盖四种组合
- **Evidence**: 单测

### AC-7: 退出码与输出落盘
- **Type**: `rule`
- **Given**: success、必需域失败（尽力）、必需域失败（严格）、用法错误、旧文件回放五种情形
- **When**: CLI 决策
- **Then**: 退出码依次为 0、1（快照仍写出）、1（不写快照、stderr 有脱敏摘要）、2、0
- **Pass Condition**: 退出码决策纯函数单测；main.go 代码检查确认接线
- **Evidence**: 单测 + 代码审查

### AC-8: 三输出同源
- **Type**: `rule`
- **Given**: 含若干非 success outcome 的同一 Model
- **When**: 分别渲染 JSON/human/CSV
- **Then**: human 的 Collection Status 段、CSV 的 `pgmetrics.collection.*` 行与 JSON collection 块在域数、状态、code 上完全一致
- **Pass Condition**: 渲染单测（构造 Model，断言三处一致）
- **Evidence**: 单测

### AC-9: 凭证脱敏
- **Type**: `rule`
- **Given**: 错误文本中嵌入 `password='s3cr3t'` KV 串、`postgresql://u:p%40ss@host/db` URI、含密码的 fmt 错误
- **When**: summary 落盘并经三种输出/strict stderr
- **Then**: 任何输出均不含 `s3cr3t`/`p@ss` 原文，仅见固定掩码；正常错误信息保留
- **Pass Condition**: sanitizer 表驱动单测 + 渲染单测对秘密字符串零命中
- **Evidence**: 单测

### AC-10: 向后兼容
- **Type**: `rule`
- **Given**: 1.21 版旧 JSON 快照（无 collection 字段）与新快照
- **When**: 新版 `--input` 分别回放
- **Then**: 旧文件解析成功、无 Collection Status 段、退出码 0；新文件正常显示状态段；现有 JSON 键与 human 主体段落不变化
- **Pass Condition**: 兼容单测 + 旧字段代码 diff 审查（仅附加，无修改）
- **Evidence**: 单测 + diff

### AC-11: 真实空值可区分
- **Type**: `rule`
- **Given**: 一个无 replication standby、无锁、无 bloat 的服务器（或等价构造）
- **When**: 审查对应 outcome
- **Then**: 相关域为 success+rows=0（或标量 success），与 timeout/permission_denied/failed 明确不同；消费者可仅凭 collection 块判定可信度
- **Pass Condition**: 执行器/渲染单测 + 代码核对
- **Evidence**: 单测

### AC-12: 设计内聚与改动克制度
- **Type**: `rubric`
- **Dimension**: 域登记集中、错误分类/脱敏/聚合单点实现；查询函数改造机械一致、无旁路记录；SQL 与指标语义零改动
- **Scale**: 1-5
- **Anchors**: 1 = 状态记录多点散落、仍有函数自行打印/退出；3 = 主路径统一但存在少数旁路；5 = 全部域经同一执行器、分类/脱敏/聚合各仅一处、无逻辑漂移
- **Pass Threshold**: >= 4
- **Evidence**: 独立代码审查

### AC-13: 构建与测试门禁
- **Type**: `rule`
- **Given**: 全部改动完成
- **When**: 运行 `go build ./...`、`go vet ./...`、`go test ./...`
- **Then**: 全部成功退出 0
- **Pass Condition**: 三条命令本地执行通过
- **Evidence**: 命令输出

## Open Questions
- 无（策略默认、退出码、必需域清单已由用户确认：默认尽力 + `--strict`；必需域失败尽力模式仍输出 exit 1；最小核心必需集）。
