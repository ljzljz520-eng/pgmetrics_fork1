# 独立代码评审报告：统一采集执行器与版本化 CollectionOutcome 契约

## 一、评审元信息

| 项 | 内容 |
|---|---|
| 评审者 | 独立代理（只读评审，未修改任何源码；本报告为唯一写入文件） |
| 评审日期 | 2026-09-19 |
| 仓库 / 模块 | `/Users/nancy/swe-project/pgmetrics_fork1` · `github.com/rapidloop/pgmetrics` |
| 基线 commit | `19fcebfc27ebad45ed198652562f0f9356addb59`（HEAD） |
| 工作树状态 | dirty：13 个已跟踪文件 modified（`model.go`、`collector/{collect,citus,log,pgbouncer,pgpool,system_linux,system_darwin,system_freebsd,system_windows}.go`、`cmd/pgmetrics/{main,report,csv}.go`）；untracked：`.trae/`、`outcome.go`、`outcome_test.go`、`collector/{executor,executor_test,outcome,outcome_test}.go`、`cmd/pgmetrics/{compat_test,csv_test,report_test}.go`。`aws.go`/`azure.go` 未修改。 |
| 评审方式 | 全量源码阅读 + 与 HEAD 的 git diff 机械对比 + 门禁命令实测；未使用网络 |

### 实测命令与结果（评审者本机复跑）

| 命令 | 结果 |
|---|---|
| `go build ./...` | 退出 0 |
| `go test -count=1 ./...` | 退出 0；`pgmetrics` ok 0.734s、`cmd/pgmetrics` ok 1.090s、`collector` ok 1.541s |
| `go vet ./...` | 退出 0 |
| `gofmt -l .` | 零输出 |
| `GOOS=linux go build ./...` / `GOOS=windows go build ./...` | 均退出 0 |
| `GOOS=darwin go build ./...` / `GOOS=freebsd go build ./...` | 均退出 0（附加验证四个 system 平台桩） |
| `go test -race -count=1 ./...` | 三包全 ok，无数据竞争（顺序执行，符合 Non-Goals） |

另做两项独立机械比对（HEAD vs 工作树，覆盖 `collector/{collect,citus,pgbouncer,pgpool,log}.go`）：

1. **SQL 字符串字面量集合**（反引号 raw string + `SELECT/SHOW/...` 双引号串，去注释后排序去重）：五个文件**逐一完全相同，零差异**。
2. **全部 `.Scan(...)` 实参集合**（跨行提取、空白规范化后）：四个文件**集合完全相等，removed=0 added=0**；`QueryContext` 计数（45/7/5/4）与 `defer rows.Close()` 计数（43/7/5/4）新旧一致，唯一无 defer 的函数新旧同为 `getWALCountsActual`（显式 Close 路径完整）。

---

## 二、总体结论

# ❌ FAIL

存在 **1 个 critical**：FR-13 明确要求保留的 tables/indexes `lock_timeout` 降级重试，因查询错误被 `fmt.Errorf("...: %w", err)` 包装后，旧的直接类型断言谓词永远失败，重试路径已事实失效——属 spec 明文要求保留的行为回归。按"任一 blocker/critical 即 FAIL"的裁决规则，总体不通过。

门禁（build/vet/test/gofmt/交叉编译/race）全绿，契约主体设计与实现质量高，但该回归与缺失的针对性测试需修复后复审。

### Findings 计数

| 级别 | 数量 |
|---|---|
| blocker | 0 |
| critical | 1 |
| major | 0 |
| minor | 4 |
| nit | 3 |

### Blocker / Critical / Major 标题清单

- **[critical] F1**：lock_timeout 降级重试被错误包装击穿，FR-13 行为回归（[collect.go:1988-1994](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L1988-L1994)）
- （无 blocker、无 major）

---

## 三、Findings

### F1 ｜ critical ｜ lock_timeout 降级重试被错误包装击穿，FR-13 保留行为回归

**位置**：
- 谓词：[collector/collect.go#L1988-L1994](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L1988-L1994)
- 新包装点：[collector/collect.go#L1944](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L1942-L1945)、[collector/collect.go#L1983](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L1982-L1984)、[collector/collect.go#L2044](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L2042-L2045)
- 重试入口：[collector/collect.go#L1862-L1879](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L1862-L1879)、[collector/collect.go#L1996-L2013](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L1996-L2013)

**证据**：

`isLockTimeoutError` 与 HEAD 完全相同，仍是**直接类型断言**：

```go
func isLockTimeoutError(err error) bool {
	if pgerr, ok := err.(*pgconn.PgError); ok {
		return pgerr.Code == "55P03"
	}
	return false
}
```

`git show HEAD:collector/collect.go` 证实：旧 `getTablesNoRetry`/`getIndexesNoRetry` 在 Query 失败时**裸返回** `return err`，因此断言能命中 `*pgconn.PgError`，55P03 触发"去尺寸列重试"。改造后两处 Query 失败（以及 tables 的 `rows.Err()`）都包了一层：

```go
return 0, fmt.Errorf("pg_stat(io)_user_tables query failed: %w", err)   // L1944
return 0, fmt.Errorf("pg_stat_user_indexes query failed: %w", err)      // L2044
```

被 `%w` 包装后，`err.(*pgconn.PgError)` 断言恒为 false，`getTables/getIndexes` 中的 `isLockTimeoutError(err) && fillSize` 分支不可达。

**后果**：并发 DDL 导致 `pg_table_size`/`pg_total_relation_size` 取锁等待 55P03 时（该重试正是为此保留），旧行为是重试成功 → 域 success + note（尺寸跳过）；新行为是 tables/indexes 域直接记 `timeout`，快照从"带说明的成功"退化为"失败/降级"（该两域可选，尽力模式 exit code 仍为 0，但 `degraded` 与缺失的表/索引数据构成实质性行为回归；严格模式同样不再恢复数据）。同一改造中分类器 [classify](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/outcome.go#L111) 已正确使用 `errors.As` 且有包装错误测试，唯独此谓词漏迁移。无任何测试覆盖"包装后的 55P03 → 重试"路径。

**违反**：FR-13（重试成功须 success+尺寸跳过说明；再失败才落 timeout/failed）。

**修复建议**：

```go
func isLockTimeoutError(err error) bool {
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) {
		return pgerr.Code == "55P03"
	}
	return false
}
```

并补一条单测：`fmt.Errorf("...: %w", &pgconn.PgError{Code:"55P03"})` 经 `getTables/getIndexes` 触发去尺寸列重试（可用注入式域函数或提取谓词测试）。

---

### F2 ｜ minor ｜ getStatRecovery 把权限相关的 ErrNoRows 空态误判为 failed

**位置**：[collector/collect.go#L3941-L3965](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L3941-L3965)

**证据**：函数注释自述"this can fail with ErrNoRows on a >=pg19 standby when the user does not have pg_read_all_stats privilege"。旧代码此处为 `log.Printf("warning...")` 后返回（不影响快照）；新代码统一 `fmt.Errorf("pg_stat_recovery query failed: %w", err)` 上卷，`sql.ErrNoRows` 不是 `*pgconn.PgError`，经分类器落 `failed/query_error`。

**原因/影响**：`stat_recovery` 是可选域，尽力模式 exit code 仍为 0，但健康的 pg19+ standby 用无 `pg_read_all_stats` 的监控账号采集时会被标成 `degraded`，与契约"避免把正常情况误判为不可信"的目的相悖；FR-10 也要求空态域的 ErrNoRows 按 success(rows=0) 保持现行语义（wal receiver 两变体已在 [L1232](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L1232-L1234)/[L1267](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L1267-L1269) 正确处理）。

**修复建议**：与 wal receiver 一致特判 `errors.Is(err, sql.ErrNoRows)` → `return 0, nil`（或落更贴切的 permission/unsupported 态）。

---

### F3 ｜ minor ｜ 尽力模式 database_list 失败后仍以无名 target 继续采集

**位置**：[collector/collect.go#L263-L292](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L263-L292)

**证据**：`--all-dbs` 下 `database_list` 是必需域，但 `runDomain` 在尽力模式对失败返回 nil，因此 `if err != nil { return c.finish() }`（L272-274）永不触发；当 `dbnames` 为空（用户未给位置参数）时，L278-279 会以 target `""` 连默认库执行整套采集。严格模式正确（errAborted → finish）。

**影响**：旧版 `getDBNames` 失败为 fatal；新版尽力模式虽不中断（方向正确），但在枚举报错后悄悄改采默认库，快照里同时存在 `database_list=failed` 与一个 target 为空的库级结果集，语义含混，消费者易误读。

**修复建议**：尽力模式下 `database_list` 失败后显式结束（`return c.finish()`）或只走集群级域，避免无名 target 连接；至少在报告 summary 中说明枚举失败后回退到默认库。

---

### F4 ｜ minor ｜ 严格中止判定不看分类结果，required+unsupported 也会中止

**位置**：[collector/executor.go#L306-L334](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L306-L334)（L327 `if required && c.strict`）

**证据**：`recordFailure` 先经 `classify` 得到 status/code，但中止条件只看 `required && c.strict`，不区分状态是否属于失败类。若某必需域查询返回 42P01/42P02/42883（分类为 `unsupported`，如 PG 分支/极端版本上的 `system_info` 函数缺失），严格模式仍会 aborted；而尽力模式下 [Aggregate](file:///Users/nancy/swe-project/pgmetrics_fork1/outcome.go#L160-L178) 只对 failure-class 计 failed，两策略对同一结果的解释不一致。

**影响**：当前必需域均查核心目录/函数，实际触发概率低，且"宁严勿松"可辩护；但与 FR-4/FR-11 的状态语义存在字面分歧。

**修复建议**：中止条件改为 `required && c.strict && pgmetrics.IsFailureClass(status)`，或在注释中明确"严格模式对必需域任何错误（含 unsupported）一律保守中止"并同步 FR 描述。

---

### F5 ｜ minor ｜ getCitusVersion 任意失败都被标成 extension_absent

**位置**：[collector/citus.go#L53-L57](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/citus.go#L53-L57)

**证据**：`getCitusVersion` 返回任何错误（含 timeout、连接错误）都 `skip(..., codeExtensionAbsent, sanitize(err.Error()))` → `unsupported/extension_absent`。旧版为 warning+返回，故非回归，但分类偏宽松：超时会被误报为"扩展缺失"。

**修复建议**：仅对 undefined_object/function 类（42P01/42883）落 extension_absent，其余错误 `return 0, err` 走正常分类。

---

### F6 ｜ nit ｜ local_probe 失败路径手工 Record，duration 恒为 0

**位置**：[collector/collect.go#L557-L570](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L557-L570)

探针失败时域函数内手工 `Record` 一条 `StartedAt==EndedAt`（duration_millis=0）的 success，使 runDomain 外层计时被跳过（runDomain 检测到终态已存在直接返回）。功能正确（summary 已脱敏、rows=1），仅时间语义轻微失真。建议复用 note 机制让 runDomain 统一计时落 success。

### F7 ｜ nit ｜ notes 仅在成功路径消费，域失败后残留

**位置**：[collector/executor.go#L252-L257](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L252-L257)

note 只在 success 分支 `delete`；若"先 note 后失败"则键残留。当前所有 note 调用点（tables/indexes 重试成功、logs 部分成功、RDS）均与成功配对，且同 `(domain,target)` 终态去重、不会二次调度，无可观察污染。建议失败路径一并删除或注释固化该不变量。

### F8 ｜ nit ｜ getStatIOs 内 version<16 分支不可达

**位置**：[collector/collect.go#L3870-L3874](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L3870-L3874)（`else { return 0, nil }`）

该函数仅经 [runVersioned(domainStatIO, "16", pgv16, ...)](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L788) 调度，低版本在门控处已落 `unsupported/version_unsupported`，函数内 else 分支永不可达（且若误被直调会把"视图不存在"记成 success+rows=0）。建议删除分支或加注释说明防御性质。

### 附：观察项（不构成 finding）

- `runFatal` 为未知模式记录的 `init` 域不在 [domainCatalog](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L120-L192) 中（不可达内部路径）；catalog 完整性单测覆盖不到该名称，若追求契约封闭可纳入登记。
- [getLogInfo](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L3993) 的 `_ = QueryRowContext(...).Scan(...)` 是全仓唯一残留的显式忽略查询错误处；它只是 logs 域的前置探针（非独立域），与旧行为一致，符合设计。
- pgpool `semversion==""`：已读 `golang.org/x/mod@v0.4.0/semver.go`，`parse("")` 得 `ok=false` → `Compare` 返回 -1 → `pp_version` 最佳努力失败后 health/backend stats 走版本 skip，行为安全。

---

## 四、验收标准裁决表（AC-1 ~ AC-13）

| AC | 裁决 | 证据（一句） |
|---|---|---|
| AC-1 契约类型与版本化 | **PASS** | [outcome.go](file:///Users/nancy/swe-project/pgmetrics_fork1/outcome.go) 六态/两策略/四总体状态、`CollectionContractVersion="1.0"`、json 键齐备且 `Required` 标 `json:"-"`；`git diff model.go` 仅版本注释、`1.21→1.22` 与末尾附加 `Collection *CollectionReport json:"collection,omitempty"`，旧键零改动；`TestCollectionJSONRoundTrip` 断言。 |
| AC-2 域目录全覆盖且键唯一 | **PASS** | 58 个域集中登记于 domainCatalog；grep 95 处 run*/skip 调度点与 catalog 一一对应、每域唯一调度点（模式/版本门控互斥）；`Record` 终态去重（L114-122）有测试；必需域集合与 FR-12 一致；唯一例外"init"为不可达内部路径（见观察项）。 |
| AC-3 错误分类正确性 | **PASS** | 19 个表驱动子用例真实断言：42501/28000/28P01、57014/55P03、42P01/42P02/42883、57P03、DeadlineExceeded、包装 PgError、DomainError、net.OpError；ErrNoRows 在 wal receiver 叶子按 (0,nil) 空态处理（L1232/L1267）。遗留：08000 族与 `net.Error.Timeout()==true` 分支有实现无测试（见测试评价）。 |
| AC-4 无进程内致命退出 | **PASS** | collector 包运行时 `log.Fatal*/os.Exit` 零命中；运行时输出仅 [executor.go:325/330](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L321-L331) 两条规范 warning/中止通道。 |
| AC-5 执行器时间与行语义 | **PASS** | runDomain（L227-264）start/end/duration、rows 透传，标量域 return 1、切片域 return 扫描行数，空循环 success+rows=0；skip 终态检测与计时断言有测试；F6/F7 为不影响正确性的 nit。 |
| AC-6 严格/尽力策略行为 | **PARTIAL** | 四种组合在**执行器原语层**有真实单测（尽力两败俱走、严格必需中止 errAborted、严格可选失败继续）；但 AC 要求的"采集编排层"（CollectWithReport 多库循环、单库连接失败 FR-16、errAborted 沿 collectFirst 链上卷、aborted 后跳过 logs/rds/azure）无任何单测，仅代码审查——F1 与 F3 正落在未测的编排/胶水路径。 |
| AC-7 退出码与输出落盘 | **PASS** | `ExitCode` 纯函数测试（nil=0、aborted/required fail=1、degraded=0）；main.go 接线核实：严格 aborted 在 process 前 [os.Exit(1) 不写快照](file:///Users/nancy/swe-project/pgmetrics_fork1/cmd/pgmetrics/main.go#L449-L455)、尽力恒渲染后退出、`--input` 恒 0（L426-439）、用法错误 12 处 `os.Exit(2)`。 |
| AC-8 三输出同源 | **PASS** | JSON 内嵌 `m.Collection`；human `writeCollectionStatus` 三模式共享调用、nil 零输出、失败排序+omitted/unsupported 折叠；CSV 显式展开 `pgmetrics.collection.*`、反射遇 `*CollectionReport` 指针自然跳过；`TestHumanCSVDomainCountConsistency`/`TestCSVCollectionRows` 等 9 个 cmd 测试交叉断言一致。 |
| AC-9 凭证脱敏 | **PASS** | 9 个 sanitize 子用例覆盖 KV（引号/裸值/大小写、password/sslpassword/sslkey/passfile）、URI（含 `p%40ss` URL 编码）、幂等性；另在仓库外用相同正则独立实验复核；全量 Summary 赋值仅 4 处且全部经 sanitize 或硬编码安全串，执行器失败路径有脱敏断言测试。 |
| AC-10 向后兼容 | **PASS** | compat_test 内联 1.21 夹具做解码/human/CSV/再编码四断言；根包 `TestOldSnapshotWithoutCollection` 验证旧文件无状态段；model.go diff 纯附加。 |
| AC-11 真实空值可区分 | **PASS** | 切片域（slots/locks/bloat/triggers 等）返回落表行数、空集合 success+rows=0；标量域 rows=1；wal receiver ErrNoRows→(0,nil) 已核实；六态中 success 独有 rows 语义，消费者可仅凭 collection 块判定。 |
| AC-12 设计内聚与改动克制度（rubric，阈值 ≥4） | **FAIL（评分 3/5）** | 加分：catalog/classify/sanitize/Aggregate 各单点、查询改造机械一致、SQL 字面量与 Scan 实参集合经独立脚本比对零差异、资源关闭计数一致。扣分：F1 构成明文要求保留逻辑的漂移（FR-13）、F2 空态语义漂移、F3/F5 边界行为与分类宽松——存在"少数旁路/语义漂移"，落在锚点 3 档，低于 4 分门槛。 |
| AC-13 构建与测试门禁 | **PASS** | build/vet/test/gofmt 均 0；linux/windows 外另测 darwin/freebsd 交叉编译；`-race` 全绿（见元信息表，均为评审者复跑）。 |

**裁决汇总：PASS 10 ｜ PARTIAL 1（AC-6）｜ FAIL 2（AC-12  rubric、总体因 F1 critical）**

---

## 五、测试覆盖评价

新增 27 个测试函数（根包 6、collector 12 含 19 个 classify 子用例与 9 个 sanitize 子用例、cmd 9），全部为真实表驱动断言（构造错误、断言状态/码/脱敏后文本/行数/退出码/渲染一致性），**无空壳测试**；`go test -race` 通过。纯逻辑层（契约聚合、分类、脱敏、退出码、catalog 完整性、三渲染一致性、1.21 兼容）覆盖扎实，与 NFR-2 相符。

主要缺口（按重要性）：

1. **F1 路径零覆盖**：无"包装后的 55P03 → tables/indexes 去尺寸列重试"测试，正是本次漏网缺陷；修复 F1 时必须补。
2. **编排层无测试**：CollectWithReport 多库循环、FR-16 单库连接失败继续/严格中止、database_list 失败（F3）、aborted 后跳过 logs/rds/azure、errAborted 沿 collectFirst/collectPostgres/collectPgBouncer/collectPgpool 上卷均只靠代码审查；AC-6 的"编排层单测"条件实际只在执行器原语层满足。
3. **分类器长尾分支**：08000/08001/08003/08004/08006/08007 等 conn SQLSTATE、`net.Error`（`Timeout()==true` 而非 `*net.OpError`）、`sql.ErrNoRows` 空态叶子（wal receiver）无用例。
4. **门控 helper**：`runVersioned/runDBVersioned/runOption/skipPlatform` 及 skip 后 strict 交互、`recordFailure` 不看分类即中止（F4）无测试。
5. 真实/伪造数据库驱动集成测试按 spec Non-Goals 明确不在本期范围，缺口可接受。

---

## 六、修复优先级建议

1. 修复 F1（`errors.As`）并补包装 55P03 重试用例——修复后 AC-12 可重评至 4-5 分、总体可转 PASS。
2. 补齐 F2 空态特判与编排层最小单测（AC-6 转 PASS）。
3. F3/F4/F5 按建议收敛边界语义；F6-F8 nit 可顺手清理。

---

## 七、修复复审（独立代理）

| 项 | 内容 |
|---|---|
| 复审者 | 独立代理（未参与实现；只读源码 + 运行测试，未修改任何 .go；本节为唯一写入） |
| 复审日期 | 2026-09-19 |
| 复审方式 | 逐条代码走查 + git HEAD 机械对比（SQL 字面量/Scan 实参集合/getStatIOs 逐字节）+ 全量门禁复跑 + 定向回归测试 |

### 复审门禁实测（仓库根，全部退出 0）

| 命令 | 结果 |
|---|---|
| `gofmt -l .` | 零输出 |
| `go build ./...` | ok |
| `go vet ./...` | ok |
| `go test ./... -race -count=1` | 三包全 ok（pgmetrics 1.473s、cmd/pgmetrics 1.996s、collector 1.502s），无数据竞争 |
| `go test ./collector/ -run 'TestIsLockTimeoutErrorWrapped\|TestStrictAbortsOnlyFailureClass' -v -count=1` | 两个测试均 PASS（collector 0.707s） |
| `GOOS=linux/windows/freebsd/darwin go build ./...` | 四平台均 ok |

另独立机械复核（HEAD vs 工作树）：`collector/{collect,citus,pgbouncer,pgpool,log}.go` 五文件反引号 SQL 字面量集合**全部完全相等（removed=0 added=0）**、全部 `.Scan(...)` 实参集合（空白规范化后）**全部完全相等**；`model.go` diff 仍为纯附加（8 insertions/1 deletion：1.22 版本注释/常量 + `Collection` 字段），旧 JSON 键零改动。

### F1-F8 逐条裁决

| Finding | 裁决 | 证据 |
|---|---|---|
| **F1** lock_timeout 重试被包装击穿 | **FIXED** | [collect.go:1985-1994](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L1985-L1994) 已改为 `errors.As(err, &pgerr)`；两处 Query 失败确为 `fmt.Errorf("...: %w", err)`（[L1941](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L1941)、[L1980](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L1980)、[L2045](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L2045)、[L2075](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L2075)），55P03 重试分支（[L1866-1874](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L1866-L1874)、[L2002-2010](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L2002-L2010)）端到端可达，重试成功后 `c.note` 键用 `c.curTarget`，与 runDBOption→runDomain 的 target 一致（[L842-844](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L842-L844)、[L865-867](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L865-L867)）；全仓 .go 文件已无任何 `.(*pgconn.PgError)` 直接断言（仅两处 `errors.As` 生产用法 + 测试构造）；[TestIsLockTimeoutErrorWrapped](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor_test.go#L267-L283) 为真实断言（裸 55P03/`%w` 包装 55P03=true、包装 57014/io.EOF/nil=false），实测 PASS，非空壳。 |
| **F2** ErrNoRows 空态误判 failed | **FIXED** | [collect.go:3963-3966](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L3963-L3966)：`errors.Is(err, sql.ErrNoRows)` → `return 0, nil`（success+rows=0，StatRecovery 指针保持 nil 与旧 warning 语义一致），其余错误仍 `fmt.Errorf %w` 上卷；门控仍只在 pg19+recovery 态调度（[L795-805](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L795-L805)）。 |
| **F3** database_list 失败后无名 target 采集 | **FIXED** | [collect.go:272-281](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L272-L281)：严格模式 `runDomain` 返回 errAborted → 首个 `if err != nil { return c.finish() }` 优先收尾（c.aborted 已置位，finish 落 aborted）；尽力模式再查 `Outcome(database_list,"")` 为 failure-class 即 finish，不再落入 L285-286 的无名 target 默认采集。决定合理：`--all-dbs` 的库枚举是该运行形态的必需域，旧版本该失败本即 fatal；现在保留最小快照（仅含 database_list 失败 outcome）+ Aggregate=failed + ExitCode 1，比"悄悄改采默认库"语义清晰，符合初评修复建议。 |
| **F4** 严格中止不看分类 | **FIXED** | [executor.go:334](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L334) 条件现为 `required && c.strict && pgmetrics.IsFailureClass(status)`；[IsFailureClass](file:///Users/nancy/swe-project/pgmetrics_fork1/outcome.go#L136-L142) 仅对 permission_denied/timeout/failed 返回 true，unsupported/omitted/success 均 false，语义与 Aggregate 完全一致；[TestStrictAbortsOnlyFailureClass](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor_test.go#L287-L312) 真实覆盖两路径：required settings 返回 codeUndefinedObject→unsupported，runDomain 返回 nil、c.aborted=false、outcome=unsupported；随后 required databases codeQueryError→failed，`errors.Is(err, errAborted)` 且 aborted=true。实测 PASS。 |
| **F5** getCitusVersion 任意失败标 extension_absent | **FIXED** | [citus.go:54-64](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/citus.go#L54-L64)：仅 `classify(err)` 得 code==codeUndefinedObject（42P01/42P02/42883）才 skip(extension_absent)，其余错误 `return 0, err` 正常上卷（超时→timeout/lock_timeout、连接→failed/conn_failed、42501→permission_denied）。classify 两条取码路径均穿透包装：DomainError 经 `errors.As` 权威取码（[outcome.go:117-123](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/outcome.go#L117-L123)），裸 fmt `%w` 包装的 `*pgconn.PgError` 经 [L138-141](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/outcome.go#L138-L141) 取得 SQLSTATE；getCitusVersion 的错误正是后者（[citus.go:103-105](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/citus.go#L103-L105)），42883 可正确命中。skip 摘要为硬编码安全串，无凭证面。 |
| **F6** local_probe 手工 Record、duration=0 | **FIXED** | [collect.go:564-571](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L564-L571)：失败时 `c.note(domainLocalProbe,"",sanitize(err.Error()))` 后 fn 始终返回 (1,nil)；runDomain 成功路径在记录前消费该 note 写入 Summary（[executor.go:252-258](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L252-L258)），start/end 包裹真实 fn 调用（[L233-249](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L233-L249)），duration 不再恒 0；getLocal 失败显式 `c.local=false`（[collect.go:1064-1068](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L1064-L1068)），system 门控（[L583-596](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L583-L596)）与 logs 门控（[L330](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L330)）均据此 skip，无回归；note 内容经 sanitize。 |
| **F7** 失败后 note 残留 | **FIXED** | recordFailure 在 Record 失败 outcome 后 `delete(c.notes, domain+"\x00"+target)`（[executor.go:320-323](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L320-L323)），键拼接与 note/noteOnce（[L273](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L273)、[L282](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L282)）逐字节一致（同一 `\x00` 分隔）。 |
| **F8** getStatIOs 不可达 else 分支 | **FIXED** | 三分支改为 `if v18 / else（v16-v17，注释固化门控不变量）`（[collect.go:3845-3871](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L3845-L3871)），不可达的 `return 0,nil` 已删；唯一调度点确有 `runVersioned(domainStatIO,"16",pgv16,...)` v16 门控（[L786](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L786)）。用脚本抽取 HEAD 与工作树两版本 getStatIOs 的反引号串逐字节比对：2 个字符串顺序/集合均 identical（长度 521/590），Scan 20 实参也未变。 |

### 通用项复核

- SQL 文本/Scan 实参：五文件相对 HEAD 的 raw-string 集合与 Scan 实参集合均零差异（见门禁节脚本结论）；修复仅触及错误处理/控制流/注释。
- 凭证面：新增失败摘要通道只有一处新外露（local_probe note，经 sanitize）；recordFailure 中心 sanitize 不变；F5 skip 使用硬编码串；grep 全仓 err.Error()→note/skip 共 2 处均包 sanitize（[collect.go:566](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L566)、[L4173](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L4173)），无新增泄漏面。
- 旧 JSON 键：model.go 纯附加；outcome 契约线值未改。
- `-race` 全绿（c.notes 仅顺序调度访问，无并发）。

### AC 重新裁决

| AC | 新裁决 | 说明 |
|---|---|---|
| **AC-6** 严格/尽力策略行为 | **PARTIAL（维持）** | F3 行为已正确修复且经走查验证，F4 新增执行器原语层两路径真实测试；但 AC-6 要求的**编排层单测**仍为零：CollectWithReport 的 database_list 失败即收尾（F3 修复点）、多库循环、errAborted 沿 collectFirst 链上卷、aborted 后跳过 logs/rds/azure 仍只能代码审查。TestIsLockTimeoutErrorWrapped 也只覆盖谓词，未覆盖 getTables/getIndexes 重试编排。行为可信但证据层级未补齐。 |
| **AC-12** 设计内聚与改动克制度（rubric，阈值 ≥4） | **PASS（4/5，原审 3/5）** | 上调依据：初评全部扣分项（F1 明文保留逻辑漂移、F2 空态漂移、F3/F5 边界宽松）均已真实修复，最高风险项 F1 配有真实回归测试；五文件 SQL/Scan 经独立脚本再次机械比对零差异、F8 两 SQL 逐字节一致、门禁（含 -race 与四平台交叉编译）全绿；修复手法克制（errors.As/errors.Is 穿透、复用 note/classify 既有单点，未引入新旁路）。未给 5：编排层（CollectWithReport 多库/中止链）仍无单测、F3 的"尽力模式枚举失败即整体 failed/exit 1"属合理但无测试锁定的刻意决策、分类器 08000 族/net.Error 长尾分支依旧无测试。 |

### 总体结论

# ✅ PASS

初评的 1 critical（F1）+ 4 minor（F2-F5）+ 3 nit（F6-F8）**全部 FIXED**，证据真实（代码走查 + 定向测试实测 PASS + git HEAD 机械对比），无 critical/major/minor 级新问题；全量门禁（gofmt/build/vet/`-race`/linux 抽查另加 windows/freebsd/darwin）全绿。

### 新发现问题

- critical：无；major：无；minor：无。
- **[nit] N1（测试深度，非阻塞）**：F1 回归测试只断言 `isLockTimeoutError` 谓词本身，未注入式覆盖"getTables/getIndexes 首次包装 55P03 → 去尺寸列二次调用成功 → success+note、二次失败 → timeout 上卷"的完整编排；F3 的 database_list 失败收尾同理无编排层单测。当前走查可证明路径可达，建议后续以可注入 DB/域函数方式补端到端单测（与 AC-6 PARTIAL 同一缺口）。
- **[nit] N2（观察，非缺陷）**：尽力模式 `--all-dbs` 枚举失败现在整体 `failed` 且 exit code 1（必需域聚合语义），相对"尽力模式环境问题可 exit 0"的直觉偏严，但与旧版 fatal 行为及 FR-12 必需域定义一致；L275-277 注释已说明意图，无需改动。

### 编排层测试复审（N1/AC-6 闭环）

| 项 | 内容 |
|---|---|
| 复审日期 | 2026-09-19（同日第二轮，针对实现者新增 [collector/orchestration_test.go](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/orchestration_test.go)） |
| 复审对象 | 3 个新测试 + [CollectWithReport](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L193-L321) / [collectFromDB](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L396-L416) / [getConn](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L355-L390) / [runDomain-recordFailure](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L227-L341) |
| 复审方式 | 独立走查每条断言到生产分支 + pgx v5.10.0 错误链源码核对 + 两条门禁命令实测（未改任何 .go） |

**a) 断言逐条核验：全部与生产代码真实行为一致，非空壳，失败由真实驱动错误触发。**

失败注入机制：[deadUnixConfig](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/orchestration_test.go#L31-L40) 指向不存在的 unix socket 目录 `/nonexistent-pgmetrics-outcome-socket`（复审者 `ls -ld` 实测确认不存在）；[collect.go:213-215](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L213-L215) 拼出 `host=/nonexistent-...`，pgx 按绝对路径走 unix 拨号 `<dir>/.s.PGSQL.5432`，内核 `connect(2)` 立即返回 ENOENT。错误链为 `*pgconn.ConnectError → errors.Join(*perDialConnectError) → fmt.Errorf("dial error: %w", *net.OpError{ENOENT})`，三层均实现 `Unwrap`（pgx v5.10.0 pgconn/pgconn.go:163、323-340；pgconn/errors.go:78-94）。[getConn](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L372-L375) 原样返回该错误（未包 DomainError），[classify](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/outcome.go#L143-L147) 经 `errors.As(*net.OpError)` 命中 → **status=failed / code=conn_failed**（ENOENT 的 `Timeout()=false`，不会误判 timeout；非 PgError；该 net.OpError 分支另有 [outcome_test.go:78-79](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/outcome_test.go#L78-L79) 单测锁定）。

| 测试（断言） | 生产代码对应行为 | 核验 |
|---|---|---|
| **Test1 尽力必需失败**（[L53-79](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/orchestration_test.go#L53-L79)）：model+report 均返回 | 无名 target 走 [L286](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L286)；[finish](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L342-L353) 恒返回二者 | 一致 |
| report 挂到 model.Collection | [collect.go:350](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L350) `c.result.Collection = c.report`（同指针） | 一致 |
| connection outcome 存在、Required、failure-class | catalog 中 connection 为必需（[executor.go:122](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L122)）；[recordFailure](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L306-L319) Record failed；[IsFailureClass](file:///Users/nancy/swe-project/pgmetrics_fork1/outcome.go#L136-L142) 对 failed 为 true | 一致 |
| HasRequiredFailure / ExitCode=1 / overall=failed | 尽力非中止 → [Aggregate](file:///Users/nancy/swe-project/pgmetrics_fork1/outcome.go#L160-L178) 命中 required+failure → failed；[ExitCode](file:///Users/nancy/swe-project/pgmetrics_fork1/outcome.go#L190-L198) 经 HasRequiredFailure → 1；recordFailure 尽力返回 nil（[executor.go:334-340](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L334-L340)） | 一致 |
| logs 域仍记录（not_local skip） | 连接失败时 [collectFromDB:409-413](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L409-L413) db==nil 返回 errTargetUnreachable；c.local 保持零值 false（仅成功后 [collect.go:1064-1069](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L1064-L1069) 才置位）；非中止 → [runLogsDomain](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L326-L338) 走 `!c.local` → skip(not_local)，映射 omitted（[outcome.go:103](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/outcome.go#L103)） | 一致 |
| **Test2 严格中止**（[L84-102](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/orchestration_test.go#L84-L102)）：overall=aborted、exit 1、connection 有记录 | [executor.go:334-338](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L334-L338) required&&strict&&failure-class → c.aborted=true、预置 aborted、返回 errAborted，经 runDomain→[collectFromDB:406-407](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L406-L407) 上卷；finish 保持 aborted | 一致 |
| logs 域不被访问 | [collect.go:301](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L301) `if !c.aborted && ...` 门控跳过 logs/rds/azure 整块——这正是 F1 复审时列为"仅走查"的中止后门控，现被真实测试双向锁定（Test1 存在 / Test2 不存在） | 一致 |
| **Test3 database_list 失败两策略**（[L106-132](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/orchestration_test.go#L106-L132)）：required failure、无 connection/settings outcome | AllDBs 走 [collect.go:263-282](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L263-L282)：[getDBNames](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L418-L423) 内 getConn 同一 ENOENT 失败；严格由 L273 errAborted 收尾，尽力由 L278-281 outcome failure-class 复查收尾（F3 修复点），两路径都在 L285 之前 return，collectFromDB 不执行（无 connection），settings 仅在 c.collect 内运行（无 settings） | 一致 |
| 尽力 failed / 严格 aborted | 与 Test1/Test2 同一聚合分支；database_list 在 catalog 中 required=true（[executor.go:123](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L123)） | 一致 |

结论：3 个测试 12 条断言全部能映射到真实生产分支，无恒真断言、无 mock 自证；失败确实由 pgx 真实拨号 ENOENT 驱动并经生产 classify 分类为 failed/conn_failed。

**b) 确定性与资源：满足。**

- **无网络依赖**：host 为绝对路径 → unix 域套接字拨号，不触 TCP/ DNS，与 5432 端口是否被占用无关（端口只参与套接字文件名）；失败是内核同步 ENOENT，无重试退避等待（pgx 单地址 `errors.Join` 即返回）。
- **耗时**：`-v` 下三用例各自 `(0.00s)`，毫秒级；整包 ok 行 0.592s（含编译/起进程）。
- **无泄漏**：失败路径 [getConn:372-374](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L372-L374) PingContext 失败即 `db.Close()`（getDBNames 的连接亦在 [collect.go:423](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L423) defer Close）；连接从未建立，sql.DB 无存活 conn/后台 goroutine，Close 后无残留句柄。
- **全局状态**：[discardLogs](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/orchestration_test.go#L43-L47) 临时替换标准 logger 并以 t.Cleanup 恢复；collector 包全部测试无 `t.Parallel`（grep 零命中），串行无竞争；Test3 循环内两次注册 cleanup 均恢复 os.Stderr，无净污染。
- 唯一理论非确定性：若被测机恰好存在该目录且其中有监听套接字——路径名含 `pgmetrics-outcome` 专用前缀，CI/开发机可视为不可能。

**c) 门禁实测（仓库根，复审者本机）：**

| 命令 | 结果 |
|---|---|
| `go test ./collector/ -run TestOrchestration -v -count=1` | 3/3 PASS，每用例 0.00s，`ok github.com/rapidloop/pgmetrics/collector 0.803s`（冷）/ 0.592s（热），退出 0 |
| `go test ./... -race -count=1` | 三包全 ok 且无数据竞争：pgmetrics 1.232s、cmd/pgmetrics 1.328s、collector 1.595s，退出 0 |

**d) AC-6 最终裁决：PASS（由 PARTIAL 上调）。**

初评/上轮 PARTIAL 列举的编排层缺口逐项核销：FR-16 单库连接失败的"尽力继续走完/严格立即中止"→ Test1/Test2 经真实驱动失败端到端覆盖；aborted 后跳过 logs/rds/azure → Test1/Test2 双向锁定（[collect.go:301](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L301)）；F3 的 database_list 失败即收尾（两策略）→ Test3 覆盖；errAborted 从 recordFailure 经 runDomain/collectFromDB 上卷至 finish 落 aborted → Test2 覆盖；尽力模式"失败有记录、流程走完、总体 failed"与严格模式"必需域处中止、总体 aborted"两条 AC-6 主干 Then 子句均在真实编排层（而非仅执行器原语层）证实，且证据强度高于 spec 允许的"假域函数单测"。可选域失败不中止的策略决策唯一存在于 [recordFailure:334](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/executor.go#L334)，已由执行器原语层 TestStrictAbortsOnlyFailureClass 锁定，CollectWithReport 对可选失败无任何附加分支，编排层行为被该原语契约穷尽决定。

**精确剩余缺口（不阻塞，记为 nit）**：[collect.go:288-298](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L288-L298) 的**具名多库循环**（dbnames ≥ 2）无直接测试：尽力模式 target1 失败返回 errTargetUnreachable 后继续 target2、严格模式 `errors.Is(err, errAborted)` 立即 break 这两个迭代控制分支未被专门执行。判定不阻止 AC-6 的理由：(1) AC-6 的 Given/When/Then 约束的是"必需/可选失败 × 两策略"四种组合，不含多目标迭代语义；(2) 循环体复用的 collectFromDB 失败→errTargetUnreachable 分支已由 Test1 经无名 target 调用（L286）真实执行，循环守卫依赖的 c.aborted 标志已被 Test2/Test3 证明门控效力；(3) 该循环不包含任何策略决策，仅剩直线式循环控制，回归风险低。建议后续以零成本用例补齐：`CollectWithReport(deadUnixConfig(false), []string{"d1","d2"})` 断言两个按 target 键入的 connection failure + logs 仍访问 + failed，严格变体断言仅 d1 有 outcome + aborted（"d1" 经 [L199-210](file:///Users/nancy/swe-project/pgmetrics_fork1/collector/collect.go#L199-L210) ParseConfig 失败自然落入选项拼串路径，无需特殊构造）。

N1 的编排层部分（CollectWithReport/database_list/abort 门控无测试）**闭环**；N1 中 F1 的 getTables/getIndexes 55P03 重试完整编排仍仅有谓词级测试，维持原 nit 不变。

**总体结论：维持 ✅ PASS，无变化**（此前 F1-F8 已全 FIXED）；AC 裁决表现为 13/13 PASS，无 PARTIAL/FAIL 残留。
