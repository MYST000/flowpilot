# FlowPilot 实验 TODO List

最后审查：2026-08-18

本文是 FlowPilot 尚缺实验与证据的唯一待办入口。`design.md` 第 14 节定义研究问题和完整实验空间；本文只记录当前代码能够验证什么、还缺什么、如何补齐，以及何时允许进入下一阶段。

## 1. 当前结论

- **implementation complete（当前范围）**：Phase 0 双向网关、Phase 1 exact Web Tool reuse、Phase 2 DCS，以及默认关闭的 Phase 3 保守语义复用控制面已实现。
- **local verification complete**：本地单元测试、静态检查和确定性 mock E2E 已通过，见下方基线。
- **production evidence insufficient**：尚无真实推理部署、真实 `web_search` 校准语料、进程级崩溃/网络分区、滚动升级、多实例负载或真实 KV 数据。
- **Phase 3 production NO-GO**：实现保持默认关闭；在语义语料、离线校准、独立测试集和隔离审计完成前不得生产启用 semantic reuse。
- **Phase 4 online performance evidence is still insufficient**：Phase 4
  forecast/SLO control code is implemented and mock-testable, but there is no
  calibrated production predictor, real OpenHands workload replay, or real KV
  connector evidence. Full KV/Tool request-2 alignment remains Phase 5.

这几个限制来自当前代码，而不是 GPU 暂时不可用本身：

- `flowpilot/gateway/router.py` 只有按模型兼容性过滤后的 round-robin，没有队列、TTFT、KV affinity 或 SLO 感知路由。
- `flowpilot/config.py` 强制 `workers == 1`，frontier 与 in-flight binding 都是进程内状态。
- `/flowpilot/health` 固定报告 `kv_telemetry=unsupported`，trace rotation 和 restart continuity 也为 unsupported。
- DCS 只支持串行、本地、非流式 native Tool calling；semantic reuse 已有默认关闭的控制面实现，但并行 Tool 执行、流式 internal continuation 和多 worker 仍不支持。

因此，下面严格区分三类事项：现在可在 CPU/mock 环境完成的实验、需要真实 GPU/推理引擎的实验、以及必须先实现功能才能进行的实验。不得用 mock 结果替代后两类证据。

## 2. 2026-08-18 无 GPU 基线

已执行：

```bash
cd /home/liyachen/workspace/flowpilot
uv run pytest -q
uv run ruff check .
uv run pyright
OPENHANDS_SUPPRESS_BANNER=1 uv run python examples/openhands_e2e.py

cd /home/liyachen/openhands/software-agent-sdk
uv run pytest -q tests/sdk/test_flowpilot.py
uv run ruff check openhands-sdk/openhands/sdk tests/sdk/test_flowpilot.py
uv run pyright openhands-sdk/openhands/sdk tests/sdk/test_flowpilot.py
```

结果：

| 检查 | 结果 |
|---|---|
| FlowPilot tests | 54 passed |
| OpenHands FlowPilot tests | 29 passed |
| FlowPilot Ruff / Pyright | 通过，0 error / 0 warning |
| OpenHands targeted Ruff | 通过 |
| OpenHands Pyright | 0 error；1 个既有 `marketplace.__all__` warning |
| mock E2E | 154 trace events，29 个相关联 LLM requests，隐私断言通过 |
| mock proxy latency | direct median 14.002 ms；proxy median 26.865 ms；本地样本开销 12.864 ms |
| mock reuse/DCS JCT | immediate 122.330 ms；Chat DCS 283.810 ms；Responses multi-Tool DCS 294.089 ms |

以上延迟均来自一次本机 mock 运行。DCS 在这个单隐藏轮次样本中更慢，不能据此宣称性能收益，也不能把它当作稳定回归阈值。

现有 `/home/liyachen/workspace/experiments/traces` 包含 3 个 `tool_calls.jsonl`、共 149 条 Tool 记录，但没有结构化 `web_search`。这些旧记录还包含完整 Tool input/result，不能直接复制进 FlowPilot metadata-only trace 或公开实验产物。

## 3. 实验产物规范

每次实验使用独立目录：

```text
/home/liyachen/workspace/experiments/flowpilot/<experiment-id>/<run-id>/
  manifest.json
  config-redacted.json
  results.json
  summary.md
  traces/
  logs/
```

`manifest.json` 至少记录：UTC 时间、FlowPilot 源文件 SHA-256 清单、OpenHands commit 与 dirty diff hash、模型和推理引擎版本、硬件、OS、Python/依赖锁版本、随机种子、工作负载版本、实验矩阵单元和重复编号。FlowPilot 当前不是 Git 仓库，不能只写“latest”。

所有结果都必须遵守：

- 每个矩阵单元先 warm-up，再至少重复 30 次；尾延迟实验应增加样本直到 P99 bootstrap 95% CI 稳定，并报告实际样本数。
- 同一工作负载使用配对随机顺序比较 baseline/treatment；报告原始分布、P50/P95/P99、均值和 95% CI，不只报告单个均值。
- trace 只保留身份、摘要、大小、时延、状态和 provenance；不保留 prompt、完整 Tool 参数/结果、API key、Authorization header 或 DCS 明文。
- 每次运行前后记录 exact/semantic 旧 SQLite 文件 SHA-256，确认 FlowPilot 实验没有修改旧 Tool-Reuse 数据库。
- 失败运行也保留 manifest、退出状态和脱敏日志，不从统计中静默删除。

## 4. 现在可做：CPU/mock/离线实验

### [ ] E00 可复现实验快照与隐私门禁（P0）

**目的**：先保证后续结果能对应唯一代码状态，并阻止旧 trace 中的敏感 payload 进入新数据集。

**如何进行**：

1. 编写只读 manifest 生成器，计算 FlowPilot 源码、`uv.lock`、OpenHands commit/dirty diff、配置和 workload 的摘要。
2. 编写 artifact audit，扫描 prompt marker、Tool input/result marker、API key、`X-FlowPilot-API-Key`、Authorization 和 WAL 明文。
3. 用 mock E2E、一个故意含 secret marker 的负例和现有 3 份旧 trace 验证审计器。

**产物/通过条件**：每个 run 都有完整 manifest；负例必须被检出；新 FlowPilot trace/WAL strings 扫描为零泄漏。旧 trace 只可在受控离线清洗流程中读取，不能作为可发布 artifact。

### [ ] E01 Phase 2 provider-visible 消息等价性（P0，RQ7）

**目的**：证明 DCS 与“每轮立即回传 Agent”产生完全一致的 provider-visible 消息序列、Tool Call identity 和最终 OpenHands 权威历史。

**矩阵**：Chat/Responses；1/2/4/8 个连续 exact hits；历史命中/in-flight follower；terminal/local-tool/capacity/TTL/lease/failure barrier；单 Tool/同一回复多 Tool；sync/async Agent loop。

**如何进行**：

1. 扩展确定性 mock workload，使 baseline 与 DCS 使用同一预定义回复和 Tool 结果序列。
2. 在 provider 边界对每次请求生成 canonical message/item hash；分别运行 immediate-return 与 DCS。
3. 对齐每个逻辑轮次，比较 system/developer/user/assistant/tool 顺序、Tool Call ID、参数、Tool Result、sampling fields 和最终 EventLog hash。
4. 单独验证多 Tool provider order 与一个 Observation 对应一个 `tool_call_id`。

**通过条件**：所有可委托用例逐消息 hash 零差异，Tool Call ID 零差异，最终权威 EventLog 零差异；不支持的 hook/critic/confirmation 等路径必须立即形成 barrier，而非继续隐藏轮次。

### [ ] E02 DCS 进程级故障与网络分区 campaign（P0，RQ7）

**目的**：把已有函数级 fault injection 提升为真实进程、文件系统和网络边界验证。

**故障点**：至少覆盖以下 12 点：delta WAL commit 前后、internal request 发出后、internal response 到达后、sync begin 前、sync chunk 中、OpenHands event prepare 中、recovery manifest 落盘后、部分 event file 后、HEAD commit 前后、ACK 成功但 manifest 删除前、reconciliation 期间。

**如何进行**：

1. 用独立 FlowPilot/OpenHands 进程运行固定 DCS workload；每个故障点注入 `SIGKILL`，而不是只抛 Python exception。
2. 用可控 TCP proxy 注入断连、超时、重复、延迟和双向 partition；分别重启 Agent、FlowPilot 和两者。
3. 每次恢复后调用 reconciliation，比较权威 EventLog、WAL range/digest、lease owner 和 provider message hash。
4. 对同一 ACK、append、publish 和 retry 重放至少两次，验证幂等和冲突拒绝。

**通过条件**：不出现重复、遗漏、乱序消息；不出现两个有效 writer；无法证明一致时必须 fail closed 为 sync-required/diverged；Tool 仅在 `context_sync_ack` 后本地执行；所有 upstream 资源关闭并产生明确 terminal trace。

### [ ] E03 DCS RTT/隐藏深度交叉点（P1，RQ7）

**目的**：回答 DCS 在什么网络往返和连续命中深度下开始有净收益，解释当前单轮 mock 反而更慢的结果。

**矩阵**：Agent↔FlowPilot RTT = 0/1/5/20/50/100 ms；隐藏深度 = 1/2/4/8；Tool Result = 1 KiB/16 KiB/256 KiB；Chat/Responses；terminal/local Tool barrier；immediate-return/DCS。

**如何进行**：

1. 在进程间链路注入固定 RTT/jitter，不修改应用内计时。
2. 使用确定性 mock inference 固定 LLM latency 和输出，从而隔离控制往返成本。
3. 采集 JCT、每轮 TTFT、控制请求数、同步字节、WAL 写放大、P95/P99 sync latency、内部 continuation 数和避免的 Agent 往返数。
4. 对每组配对样本计算 `JCT_DCS - JCT_immediate` 和置信区间，画出 crossover curve。

**通过条件**：不要求 DCS 在所有条件下胜出；必须得到可复现的正/负收益区域和禁用建议。若实际可部署 RTT 区间内无收益，应缩小或放弃 DCS 性能主张。

### [ ] E04 exact historical/in-flight reuse 压力与失败语义（P1，RQ2/RQ3 的 exact 部分）

**目的**：验证重复执行消除、follower 净收益、lease/失败行为和租户隔离。

**矩阵**：并发 follower = 1/2/8/32/128；leader latency = 10 ms/1 s/30 s；结果大小与 output budget；leader success/fail/cancel/lease expiry；follower cancel；same/cross tenant、auth scope、locale、region、freshness。

**如何进行**：

1. 使用确定性只读 Web Tool stub，记录真实本地 execution count。
2. 对每个 descriptor 同时启动 leader/follower，扫描完成顺序和故障点。
3. 比较无 reuse、history only、history + exact in-flight 三组。
4. 采集 hit/join ratio、每 leader follower 数、避免执行次数/时间、follower wait、P95/P99 JCT、scope rejection 和 provenance。

**通过条件**：每个 binding 最多一个本地 leader 执行；取消 follower 不取消 leader；leader 失败/租约到期后 follower 可安全 re-resolve；跨 scope 命中为 0；trace 不包含 query/result payload。

### [ ] E05 line-tail frontier 状态规模与协议压力（P1，RQ6 的状态部分）

**目的**：验证在线状态随 active lines/dependencies 而非累计历史增长，并量化原子替换、环检测和阻塞计数开销。

**矩阵**：active lines = 10/100/1k/10k；fan-out/fan-in/chain/random DAG；每 line 历史轮次 = 1/10/1k；并发 dependency update 和 LLM tail replacement。

**如何进行**：

1. 直接驱动 control API，预生成可复现 DAG 更新序列。
2. 采集进程 RSS、frontier object count、update latency、cycle rejection、stale version rejection 和 line finish 释放时间。
3. 固定 active frontier，增加累计历史轮次，检查热路径内存不随历史线性增长。

**通过条件**：协议结果无 stale overwrite/漏释放；状态规模与 active frontier 符合设计预期。当前没有 blocking-aware priority/fairness scheduler，因此本实验不能回答 RQ6 的 JCT 和 Jain fairness 部分。

### [ ] E06 trace/WAL 存储与降级实验（P1）

**目的**：验证 trace/WAL 在磁盘慢、满、只读、截断和重启下不会产生“看似完整”的错误数据集。

**如何进行**：

1. 在受限临时文件系统中注入 ENOSPC、EIO、只读目录、慢 fsync 和进程崩溃。
2. 检查 `/flowpilot/health`、`/metrics`、trace failure/drop counter、SQLite integrity 和 DCS fail-closed 行为。
3. 测量 JSONL append、WAL 加密、sync batch 对延迟和写放大的影响。

**通过条件**：trace failure 必须 degraded 且计数；DCS 持久化失败不能继续委托；无 silent loss。注意：rotation/restart continuity 当前未实现，相关用例应记录为预期 unsupported，而不是通过。

## 5. 需要真实 Web 数据，但不必依赖本地 GPU

### [ ] E07 `web_search` 校准语料构建（P0，Phase 3 前置）

**目的**：补齐当前 149 条 Tool 记录中 0 个结构化 `web_search` 的硬缺口。

**如何进行**：

1. 先冻结目标 Web Tool registry、schema version、scope 字段和 freshness class。
2. 收集真实 OpenHands `web_search` 调用，只保存脱敏 descriptor、时间、scope、结果摘要/特征和人工标注所需的最小受控数据；原始 payload 放在访问受限存储，不进入 FlowPilot trace。
3. 分层覆盖：同义改写、近似但不同约束、时效查询、地域/语言差异、安全搜索、授权/tenant 冲突、空结果、错误和长尾结果。
4. 先做至少 200 对 pilot 并据此做 power analysis，再冻结正式样本量；train/calibration/test 按原始查询簇和时间切分，禁止近重复跨集合泄漏。

**通过条件**：数据卡说明来源、许可、PII 处理、分层分布、标注协议和 inter-annotator agreement；测试集在阈值冻结前不可见；跨 tenant/auth 的候选必须保留为负例。

### [ ] E08 semantic historical/in-flight 离线校准与 Phase 3 GO/NO-GO（P0，RQ2/RQ3）

**前置**：E07 完成；semantic reuse 实现保持默认关闭。

**如何进行**：

1. 对每个 Tool family 在硬约束过滤后扫描相似度阈值和 freshness；先 history，再 in-flight，禁止改变查找顺序。
2. 在 calibration 集选阈值，在独立 test 集报告 precision/recall、false reuse、stale reuse、scope rejection 和 follower 净收益。
3. 对所有错误复用做人工审计，按时间、地域、权限、细微约束和结果截取分类。
4. 预注册每个 Tool family 的最低 precision/最大风险目标；只有 test 集 95% CI 满足目标才可 GO。

**通过条件**：跨 tenant/auth false reuse 必须为 0；其他阈值必须在看 test 集前冻结。若无法达到预注册目标，Phase 3 保持 NO-GO，exact reuse 不受影响。

## 6. 需要 GPU/真实推理实例

### [ ] E09 真实 OpenAI-compatible 网关一致性与开销（P0）

**资源**：至少 1 个 GPU 推理实例；固定模型、引擎版本和 serving 参数。

**矩阵**：Chat/Responses；sync/async；stream/non-stream；text/single Tool/multi-Tool；成功、4xx/5xx、disconnect、cancel、timeout、malformed/incomplete SSE。

**如何进行**：

1. 对同一冻结请求分别直连 inference 和经 FlowPilot 代理，使用确定性解码或保存请求配对。
2. byte-level 比较 status/body/chunk order/repeated headers/finish reason/usage/Tool fragments；比较 provider-visible request body digest。
3. 采集 proxy 增量 TTFT、TPOT、JCT、CPU、RSS、连接数和 trace 开销。

**通过条件**：除 FlowPilot 自有响应头外协议零差异；所有终止路径离开
`ACTIVE`（旧接口中的 `LLM_RUNNING`），关闭 upstream 并产生 terminal trace；
报告开销分布，不用 mock 延迟替代。

### [ ] E10 真实 OpenHands 工作负载端到端 A/B（P0，RQ2/RQ3/RQ7）

**资源**：1+ GPU，真实 OpenHands、本地 Web Tool 和 E07 冻结 workload。

**基线**：direct；FlowPilot/no reuse；exact history；exact history + in-flight；exact + DCS immediate-return off/on。

**工作负载**：Multi-line Web Research、Search-heavy Assistant、Code Agent、本地 Tool barrier，以及其混合。对每个任务固定输入、模型和 seed；核验最终答案/事件历史等价性。

**采集**：Job JCT P50/P95/P99、TTFT/TPOT、Web Tool 执行数、hit/join ratio、避免时间、DCS 往返和同步成本、失败率、最终任务质量。

**通过条件**：先通过 E01 等价性与 E02 故障门禁；性能结论必须来自配对重复和 CI。DCS 若只在特定 RTT/命中深度有益，应据 E03 形成启用策略，不可默认泛化。

### [ ] E11 多实例路由基线（P1，RQ1）

**资源**：至少 2 个独立 GPU 实例；异构实验还需不同 GPU/模型配置。

**当前限制**：现有 router 只是 round-robin。因此现在只能测 direct binding 与 round-robin proxy 基线，不能声称已验证 design 中的队列/KV/SLO 感知路由。

**如何进行**：

1. 先跑 direct static binding 和当前 round-robin，扫描并发、prompt/output 长度和 burstiness。
2. 采集每实例 queue wait、TTFT、TPOT、JCT、queue depth、负载方差和错误/重试。
3. 实现 queue-aware/affinity-aware routing 后，用同一 trace replay 与相同 arrival schedule 重跑。
4. 混合 tenant/job，报告 per-tenant slowdown、Jain fairness 和 deadline miss。

**通过条件**：协议正确性零回归；路由收益针对 JCT/queue tail 和公平性报告。GPU utilization 只能作为诊断指标，不能单独证明收益。

### [ ] E12 真实 KV telemetry connector 与测量校准（P0，Phase 4/5 前置）

**资源**：能暴露真实 KV handle/tier/bytes、restore/migration/rematerialization cost 的推理引擎。

**如何进行**：

1. 先冻结 engine-specific versioned envelope；从引擎读取真实事件，不从 token 数估算 KV bytes。
2. 对不同 prompt 长度、并发、GPU/CPU/NVMe tier 执行 keep/offload/restore/drop-rematerialize。
3. 交叉核对引擎指标、FlowPilot trace 和外部 GPU/CPU/NVMe 计数器。
4. 测量 restore stall、迁移字节、I/O 带宽、重算 token 和 measurement overhead。

**通过条件**：事件能与 tenant/job/line/tail/LLM/session/instance 相关联；bytes/cost 与引擎事实一致；缺字段时继续报告 unsupported，禁止补估值。

## 7. 必须先实现功能，再做实验

### [ ] E13 queue/SLO/blocking-aware 路由与公平性（P2，RQ1/RQ6）

**实现前置**：实例队列/吞吐信号、request profile、deadline/slack、Job/tenant deficit 和 blocking-aware priority。目前代码没有这些策略。

**实验**：在 E11 workload 上比较 static、round-robin、queue-aware、queue+SLO、queue+SLO+blocking；报告 JCT、deadline miss、goodput、per-tenant slowdown、Jain fairness 和 scheduler overhead。

### [ ] E14 ToolAnalysis/Heavy Profile 校准与消融（P2，RQ4）

**实现前置**：last-response ToolAnalysis、intrinsic/effective/remaining cost、output predictor、`tool_share`、二元 heavy label、ContinuationHint 版本/失效和 readiness 解耦。

**实验**：先用真实 trace 离线训练/校准并按任务/时间切分；报告 duration/output error、heavy-label precision/recall、重分类率和 readiness 切换。再消融 duration-only、type-only、intrinsic-only、binary-only、无 SLO、无 hysteresis、hit 后不重算。

**通过条件**：离线预测提升必须在独立 test 上成立，并进一步在在线 E10/E11 workload 中改善 JCT/SLO；仅分类准确率提高不够。

### [ ] E15 multi-worker、滚动升级与分布式状态（P1）

**实现前置**：共享 frontier、分布式 in-flight binding/lease、DCS writer fencing、版本兼容与 trace rotation。当前 `workers != 1` 会直接拒绝启动。

**实验**：1→2→N worker 滚动升级/回滚；请求与 follower 在进程间漂移；worker crash；旧/新协议混跑；网络 partition 和 lease expiry。验证单 leader、单 writer、无跨版本错误 ACK、无消息丢重乱序，并报告 failover stall。

### [ ] E16 Phase 5 KV/Tool 联合调度与核心消融（P3，RQ5）

**实现前置**：E12 真实 KV、Phase 4 profile/SLO 闭环、Tool Cache residency/accounting、CPU/NVMe I/O telemetry，以及 PC-JR、Coupled ARC、Slowdown Equalization、Wait-Age Tiering、Dependency-Frontier Guard。

**基线**：独立 LRU、固定分区、仅联合容量、完整联合但 immediate-return、完整联合 + DCS、Coupled ARC、Slowdown Equalization、offline trace oracle。

**实验**：扫描 GPU/CPU/NVMe 容量、I/O 带宽、KV/Tool size、Tool ready time、SLO mix 和 workload phase shift；分别覆盖 KV/Tool 共享资源域与物理分离资源域。

**采集**：每 GB 保存的 JCT、KV residency/migration/restore stall/rematerialization、Tool eviction loss、hit 后 residual stall、I/O queue/bandwidth、JCT/deadline/fairness。

**通过条件**：端到端 JCT/SLO 获益且 failure fallback 正确；必须报告 Tool hit 被 KV restore 抵消的情况。物理分离时只能主张时间耦合/全局成本收益，不能主张竞争同一 DRAM。

## 8. 执行顺序与阶段门禁

建议顺序：

1. **无 GPU 立即完成**：E00 → E01 → E02 → E03 → E04 → E05 → E06。
2. **并行准备 Phase 3 数据**：E07 → E08；E08 未通过前 semantic reuse 保持 NO-GO。
3. **GPU 恢复后的第一批**：E09 → E10 → E11 → E12。
4. **实现后再实验**：E13/E14/E15；最后才是 E16。

阶段门禁：

| 进入阶段 | 必须具备的证据 |
|---|---|
| Phase 2 production pilot | E01、E02、E06、E09 通过；E03 给出明确启用区间 |
| Phase 3 production enablement | E07 数据卡完成，E08 在独立 test 上 GO，隔离错误为 0 |
| Phase 4 online experiment | E10 真实 workload 可重放，E14 离线校准通过 |
| Phase 5 experiment | E12 真实 KV 事实、E13/E14 闭环、E15 分布式安全完成 |

## 9. 研究问题覆盖检查

| Design RQ | 对应实验 | 当前状态 |
|---|---|---|
| RQ1 多实例路由 | E11、E13 | GPU + 实现阻塞 |
| RQ2 history reuse | E04 exact；E07/E08 semantic；E10 online | exact 可做；semantic production NO-GO |
| RQ3 in-flight merge | E04 exact；E08 semantic；E10 online | exact 可做；semantic production NO-GO |
| RQ4 ToolAnalysis/profile | E14，后接 E10/E13 | 未实现（不属于当前 Phase 4 contract） |
| RQ5 KV/Tool 联合调度 | E12、E16 | GPU + 未实现 |
| RQ6 line-tail/fairness | E05 状态；E13 调度 | 状态可做；调度未实现 |
| RQ7 DCS | E01/E02/E03/E10 | CPU 正确性可做；真实性能缺 GPU |

完成某项时，将 `[ ]` 改为 `[x]`，在该项下追加 run 目录、版本、样本数、关键结果和结论。不得只因脚本退出码为 0 就标记完成。
