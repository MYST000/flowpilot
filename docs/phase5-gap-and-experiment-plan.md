# Phase 5 当前不足与补足实验计划

最后审查：2026-08-31  
权威依据：[`../design.md`](../design.md) 第 7.4、7.6--7.10、9、14 节；本文件只记录当前实现与证据状态。

## 结论

当前状态应标记为：

- **implementation complete：否**。已有 `T_need/T_KV/T2`、恢复队列、KV lease/fencing、独立容量数据结构和滚动重算的局部原语，但它们还没有组成请求 2 的可执行调度闭环。
- **local verification complete：部分**。当前 FlowPilot 测试为 117 passed，Ruff 和 Pyright 通过；这些测试主要覆盖协议、Mock KV 和局部状态转换。
- **production evidence insufficient：是**。没有真实 vLLM KV 扩展、真实 KV restore/migration 成本、Tool Cache 容量/预热实现、真实 OpenHands 工作负载或 SLO goodput 对比。

因此 Phase 5 目前不能宣称完成，也不能用 Mock KV 测试结果证明跨层调度收益。标准 vLLM OpenAI-compatible API 没有本设计所需的逐会话 KV handle/tier/bytes/restore 成本；在获得版本化扩展前必须保持 `kv_telemetry=unsupported`，退化为普通路由。

## 已确认缺口（按严重性）

| 严重性 | 违反的 owner/契约 | 当前证据 | 可观察失败 | 补足与回归测试 |
|---|---|---|---|---|
| P0 | FlowPilot Temporal Coordinator；设计要求请求 2 在 `max(T_need,T_KV)` 后进入 ready queue | `RollingAlignmentController.recompute_line()` 只生成 snapshot 并 `upsert` 恢复条目（`flowpilot/scheduling/alignment.py:349-391`）；`TemporalKVCoordinator` 没有在 `create_app()` 中实例化或运行 worker；网关仍在 `LLMGateway.proxy()` 中直接 `_send_with_failover()`（`flowpilot/gateway/service.py:216-225`） | `T2` 只是查询结果，不会阻止/释放请求 2；没有 restore 完成事件驱动的真实启动时序 | 实现 per-instance coordinator + restore worker + request-2 ready queue；动作执行前校验 tail version，按 lease/fencing/idempotency 提交给 vLLM。回归：Tool ready=10 ms、KV restore=100 ms 时，记录请求 2 不早于 `max`；tail 替换时旧动作必须丢弃。 |
| P0 | Tool Cache owner；Phase 5 要求预测预热、命中/未命中和独立容量 | `_forecast_prewarm()` 仅把 `ForecastResult` 存进 `ToolResolutionStore`，明确注明“可由部署替换”（`flowpilot/app.py:194-199`）；`ResourceCapacities` 只有纯函数，未接入 Settings、缓存或 admission/eviction；复用 SQLite 没有总字节预算 | 没有候选条目预热、有效/浪费预热统计，也无法证明 Tool Cache 与 KV Cache 的独立容量约束 | 增加 metadata-only prewarm adapter、Tool Cache byte accounting、freshness/admission/eviction 和独立配置；回归：Tool 容量溢出只驱逐 Tool，不改变 KV 预算，跨类型容量交换测试必须失败。 |
| P0 | vLLM capability-gated KV contract；KV 成本必须是真实、版本兼容、可关联事实 | 当前 `MockVLLMKVAdapter` 只提供内存协议证据；`Settings` 只接受 schema/endpoint 配置（`flowpilot/config.py:38-63`），没有真实引擎实现；健康检查也区分 configured-only 与 runtime-negotiated | 可以通过 Mock/配置看似“supported”，但无法证明 GPU/CPU/NVMe bytes、restore/rematerialization cost 或动作收益 | 提供 engine-specific `flowpilot-vllm-kv-v2` 扩展并先做 capability negotiation；缺字段或重启后回到 unsupported。见 E5 实验门禁。 |
| P1 | 设计 7.10/8 的事件驱动滚动重算 | 已接入的重算点主要是 `/events/tools`、`/events/kv`、reuse resolve 和 dependency update；forecast callback 只写 trace，LLM 完整回复在 gateway 内写 resolution 后没有通知 alignment；line finish、DCS ACK/continuation、tail replacement 也没有统一事件总线或下游 affected-line 重算 | forecast、真实 Tool Call、LLM response 或 prerequisite finish 后可能仍使用旧 projection/restore priority；依赖释放的 waiter 不会自动进入新的恢复/请求队列 | 建立 typed scheduling event bus，覆盖 forecast accepted/discarded、LLM response、Tool resolution、DCS、tail/dependency、KV watermark/I/O、instance load；只重算受影响 lines。回归：每种事件都产生一个新 projection；旧 tail 动作不可执行；finish prerequisite 会刷新所有 waiter。 |
| P1 | `T_KV` 与 DROP/rematerialize 事实语义 | `RollingAlignmentController` 只在 `fact.restore_cost_ms is not None` 时入队（`alignment.py:372-379`），因此只有 `rematerialization_cost_ms` 的 DROPPED fact 被漏掉；`KVDirectory.recommend_action()` 在没有 `t_need`/restore cost 时返回 `offload`（`kv.py:741-744`），没有 wait-age/pressure fallback、DAG/SLO 或 DROP 策略 | DROPPED session 可能永不进入 restore queue；无 ready-time 时会收到无目标的 offload 建议，导致错误动作或无法执行 | 统一 `restore_cost = restore_cost_ms | rematerialization_cost_ms` 的事实选择；无事实时走 `wait_age_tier_decision`，而非猜测 offload；回归覆盖 CPU/NVMe/DROPPED、成本缺失、压力阈值、cooldown。 |
| P1 | Job fairness 与 ready-queue 契约 | `WeightedFairRequestQueue` 已定义但没有被网关或任何调度 worker 使用；`InferenceRouter.candidates()` 只做实例排序，网关请求仍立即发送 | line 数量、blocking boost 或 deadline 不能形成真正的份额控制；无法宣称 DAG/SLO-aware request scheduling 或公平性 | 将 queue 接入 LLM request admission/dispatch，内部 continuation 与 Agent 请求共用 Job deficit；回归：多 Job burst 下份额、deadline/blocking 优先级和取消/过期行为符合预期。 |
| P1 | Phase 5 telemetry/evaluation contract | `scheduling/metrics.py` 只有未加权 miss rate、goodput fraction 和 Jain fairness；trace 没有统一记录 `T_need/T_KV/T2`、restore stall、action outcome、prewarm waste 或 residual stall | `/metrics` 不能产出设计要求的 SLO-satisfied goodput、请求 2 delay、`|T_need-T_KV|`、KV rematerialization 和容量/I/O 指标，实验无法复现 | 增加 metadata-only alignment/action/queue metrics 与 schema；实现按 Job weight 的 goodput、JCT、deadline miss、residual stall、forecast overlap/waste、restore-laxity miss。回归 trace correlation、隐私扫描和计数器降级。 |
| P1 | 分布式/生产运行契约 | `create_app()` 对 `workers != 1` fail-closed；frontier、resolution、alignment queue 仍是进程内状态，SQLite shared backend 只验证契约 | 多副本、worker crash、滚动升级时无法保持单 writer、lease 和恢复队列一致性 | 在多 worker 实验前实现共享 frontier、action/queue lease 和版本兼容；否则明确保持单 worker pilot，不把本地结果外推生产。 |

## 补足顺序

1. **先闭合控制路径**：把 `T2`、restore queue、lease/fencing、tail-version guard 和 ready queue 接入同一个调度服务；unsupported KV 时明确旁路。
2. **再补 Tool Cache**：只接收版本化 forecast metadata，实际 Tool 名称、参数、scope、freshness 和 hit/miss 仍由 OpenHands/Reuse 事实覆盖；加入独立字节预算和 eviction。
3. **补事件总线与状态失效**：统一触发 affected-line projection 重算，保证 stale action 丢弃、DAG waiter 释放和 forecast 晚到降级。
4. **补度量与 trace**：先保证所有 Phase 5 指标可由脱敏 trace 重建，再运行性能实验。
5. **最后接真实 vLLM/OpenHands**：先 capability negotiation 和 KV 成本校准，再进行 GPU 端到端 A/B；真实 Tool 执行始终由 OpenHands 所有。

## Phase 5 实验计划表

实验产物目录统一为：

```text
/home/liyachen/workspace/experiments/flowpilot/phase5/<experiment-id>/<run-id>/
  manifest.json  config-redacted.json  results.json  summary.md
  traces/  logs/
```

每次运行记录 FlowPilot 源文件 SHA-256、OpenHands commit/dirty diff hash、模型/引擎/硬件、配置、workload 版本、随机种子、矩阵单元和重复编号。至少 warm-up 后重复 30 次；尾延迟报告 P50/P95/P99、均值和 95% CI。trace 不得包含 prompt、完整 Tool 参数/结果、凭据或授权 header；运行前后 hash 旧 Tool-Reuse SQLite 文件。

| ID | 目标/对应 RQ | 前置条件 | 方法与矩阵 | 必须报告的指标 | 通过门槛/产物 |
|---|---|---|---|---|---|
| E0 | 控制路径正确性（RQ5） | 完成 coordinator、ready queue、event bus；可用 Mock KV 仅做协议测试 | 固定 Tool ready 与 KV restore：`T_need`/`T_KV` 交叉组合；KV GPU/CPU/NVMe/DROPPED；tail replace、dependency finish、cancel、lease expiry、duplicate action | request-2 实际 dispatch time、`T_need/T_KV/T2`、stale discard、action status、queue wait | 所有请求满足 `dispatch >= max(T_need,T_KV)`（unsupported 时只验证无伪造 KV）；无重复动作、无旧 tail 动作；保存 pytest + 脱敏 trace |
| E1 | forecast/Tool Cache 联动（RQ4/RQ5） | Tool Cache metadata prewarm、TTL、容量 accounting | no predictor；predictor valid；timeout/error/cancel/low-confidence/late；Top-N coverage；Tool hit/miss；Tool cache 预算 1/10/100 MiB | forecast overlap、prewarm precision/recall、effective/wasted prewarm cost、lookup latency、hit 后 residual KV stall | 请求 1 不等待 forecast；forecast 不能执行 Tool/改控制流；真实 resolution 覆盖预测；Tool/KV 容量指标分开；结果含 discard reason |
| E2 | restore queue 与 SLO/DAG 策略（RQ5/RQ6） | E0；多 line projection、Job deficit | blocking count 0/1/8；deadline slack 正常/临界/逾期；restore cost 长尾；wait-age、cooldown、DROPPED rematerialize；策略消融：无 DAG、无 SLO、无 age、无 dependency guard | SLO goodput、weighted JCT、deadline miss、restore-laxity miss、overdue wait、fairness、migration jitter | 完整策略在相同 arrival trace 下不得违反公平/尾部保护；每个消融可解释；输出 policy config、原始分布和 CI |
| E3 | 事件驱动与故障降级（RQ5/RQ7） | E0/E1；fault-injection hooks | forecast late、Tool start/finish/fail、KV watermark、instance load、DCS append/ACK、line finish；事件重复/乱序；scheduler restart、网络断连、磁盘满 | recompute latency、affected-line 数、stale action、queue recovery、WAL/trace drop、terminal reason | 无法证明一致性时 fail-closed；不重复/漏应用；trace failure 只降级健康不改变 Tool result；保留失败 run manifest |
| E4 | Mock/OpenHands 端到端闭环（RQ5/RQ7） | E0--E3；OpenHands adapter 与 mock inference | Chat/Responses、sync/async/streaming；history hit、in-flight follower、local Tool barrier、terminal；DCS on/off；`T_need>T_KV` 与 `T_KV>T_need` | provider-visible message hash、Tool Call identity、JCT、TTFT、Prefill、Agent RTT、请求 2 delay、residual stall | 消息序列/identity/最终权威历史零差异；报告 DCS 正负收益区间；不得用 mock KV 宣称 GPU 收益 |
| E5 | 真实 KV connector 校准（RQ5） | 版本化 vLLM KV 扩展；真实 handle/tier/bytes/cost；至少 1 GPU | prompt/context 长度、并发、GPU/CPU/NVMe 压力；KEEP/OFFLOAD/RESTORE/DROP；engine restart/epoch change | bytes、restore/migration/rematerialization cost、I/O 带宽、stall、重算 token、采集开销、FlowPilot/engine 事实偏差 | 字段与引擎计数一致；epoch/restart 后 stale 被拒；缺能力时明确 unsupported；产出 connector 版本与校准报告 |
| E6 | 真实多实例联合调度 A/B（RQ1/RQ5/RQ6） | E5；至少 2 个真实推理实例；E07/E08 脱敏 workload | Baseline：direct、round-robin、独立 Tool LRU+KV、无 predictor、无 hit→restore、无 ready-time alignment；Treatment：完整 SLO-Aware alignment；Tool-heavy/Code-heavy/Mixed Job；容量、I/O、SLO、phase shift 扫描 | SLO-satisfied workflow goodput、JCT P50/P95/P99、deadline miss、per-Job slowdown/Jain、`|T_need-T_KV|`、residual stall、KV/Tool 各自容量/I/O、duplicate Tool/KV cost | 配对重复 + 95% CI；Treatment 只有在正确性门禁通过后比较；不得报告 Tool/KV 跨类型容量交换收益；生成可复现 manifest/results/summary |
| E7 | 生产安全门禁（RQ5/RQ6） | E6；共享状态实现或明确单 worker pilot | worker crash、滚动升级、lease split-brain、旧/新 schema、Tool 复用策略冲突、删除与恢复 | 单 writer/leader、failover stall、硬约束 rejection、隐私扫描、旧 SQLite hash、health/metrics 降级 | 在共享状态完成前只允许单 worker pilot；任何一致性不确定都 terminal/diverged；形成 GO/NO-GO 记录 |

## Baseline 与结论规则

Phase 5 至少比较以下可解释基线：

1. `independent`: Tool Cache 与 KV 各自策略，不交换 ready-time；
2. `no_predictor`: 关闭请求 1 forecast/prewarm；
3. `no_hit_restore`: Tool hit 后不重排 KV restore；
4. `no_ready_alignment`: 不使用 `T_need/T_KV` 对齐；
5. `no_dag_slo`: 仅等待年龄/水位；
6. `full_alignment`: 完整 `T2=max(T_need,T_KV)`、DAG/SLO/age、restore queue 和公平 ready queue；
7. `oracle`：离线知道真实 Tool ready/KV cost 的上界，不作为可部署方案。

结论顺序固定为：先正确性/隔离/故障门禁，再 SLO goodput，再 weighted JCT 与公平性，最后才讨论 raw throughput、命中率或 GPU 利用率。Tool Cache 与 KV Cache 的容量、队列和 I/O 始终分别报告，不能声称共享物理容量收益。

## 当前可执行性

- **现在即可做**：E0--E4 的协议、事件、Mock/离线 trace replay 和隐私/故障测试；它们只能证明控制面正确性与调度算法行为。
- **需要真实引擎**：E5--E6 的 KV 成本、restore stall、GPU/CPU/NVMe I/O 和端到端性能。
- **需要先实现功能**：若 E0/E1/E2/E3 未完成，E6 的“Phase 5 收益”实验无效，只能报告为 blocked。
