# FlowPilot 基于当前代码的成本调度重构方案

本方案指导将现有 SLO 调度实现改为 `W−K` admission 与 `H=G+Q_hat` 首次 KV 驻留选择。重构集中在 FlowPilot 的队列、时间统计、retention 输入和配置消费端，复用现有 prefix 查询、离线成本模型、credit 与引擎回执机制。

代码核对日期为 2026-10-10，FlowPilot HEAD 为 `85d7d9fb473c210c8cf05b64f48e0a08f8c1dcae`。以下“现状”来自该工作区源码；“目标”和代码示意是待实施改动。本文交付重构方案，不表示运行代码已迁移。设计依据为 [design.md](../design.md) §§5、7.8、9、16、[最简成本方案](simple_cost_model_for_flowpilot.md) 与 [框架表述](flowpilot_paper_framing_without_slo.md)。

## 1 当前实现中必须一起改动的地方

优先级表示偏离新设计的影响，不把旧策略在其原契约下的行为误报为生产故障。

| 优先级 | 当前源码与确认事实 | 重构不完整时的可观察结果 | 验收回归 |
| --- | --- | --- | --- |
| P1 | [admission.py](../flowpilot/scheduling/admission.py) 的 `_order()` 仍按 `deadline-cost`，或 `weighted` 分数排序；`_dispatch_ready()` 还执行过期分流与 best-effort 额度 | 仅改变 deadline 就能改变顺序或阻止派发，违反唯一 `W−K` 排序 | 固定 W/K 与候选集合，改变 deadline、weight、blocking lines，不改变派发顺序和额度 |
| P1 | 同文件 `acquire()` 用 `monotonic()-(UTC_now-arrived_at)` 设置 `_Waiting.entered`；[runtime.py](../flowpilot/scheduling/runtime.py) 先 tokenize，再传 `call.gateway_received_at` | tokenize、网关前处理被算进 W，两个实际同刻入队的请求可能因前处理时间不同而获得不同优先级 | 人为延长前处理，实际入队后 W 仍从零开始；墙钟变化不影响 W |
| P1 | `engine_load()` 的 `demand` 使用 `_has_live_waiter()`，该函数只识别未过期请求 | 删除排序中的 deadline 后，自适应 credit 仍可能区别对待无 deadline 的负载 | 相同真实 waiting/inflight 和引擎负载，deadline 不影响额度调整 |
| P1 | [retention.py](../flowpilot/scheduling/retention.py) 的 `_refresh_source()` 传入 `projection.deadline_slack_ms`；`_cost_retention()` 首先比较预算超支 | 仅换请求排序，KV 去留仍受 SLO 控制 | 相同 prefix、G/Q、容量及标定，改变 deadline 不改变动作 |
| P1 | `choose_retention()` 将 `phase==READY` 直接变为 `gap=0`，并把 READY 视为近端需求；没有队列反馈接口 | Tool 已命中、队列很忙时，H 仍被当成零 | G=0、Q>0 时 H>0，并按该窗口比较动作 |
| P2 | `_projection()`、`_order()`、`snapshot()` 都包含旧排序逻辑；整轮 FIFO 目前没有批次状态 | 派发走 FIFO，snapshot 却展示成本顺序；未知项被派发后同一轮又切回成本排序 | 单轮多 credit、混合已知/未知、取消与新到请求的顺序/观测一致 |
| P2 | [protocol.py](../flowpilot/protocol.py) 的 `SchedulingProjection` 与 forecast、resolution 共用 `PHASE4_PROTOCOL_VERSION`；[app.py](../flowpilot/app.py) 仍公开旧投影参数 | 直接修改公共版本常量会连带破坏预测器和 Tool resolution 协议 | readiness 投影独立迁移，现有 forecast/resolution envelope 继续通过 |
| P2 | 9B/27B profile、SDK 集成测试和真实 admission probe 消费旧 policy/字段 | 核心队列更新后，实验入口启动失败，或继续用“紧迫请求先行”断言验收新算法 | 真实配置加载、SDK 路径与 probe 共同迁移 |

这些问题不能通过只改 `_order()` 或把 `remaining_slo_seconds` 传成 `None` 完成迁移。后者会留下旧配置、旧观测语义以及 READY 清零窗口的问题。

## 2 保留的结构与目标数据流

保持一条外部 admission queue；不用 `gateway/router.py` 的 `WeightedFairRequestQueue` 替换它。`Settings` 已限定 scheduling 开启时恰好一个实例。旧 router 的实例评分不等于当前 admission 顺序，其测试也不能代替本次验收。

```text
LLMGateway.proxy
  -> SchedulingRuntime.admit
     -> tokenize / prepare request facts
     -> AdmissionQueue.acquire: record actual queue entry
     -> TargetPrefixQueries.refresh: full candidate snapshot, outside queue lock
     -> estimate_work: conditional K
     -> choose sweep order: W-K or whole-sweep FIFO
     -> reserve credit + record one admission wait, under queue lock
  -> revalidate current tail
  -> ordinary inference HTTP request
  -> GatewayCall terminal -> release credit exactly once

completed response + factual Tool resolution + descriptor
  -> RetentionController first decision
     <- read-only queue wait estimate from AdmissionQueue
     -> G + Q_hat = H
     -> legal KEEP / OFFLOAD / DROP cost comparison
     -> freeze inputs and selected action
     -> existing apply / receipt / retry / expiry lifecycle
```

OpenHands 继续执行真实 Tool、维护权威历史；vLLM 继续完成实际 prefix acquire、恢复/重算与内部调度。此轮不改 Tool Cache eviction，不新增 RESTORE RPC、恢复队列、GPU-ready 门槛或第二条请求队列。

## 3 AdmissionQueue 的具体重构

### 3.1 精简策略输入与配置

在 `admission.py` 中按下表处理，保持 credit 锁和异步 future 的生命周期。

| 对象或函数 | 操作 |
| --- | --- |
| `AdmissionConfig.policy` | 改为 `Literal["wait_cost", "fifo"]`，目标默认 `wait_cost`；`enabled=False` 默认保持。`fifo` 是显式对照策略 |
| `PriorityWeights`、`priority_score()` | 删除；不能保留为新策略的平分项或 snapshot 中的有效 score |
| `BestEffortAdmissionConfig`、`_inflight_live()`、`_has_live_waiter()`、`_remaining_slo_seconds()`、`_update_quiet()`、`_best_effort_limit()` | 删除及清理所有调用；保留一般 credit 和 `CapacityFeedback` |
| `age_reference_seconds`、`work_reference_tokens`、`weights`、`best_effort` | 从有效配置移除；输入旧字段时给出明确迁移错误，不能忽略或静默映射 |
| `RequestPriority` | 暂保留类名，字段缩为关联 key/job、prompt tokens 和 `RequestWork`；移除用于排序的 workflow start/deadline/blocking lines 与 `cp_seconds` |
| `priority_from_snapshot()` | 退出生产路径；Runtime 直接构造精简请求，不再从 frontier metadata 派生调度权重 |
| `_Waiting.entered`、`sequence`、`future` | 保留；`entered` 在持有 queue lock、真正插入 waiting 时赋当前 monotonic 值 |
| `_InFlight` | 保留 credit 所需关联信息，移除 `deadline_monotonic`；不要因去掉 deadline 改变 key 的唯一性 |

`W_ms=(now_monotonic-entered)*1000`，`K_ms=work.cost_seconds*1000`。内部成本模型继续使用秒，展示和公式统一为毫秒，不重写既有标定 JSON。网关收包时间可继续用于独立的端到端 trace，不能再回填 `_Waiting.entered`。

保留 `_prompt_tokens()` 的当前位置和输入支持范围，先修正队列计时；把 tokenize 搬入队列或取消与 target query 的重复 tokenization 属于独立优化，不与本轮策略迁移混做。

### 3.2 把排序从单项函数改为批次选择

`_order(projection)` 无法独立决定“任一成本未知则整轮 FIFO”。新增队列内部的批次选择 helper，接收一个候选快照和统一的 `now`，返回本轮顺序、`ordering_basis` 及成本缺失原因；派发与 snapshot 复用同一套规则。

```python
# 逻辑示意；输入是本轮仍然存活的 waiting entries。
candidates = live_entries_from_refreshed_snapshot()
invalidate_expired_target_costs(candidates, now)
missing = [entry for entry in candidates if entry.request.work_cost is None]

if configured_policy == "fifo":
    basis = "fifo:configured"
elif missing:
    basis = "fifo:cost_unknown"
else:
    basis = "wait_cost"

if basis == "wait_cost":
    ordered = sorted(candidates, key=lambda e: (e.entered + e.cost_seconds, e.sequence))
else:
    ordered = sorted(candidates, key=lambda e: e.sequence)
```

示意中的 `work_cost/cost_seconds` 是从 `RequestWork` 取值的局部量，不要求给 request 增加第二份成本 owner。实际实现要区分 `None` 与合法的零成本，不能使用 `cost or 0`。

必须同时处理以下批次语义：

1. `_refresh_and_dispatch()` 继续在锁内复制 waiting，在锁外执行 RPC，再在锁内按 key 与原 `_Waiting` 对象身份更新。已取消/被替换的项不恢复。
2. 新到项不进入已在执行的 sweep；不能让其未知成本污染上一轮候选，也不能偷用上一轮结果派发。下一轮纳入。
3. 已取消项在确定本轮模式前剔除。如果剩余候选中仍有任意 K 未知，本轮统一 FIFO。未知项先被派发后，本轮余下名额继续 FIFO，不能切回 `wait_cost`。
4. 成本失效发生在尚未派发项上时，先做有效 cold/unknown 处理，再决定剩余派发顺序；已经消费的 credit 不撤销。一次锁内原子选择使用统一时间快照，不在该临界区发 RPC。
5. 保留 full-sweep 异常的显式失败语义。正常能力缺失/查询失败已由 `TargetPrefixQueries` 表达为 cold/unknown；缺少结果 key、内部异常不能被宽泛捕获后伪装成合法 FIFO。
6. 无论哪种排序，都须重新检查健康、最新 `effective_limit` 和取消状态。派发后不抢占。

`_projection()` 只计算 W、K、score 和来源。`score_ms` 在 K 未知时为 `None`；配置 FIFO 或整轮 FIFO 时，已知项的 score 可以作为诊断保留，但必须同时显示该分数本轮不决定顺序。

`snapshot()` 不再独立调用旧 `_order()`。等待队列的展示顺序是读取时的候选视图；记录实际派发的 sweep 标识、候选范围与 `ordering_basis`，不要把展示顺序当作下一次派发承诺。snapshot 不触发采样、query 或 credit 消费。

### 3.3 修正两个附带的 SLO 依赖

`engine_load()` 的 `demand` 改为“存在真实 waiting，或 inflight 已占满当前额度”，使用剔除已取消项后的状态。保留 [capacity.py](../flowpilot/scheduling/capacity.py) 的实测引擎指标、滞回、上下界及失效处理；不改为 decode ETA。实验比较 `W−K` 时先固定 credit。

`refresh_dependencies()` 不再更新 blocking-line 加分。可收敛为队列的 `notify_state_changed()`，由 `SchedulingRuntime.dependencies_changed()` 触发已有刷新/派发；Runtime 获取 credit 后、发送 HTTP 前对 tail/version/dependencies 的事实校验继续保留。不要把“不再为阻塞线路加权”误改成“忽略依赖”。

`queue_work_before_tokens` 当前依赖旧 `_order()` 筛选“前置”项。新策略会在后续 sweep 改序，入队时不能再宣称预测前方工作量。改为明确的 `queued_prompt_tokens_at_entry` 与完整性标记，计算插入前快照中已知 token 总量，或移除无消费者的旧字段；不把它拿来估计 Q。

## 4 保留真实 prefix 和成本估计路径

[prefix.py](../flowpilot/scheduling/prefix.py) 的 `TargetPrefixQueries.refresh/_query/_cold` 与 [cost.py](../flowpilot/scheduling/cost.py) 的 `estimate_work()` 已提供本轮需要的主要能力：真实 target 输入、GPU/CPU 范围、CPU bytes、标定 identity、条件 CPU 成本及 cold/unknown 原因。

保留 `K=min(K_gpu,K_cpu)` 的现有条件候选口径；没有可评估 CPU 候选时使用有效的 GPU/recompute 成本。它不是引擎必须采用该恢复方案的指令。GPU 命中仍使用标定残余 prefill；保留 N-1、hybrid checkpoint 与 backend 范围校验。

本轮必要修改限于：精简 request 类型后的参数适配、陈旧结果的统一失效处理，以及向 admission 输出足够的原因/来源。现有 `_dispatch_ready()` 在 TTL 过期时创建成本 unknown 的 cold `RequestWork`，第一版可直接令本轮 FIFO；若复用 `_cold()` 重新估计，仍须具备真实 P 和匹配引擎 identity，不能仅凭配置文件存在就认定 cold 成本有效。

当前 target 路径校验 query_id、epoch/layout，保存 `state_version` 和 observation 起始时间；不能把水位数值描述成驻留保证或完整 KV inventory。现有全量 sweep 是主要刷新方式；引擎 watermark 若没有可关联的失效事实，不凭空实现“精确失效通知”。

9B/27B 的分桶、分段、D2H/H2D 标定及 uncertainty 不因排序符号变化重新拟合。trace 通过 calibration source/version 关联原标定，不能把点估计记作引擎实测值。

## 5 新增实测 admission wait 反馈

### 5.1 数据结构与采样点

建议新增小型模块 `flowpilot/scheduling/wait_feedback.py`，只放滑动统计、配置和不可变快照类型。它不导入 `AdmissionQueue`、Runtime 或 RetentionController，避免当前 `prefix -> admission` 等依赖链进一步成环。

| 对象 | 所有者与字段 |
| --- | --- |
| `AdmissionWaitFeedback` | 由唯一 AdmissionQueue 持有；记录 `(dispatch_monotonic, wait_ms)`，维护近期均值 |
| `QueueWaitEstimate` | 不可变快照：`estimate_ms`、`source`、`sample_count`、`window_seconds`、`observed_at_monotonic`、最近真实样本时间 |
| 窗口配置 | 明确为时间滑动窗口；实现时提供可调 `window_seconds`，窗口外样本失效。实验可从显式 30 秒配置起步，该值是待验证选择，不是现有默认或生产推荐 |

采样点放在 `_dispatch_ready()` 内：实际从 waiting 移入 inflight、交付获准结果时，在同一 queue lock 下记录一次 `now-entered`。这定义的是**获得外部 admission credit 的等待**，不是 HTTP 首字节或引擎内部等待。

排队期间取消的项没有样本；已获得 credit 后取消的项已发生 admission，保留这一次样本并另记终态。重复 release、snapshot、heartbeat 和轮询不增加样本。采样不用单独后台任务，不等待请求完成；pending/取消等待单独观察，不能当已完成 wait 填入均值。

读取时，只有 queue 为空、健康有效且有 free credit，才返回 `source=idle_capacity` 的零快照；它不插入统计样本。否则返回有效窗口内均值；无有效样本为 `estimate_ms=None`，来源区分 no_samples/expired。均值是粗略负载反馈，未派发的长等待和长 decode 都可能使它滞后。

### 5.2 接线与锁顺序

目前 [app.py](../flowpilot/app.py) 先构造 `RetentionController` 作为参数，再由 `SchedulingRuntime.__init__()` 创建 queue。最小改动是给 retention 增加 `set_queue_wait_provider()`，Runtime 在 queue 创建后、`start()` 前绑定其只读异步 getter。

```text
AdmissionQueue.queue_wait_estimate() -> QueueWaitEstimate
SchedulingRuntime.__init__()        -> bind getter to retention
RetentionController._refresh_source()
  -> only when source.decision is None and response inputs are ready
  -> read estimate, revalidate source/tail, choose once
```

getter 只在 queue lock 下复制统计快照，不执行 RPC、不回调 retention。当前 retention 刷新持有自身锁，允许其读取短时 queue 快照，但 queue 持锁代码不能再反向等待 retention；无需引入新的跨组件锁。

admission 关闭时 provider 返回明确的 `admission_disabled/unknown`，不声称测得零排队。retention-only、去掉 Q 的消融使用显式 `window_basis=tool_only`，使 H=G，并记录这是实验模式。组合策略的默认目标为 `window_basis=tool_and_queue`。未知 Q 走 design 已规定的 retention unknown 路径，不阻塞普通回复或等待未来样本。

## 6 Retention 的具体重构

### 6.1 首次选择只读取一次新输入

保留 `finished()`、`_collect_response_inputs()`、descriptor resolve、`_source_live()` 与输入就绪逻辑。在 `_refresh_source()` 的 `source.decision is None` 分支中完成以下步骤：

1. 从事实 readiness 和串行 Tool 估计得到 G。历史 hit 为零，follower 等真实 leader，未知本地执行或未满足依赖仍 unknown。
2. 读取当前 queue estimate；组合模式下任一必要项未知则 H unknown，否则按同一单位相加。`tool_only` 消融单独记录 H=G。
3. 读取 feedback 的 await 之后再次执行 `_source_live()` 与 `_current()` 校验；不能对已替换 tail 下发首次动作。
4. 调用纯决策函数，冻结动作以及 G、Q 快照、H、容量/pressure、标定 source/version、候选成本与排除原因。

建议将 `choose_retention()` 的 `need_in_seconds` 明确改名为 `retention_window_seconds`，删除 `remaining_slo_seconds`。`phase` 仅保留 TERMINAL 清理用途，移除 `phase==READY` 的零窗口与 near 捷径。`_refresh_source()` 中 `projection.ready -> G=0` 可以保留其事实语义，但不能将 G 当成已经包含队列的 H。

### 6.2 删除预算比较，保留真实动作约束

改写 `_cost_retention()`，使用下列同单位成本：

```text
J_KEEP    = residual_prefill_gpu + effective_gpu_price * gpu_GiB * H_seconds
J_OFFLOAD = new_D2H + H2D + residual_prefill_cpu + cpu_price * cpu_GiB * H_seconds
J_DROP    = cold_prefill
action    = minimum J among legal and evaluable candidates
```

删除 `budget=remaining-gap` 和 tuple 第一项的超支比较。把旧 `gap` 用作驻留时间和 D2H 窗口的位置全部改为 H，不能只改函数名。保留 existing pressure 倍数、真实 bytes、引擎能力、safe DROP、CPU backend 与引擎自主 reuse 的要求。

完整 `cpu_standalone_tokens` 已覆盖 offload target 时 `new_D2H=0`，无需新增写回标定；部分覆盖或 OFFLOAD 回执不等于完整 CPU 就绪。`new_D2H<=H` 只用于条件候选判断，不成为后继请求的恢复屏障。成本相同采用稳定顺序即可，第一版可沿用当前候选构建顺序 DROP、KEEP、OFFLOAD，不引入新的隐藏权重。

成本函数返回 `RetentionDecision` 时附带候选成本/排除原因，或附带独立不可变评估对象，避免 controller 为 trace 再算一遍。纯成本无法完整评估时，保留明确的 unknown 返回与既有能力/容量处理；近端 KEEP 只能由已知 H 判定，不能靠 READY 填零。终止或确认不可恢复 prefix 的清理优先级不变。

### 6.3 不重写已经工作的执行生命周期

`_Source` 扩展首次决策快照，`tool_gap_seconds` 继续指 G，另增 Q/H，不能把原字段悄悄改义。

以下分支继续使用已有机制：

- `source.decision` 已存在：后续 Tool/队列/pressure 不重选，包括首次选出 unsupported 的情况。
- lost apply response：重试同一 `pending_command` 和幂等 ID。
- OFFLOAD 的 FAILED/PARTIAL：延时、重查、用新 action/policy version 重试已选 OFFLOAD，不重新比较 J。
- line finish：执行需求释放 DROP；与首次 placement 分开记录。
- tail 替换、epoch 变化、metadata 到期：失效、清理引用，晚回执不能复活 source。

Q 的更新不调用 `line_changed()`，不把所有 source 标脏，也不触发全量 descriptor 查询。它只被后续尚未选择的 source 读取。

## 7 Readiness 投影与观测契约

[projection.py](../flowpilot/scheduling/projection.py) 保留 `for_line()` 中 resolution 读取、串行时长累计、真实依赖使 T_need unknown、tail version 校验；删除策略用的 `slo_urgency()`、`dag_importance()`、`_downstream_depth()` 和 weight/slack 派生。

`SchedulingProjection` 收敛为身份、tail version、ready、T_need、事实阻塞诊断和 computed_at。现有 `estimated_inference_ms`、critical-path、SLO/importance 等字段不得通过填零或默认一继续伪装有效计算。确有离线消费需求的字段单独保留为兼容元数据，不被 admission/retention 读取。

迁移投影响应时使用**投影专属版本**，例如新增 `READINESS_PROJECTION_VERSION`；不要全局修改 `PHASE4_PROTOCOL_VERSION`，因为它还被 `ForecastRequest/Result`、`ToolResolutionRecord` 和 forecast validator 使用。同步 `scheduling/__init__.py` 导出及 app 投影入口。旧 `estimated_inference_ms/downstream_depth` 查询参数应明确标为废弃；若选择拒绝，需显式检测并返回迁移错误，不能以为删除 FastAPI 形参就会自动拒绝未知参数。

Job/Line 注册中的 deadline、default_slo、weight 第一轮可以保留 wire 兼容，角色为外部元数据。forecast 的 deadline 字段不在本次共享版本变更范围；预测仍不能据此影响新排序。离线 `metrics.py` 的 SLO helper 不构成调度控制，不必为了删掉字符串而破坏已有评估脚本。

| 观测出口 | 目标字段与语义 |
| --- | --- |
| `request_admitted` / queue state | `policy`、`ordering_basis`、`queue_wait_ms`、`kv_start_cost_ms`、`score_ms`、sequence、sweep 范围、unknown 原因；保留 prefix basis、标定来源、credit |
| queue wait feedback | 均值或 unknown、样本数、window、观测时间、最近样本时间、idle/measured/disabled 原因 |
| `kv_retention_decision` | G/Q/H、window basis、选定动作、每个候选 J 或不可评估原因、容量/pressure、标定 source/version |
| `kv_policy_receipt` | 保持现有 command/receipt 身份与真实状态，不能与选择事件合并 |

不继续发布 `prefill_slack_seconds`、`latest_prefill_start`、`slo_status`、旧 contributions 和 best-effort 状态作为当前策略结果。必要的兼容读取放在报告工具中，通过事件字段/版本识别；不能将旧 `score` 数值改成毫秒却保留相同含义说明。

## 8 配置和消费端的迁移清单

| 文件或入口 | 必须同步的改动 |
| --- | --- |
| [config.py](../flowpilot/config.py) 的 `Settings.from_env()` | 保持 `FLOWPILOT_ADMISSION_JSON` 和成本文件加载；新模型校验旧 policy/字段时给出迁移说明 |
| [9B config](../examples/experiments/qwen35_9b_tp4/config.json)、[27B config](../examples/experiments/qwen35_27b_tp4/config.json) | `prefill_slack -> wait_cost`，删除 weights；显式记录窗口与 retention window basis。保留现有标定、timeout、credit 配置 |
| [9B profile loader](../examples/experiments/qwen35_9b_tp4/profile.py)、[27B launch](../examples/experiments/qwen35_27b_tp4/launch.py) | 核查共同配置加载链与 override；不能假设 27B 有独立 `profile.py`，或改策略时丢掉 shared cost model |
| [SDK 集成矩阵](../integration/test_openhands_reuse.py) | 删除 `BestEffortAdmissionConfig` import，更新 policy Literal；`capacity_controls` 当前将 best-effort 与 adaptive 绑在一起，应拆出独立 adaptive 验收 |
| [real_admission_probe.py](../integration/real_admission_probe.py) | 删除 high/low urgency 顺序断言；改为记录实测 W/K 与实际顺序，成本缺失时验收整轮 FIFO；不能凭请求标签决定预期 |
| [verify_slo_prefix.py](../integration/verify_slo_prefix.py) | 审核实际验证对象后更新名称/说明；其中 `deadline=monotonic()+timeout` 是 RPC 轮询期限，不是 workflow SLO，应保留 |
| [调度文档](scheduling.md)、[验证入口](verification.md)、9B/27B README | 更新有效配置、trace 字段和新验收结果；历史报告保留原策略标签 |
| `gateway/router.py`、`profile.py` 中旧诊断模型 | 首轮不扩展为多实例/重型请求建模；确认不在新策略路径，不以其旧测试证明本轮完成 |

旧 policy 值 `prefill_slack/slo_unexpired_first/weighted` 以及已移除的 weights/best_effort 等必须明确报配置迁移错误。用户主动选择 `fifo` 与运行时 `fifo:cost_unknown` 是两种不同状态。示例 profile、测试参数和 core config 在同一可用提交中更新，不能让正常实验入口依赖已经删除的类。

## 9 测试按行为迁移

| 现有测试文件 | 替换或新增 | 必须保留 |
| --- | --- | --- |
| [test_admission.py](../tests/test_admission.py) | 旧过期/slack/weighted 用例改成 W−K、同分 FIFO、C/A/B、deadline 不变性、实际入队时钟、整轮 FIFO；`test_full_sweep_reorders_all_waiters_after_cache_loss` 的优先方向随新公式改写 | 取消中 query、credit 恰好归还、heartbeat 失效、SSE 完整生命周期、断连回滚、单实例限制 |
| [test_admission_capacity.py](../tests/test_admission_capacity.py) | 删除 best-effort/quiet ramp 专用断言；新增无 deadline waiting 也构成 demand | 实测 metrics、滞回、指标失效、额度降低不抢占、query 期间额度变化重新检查 |
| 新的 `test_wait_feedback.py` | 可控单调时钟下均值、窗口失效、idle 零不入样、unknown、派发一次采样、排队取消无样本、预留后取消不重复采样 | 不通过 sleep 或预测 decode 构造虚假的实测等待 |
| [test_prefix_cost.py](../tests/test_prefix_cost.py) | 新 request/config 适配；TTL 过期后的 unknown 与整轮 FIFO 联动 | identity/epoch 校验、真实 bytes、条件 CPU 成本、标定范围、串行 Tool 剩余时间 |
| [test_retention.py](../tests/test_retention.py) | `test_calibrated_retention_compares_gap_capacity_and_remaining_slo` 改为 H/容量/J 比较；READY+Q>0；unknown G/Q；纯成本选动作 | CPU 完整覆盖、receipt 不证明就绪、capabilities 独立、幂等命令、OFFLOAD 重试、TTL/epoch/终止清理、CPU-only 普通提交 |
| [test_response_retention.py](../tests/test_response_retention.py) | 多种完成顺序下冻结 G/Q/H；首选后改 Q/pressure 不重选；hit 的 G=0 与 H>0 分开断言 | 预测不阻塞回复/credit、事实完成覆盖预测、SDK resolve 顺序、串行多 Tool、预测失败 |
| [test_phase4.py](../tests/test_phase4.py)、[test_protocol.py](../tests/test_protocol.py) | 把 SLO/DAG weight 投影用例改为 readiness 与独立版本；旧投影字段迁移 | forecast 版本/非阻塞/迟到丢弃、resolution 事实覆盖、tail version guard |
| [test_qwen27b_costs.py](../tests/test_qwen27b_costs.py) 与 9B profile 测试 | policy、ordering_basis、retention 新签名及字段更新 | shared calibration、未知成本分量、真实 bytes 范围、CPU-only 进入普通推理 |
| [integration/test_openhands_reuse.py](../integration/test_openhands_reuse.py) | 用 wait_cost/fifo 与新状态断言替换旧 SLO 策略专用用例 | OpenHands 真实 SDK 路径、history/in-flight、DCS、流式 Tool 边界、真实本地 Tool fixture |

批次回归至少构造：两个 credit，第一项 K unknown；第二、三项的 FIFO 与成本顺序相反。预期同一轮两个获准项严格 FIFO，后续新 sweep 才可恢复成本排序。再分别覆盖 query 期间取消未知项、新到未知项、观察过期及 `effective_limit` 降低。

不要只断言字段或函数名变化。需要检查实际获准先后、派发数量、source 冻结、命令次数以及最后的 `inflight/free`。将测试 clock 注入新统计组件或使用现有 fake clock；不要靠放大 wall-clock sleep 稳定竞争测试。

## 10 可独立验收的提交顺序

| 提交 | 范围 | 完成条件 |
| --- | --- | --- |
| A 反馈统计基础 | 新统计模块、快照类型与纯统计测试，尚不改变运行策略 | 均值/窗口/unknown/idle 语义确定；不会导入 Runtime 或 retention 造成环 |
| B 请求排序和配置原子迁移 | admission、runtime、prefix 类型适配、capacity demand、profiles、相关测试/import 与 probe 旧配置消费 | 新策略可运行，旧配置明确拒绝；W−K/整轮 FIFO/credit 全部定向通过；保留 K 模型 |
| C 驻留反馈闭环 | Runtime provider 接线、retention G/Q/H 与 J、冻结 trace、相关 controller/response 用例 | hit+busy、unknown、首选冻结、异步回执/重试/到期通过；无 SLO 入参进入决策 |
| D 投影和说明收敛 | readiness DTO 独立版本、projection helpers、app 参数、导出、文档及消费端 | forecast/resolution 协议未被连带改坏；无旧投影值回流策略；字段意义清晰 |
| E 集成与真实证据 | SDK 矩阵、真实 vLLM 后继请求、FIFO/成本/反馈消融 | 功能正确性与收益分别报告；失败/取消和任务质量共同统计 |

提交 B 移除旧类时同步调整所有 import 和配置 fixture；不能把明显会导致整仓导入失败的清理拖到 D。提交 D 清理投影前，C 已停止读取 deadline_slack；保留合法跨阶段的事实 readiness 与公共 envelope。

配置也随字段落地分批迁移：B 更新 admission policy/weights 与统计配置；C 在 `RetentionConfig` 接受 `window_basis` 时再更新 profile 的对应字段。B 仍可能运行旧 retention，只是可验证的过渡提交，不能当作完整无 SLO 系统部署或用于最终收益实验。

每个提交先跑受影响的定向测试；改动完成后统一做以下检查。命令是实施后的验收入口，本次方案编写没有执行它们：

```bash
.venv/bin/python -m pytest -q tests/test_admission.py tests/test_admission_capacity.py tests/test_prefix_cost.py tests/test_retention.py tests/test_response_retention.py tests/test_phase4.py tests/test_protocol.py tests/test_qwen27b_costs.py
.venv/bin/python -m pytest -q tests/test_wait_feedback.py
.venv/bin/python -m ruff check flowpilot integration tests examples/experiments
.venv/bin/python -m pyright flowpilot
git diff --check
```

`test_wait_feedback.py` 是提交 A 新增的目标文件。FlowPilot 全套测试在最终配置/协议收敛后执行；SDK 集成需可导入 SDK/Tools 的环境，从仓库根运行：

```bash
PYTHONPATH=/home/liyachen/workspace/flowpilot /home/liyachen/openhands/software-agent-sdk/.venv/bin/python -m pytest -q integration/test_openhands_reuse.py
```

真实 vLLM 验收首先证明明确标识的 Tool 后继请求以 CPU-only prefix 普通提交、由引擎自主恢复/重算，credit 覆盖完整调用；然后在固定质量、资源、负载与 credit 下比较 FIFO、仅 W−K、W−K 加 G+Q 反馈。报告 mean/P95 workflow JCT、成功完成 workflow/s、失败/取消率、K 误差/覆盖率、整轮 FIFO 比例和真实恢复/重算量。长 decode 主导时收益有限也是有效结果。

完成标准是新控制链路不再读取 SLO/importance、全套入口能加载新配置、关键生命周期回归通过，且报告区分实现完成、本地验证和端到端证据。没有 GPU 实验时，可以交付完成的重构与 mock-compatible 验证，但不能宣称真实恢复收益或生产效果。
