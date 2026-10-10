# 请求调度

核对日期：2026-10-10。调用链为 LLMGateway -> SchedulingRuntime -> AdmissionQueue。
规范见 [design.md](../design.md) §§5–7；本页记录成本策略的当前实现。

## 启用与迁移

```bash
export FLOWPILOT_ADMISSION_JSON='{"enabled":true,"limit":8,"policy":"wait_cost","wait_feedback":{"window_seconds":30}}'
export FLOWPILOT_RETENTION_JSON='{"enabled":true,"owner_scope":"flowpilot-local","window_basis":"tool_and_queue"}'
export FLOWPILOT_COST_MODEL_PATH=/absolute/path/cost-model.json
```

admission 和 retention 默认关闭，独立启用，只支持一个固定实例。
准入策略只有 `wait_cost`（启用后的默认值）和显式对照 `fifo`。
旧 `prefill_slack/slo_unexpired_first/weighted` 以及 `weights/best_effort/age_reference_seconds/work_reference_tokens`
会产生配置迁移错误，不再静默解释为其他策略。Job/Line 的 deadline、weight 仍为兼容元数据。

## 等待与启动成本

```text
a = actual queue entry, monotonic seconds
W_ms = (now - a) * 1000
K_gpu = calibrated_prefill(P, H_gpu)
K_cpu = calibrated_H2D(actual_object_bytes) + calibrated_prefill(P, H_all)
K_ms = minimum evaluable startup cost * 1000
score_ms = W_ms - K_ms                  # descending
QueueKey = (a + K_ms / 1000, sequence)  # ascending; equal keys use FIFO
```

W 不包含 tokenize、网关预处理、过去 Tool 时间或 workflow age。两项都以毫秒展示；
K 不包含 decode 或引擎内部排队，不承诺后继 workflow 长度。
没有 CPU 候选成本时使用有效 GPU/cold 成本；未知成本保持 null，不用 tokens 代替秒。
SLO、importance、Job 在途数与 blocking lines 不参与排序或额度划分。

每轮先固定候选快照，在锁外刷新全部目标 prefix，再在锁内原子选择并预留 credit。
任一存活候选仍无有效 K 时，该轮全部 credit 按 FIFO 派发，记录
`ordering_basis=fifo:cost_unknown` 及缺失原因。未知项离队不会使同轮剩余名额切回成本排序。
全部成本有效的后续 sweep 恢复 `wait_cost`；主动选择 FIFO 则记录 `fifo:configured`。
取消项不会复活，查询期间的新请求留待下一轮；缺少结果 key 或内部 sweep 异常显式失败。

插入、heartbeat、release 和依赖变化合并触发 full sweep，只查询 waiting 请求。
`/v1/kv/query-target` 使用真实 Chat/Responses 渲染和原生 lookup 提供 P、H_gpu、H_all
及 CPU 对象 bytes。epoch/query identity 不匹配或查询失败走显式 cold/unknown；
过期观察在选择和 snapshot 中都失效，默认 prefix TTL 为 2 秒。观察不是 pin 或租约。
vLLM 在普通推理入站时重新 lookup/acquire，并自主决定恢复或重算。

健康探测默认每秒一次、TTL=5 秒、timeout=1 秒；失效只停止新派发。
一个锁保护 waiting/inflight；终态、取消和发送失败各自恰好归还一次 credit。
credit 覆盖整个调用，包括 CPU 恢复、计算和 SSE；没有 RESTORE RPC 或 GPU-ready 屏障。

`request_admitted` 记录 W/K/score、sequence、sweep_id、candidate_sequences、ordering_basis、
cost_unknown_reasons、prefix/cost 来源及 effective_limit。state 的 queued 是读取时视图，
last_sweep 保存最近一次选择范围与实际派发序号，不承诺下一次选择顺序。
`queued_prompt_tokens_at_entry` 是入队前队列的已知 token 总量，配有完整性标志，不作为 Q 的估计。
旧 slack、SLO status、contributions 和 best-effort 字段已移除。

## 实测排队反馈

AdmissionQueue 在 waiting 移入 inflight 时只记录一次 `dispatch-entry`。
`wait_feedback.window_seconds` 控制时间滑动窗口；本次实现初值及实验配置为 30 秒，
这是待按负载标定的实验选择，不是生产推荐。窗口外样本失效，无后台采样任务。
轮询、heartbeat、重复 release 不增加样本；排队取消只计 cancelled_wait_count/ms，
获准后取消保留已经完成的一次 admission wait。

反馈快照版本为 `admission-wait-v1`，含 estimate_ms、source、sample_count、window_seconds、
observed_at_monotonic、last_sample_at_monotonic。空队列且健康、有 free credit 时返回
`idle_capacity` 零快照，但不插入零样本；否则使用 `measured` 均值，无样本为
`no_samples/expired` unknown。关闭 admission 为 `admission_disabled` unknown。
Q 是实例负载反馈，未派发长等待、负载突变和长 decode 会使它滞后，不是单请求 ETA。

## 可选自适应额度

```json
{
  "enabled": true,
  "policy": "wait_cost",
  "limit": 32,
  "wait_feedback": {"window_seconds": 30},
  "adaptive": {"enabled": true, "initial_limit": 24, "min_limit": 16}
}
```

独立控制器仍使用 `/metrics` 的真实 running、waiting、抢占和完成计数，保留滞回与上下界。
存在真实 waiting 或 inflight 已占满额度即构成 demand，无需 deadline。
默认每 10 秒采样、30 秒窗口；减额低于在途数时等待正常终态，不抢占。
指标缺失/陈旧显式报告并保持最近额度，不当作零负载；健康 TTL 独立门控派发。
比较排序策略时应固定 credit，避免将容量反馈收益归给 W−K。

## 冻结的七特征 cadence 成本

当前分发的唯一成本配置为
[27B 七特征参数](../examples/experiments/qwen35_27b_tp4/cost-model.json)，版本
`seven-feature-cadence-frozen-d2h-20261010`，对应 Qwen3.5-27B / TP4 / seq256 /
2048 token budget / 784-token Mamba align。27B 专用入口默认加载；
9B 原标定配置已移除，未提供兼容参数时保持成本 unknown。
通用入口仍可设置 `FLOWPILOT_COST_MODEL_PATH`。

[OfflineCostModel](../flowpilot/scheduling/cost.py) 的 `prefill_cadence` 与旧
`prefill` 分桶格式互斥。保留旧格式读取及离线拟合工具用于外部历史证据，不分发旧参数。
七特征使用已经反缩放的冻结系数：

```text
T = theta · [1, N_pre, B_dec, sum(q²), C, sum(q*h), N_pre*(C+sum(h))]
```

vLLM 的 target/descriptor query 只读返回 `prefill_load`，由引擎负责 B_dec/C、
活跃 prefill 数、配置、身份与观察时间。FlowPilot 使用 `candidate_prefill_frozen_decode`
场景：固定当前 B/C，让候选使用 M−B 的预算，按原实验的 block/partial-prefix/tail
规则拆分，再累加各轮七特征。不会凭空生成未来首 batch，也不把其他 prefill 的
未来预算当作已知。retention 对后继 prefix+1 的场景使用同一份负载快照并冻结选择。

`RequestWork.prefill_estimates` 含特征和、场景、负载和外推标志；GPU 与条件 CPU
路径的结果分别保存。已有 epoch/identity 检查与本地 prefix TTL 保持不变，配置
不匹配、负载缺失、无法形成 aligned chunk 或模型输出非正数时，成本为明确 unknown，
不默认为空闲、不裁剪为零，admission 使用整轮 FIFO。高命中仍至少计一个 token。

cadence 是引擎完成间隔的异步时间归属，不是 GPU kernel time，也不是 TTFT 或
内部排队 ETA。原生首实际 batch 预测器在 234 条成功请求上的 P90 APE=54.21%、
WAPE=24.53%，不能直接套用到新的派发前场景；当前没有在线收益或新实机验证。
标定 prompt 最高 96,965，超过实测 prompt/特征范围的估计带 extrapolated 标记。

restore 复用四特征实验的独立 H2D 冻结拟合，原实测引擎 identity 与当前完全一致。
它有 10 条训练和 6 条真实单请求恢复测试，后者 P90 APE=27.99%；未验证并发恢复竞争。
`fixed+actual_bytes*rate` 与七特征残余 prefill 组合为条件 CPU 成本；实际恢复仍由普通
请求触发。禁止 token→byte 推算和 worker 耗时相加。
offload 使用同配置独立 D2H 实测的冻结参数：
`T(s)=0.004039796055271255+2.044685376438999e-11*new_bytes`，零新增字节为零。
14 条训练，测试前冻结；12 条独立留出中位/P90 APE=8.61%/19.70%，最大 31.09%。
这是空闲传输微基准，含观测开销，未验证并发竞争、部分副本组合和跨会话精度。
vLLM 查询按对象给出 `offload_new_object_bytes`，排除已经完成的 CPU 副本；
目标含未完成 CPU 写入时返回 null。FlowPilot 缺少该值时保留未知成本，不能用
完整目标 bytes 或 token 比例代替；完整目标仍用于 H2D 与 CPU 驻留计费。
完整公式、来源、使用命令和限制见 [27B 说明](../examples/experiments/qwen35_27b_tp4/README.md)。

## Forecast 与事实 Tool ready-time

`FLOWPILOT_FORECAST_ENABLED` 默认 false。
默认 [NoOpForecastAdapter](../flowpilot/scheduling/forecast.py) 不预测；
create_app 可注入 adapter，另有 TraceReplayForecastAdapter。
超时默认 0.25 秒、TTL 30 秒、Top-N=3。

预测与推理异步进行，版本、置信度、TTL、取消及晚到结果均需校验。
当前 prewarm 回调只保存版本化 forecast metadata，没有真实 payload 预取。
事实 Tool Call、复用结果和本地事件覆盖预测。
[ToolResolutionStore](../flowpilot/scheduling/resolution.py) 保存 ready-time 事实，
[ProjectionCalculator](../flowpilot/scheduling/projection.py) 按需计算 T_need；
确定性 Tool 时延估计尚未生产校准。真实链路验收可显式开启
`FLOWPILOT_SYNTHETIC_TOOL_DURATIONS=1`，可选设置
`FLOWPILOT_SYNTHETIC_TOOL_DURATION_SEED` 以复现实验序列。事实 Tool
复用未命中后，名称包含 `search`（不区分大小写）的 Tool 生成 1–2 秒估计，
其余生成 100–200 毫秒估计，并在 resolution 上标记
`synthetic_factual_family_v1`。这只生成 ready-time 预测；不暂停或延长真实
Tool 执行。实际完成时长仍以 OpenHands 上报的事实为准。复用命中不套用该先验。
forecast 不进入 admission 分数，不执行 Tool，不改变物理缓存 LRU。

response 侧可注入的 `tool_duration_adapter.on_response()` 与上述 request forecast
是两个入口。它只提交工作，返回 awaitable，完成时应已将有效预测通过版本校验写入
ToolResolutionStore；返回 None 表示无待收集预测。预测器负责自身 timeout，
异常/超时/取消批次的估计在本次 KV 决策中记为 unknown，沿用显式 fallback。
不能启动后台任务后返回 None，同时期望 KV 等待该任务。当前此 hook 只支持非流式
完整回复，仓库未内置生产 Tool 时长预测器。

## KEEP/OFFLOAD/DROP

response 后按 line 的当前 tail resolve descriptor。引擎 descriptor ID 属于某次完成
request/output branch，记录不可变 hash/manifest；下一轮替换当前引用并生成新 ID。
旧策略在同 line 新 call、替代动作、过期时撤销；物理淘汰不等于删除 metadata。

FlowPilot 也按 descriptor 有效期清理本地引用。resolve 的引擎采样时间与到期时间
先相减得到剩余 TTL，再换算到本地 RPC 开始时间；查询和重试不续期。
本地定时器不依赖新请求或控制 RPC，接口不可用、回执未返回时仍能清理引用。
匹配的 DESCRIPTOR_EXPIRED 事件同样触发清理，事件丢失由 TTL 处理，不增加 tail 查询。
清理仅移除本地引用；已提交的 OFFLOAD/DROP 继续由引擎安全完成。
引擎重启、tail 替换、DROP 完成和服务关闭会取消相关定时器。
状态接口提供 remaining_ttl_seconds 和 source_expirations。
该功能需要包含 observed_at_monotonic 的新版 resolve 响应，两端应配套更新。

首次选择时读取事实 Tool gap G 和只读队列反馈 Q，组合模式 `window_basis=tool_and_queue`
使用 H=G+Q；任一必要输入未知则 H unknown。`tool_only` 是显式消融，使用 H=G。
READY 或 Tool hit 仅证明 G=0，队列繁忙时 H 仍为正。

```text
J_KEEP = residual_prefill_gpu + gpu_price * gpu_GiB * H_seconds
J_OFFLOAD = new_D2H + H2D + residual_prefill_cpu + cpu_price * cpu_GiB * H_seconds
J_DROP = cold_prefill
```

在合法且可评估的动作中选最小 J，平分稳定按 DROP、KEEP、OFFLOAD；不读取 SLO 预算。
完整 cpu_standalone_tokens 覆盖 offload target 时 new_D2H=0；回执与部分覆盖不证明完整副本。
new_D2H<=H 只是候选窗口假设，不构成请求恢复屏障。
GPU/CPU 驻留价格沿用 1/0.01 秒/GiB/秒，GPU 承压时乘 2，均为待标定策略参数。
未来 Tool 输出未知，response 侧使用已知 prefix 加一个 token 的 ASSUMED_CONTINUATION。

本地多 Tool 的 duration 按 provider 顺序串行累计，已开始项减去已执行时间；
未知时长或 line 依赖使 T_need 保持 unknown。缓存命中的 READY 项对 KV Tool gap
贡献为 0，不使用其已有 duration 估计；全部命中且无其他依赖时 gap=0。
in-flight 尚未完成时仍按 leader ready-time 等待。事实完成覆盖估计。
缺失必要窗口或兼容标定时保留显式 fallback：已知 H 近端且无压力 KEEP，其余支持 CPU 时 OFFLOAD，
否则 KEEP/unsupported。TERMINAL 或确认无可恢复 prefix 时 DROP。

每个 response 的 placement 只选择一次。完成回复后，Tool Cache 匹配、执行时长预测
和后台 KV 查询并行；匹配范围来自请求的 reuse policy。无 policy 时仍等待 SDK
对各 Tool 的 resolution 或实际 START/终态事件，不把 header 缺失当作 miss。
串行 SDK 边界逐个上报时，KV 选择相应推迟。必要输入收齐后选择并冻结
action、reason、G/Q/H、容量、候选 J/排除原因与标定来源。全部命中时无需等待本地执行预测。
DCS 内部各轮也记录 resolution；SSE 的匹配沿用 SDK Tool 边界上报，不阻塞流转发。
普通 response 返回和 admission credit 归还不等待 KV。过期或被新 tail 替换的 source
不补发旧策略。唯一选择记录为 kv_retention_decision，状态接口提供 inputs_ready、
selected_action、tool_gap_seconds 和 decision_inputs（冻结的 Q/H、window_basis、缺失输入、容量及成本）。

后续 Tool/压力/队列反馈变化不重新选择；周期刷新只处理首次尚未完成的选择、回执、过期和
执行重试。幂等重试沿用原命令；PARTIAL/FAILED 不等于成功，OFFLOAD 的 PARTIAL/FAILED
等待至少一个刷新周期后重新查询，以新 action_id / policy_version 重试相同 OFFLOAD。
line finish 另发 DROP 释放需求；DROP 的 PARTIAL 交给引擎延迟清理。

单次控制 RPC timeout 与 descriptor 解析总寿命分离：后者使用引擎 capabilities 中的 metadata_ttl_seconds。短暂协商失败不丢失已知引擎的 ingress binding，但暂停策略动作；明确 unsupported 停止绑定。旧引擎缺少 TTL 时保留单次 timeout 窗口。事件触发合并，单个 source 失败不阻断其余 source。

引擎 OFFLOAD 全部 READY 后，立即回收没有活跃引用、复制保护和其他有效 KEEP 的
目标 GPU 映射；受保护范围在保护解除后重试。回收是 KV 池内复用，不是释放 CUDA 池。
共享块仍须按有效需求聚合；这不能替代 KEEP 生命周期管理。
详见 [vLLM KV 控制](vllm-kv-control.md)。


## Readiness 投影版本

`/flowpilot/v1/scheduling/projections/{line_id}` 使用独立版本 `flowpilot-readiness-v1`，
只返回身份、tail version、事实 ready/T_need、依赖及未完成 Tool 数等诊断。
旧 estimated_inference_ms/downstream_depth 查询参数显式返回 422 迁移错误。
ForecastRequest/Result 和 ToolResolutionRecord 继续使用原 phase4 envelope 版本；
compatibility deadline 和离线 SLO 报告不参与在线策略。
