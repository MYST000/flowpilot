# 请求调度

核对日期：2026-09-27。调用链为 LLMGateway → SchedulingRuntime → AdmissionQueue。
规范见 [design.md](../design.md) §§5–7。

## 启用

```bash
export FLOWPILOT_ADMISSION_JSON='{"enabled":true,"limit":8,"policy":"prefill_slack"}'
export FLOWPILOT_RETENTION_JSON='{"enabled":true,"owner_scope":"flowpilot-local"}'
export FLOWPILOT_COST_MODEL_PATH=/absolute/path/cost-model.json
```

两个功能开关默认关闭，独立启用，只支持一个固定实例。
模型文件可不配置；没有匹配标定时成本为 unknown，队列明确按 deadline 排序，
retention 使用注明 `fallback_cost_unknown` 的 ready-time/容量规则。
旧连续加权分数通过 `policy=weighted` 显式选择，便于对照。
过期降级通过 `policy=slo_unexpired_first` 显式选择；默认仍为 `prefill_slack`。

## 队列与全量查询

```text
CP = arrival - workflow_start           # 到达后固定
Age = now - arrival                    # 单调时钟增长
R = deadline - workflow_start - CP - Age
C_gpu = prefill(P, H_gpu)
C_cpu = restore(object_bytes) + prefill(P, H_all)
L = R - min(C_gpu, C_cpu)               # 已知候选；越小越先派发
```

没有 CPU 候选成本时使用 C_gpu；没有模型则不把 tokens 当秒数。
CPU 成本是引擎选择该候选时的条件估计。引擎实际恢复或重算仍自主决定。
无 deadline 的项排在有 deadline 项之后，同主键按可选 Job 在途数、blocking lines、
年龄与 FIFO 处理；没有强公平保证，不预测 decode 或引擎内部排队时间。

`slo_unexpired_first` 使用同一个队列：`R>0` 的请求优先，并在这部分内沿用
上述 prefill slack 排序；`R<=0` 与无 deadline 的请求按 FIFO 使用剩余名额。
判定依据是实际剩余 SLO，负的 prefill slack 不等于已过期。每次派发重新计算
`slo_status=unexpired|expired|no_deadline`，查询期间到期的项会降级。
查询期间新到且尚未纳入本轮 sweep 的未过期请求也阻止尽力完成项先派发。
已提交调用正常结束后归还 credit；不抢占、不自动取消 Job 或 Tool。
没有未过期等待项时，尽力完成项可占用空闲 credit；后续新到请求可能仍要等待
这些调用结束。持续高负载下，尽力完成项可能等到原客户端超时。

### 可选背景额度与自适应总准入

以下配置在保留单队列、SLO 评估语义和原始超时的基础上启用两个控制：

```json
{
  "enabled": true,
  "policy": "slo_unexpired_first",
  "limit": 32,
  "best_effort": {
    "enabled": true,
    "limit": 4,
    "relax_when_quiet": true,
    "quiet_seconds": 60,
    "ramp_interval_seconds": 30,
    "ramp_step": 4
  },
  "adaptive": {
    "enabled": true,
    "initial_limit": 24,
    "min_limit": 16
  }
}
```

过期及无 deadline 调用共享背景额度。已提交调用过期也计入背景占用；
超过额度时不撤回、不提前归还 credit，只停止补发。未过期请求优先使用
有效总额度。相应开关默认关闭，旧配置维持固定准入及原有排队行为。

启发式放宽从 waiting 和 inflight 均没有未过期请求时计时；有背景工作且
连续 60 秒满足时放宽至 8，此后每 30 秒加 4，最多到有效总额度。
新未过期请求入队立即恢复到 4；已提交的超额背景调用只能等正常结束。
没有任务时不累计时间。无论连续多少次 dispatch/RPC 都不能加快窗口。
它看不到 Tool 等待中的 Job、未来任务或入队前请求，可能误判；状态始终
标记 `drain_confirmed=false`，放宽时为 `mode=heuristic_relaxed`。
`relax_when_quiet=false` 可关闭启发式并保持固定额度。

自适应控制单独每 10 秒读取引擎 `/metrics`，按 30 秒窗口比较实测 waiting、
抢占增量和完成速率。平均 waiting>=2，且抢占增加或等待不下降，完成速率
提升不超过 5%，连续两窗口减 4；waiting<2、无抢占、仍有容量需求并且有
实际完成，连续三窗口加 1。有效总额度限定在 16–32，初始 24。这是待验证
的实验设置，不是完整 workflow 完成时间预测，也不改变引擎 decode/restore。
减额低于已占用 credit 时等待自然归还；不因 KV 使用率高就独立减额。

缺失/失效指标显式暴露 `metrics_unavailable` 或 `metrics_stale`，保持最后额度；
计数回退、指标故障或采样间断后重建窗口，不按零负载处理。
指标需为同一固定引擎的 `vllm:num_requests_running`、
`vllm:num_requests_waiting`、`vllm:num_preemptions_total` 和
`vllm:e2e_request_latency_seconds_count`，不支持的指标形式会显式报不可用。
观察写入 `admission_capacity_observation`；scheduling state 暴露控制器窗口、
调整原因、有效额度和背景超额数。独立的健康 TTL 继续门控所有新派发。

插入、heartbeat、release 和依赖变化合并触发 sweep，查询范围只包括所有排队请求。
每轮获取能力/epoch，对各项发送 `/v1/kv/query-target`；实际 Chat/Responses 渲染与
引擎原生 hash/lookup 提供 P、H_gpu、H_all 和 CPU 对象 bytes。
不以旧 descriptor 猜新 prompt，也不扫描全部 line tail。
查询在锁外并发执行；返回后只更新仍在等待的项，新增项留待下一 sweep。
每轮可派发多个 credit，不为每个 credit 重查整个队列。

epoch、query identity 和默认 2 秒 TTL 防止使用已知失效快照；查询失败显式降级。
查询后仍可能被淘汰，TTL 不是租约。真实请求由 vLLM 再次 lookup/acquire。
没有外部 RESTORE、CPU-only 等待队列或 GPU-ready 屏障。

健康探测默认每秒一次、TTL=5 秒、timeout=1 秒；limit 是网关配置上限。
一个锁保护 waiting/inflight；取消、失败和 terminal 归还 credit，重复 release 不增额。
流式 credit 覆盖整个 GatewayCall，包括引擎恢复和计算。
`/flowpilot/v1/scheduling/state` 暴露 slack、成本/观察来源、查询范围、credit 和回执。
`queue_work_before_tokens` 只记录插入时前置完整 prompt 工作量，不是等待时间。

## 离线标定

[OfflineCostModel](../flowpilot/scheduling/cost.py) 支持：

- prefill：按总上下文 P 分桶，`fixed_seconds + (P-H)*seconds_per_token`。
- 桶内可提供 `segments`，按 `P-H` 选择 `max_uncached_tokens` 分段；每段独立
  保存固定项、逐 token 系数与残差。未提供时保持原单直线格式。
- GPU→CPU 和 CPU→GPU：分别拟合 `fixed_seconds + actual_bytes*seconds_per_byte`。
- 来源、版本、实测时间、模型、engine identity、测量口径与拟合残差。

每个上下文桶应覆盖多个 P-H、冷热 cache 及预期并发负载；单 token 速度只是桶内近似。
采样应计引擎 prefill 执行时间，不直接把 HTTP TTFT 当作 prefill。
传输采样必须使用真实对象 bytes 和所有必要 worker 完成的墙钟时间，
不能以 worker 耗时之和作墙钟，也不能将 hybrid tokens 换算成假定字节数。
预热、同步 GPU 测量、多次重复，并在 measurement_basis 写明 GPU、TP、dtype、
batch/chunk 设置；引擎身份匹配不能自动证明硬件和负载不变。

准备实测 CSV（每种拟合至少两个不同工作量，prefill 桶不能有空洞）：

```text
kind,prompt_tokens,cached_tokens,bytes,seconds
```

kind 取 prefill/offload/restore。prefill 填 P/H，传输填真实 bytes；seconds 均为实测值。
使用拟合工具生成可直接加载的 JSON：

```bash
.venv/bin/python integration/fit_cost_model.py measurements.csv cost-model.json \
  --model YOUR_MODEL --engine-identity-digest DIGEST_FROM_CAPABILITIES \
  --measurement-basis 'GPU/TP/dtype/batch; prefill device time; transfer wall time' \
  --measured-at 2026-09-27T08:00:00+00:00 --context-bounds 1024,2048,4096
```

不存在默认的伪生产测量文件。超过最高 context 桶或 identity 不匹配为 unknown；
桶内未采样命中比例、低于最小实测输入和传输 bytes 范围外仍按现有模型近似/外推，
不能称为已验证的实测范围。
uncertainty 保存拟合最大绝对残差；当前排序用点估计，不自动增加安全裕量。
拟合工具不代替硬件采样；上线前需对真实推理时间校验误差。

`--piecewise-prefill` 可按桶内相邻实测工作量拟合分段，适用于同时采集冷请求、
部分命中与少量残余 prefill 的数据；不能用一条 cold 样本推导高命中成本。
四卡 Qwen3.5-9B [实验配置](../examples/experiments/qwen35_9b_tp4/README.md) 默认
接入本机实测的精简成本文件，admission 和 retention 共享该模型。引擎身份不匹配
仍为 unknown，未改变一般部署的默认配置。

当前 Qwen3.5-27B / TP=4 / CPU KV 总预算 64 GiB 实验使用独立的
[27B 成本文件](../examples/experiments/qwen35_27b_tp4/cost-model.json)，版本
`offline-20261002T154105Z`。通过
`python -m examples.experiments.qwen35_27b_tp4.launch gateway` 启动时默认加载；
配置校验、凭据和 registry 用法见 [27B 实验说明](../examples/experiments/qwen35_27b_tp4/README.md)。
通用网关入口可在其余运行配置已设置时显式选择：

```bash
export FLOWPILOT_COST_MODEL_PATH="$PWD/examples/experiments/qwen35_27b_tp4/cost-model.json"
```

27B 模型提供 cold/部分 GPU 命中/近全 GPU 命中/CPU 恢复后的残余 prefill，
以及基于真实对象 bytes 的 H2D。admission 使用 `min(F(P,H_gpu), R(B)+F(P,H_all))`
计算剩余 prefill slack；retention 共用相同系数计算 KEEP、DROP 和已有完整 CPU
副本的恢复成本。新增独立 D2H 使用 21 条无推理重叠、四个 worker 完成的实际
复制数据，bytes 范围 205324288–17058037760；原 prefill/H2D 系数保持不变。
Tool gap 未知时仍是显式 ready-time/容量 fallback，可能继续选择 OFFLOAD。
目标查询的单请求/四并发 RPC 成本单列，不并入引擎标定。该文件不提供 Tool 时长
或 decode/引擎排队估计；GPU/CPU 驻留价格
仍是策略参数。原始实测与并发样本单独保存，当前排序模型不是并发延迟或完整 JCT 预测。

D2H 单条线性模型是粗略估计：同尺寸留出中位相对误差 42.8%，最大 117.5%，
最大拟合绝对残差 0.256 秒。小对象误差较大；uncertainty 已保存在文件中，当前
retention 使用点估计，不会自动加入裕量。实测与模型误差见 27B 实验说明。

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

具备匹配标定和已知 Tool gap 时，比较 KEEP 的残余 prefill、OFFLOAD 的双向传输
加残余 prefill、DROP 的 cold prefill。先最小化相对 `remaining_SLO-gap` 的预计超支，
再比较运行成本和驻留价格；只有 D2H 能在 gap 内完成才考虑 OFFLOAD。
如果引擎报告的 cpu_standalone_tokens 已覆盖 offload target，则副本已经就绪，D2H 成本为零，
不再要求卸载标定或正的等待 gap；仍比较 H2D、残余 prefill 和 CPU 驻留价格。
已接受的 OFFLOAD 策略本身不证明副本就绪，未知或部分 CPU 覆盖仍按需要复制处理。
默认 GPU/CPU 驻留价格分别为 1/0.01 秒/GiB/秒，GPU 承压时价格加倍。
这些是可调策略参数，不是实测速度；GPU bytes 去重于单 descriptor，非全局边际成本。
未来 Tool 输出未知，response 侧只估已知 prefix 加一个 token，不保证整条 workflow SLO。

本地多 Tool 的 duration 按 provider 顺序串行累计，已开始项减去已执行时间；
未知时长或 line 依赖使 T_need 保持 unknown。缓存命中的 READY 项对 KV Tool gap
贡献为 0，不使用其已有 duration 估计；全部命中且无其他依赖时 gap=0。
in-flight 尚未完成时仍按 leader ready-time 等待。事实完成覆盖估计。
没有模型时保留显式 fallback：近端且无压力 KEEP，其余支持 CPU 时 OFFLOAD，
否则 KEEP/unsupported。TERMINAL 或确认无可恢复 prefix 时 DROP。

每个 response 的 placement 只选择一次。完成回复后，Tool Cache 匹配、执行时长预测
和后台 KV 查询并行；匹配范围来自请求的 reuse policy。无 policy 时仍等待 SDK
对各 Tool 的 resolution 或实际 START/终态事件，不把 header 缺失当作 miss。
串行 SDK 边界逐个上报时，KV 选择相应推迟。必要输入收齐后选择并冻结
action、reason 和 tool_gap_seconds。全部命中时无需等待本地执行预测。
DCS 内部各轮也记录 resolution；SSE 的匹配沿用 SDK Tool 边界上报，不阻塞流转发。
普通 response 返回和 admission credit 归还不等待 KV。过期或被新 tail 替换的 source
不补发旧策略。唯一选择记录为 kv_retention_decision，状态接口提供 inputs_ready、
selected_action 和 tool_gap_seconds。

后续 Tool/压力变化不重新选择；周期刷新只处理首次尚未完成的选择、回执、过期和
执行重试。幂等重试沿用原命令；PARTIAL/FAILED 不等于成功，OFFLOAD 的 PARTIAL/FAILED
等待至少一个刷新周期后重新查询，以新 action_id / policy_version 重试相同 OFFLOAD。
line finish 另发 DROP 释放需求；DROP 的 PARTIAL 交给引擎延迟清理。

单次控制 RPC timeout 与 descriptor 解析总寿命分离：后者使用引擎 capabilities 中的 metadata_ttl_seconds。短暂协商失败不丢失已知引擎的 ingress binding，但暂停策略动作；明确 unsupported 停止绑定。旧引擎缺少 TTL 时保留单次 timeout 窗口。事件触发合并，单个 source 失败不阻断其余 source。

引擎 OFFLOAD 全部 READY 后，立即回收没有活跃引用、复制保护和其他有效 KEEP 的
目标 GPU 映射；受保护范围在保护解除后重试。回收是 KV 池内复用，不是释放 CUDA 池。
共享块仍须按有效需求聚合；这不能替代 KEEP 生命周期管理。
详见 [vLLM KV 控制](vllm-kv-control.md)。
