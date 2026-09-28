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

不存在默认的伪生产测量文件。范围外或 identity 不匹配为 unknown。
uncertainty 保存拟合最大绝对残差；当前排序用点估计，不自动增加安全裕量。
拟合工具不代替硬件采样；上线前需对真实推理时间校验误差。

`--piecewise-prefill` 可按桶内相邻实测工作量拟合分段，适用于同时采集冷请求、
部分命中与少量残余 prefill 的数据；不能用一条 cold 样本推导高命中成本。
四卡 Qwen3.5-9B [实验配置](../examples/experiments/qwen35_9b_tp4/README.md) 默认
接入本机实测的精简成本文件，admission 和 retention 共享该模型。引擎身份不匹配
仍为 unknown，未改变一般部署的默认配置。

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
默认 GPU/CPU 驻留价格分别为 1/0.01 秒/GiB/秒，GPU 承压时价格加倍。
这些是可调策略参数，不是实测速度；GPU bytes 去重于单 descriptor，非全局边际成本。
未来 Tool 输出未知，response 侧只估已知 prefix 加一个 token，不保证整条 workflow SLO。

本地多 Tool 的 duration 按 provider 顺序串行累计，已开始项减去已执行时间；
未知时长或 line 依赖使 T_need 保持 unknown。缓存命中、in-flight 完成和生命周期事实覆盖估计。
没有模型时保留显式 fallback：近端且无压力 KEEP，其余支持 CPU 时 OFFLOAD，
否则 KEEP/unsupported。TERMINAL 或确认无可恢复 prefix 时 DROP。

response resolve、Tool 生命周期、line finish 标记相关 source；周期刷新只轮询回执与
读取 telemetry，用缓存观察复算动作。仅相关 source 或动作改变时重新查询 descriptor，
不全量扫描所有 tail 的 KV。幂等重试沿用原命令；PARTIAL/FAILED 不等于成功，
OFFLOAD 的 PARTIAL/FAILED 等待至少一个刷新周期后重新查询，若当前策略仍要求 OFFLOAD，则生成新 action_id / policy_version 重试；DROP 的 PARTIAL 交给引擎延迟清理。

单次控制 RPC timeout 与 descriptor 解析总寿命分离：后者使用引擎 capabilities 中的 metadata_ttl_seconds。短暂协商失败不丢失已知引擎的 ingress binding，但暂停策略动作；明确 unsupported 停止绑定。旧引擎缺少 TTL 时保留单次 timeout 窗口。事件触发合并，单个 source 失败不阻断其余 source。

引擎 OFFLOAD 全部 READY 后，立即回收没有活跃引用、复制保护和其他有效 KEEP 的
目标 GPU 映射；受保护范围在保护解除后重试。回收是 KV 池内复用，不是释放 CUDA 池。
共享块仍须按有效需求聚合；这不能替代 KEEP 生命周期管理。
详见 [vLLM KV 控制](vllm-kv-control.md)。
