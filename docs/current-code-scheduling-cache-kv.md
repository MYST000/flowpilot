# 当前代码中的 Request 排队、Tool Cache 淘汰与 KV 管理

核对日期：2026-09-21。本文从当前工作区的实现、调用链和测试出发，不以 `design.md` 或其他设计文档推导已实现行为。

代码范围：

- FlowPilot：`/home/liyachen/workspace/flowpilot`，HEAD 为 `54ad83b1bde4458f896d2f4786d2c1ca3694b670`，**包含当前未提交修改和新增文件**。
- vLLM：`/home/liyachen/vllm`，基线 HEAD 为 `98dff2a81d747d1dba01a47f939f48c3526d4206`，**包含本地 KV 控制扩展及相关修改**，不能视为上游原版行为。
- 本文描述代码开启相应功能后的行为；没有读取正在运行的服务配置，因此不声称这些功能已在当前线上实例启用。本文仅新增文档，没有修改生产代码。

## 1. 四部分的实际关系

| 部分 | 当前算法 | 主要实现 |
|---|---|---|
| Request 排队 | 连续加权分数，最高分先派发，同分 FIFO；健康检查和并发 credit 控制准入 | `AdmissionQueue`、`SchedulingRuntime` |
| Tool Cache 淘汰 | 先删过期结果，再按“节省执行时间 / 字节”的价值从低到高删除；价值同分才用 LRU | `ReuseCache.maintenance()` |
| KV 动作选择 | 根据 line 阶段、预计多久需要后继、GPU 可分配块数和 capability，按固定分支选择 KEEP/OFFLOAD/DROP | `choose_retention()` |
| KV 动作执行 | GRACE 引用保护、GPU 空闲块优先级、原生 CPU store/load、带 generation 检查的定向删除 | 本地 vLLM `KVControlManager`、`BlockPool`、`OffloadingConnectorScheduler` |

这四者没有合并成一个统一优化器。Tool 结果可以影响后继请求的预计形成时间，但 Tool payload 容量、CPU KV 容量和 GPU KV 容量分别管理。当前排队分数不使用真实 GPU/CPU prefix 命中量或 restore 成本。

```text
Complete request
  -> FlowPilot: identity/context validation
  -> tokenize -> AdmissionQueue -> consume credit
  -> ordinary vLLM inference
       -> native GPU lookup / CPU load / computation
       -> finish: descriptor + optional GRACE
  -> FlowPilot: complete response + return credit
       -> background resolve/query -> choose KEEP/OFFLOAD/DROP
       -> vLLM apply/status

Actual Tool Call
  -> historical reuse / in-flight follower / local execution
  -> Tool resolution and ready-time estimates
  -> retention refresh; later a complete successor enters AdmissionQueue
```

## 2. Request 排队算法

### 2.1 真正接入网关的是哪条队列

调用链为 `LLMGateway._proxy_once()` → `SchedulingRuntime.admit()` → `AdmissionQueue.acquire()`；获得 credit 并复核 tail 后，网关才调用 `_send_with_failover()` 发出 HTTP 请求。

`AdmissionQueue` 用 `_waiting` 保存等待项，用 `_inflight` 保存已经占用 credit 的 `(job_id, llm_call_id)`。队列和 credit 由同一个 `asyncio.Lock` 保护。它们属于当前 `SchedulingRuntime` 的进程内状态；该实现不是持久化队列，也不是跨 worker 的共享 credit 账本。

`gateway/router.py` 中仍存在 `WeightedFairRequestQueue` 和多实例路由代码，但当前生产调用链没有实例化前者，只有测试使用它。开启 admission 或 retention 时，`Settings` 要求恰好一个 inference instance，因此不能把这些旧类当成当前外部请求队列算法。

源码：[网关入口](/home/liyachen/workspace/flowpilot/flowpilot/gateway/service.py:626)、[SchedulingRuntime](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/runtime.py)、[AdmissionQueue](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/admission.py)、[配置约束](/home/liyachen/workspace/flowpilot/flowpilot/config.py:60)、[其他路由类](/home/liyachen/workspace/flowpilot/flowpilot/gateway/router.py)。

### 2.2 排序公式

`priority_score()` 返回六个已经乘权重的分量，总和为最终分数：

```text
Score(q) = ws * U + wa * A + wp * I + wd * D - wc * C - wf * F

默认：ws=0.55, wa=0.35, wp=0.05, wd=0.03, wc=0.02, wf=0
```

分数越大越先派发。没有独立的风险档位、cache tier 档位或 Job 公平资格层。

| 项 | 实际计算 | 含义 |
|---|---|---|
| SLO 紧迫度 `U` | 设 `S=max(deadline-workflow_started_at, 0.001秒)`，`R=deadline-now`；`R>=0` 时 `U=S/(S+R)`，否则 `U=1+min(1,-R/S)`；无 deadline 时为 0 | 临近 deadline 连续升高，逾期后上限为 2 |
| 等待年龄 `A` | `max(0,age_seconds)/age_reference_seconds`，参考值默认 5 秒 | 不截断，等待越久越高 |
| 工作流进度 `I` | `min(1,CP/S)`，其中 `CP=max(0,request_arrival-workflow_started_at)`；无 deadline 时分母用 60 秒 | CP 使用固定到达时间，不随排队增长 |
| 释放价值 `D` | `blocking_lines/(1+blocking_lines)` | 直接依赖当前 line 的其他 line 数量，不是整个 DAG 的深度或关键路径长度 |
| 工作成本 `C` | `P/(4096+P)`，4096 可配置 | 长 prompt 受到小幅惩罚；当前 `P` 是完整 prompt 的 cold token 工作估计 |
| Job 并发惩罚 `F` | `job_inflight/(1+job_inflight)` | 只统计此队列中相同 Job 已占用的 credit，默认权重为 0 |

deadline 优先使用 line 的 deadline，没有时使用 Job deadline。队列年龄通过 monotonic clock 累加，并回溯计入从 `gateway_received_at` 到实际插入队列的时间，因此 tokenize 等入队前处理时间也包含在年龄中。

默认年龄项每等待 1 秒增加 `0.35/5=0.07`。它最终可以超过其他有界分量的优势，但不等于每个 Job 有固定服务份额；默认配置没有启用 Job 并发惩罚。`ProjectionCalculator` 中另有乘法式 `request_weight`、SLO urgency 和 DAG importance，**当前 admission 不使用这些值作为上述公式的输入**。

源码：[分量及默认参数](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/admission.py:17)、[真实依赖计数](/home/liyachen/workspace/flowpilot/flowpilot/frontier/store.py:775)、[独立投影计算器](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/projection.py)。

### 2.3 `P` 如何得到，KV 命中是否参与排序

`SchedulingRuntime._prompt_tokens()` 按以下顺序处理：

1. `prompt` 为整数 token ID 列表时直接取长度。
2. 有 `messages` 或字符串 `prompt` 时，请求固定实例的 `/tokenize`，读取 `count`。
3. 不支持的载荷、tokenize HTTP 错误或非法 count，记为 `None`，增加 unknown 计数。比如没有 `messages`/字符串 `prompt` 的 Responses `input` 载荷走 unknown 分支。

`P=None` 时成本分量贡献为 0，但状态仍报告 `work_basis=unknown`，不能解释为实际需要 0 tokens。与已知长 prompt 相比，这类请求不会承受成本惩罚。

当前队列状态明确写入 `prefix_basis="COLD:no_target_proof"`。已完成请求 descriptor 的查询结果仅用于 KV retention，不会从 `P` 中减去 `gpu_ready_tokens`，也不会把 CPU 恢复估计加入 Score。读取旧 descriptor 不证明下一次请求的输入真的延续该前缀。

源码：[tokenize 和 admission](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/runtime.py:73)、[队列投影](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/admission.py:201)。

### 2.4 派发、刷新和 credit 归还

每次 `_dispatch()` 在持锁状态下：

```text
while health_is_fresh and inflight < configured_limit and waiting is not empty:
    重新计算所有 waiting 项的当前分数
    选择 max(score, -arrival_sequence)
    从 waiting 删除，加入 inflight
    唤醒该请求的 acquire Future

调用者随后在锁外提交 HTTP
```

所以这不是固定堆中只更新几个请求的实现：每选一个请求，都会扫描当前等待项重新评分；一次派发 k 个、队列 N 项时，选取工作大致为 `O(kN)`，此外每项计算 Job inflight 还会扫描 credit 表。snapshot 也会重新计算分数并排序，但不自行派发。

派发触发点包括插入、heartbeat、release 和依赖刷新。默认每 1 秒请求 `/health`，heartbeat 有效期为 5 秒，探测超时为 1 秒。健康只来自 HTTP 探测是否成功；并发上限默认 8，来自运维配置，**不是 vLLM 实时上报的内部 batch 空位、GPU 空闲比例或动态容量**。

heartbeat 失败或过期会停止新派发，已派发请求继续运行。`acquire()` 返回后还会复核当前 line 的 version、tail、llm_call、`ACTIVE` 阶段及依赖为空，不符合则归还 credit 并报冲突。

credit 一直占用到 GatewayCall terminal，包括引擎内部的 CPU 加载、计算和流式响应生命周期。完成、取消、断连或提交失败通过 terminal/release 路径归还；重复 release 只删除同一个 key，不额外增加额度。

`queue_work_before_tokens` 是插入前分数不低于新请求的等待项 token 数之和；未知项导致 `queue_work_complete=false`。该值是插入时诊断快照，不加进 Score，也不会随着排队位置改变自动重算。

源码：[入队及 credit](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/admission.py:139)、[派发](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/admission.py:225)、[heartbeat/terminal](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/runtime.py)。

## 3. Tool Cache 淘汰策略

### 3.1 容量对象与触发时机

缓存是 SQLite 中的历史 Tool 结果。一个 `origin_id` 保存一份 payload，exact/semantic 索引指向它，不因 follower 数量复制多份历史 payload。容量按 committed publication 的 `result_size` 求和，默认 `512 MiB`。

这个限制控制的是逻辑结果 payload 字节，**不是 SQLite 文件总大小**；索引、receipt、审计信息、WAL 和 DCS WAL 不包含在该数值中。代码执行被动 WAL checkpoint，没有在每次淘汰后 `VACUUM`，因此删除结果不意味着数据库文件立即缩小。

维护发生在发布结果后、后台定时维护（默认 60 秒一次）以及显式 `/flowpilot/v1/reuse/maintenance` 请求。当前采用“先提交结果，再执行维护”的方式，没有先预测复用收益再拒绝写入的独立 admission 算法。

源码：[缓存表和 commit](/home/liyachen/workspace/flowpilot/flowpilot/reuse/store.py:37)、[发布后维护](/home/liyachen/workspace/flowpilot/flowpilot/reuse/controller.py:941)、[后台维护](/home/liyachen/workspace/flowpilot/flowpilot/app.py:265)、[默认容量](/home/liyachen/workspace/flowpilot/flowpilot/config.py:39)。

### 3.2 TTL 优先，然后按价值密度淘汰

`ReuseCache.maintenance()` 的执行顺序为：

1. 删除 `expires_at <= now` 的 committed 结果。
2. 若传入当前 embedding index ID，删除其他版本的 semantic vectors。
3. 统计剩余 committed payload 总字节，超过容量才进行价值排序和淘汰。
4. 清理超过 retry window 的删除墓碑及旧审计记录，执行 WAL checkpoint。

每条结果的价值为：

```text
Freshness = clamp((expires_at-now) / max(0.001, expires_at-observed_at), 0, 1)

Value = max(0, measured_latency_ms) * (1+hit_count) * Freshness
        / max(1, result_size)

EvictionKey = (Value, last_used_at, origin_id)    # 升序，先删最小
```

`measured_latency_ms` 来自可信执行 receipt 的 FINISH；缺失时贡献为 0。不可缓存 publication 的 Value 也为 0。`hit_count` 是 reuse entry 的触达计数；`last_used_at` 没有索引项时使用 `committed_at`。因此昂贵、经常复用、较新鲜且较小的结果更容易留下；只有 Value 相等时才按 LRU 和确定性的 origin ID 打破平局。

例：两条结果大小相同、均未命中且剩余有效期比例相同，执行耗时 1000 ms 的旧结果比执行耗时 1 ms 的新结果价值高 1000 倍，容量只容纳一条时保留旧结果。测试 `test_value_eviction_keeps_expensive_old_result_over_recent_cheap_result` 验证了这一行为。

TTL 在 publication 创建时固定为 registry/default、max TTL、scope TTL 与调用报告 TTL 按代码取最小值后的有效时长，从 `observed_at` 开始计算。缓存命中只更新 `hit_count/last_used_at`，不会延长 `expires_at`。

源码：[价值公式](/home/liyachen/workspace/flowpilot/flowpilot/reuse/store.py:18)、[维护排序](/home/liyachen/workspace/flowpilot/flowpilot/reuse/store.py:352)、[TTL 计算](/home/liyachen/workspace/flowpilot/flowpilot/reuse/controller.py:886)。

### 3.3 什么会受到保护，什么时候更新命中信息

容量淘汰会跳过两类 origin：

- 当前正在执行 `_deliver_protected()` 的结果，使用 `protect_delivery()` 的进程内引用计数保护。
- 已有 publication 且仍有 followers 的 binding。follower poll 可重试，首次返回结果后不会立刻解除保护；保护持续到 follower 取消或 binding 按已有终态保留期限清理。终态保留期来自 reuse lease 配置，默认 30 秒。

所有候选都受保护时，允许 payload 超出预算，并报告 `over_capacity_bytes`；不会为了容量强行删除当前交付所需结果。DCS 已保存的完整结果由其 WAL 独立持有。

上述保护只针对**容量淘汰**，TTL 删除和显式 revoke 不受它豁免。交付路径在适配结果后重新读取 publication，检查删除/撤销/有效期，随后调用 `touch()`，最后再检查一次有效期。因此触达统计来自真实结果交付路径，而不是预测或索引查询；不过最后一次 TTL 检查前已 touch，不能把计数严格当成客户端确认成功次数。重复 follower poll 也可能重复 touch。

删除会移除 payload、entry、执行引用及关联审计，semantic vector 随 entry 删除；publication 保留带状态的墓碑，在默认 86400 秒 retry window 后才清理，避免重试重新创建已删除的 payload。

当前默认 forecast prewarm 回调仅保存 forecast 元数据，没有预取 Tool payload，也不改变物理淘汰顺序。Value 没有使用 SLO、DAG、预测命中概率或 KV 空闲量。

源码：[交付引用](/home/liyachen/workspace/flowpilot/flowpilot/reuse/store.py:339)、[follower 保护](/home/liyachen/workspace/flowpilot/flowpilot/reuse/controller.py:1217)、[touch 位置](/home/liyachen/workspace/flowpilot/flowpilot/reuse/controller.py:669)、[默认 forecast 回调](/home/liyachen/workspace/flowpilot/flowpilot/app.py:182)。

## 4. KV 策略选择：什么时候 KEEP / OFFLOAD / DROP

### 4.1 决策是固定分支，不是收益模型求最优

`choose_retention()` 的输入只有：配置、capabilities、descriptor observation、line phase、`need_in_seconds` 和 `free_gpu_allocations`。

定义：

```text
near = phase == READY
       or (need_in_seconds 已知且 <= keep_horizon_seconds)

pressure = free_gpu_allocations <= gpu_free_reserve_allocations

默认 keep_horizon_seconds = 1.0
默认 gpu_free_reserve_allocations = 128
```

**必须按下表从上到下判断，先命中的分支决定结果：**

| 顺序 | 条件 | 结果 |
|---|---|---|
| 1 | `phase==TERMINAL`，或查询 COMPLETE 且 `recoverable_tokens==0` | 支持 `safe_direct_drop` 时 DROP，否则不发送动作；不会继续尝试 KEEP/OFFLOAD |
| 2 | `near`，没有 pressure，支持 `gpu_retention_preference` | KEEP，原因 `near_factual_successor` |
| 3 | 同时支持 `cpu_backed_eviction_preference`、`cpu_store`、`engine_cpu_reuse` | OFFLOAD；有 pressure 标 `gpu_pressure`，否则标 `waiting_for_successor` |
| 4 | 前面未选择，支持 `gpu_retention_preference` | KEEP，原因 `cpu_retention_unsupported`，即使有 GPU pressure 也如此 |
| 5 | 都不满足 | 不发送动作，`retention_unsupported` |

由此可以直接得到：

- READY 且 GPU 可分配块大于 128：通常 KEEP。
- BLOCKED、预计 0.1 秒后需要后继且无压力：通常 KEEP。
- BLOCKED、后继时间未知或超过 1 秒：CPU 三项能力齐全则 OFFLOAD。
- READY 但 GPU 可分配块不超过 128：CPU 能力齐全则 OFFLOAD。
- CPU 能力缺失且 KEEP 可用：退回 KEEP，不因为压力自动选择 DROP。
- `recoverable_tokens=None`/PENDING 是未知，不满足“可恢复长度为零”的 DROP 条件。

`free_gpu_allocations` 来自 vLLM `BlockPool.get_num_free_blocks()`，表示当前可供分配的块，包含仍有缓存内容但引用数为零、可以被重用的块。它不是“GPU 中没有任何 KV 内容的块数”，也不是显存空闲字节。这个阈值不会直接检测 CPU 容量是否足够；CPU_CAPACITY 等结果要等 OFFLOAD 执行回执。

该函数不使用 request weight、deadline、DAG importance、KV 字节、实测带宽或 restore 成本；`gpu_ready_tokens` 的具体长度也没有参与动作比较。当前是 readiness/pressure/capability 启发式，不是三种方案的成本最小化。

源码：[完整选择函数](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/retention.py:76)、[真实容量来源](/home/liyachen/vllm/vllm/v1/kv_control/manager.py:647)。

### 4.2 `T_need` 从哪里来

`ProjectionCalculator.for_line()` 读取当前 tail 的 ToolResolutionRecords：

- unresolved 排除 `ready/failed/cancelled` 状态。
- 至少有一个 `ready_at_estimate`，且每个 unresolved Tool 都有估计时，取所有已有估计的最大值，再加 `continuation_cost_ms`。
- 否则 `t_need=None`。retention 调用没有额外传 continuation cost，默认是 0。

历史命中或 follower 已拿到结果时，估计时间设置为当前时间；仍在等待的 follower 可以用 leader 的剩余耗时；真实 Tool FINISH 使用其 `observed_at`。若启用并存有兼容 forecast，`observe_tool_call()` 还可能用实际出现 Tool family 的 `duration_p50` 建立初始估计。故 `T_need` 是基于实际 Tool 的时间估计，并不总是已经发生的事实完成时间。

这里直接取最大绝对时间，没有在 retention 侧把多个串行 Tool 的预测时长相加，也没有计算依赖 DAG 全部完成的 ETA。`choose_retention()` 使用的是原始 `phase==READY` 或时间接近条件，没有读取投影的 `ready` 布尔值来额外限制 near。

还有一个容易误解的事实：**无 Tool Call 的正常 response 通常把 line 变为 READY，而不是 TERMINAL。** 因此自然语言“最终回答”并不自动触发 DROP；要由 line finish 等显式生命周期事件产生终止事实，或者满足查询已无可恢复 prefix 的条件。

源码：[T_need 计算](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/projection.py:61)、[Tool 初始估计](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/resolution.py:144)、[复用事件映射](/home/liyachen/workspace/flowpilot/flowpilot/app.py:190)、[响应阶段转换](/home/liyachen/workspace/flowpilot/flowpilot/frontier/store.py:439)。

### 4.3 触发时机与控制 RPC

retention 默认关闭，配置来自 `FLOWPILOT_RETENTION_JSON`。开启后先查询 `/v1/kv/capabilities`；可查询时给普通请求添加 `kv_transfer_params.kv_control_binding`，绑定 owner/job/line/request/llm_call/attempt/context。

response 完整结束后，`finished()` 创建后台任务，用 `/resolve` 查该次调用的 descriptor。PENDING 在默认 1 秒 timeout 内每 20 ms 重试；它不阻塞响应交付，也不持有 admission credit 等待策略完成。

注册 source 后立即刷新；后台 `run()` 默认每 1 秒重新协商能力并刷新 source。每次刷新先请求 `/telemetry`，再对仍属于当前已完成 tail 的 descriptor 发 `/query`，重新计算投影和动作。当前接线不是每个 Tool/KV 事件立即 push 一次动作，后续事实通常由这个周期刷新消费。

动作经 `/apply` 提交，包含 epoch、owner、source call、tail version、递增 policy version，以及同一个 `action_id/idempotency_key`。HTTP 响应丢失时保留 `pending_command`，下次重发完全相同命令。`ACCEPTED` 表示操作在途，下次通过 `/status` 查询；在途期间不重新选动作。

两点实际限制：

1. `last_action` 相同时不会再下发新动作。即使回执为 FAILED/PARTIAL/EXPIRED，当前代码也没有“动作不变时自动提高版本重试”的策略；网络超时重发旧命令与此不同。
2. 新 tail、ACTIVE 阶段或不匹配的 call/version 会使旧 source 停止决策；引擎新请求入站也会失效该 line 的旧策略并取消其 GRACE。已经开始的传输仍按引擎安全完成路径收尾。

源码：[完成后后台 resolve](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/retention.py:247)、[refresh/apply](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/retention.py:289)、[回执处理与循环](/home/liyachen/workspace/flowpilot/flowpilot/scheduling/retention.py:412)。

## 5. vLLM KV 框架如何执行这些动作

### 5.1 配置、记录和控制入口

本地扩展位于 `vllm/v1/kv_control/`。HTTP `/v1/kv/{capabilities,resolve,query,apply,status,telemetry}` 使用现有 `call_utility_async()` 进入 EngineCore，权威 KV 修改在引擎控制路径中完成；API 进程不直接操作 GPU BlockPool。

`additional_config["kv_control"]` 默认如下：

```json
{
  "enabled": false,
  "finish_grace_ttl_ms": 0,
  "metadata_ttl_seconds": 300,
  "retention_preferences": true
}
```

因此即使开启框架，默认也没有非零 GRACE 保护窗口，必须显式配置 TTL。EngineCore 还要求使用具备自主空闲循环的 `EngineCoreProc` 多进程模式。初始化要求原生 `OffloadingConnector + CPUOffloadingSpec`、prefix caching，以及支持的布局；当前检查要求 PP/DP/DCP/PCP 均为 1，拒绝 speculative 和 canonical layout；缓存组限 FullAttention/Mamba，Mamba 要求 align 模式。TP 可大于 1，但具体模型和布局仍需对应推理验证。

主要记录是 descriptor、GRACE、policy、operation，以及按 GPU hash/CPU key/allocation 建立的反向索引。descriptor 保存引擎 hash、已计算长度、候选存储长度和恢复对象信息；它没有永久固定的 GPU/CPU tier，也不赋予恢复或驻留保证。

capabilities 目前明确返回 `restore_cost_estimate=false`、`continuation_proof=false`。存在传输统计能力，不等于已经建立 restore 成本估计模型。

源码：[配置和协议](/home/liyachen/vllm/vllm/v1/kv_control/protocol.py:24)、[初始化限制及能力](/home/liyachen/vllm/vllm/v1/kv_control/manager.py:141)、[HTTP/UTILITY](/home/liyachen/vllm/vllm/v1/kv_control/api_router.py)、[Scheduler 接线](/home/liyachen/vllm/vllm/v1/core/sched/scheduler.py:312)。

### 5.2 请求结束时：登记 descriptor，建立 GRACE

Scheduler `_free_request()` 先调用 `kv_control.finish(request)`，然后走 connector finish 和 request free，因此可在普通 request 引用释放前建立保护。

已计算范围使用：

```text
computed = min(num_tokens, max(0, num_computed_tokens-num_in_flight_tokens))
```

OFFLOAD candidate 再受 `offload_prompt_only`、`max_offload_tokens` 限制。原生 `offload_prompt_only` 默认 true，所以 descriptor 描述了生成范围，不代表默认会把全部 decode KV 放进 CPU。`max_offload_tokens=0` 也是有效上限。

对正常成功结束、未失效且 GRACE TTL>0 的 descriptor，引擎从当前索引取得已完成写入的真实 GPU replicas，按 `(block_id, allocation_generation)` 去重后调用 `pool.touch()`。touch 增加引用；原来 ref=0 的块从可回收队列移除。后续普通请求引用释放后，GRACE 引用仍保护这些块。

deadline 使用 monotonic time 和最小堆。`poll()` 到期调用 `_end_grace()`，通过 `pool.free_blocks()` 减引用；EngineCore 空闲时也把最近 deadline 作为输入等待的 timeout，因此不需要有新推理请求才释放。查询/幂等重试不会续期。

GRACE 只保护当前仍存在的范围，不重建缺失 KV。额外引用可能来自其他 GRACE、请求或传输，所以解除一个 owner 的 GRACE 不等于整个物理块已可回收。本地 common-prefix 判断也已改为检查真实 request block-table 共享关系，避免把 GRACE/传输引用误算成共享请求。

源码：[完成钩子顺序](/home/liyachen/vllm/vllm/v1/core/sched/scheduler.py:2513)、[descriptor/GRACE](/home/liyachen/vllm/vllm/v1/kv_control/manager.py:444)、[空闲 deadline](/home/liyachen/vllm/vllm/v1/engine/core.py:1498)、[真实共享关系](/home/liyachen/vllm/vllm/v1/core/single_type_kv_cache_manager.py:831)。

### 5.3 KEEP：提高回收优先级数值，不继续 pin

`apply(KEEP)` 的顺序为：登记新 policy/version → 刷新偏好 → 对目标范围 `_handoff()` → 释放对应 GRACE 引用 → 返回 APPLIED。

KEEP 本身不额外 `touch()`，也不续 GRACE。实现依靠 `pool.free_block_selector = manager._select_free`，在原生分配器需要块时从一个带版本的最小堆选择 **ref_cnt==0** 的块。

| `_priority()` 返回值 | 空闲块类别 | 回收顺序 |
|---|---|---|
| -1 | 没有缓存 hash 的块 | 最先 |
| 1 | 有有效 OFFLOAD policy，且整个目标恢复范围已经 CPU-backed | 其次 |
| 2 | 普通缓存，或没有完整有效 CPU 备份偏好的缓存 | 再次 |
| 3 | 至少一个有效 owner 对该范围有 KEEP policy | 最后 |

堆实际按 `(priority, free_recency, block_id, allocation_generation, version)` 排序。同类别按进入可回收状态的先后排列；只刷新偏好不会重置原来的 recency。旧 generation/version 堆条目会被跳过。共享块只要有有效 KEEP，优先级直接为 3，覆盖其他 descriptor 的 CPU-backed 倾向。

`BlockPool.get_new_blocks()` 取到候选后撤销旧缓存映射，推进 allocation generation 并为新分配取得引用。KEEP 块仍可被选中，因此 KEEP 的 APPLIED 只证明偏好已接管，**不保证下一请求到达时 KV 仍在 GPU**。

源码：[KEEP 分支](/home/liyachen/vllm/vllm/v1/kv_control/manager.py:1094)、[四类优先级和选择器](/home/liyachen/vllm/vllm/v1/kv_control/manager.py:1389)、[原生分配](/home/liyachen/vllm/vllm/v1/core/block_pool.py:667)。

### 5.4 OFFLOAD：取得复制保护，提交原生 D2H，完成后降低 GPU 偏好

FlowPilot 当前不指定 `target_resume_token_end`，vLLM `_target()` 为 OFFLOAD 选择 candidate 范围内对齐的恢复点，按所有 cache groups 生成所需对象。FullAttention 需要相应前缀 chunks，Mamba/COW 类型按目标检查点处理，不把不同组对象数量简单相加当成可恢复前缀。

逐对象处理：

1. CPU READY：复用已有副本，计入 reused bytes，交接该对象的 GRACE。
2. CPU PENDING：成为已有原生 store job 的 observer，不重复复制。
3. CPU 缺失：解析当前 GPU hash 对应的有效 allocation，调用 `enqueue_store_for_descriptor()`。
4. connector 用 CPU manager `prepare_store()` 接纳目标，再为去重后的 GPU 源块 `pool.touch()`，创建原生 `TransferJob` 并登记 `_block_id_to_pending_jobs` fence。
5. 已取得复制保护后，控制器才释放这些对象对应的 GRACE；同一 allocation 关联多个对象时，要全部满足交接条件才释放该 GRACE 引用。

新任务使用 descriptor 自己的 `ReqContext` 和 `request_owned=False`，因此旧 Request 对象清理后仍可执行。worker 使用原生 store/transfer 路径；所有必需 worker 报告完成后，manager 才将成功的 CPU 数据发布为 READY。失败按原生完成路径清理，最后释放复制持有的 GPU 引用。

有 pending job 时回执为 ACCEPTED。结束时 `_offload_terminal()` 检查完整目标：全部目标 CPU READY 且原生 CPU-only prefix 查询能覆盖目标，才为 APPLIED；只有部分对象 READY 为 PARTIAL；都不满足则 FAILED。

**OFFLOAD 完成不会调用 GPU 定向删除。** 它将“该 descriptor 的整个目标范围可独立从 CPU 恢复”记为 cpu_backed，使相应 GPU 空闲块优先级变为 1；没有分配压力时 GPU+CPU 两份可以同时存在。CPU 副本后来被淘汰时，反向索引使 descriptor 变 dirty，完整性重算后撤销失效的 cpu_backed 偏好。

CPU 不足、STORE_THRESHOLD、SOURCE_LOST 或 CONFIG_LIMIT 会进入 skipped/失败/部分完成结果，不被当成成功卸载；未交接的 GRACE 继续按原 TTL 处理。

源码：[目标恢复点](/home/liyachen/vllm/vllm/v1/kv_control/manager.py:900)、[OFFLOAD 分支](/home/liyachen/vllm/vllm/v1/kv_control/manager.py:1105)、[原生 descriptor store](/home/liyachen/vllm/vllm/distributed/kv_transfer/kv_connector/v1/offloading/scheduler.py:1023)、[完成聚合](/home/liyachen/vllm/vllm/distributed/kv_transfer/kv_connector/v1/offloading/scheduler.py:1842)、[完整性与终态](/home/liyachen/vllm/vllm/v1/kv_control/manager.py:1190)。

### 5.5 DROP：撤销本 owner 需求，安全删除可处理副本

`apply(DROP)` 先记录当前目标 GPU allocation generation、cache mapping generation 和 CPU generation，然后解除对应 GRACE 并尝试删除。

GPU 删除检查其他 owner 的有效 KEEP/OFFLOAD 需求、connector 在途任务和 `ref_cnt`。只有安全时才调用 `pool.evict_if_idle()`；该函数撤销 hash 映射并把块前置为易分配候选，**不会再次减掉请求引用，也不会释放整个预分配 CUDA 内存池**。

CPU 删除要求 generation 一致、数据 READY 且引用数为零，之后撤销 CPU 索引并归还 CPU slot。遇到 `OTHER_DEMAND`、`IN_FLIGHT`、`SHARED_REQUEST_OR_GRACE` 等条件会跳过并返回 PARTIAL。后续相关事件通过 dirty descriptor 触发 `_retry_drop()`，可以继续清理；新策略、新 allocation 或新 mapping 不会被旧 DROP 意图误删。

DROP 的 PARTIAL 回执保持当时快照，后续清理完成不一定把该 receipt 改成 APPLIED；现有测试明确验证了这一点。应结合后续 query/telemetry 观察当前驻留，不能仅看旧 receipt 判断现在是否仍有 KV。

源码：[DROP 分支](/home/liyachen/vllm/vllm/v1/kv_control/manager.py:1099)、[捕获和重试删除](/home/liyachen/vllm/vllm/v1/kv_control/manager.py:1253)、[GPU 删除原语](/home/liyachen/vllm/vllm/v1/core/block_pool.py:802)、[CPU 删除原语](/home/liyachen/vllm/vllm/v1/kv_offload/cpu/manager.py:113)。

### 5.6 CPU KV 的容量淘汰与后继请求恢复

CPU KV 容量由 `CPUOffloadingManager` 独立管理。`CPUOffloadingSpec` 的 `eviction_policy` 默认是 `lru`，也支持配置 `arc` 或其他注册 policy；框架不会根据 FlowPilot 的 SLO 自动切换这些 policy。

默认 LRU 维护 `evictable_blocks` OrderedDict。store 完成、load 完成回到可回收状态、显式 touch 会影响其位置。纯 `lookup()/replica_state()` 不 touch。引用状态为：`-1` 表示正在写入，`0` 表示 READY 且可淘汰，正数表示正在被 load 使用。容量不足时只从可淘汰对象中取最旧项，并跳过本次 `prepare_store()` 的保护集合；不能满足所需数量则拒绝本次接纳。

CPU `store_threshold>=2` 时，还会按对象被提交存储的次数过滤候选；默认 spec 值为 0，不启用计数门槛。FlowPilot 的 OFFLOAD 同样走该接纳机制，没有绕过 CPU policy。OFFLOAD policy 也没有给 CPU 副本增加永久保留引用。

后继普通请求进入 vLLM 后，原生 `get_num_new_matched_tokens()` 使用真实请求 hash 和 backend lookup 获取可加载范围；`update_state_after_alloc()` 在 GPU 目标分配后 `prepare_load()`，提交 CPU→GPU 的原生 load job。引擎可以在自己的 `WAITING_FOR_REMOTE_KVS` 等状态中等待异步加载；**FlowPilot 外部 admission 没有这个等待门槛**。

当前这条 CPU 路径主要由 native lookup、布局/范围限制、资源条件和传输状态驱动，没有在 FlowPilot 侧用“restore 估计比重算便宜”来选择是否恢复。CPU 副本不可用时，实际未命中范围由引擎正常计算路径处理。

源码：[CPU 接纳和引用](/home/liyachen/vllm/vllm/v1/kv_offload/cpu/manager.py:170)、[默认 LRU](/home/liyachen/vllm/vllm/v1/kv_offload/cpu/policies/lru.py)、[CPU policy 配置](/home/liyachen/vllm/vllm/v1/kv_offload/cpu/spec.py:135)、[普通请求 CPU 命中/加载](/home/liyachen/vllm/vllm/distributed/kv_transfer/kv_connector/v1/offloading/scheduler.py:1130)。

### 5.7 descriptor 查询能证明什么

`_observe()` 通过只接受已完成写入块的 `ReadyBlockPool` 视图，复用 coordinator 的最长 GPU prefix 算法，再通过 connector `query_prefix()` 得到 GPU+CPU 可恢复范围和 CPU 独立范围。结果区分块驻留数量与真正可消费 prefix；中间缺块、混合组不完整或 CPU PENDING 都会影响可恢复结果。

ID-only 查询返回 `DESCRIPTOR_ONLY`，不声明新请求 token 工作量；额外传入下一请求长度也只标 `ASSUMED_CONTINUATION`，并约束最多使用到 `next_prompt_tokens-1`。已知 skip-cache/prompt-logprobs 选项可以使查询命中量归零，但保留驻留计数。

prefix lookup 本身不 touch、pin 或创建 load/store；不过公开 `query()` 会先调用 `poll()`，因此该请求可能顺便处理已经到期的 GRACE 或已完成操作。准确说法是“查询不为了命中而获取引用或恢复”，而不是整个 RPC 绝对不推进任何引擎状态。

源码：[query 和动态观察](/home/liyachen/vllm/vllm/v1/kv_control/manager.py:634)、[只读 CPU prefix 算法入口](/home/liyachen/vllm/vllm/distributed/kv_transfer/kv_connector/v1/offloading/scheduler.py:1004)。

## 6. 本次核对和验证

本次以生产调用链为主，测试作为补充证据。运行结果如下：

| 仓库 | 实际命令 | 结果 |
|---|---|---|
| FlowPilot | `.venv/bin/python -m pytest tests/test_admission.py tests/test_retention.py tests/test_cache_retention.py -q` | 31 passed，1.14 秒 |
| vLLM | `.venv/bin/python -m pytest tests/v1/engine/test_kv_control_manager.py tests/v1/engine/test_kv_control_protocol.py -q` | 34 passed，13.47 秒；51 warnings，主要为 NVML 初始化和 Torch 弃用提示 |

FlowPilot 用例覆盖连续评分、credit/取消/断连、heartbeat 过期、动作选择、异步 receipt、幂等重试、CPU-only 不阻塞提交、出站协议与本地 vLLM 类型一致，以及 Tool Cache 的价值淘汰和交付保护。

vLLM 用例使用真实原生 pool/manager 加测试 fixture，覆盖 GRACE、软 KEEP 可被回收、复制引用生命周期、CPU eviction 后偏好撤销、共享 DROP 重试、generation 隔离、空闲 TTL、partial handoff、prefix 缺口等。部分 worker 完成消息由测试构造；**这些通过结果证明局部代码和协议行为，不等于真实 GPU DMA、目标模型数值正确性、线上性能或端到端推理已在本次得到验证**。

本文没有运行新 GPU 推理、性能基准或全仓库测试，也没有修改 OpenHands、FlowPilot 或 vLLM 的生产实现。当前能据代码确认的是：request 使用连续加权准入，Tool Cache 使用 saved-work-per-byte 淘汰，KV 动作使用阈值分支，vLLM 用短时引用保护和后续软偏好执行这些动作；prefix/restore 成本驱动的统一调度尚未接入当前 admission 算法。
