# KV Cache 与 Tool Cache 联合调度：最简请求排序方案

建议请求调度只使用两个因素：**已经等待的时间**和**当前 KV 启动成本**。Tool prediction 继续指导 KV 去留；调度器不预测剩余轮数或 decode 时间，也不求解全局优化。

以下是依据三份附件提出的设计方案，尚未验证真实系统收益。

## 1. 请求排序只用一个公式

对已经形成、依赖屏障已满足的完整请求 $r$，定义：

$\boxed{P_r = W_r - K_r}$

**分数越大，越优先进入 inference engine。**

- $W_r$：当前请求在外部 admission queue 中已经等待的时间。
- $K_r$：当前输入在已有 KV 状态下，预计需要的恢复与残余 prefill 时间。

两项都使用毫秒，第一版直接相减，不增加权重参数。

$K_r$ 复用 design.md 已有的真实 target-prefix query 和离线成本标定：

| 当前可用状态 | KV 启动成本 $K_r$ |
|---|---|
| GPU 可消费 prefix | 残余 prefill 时间；接近完整命中时较低 |
| CPU 恢复候选 | 条件 H2D 恢复时间 + 残余 prefill 时间 |
| 无可用 prefix | cold prefill 时间 |

GPU 命中不能一律视为零成本；CPU 回执也不能替代实际恢复范围。该成本只描述启动，不包含后续 decode 或完整 workflow 时长。

这个排序有两个直接效果：**等待相同，优先低启动成本；启动成本相同，优先已经等待更久的请求。**

例如，下面仅是说明排序规则的假设数值：

| 请求 | 等待 $W_r$ | 启动成本 $K_r$ | 分数 $P_r$ |
|---|---:|---:|---:|
| A：GPU 高命中 | 200 ms | 20 ms | 180 ms |
| B：冷启动 | 200 ms | 300 ms | −100 ms |
| C：等待较久的冷请求 | 600 ms | 300 ms | 300 ms |

排序为 **C → A → B**。缓存就绪带来优势，累计等待又能补偿较高的启动成本。持续有可用 credit、启动成本有界时，aging 能避免新到达的低成本请求长期压住旧请求；它不构成完整 workflow 的公平份额保证。

## 2. Tool prediction 保持现有职责

Tool 侧继续提供预计剩余 gap $G_r$：

- 已确认的历史缓存命中：gap 为零。
- in-flight follower：等待真实 leader，预测其剩余时间。
- 本地执行：预测尚未完成的执行时间。
- 未知或低置信度：保留 unknown，走现有显式处理路径。

多个 Tool 继续遵循当前串行执行语义。**预测 gap 归零不等于请求已经 ready。** 只有真实结果、依赖和上下文条件满足，并形成完整请求后，才进入 admission queue。

请求入队后，Tool 结果已经体现于上下文和实际 KV 查询中，无需再把“Tool hit”加成到排序公式。这样不会对同一次命中重复奖励。

## 3. 闭环只增加一条反馈

请求排序影响外部排队等待。把这个等待反馈给 KV 模块，将下一次使用前的窗口近似为：

$\boxed{H_r = G_r + \widehat Q}$

$\widehat Q$ 是近期完整请求的**实测 admission wait 的滑动平均**；当前队列为空且有可用 credit 时，可以取零。没有有效观测时保持 unknown，不猜造数值。它是粗略的负载反馈，不预测每条请求的 decode，也不保证未来等待时间。

KV 模块在现有首次 `choose_retention()` 时读取该反馈，用 $H_r$ 评估驻留窗口；KEEP/OFFLOAD/DROP 仍使用现有 prefix、传输、容量和能力判断。窗口估计不成为恢复屏障，也不阻止 CPU-only 请求正常提交。

一个重要情形是：**Tool 已命中，但队列仍很忙。** 此时 $G_r=0$，$H_r$ 仍可能较长，所以 KV 不应仅因为 Tool hit 就被优先保留在 GPU。

闭环由以下关系构成：

> Tool cache 与时长预测决定 gap → gap 与实测排队反馈指导 KV 去留 → 实际 KV 状态决定请求启动成本 → 启动成本与累计等待决定 admission 顺序 → 新的实测等待反馈到后续 KV 决策。

这是按实际请求轮次更新的反馈。第一版保留现有 placement 选择后的冻结机制，不增加同一份 KV 的反复重评估。

## 4. 融合到 design.md 的最小修改

| 模块 | 改动 |
|---|---|
| Admission | 将“剩余 SLO 减去条件 prefill 成本”排序替换为 `queue_wait_ms - kv_start_cost_ms`，按分数降序。 |
| Cost / target-prefix query | 复用已有查询和标定；刷新排队请求的真实 prefix 状态。缺失成本继续走明确的既有处理路径。 |
| Scheduling telemetry | 维护近期实测 admission wait 的滑动平均，作为粗略排队反馈。 |
| Retention | 在首次选择时使用 `tool_gap + estimated_queue_wait` 评估驻留窗口；动作合法性、冻结和回执机制保留。 |
| Tool reuse / Forecast | 保留现有 hit、follower、local execution 和事实覆盖预测的语义；第一版不修改 Tool-cache eviction。 |

调度仍在 FlowPilot 外部队列中进行；vLLM 继续负责内部 batch、实际 KV 恢复和重算。这里的两个时间成本不要求更改 Tool executor 或引擎恢复顺序。

## 5. 需要验证的范围

主张限定为：**用 KV 启动成本修正普通等待排序，并让排队负载反馈到 Tool-aware KV retention。** 不将它表述为已保证端到端 SLO 的调度器。

最小对照使用 FIFO、仅加入 `W−K` 排序、再加入 `G+estimated_queue_wait` 的 retention 反馈。比较 workflow JCT、P95 JCT、完成 workflow/s，以及实际恢复和重算开销。

该策略主要针对启动与缓存造成的阻塞。若长 decode 主导 credit 占用，其收益可能有限，需要由端到端实验确认。
