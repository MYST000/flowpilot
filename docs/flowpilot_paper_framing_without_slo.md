# 采用 W−K 调度后，FlowPilot 论文应如何组织

## 核心建议

论文可以围绕 **“协调 Tool reuse 与推理续接，减少 Agent 多轮执行中的等待和 KV 启动开销”** 展开。

Tool 结果提前返回，不等于推理立即恢复。恢复取决于依赖是否满足、KV 是否仍可用，以及完整请求是否被准入。FlowPilot 的主要价值是把这些条件连接起来，使 Tool reuse 的局部收益有机会转化为端到端完成时间和系统完成吞吐的改善。

`W−K` 是这一协调机制中的一个轻量准入策略。它不是整篇论文的中心，也不是端到端成本目标函数。

## 1. W−K 的准确含义

对完整、可提交的请求，使用：

$$P_r=W_r-K_r$$

分数越大越优先。$W_r$ 是当前调用在外部队列中的等待时间；$K_r$ 是真实目标输入在当前 KV 状态下的恢复与残余 prefill 成本。

它表达两种偏好：

- 等待相同，优先启动成本较低的请求。
- 启动成本相同，优先已经等待更久的请求。

更直观的解释是：**缓存带来的启动优势，可以抵消一部分到达时间差；等待较久的请求仍能获得优先权。**

设请求到达时间为 $a_r$，因为 $W_r=t-a_r$，有：

$$\arg\max_r(W_r-K_r)=\arg\min_r(a_r+K_r).$$

因此，可以将其理解为用 KV 启动成本修正 FIFO。两条请求的启动成本相差 200 ms 时，较便宜的请求可以在晚到不足 200 ms 的情况下获得更高优先级。若当前 KV 状态固定，两个已经排队请求之间的相对顺序不会仅因时间流逝而改变；实际 KV 状态更新会改变排序。

这个策略没有估计剩余 decode、剩余轮数或当前分支的重要性。因此，不能从该公式推出最短 workflow 优先、关键路径优先、最优 JCT 或最大完成吞吐。较低的启动成本也不代表整个 LLM 调用较短。

## 2. 论文主线应如何连接三个模块

建议主线为：**Tool reuse 改变推理需求何时到达；KV placement 改变需求到达后的启动成本；admission 根据实际启动成本和等待时间决定推进顺序。**

| 模块 | 解决的问题 | 与下一模块的关系 |
|---|---|---|
| Tool reuse / in-flight coalescing | 避免可复用的外部执行与重复工作 | 改变下一轮推理的形成时间和剩余 gap |
| Tool-aware KV placement | 在空闲驻留与后续恢复之间取舍 | 改变后继完整请求的实际 KV 启动成本 |
| W−K admission | 在低启动成本与请求等待之间取舍 | 改变外部队列延迟，并向后续 placement 提供负载反馈 |

如果加入上一份简化方案的排队反馈，KV 的预计空闲窗口使用：

$$H_r=G_r+\widehat Q.$$

这里 $G_r$ 来自真实 reuse resolution 与 Tool 时长估计，$\widehat Q$ 来自近期实测 admission wait。它是粗略反馈，不需要预测完整 workflow。当前 response 的 placement 仍可保持首次选择后冻结，在后续轮次读取新的反馈。

一个有代表性的失败情形是：Tool hit 使后继推理很快形成，但原先按较长 Tool duration 选择的 OFFLOAD 带来恢复开销；另一个情形是：Tool 已完成但其他依赖或 admission queue 仍造成等待，此时继续 KEEP 会占用其他请求需要的容量。

论文应解释系统怎样识别这些条件并协调决策。请求排序本身不会凭空减少总 prefill 工作；其端到端收益取决于队列竞争、缓存状态变化和后续执行，必须实测。

## 3. Critical path 的位置

Critical path 仍适合作为背景，解释为什么只有影响决定完成时间的分支的延迟减少，才能缩短并行 workflow 的完成时间。

但 `W−K` 没有识别完整未来 DAG、剩余关键路径或分支 slack，不能将系统命名或描述为由它驱动的 critical-path-aware scheduler。

可以写：

> FlowPilot targets repeated tool waits, KV recovery, and admission delays during agent execution. Coordinating these stages can reduce end-to-end latency when the affected delays lie on the realized critical path.

上述句子描述机制及其生效条件。若实验做了真实执行路径归因，可以进一步报告关键路径上哪些等待或恢复被减少。

若希望保留“优先调度关键路径”的强主张，需要额外的关键路径识别或可信近似机制及证据。这不是简化公式自带的性质。

## 4. 附件论文中需要修改的主线

本节页码已对照所提供的 10 页 PDF 核对。

| 当前位置 | 当前叙事 | 建议调整 |
|---|---|---|
| 标题、摘要、引言（第 1–2 页） | SLO-aware serving；slack 与 importance 指导 admission | 围绕高效多轮续接，解释 Tool/KV/admission 的相互影响；把 SLO goodput 保留为可选评价指标 |
| §2.2.1（第 2–3 页） | Tool Reuse Shifts the Timing of KV Demand | 保留，作为核心 observation |
| §2.2.2（第 3 页） | Workflow SLOs Determine the Cost of KV Recovery | 改为启动成本与等待的相互影响：同样 ready 的请求可能具有不同恢复成本，Tool 省时也可能被续接开销抵消 |
| §2.3（第 3–4 页） | 识别 shifting critical path、uncertain slack、slack-aware prioritization | 改为估计实际续接时间、取舍 KV 驻留与恢复、在启动效率和累计等待之间排序 |
| §3.2（第 4 页） | SLO Steering Workflow Coordination | 解释时序闭环：reuse outcome → gap → placement → actual startup cost → admission → observed queue wait |
| §4.2.2（第 6–7 页） | 先最小化预算超支，再比较成本 | 如果采用整体无 SLO 方案，去掉预算超支这一首要分支，保留合法动作下的恢复、传输、驻留比较 |
| §5（第 7–8 页） | Slack- and Importance-Aware Admission | 改为 Resumption-Cost-Aware Admission，给出 W−K 的含义、真实 prefix 刷新和执行边界 |

需要强调：如果只有 admission 改成 `W−K`，而 retention 仍优先使用 remaining SLO budget，那么系统依旧是部分 SLO-aware。全文是否去掉 SLO 主线，要与实际采用的 retention 方案一致。

## 5. 新旧策略改变了什么

附件 §5 原来按较小的剩余 prefill slack 优先：

$$L_r=D_j-t-K_r.$$

| 比较维度 | 原 slack 策略 | 新 W−K 策略 |
|---|---|---|
| 主要判断 | 距 deadline 还剩多少余量 | 已等多久、当前启动是否便宜 |
| 相同 deadline 下 | K 大的请求更紧迫 | 仅在 W 相同时，K 小的请求优先 |
| 关注点 | deadline 风险 | 当前续接效率与等待补偿 |
| 所需局部信息 | deadline、当前启动成本 | 请求到达时间、当前启动成本 |

这是一项策略取舍的变化，不能仅把原文的 SLO 词汇删掉、保留其余关键路径推理。

## 6. 可用的论文中心表述与标题方向

建议中心表述：

> FlowPilot coordinates tool-result reuse, KV retention, and request admission to reduce repeated waiting and resumption overhead in agent serving.

更突出 insight 的表述：

> Tool reuse changes when inference state is needed. FlowPilot propagates this timing change into KV management and uses the resulting resumption cost to guide request admission.

可保留现有标题的前半部分，候选标题为：

> When Tool Reuse Changes Cache Demand: FlowPilot for Efficient Agent Serving

这里的标题与句子是建议，未替换原论文，也未进行标题碰撞检索。

## 7. 需要什么证据支撑这条主线

主要结果使用 mean/P95 workflow JCT，以及固定资源和负载条件下的成功完成 workflow/s；任务质量保持一致。SLO attainment 可以辅助报告，但不是新排序公式的直接优化保证。

最小对照至少包括 Tool reuse only、Tool-aware KV placement，以及二者加 W−K admission；admission 再与 FIFO 对照。若纳入 G+Q 反馈，单独去掉该反馈，检验新增闭环是否有效。

最有价值的机制证据是：Tool 时长下降后，后继请求的外部等待、实际 KV 恢复/重算和最终完成时间分别怎样变化。并行 workflow 的分支等待不能简单相加当作 JCT，需要区分请求级统计和 workflow 完成路径归因。

附件第 9 页目前展示的是 Tool prediction 的离线误差和独立 prediction overhead，且明确说明未隔离对端到端 SLO goodput 的贡献。这些结果仍能支撑预测器选择，但不足以证明新的多模块协调主线。还需要真实 workflow 的端到端和消融结果。
