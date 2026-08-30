# DAG、Tool Cache 与 KV Cache 的 SLO 联合调度

> 文档定位：本文件是便于阅读的专题说明；权威系统契约位于 [`../design.md`](../design.md)。若两者存在差异，以 `design.md` 为准。预测器由外部模块实现，本文件只描述 FlowPilot 的占位接口消费语义。

## 1. 核心结论

三方调度的职责分别是：

- **DAG** 决定请求在 workflow 中的重要性；
- **SLO** 决定请求当前的紧迫性；
- **Tool Cache** 决定 Tool Result 何时可用；
- **KV Cache** 决定 Tool Result 可用后，请求 2 还需要等待多久。

Tool Cache 与 KV Cache 不共享物理容量，因此不在二者之间做容量竞争。它们通过请求 1 到请求 2 的时序发生耦合：

~~~text
Tool Cache 命中或 Tool 执行
        -> Tool Result ready
        -> Agent 构造请求 2
        -> KV ready
        -> 请求 2 开始 LLM 推理
~~~

联合调度的直接目标是使 Tool ready 与 KV ready 尽可能接近，减少其中一个阶段完成后等待另一个阶段的时间。

## 2. 整体优化目标

不单独最大化原始吞吐率、Tool Cache 命中率或 KV Cache 命中率。主要目标是：

> 最大化在 SLO 内完成的 workflow 数量，即 SLO goodput；在此基础上最小化 workflow 完成时间、重复 Tool 执行和 KV 恢复或重算开销。

设 workflow \(j\) 的到达时间、deadline、实际完成时间、权重和 SLO 长度分别为 \(A_j,D_j,C_j,w_j,S_j=D_j-A_j\)。

SLO goodput 为：

\[
Goodput_{SLO}
=
\frac{1}{H}\sum_j w_j\mathbf{1}[C_j\le D_j]
\]

整体目标函数为：

\[
\begin{aligned}
\min J ={}&
\lambda_m\sum_j w_j\mathbf{1}[C_j>D_j] \\
&+\lambda_l\sum_j w_j\frac{[C_j-D_j]^+}{S_j}\\
&+\lambda_f\sum_j w_j\frac{C_j-A_j}{S_j}\\
&+\lambda_T C_{\text{duplicate-tool}}\\
&+\lambda_K(C_{\text{restore}}+C_{\text{rematerialize}})\\
&+\lambda_W C_{\text{wasted-prewarm}} .
\end{aligned}
\]

建议满足：

\[
\lambda_m \gg \lambda_l \gg \lambda_f
\]

优化优先级依次是：

1. 减少 SLO miss；
2. 减少超时程度；
3. 减少 workflow 完成时间；
4. 减少重复 Tool 执行；
5. 减少 KV restore、重算和无效预热。

因此，系统优化的是 **SLO-satisfied workflow throughput**，而不是不区分请求重要性的原始吞吐率。

## 3. DAG 与 SLO：请求调度权重

DAG 不加入预测 Tool，也不估算请求 1 到请求 2 的完整未来路径时间，只描述已经存在的依赖关系。

对请求 \(q\) 定义：

- \(B_q\)：完成 \(q\) 可以解除阻塞的活跃后继请求数量；
- \(H_q\)：\(q\) 在 DAG 中的下游结构深度；
- \(Age_q(t)\)：\(q\) 已经等待的时间；
- \(A_{\text{ref}}\)：等待年龄参考值。

请求的结构重要性为：

\[
\kappa_q(t)
=
1
+\alpha\log(1+B_q)
+\beta\frac{H_q}{H_{\max}}
+\gamma\frac{Age_q(t)}{A_{\text{ref}}}.
\]

该公式不使用 Tool 执行时间或 decode 时间，只回答：

> 请求 \(q\) 完成后，对 workflow 的推进有多重要？

### 3.1 SLO 紧迫度

不预测完整剩余关键路径，直接根据剩余 deadline budget 计算：

\[
U_j(t)
=
\min\left(
U_{\max},
\frac{S_j}{\max(D_j-t,0)+\epsilon S_j}
+\lambda_o\frac{[t-D_j]^+}{S_j}
\right).
\]

越接近或超过 deadline，\(U_j(t)\) 越大。

请求的联合权重为：

\[
W_q(t)=w_j\kappa_q(t)U_j(t).
\]

### 3.2 请求队列优先级

先按照 tenant/job 分配公平份额，再在份额内使用：

\[
Priority_{\text{request}}(q)=W_q(t).
\]

DAG 只决定重要性和阻塞价值，不负责预测未来 Tool，也不创建虚假的 DAG 节点。

## 4. 请求 1 到达：预测与推理重叠

请求 1 到达 Scheduler 时，同时启动两条路径：

~~~text
路径 A：请求 1 -> LLM 推理
路径 B：Tool 类型和时间预测 -> Tool Cache 预热
~~~

预测器输出 Top-N 候选：

\[
\mathcal P_q=
\{(f_k,p_{qk},\hat\tau^{50}_{qk},\hat\tau^{90}_{qk})\}_{k=1}^{N},
\]

其中：

- \(f_k\)：预测的 Tool 类型；
- \(p_{qk}\)：该类型出现的概率；
- \(\hat\tau^{50}_{qk},\hat\tau^{90}_{qk}\)：执行时间的 P50 和 P90 预测。

预测开销与请求 1 的 LLM 推理重叠，不应成为请求 1 的额外串行路径。若预测任务争用资源并拖慢推理，则应降低预测优先级或取消低价值预测。

预测不改变 DAG，不执行 Tool，也不能在 Tool Call 尚未到达时认定已经命中某个具体缓存结果。

## 5. Tool Cache 预热与驻留

设 Tool Cache 条目 \(o\) 的类型为 \(f_o\)，并定义：

- \(m_{qko}\)：预测类型为 \(f_k\) 时，未来参数与条目 \(o\) 匹配的概率；
- \(Fresh(o)\)：条目 freshness 的有效度；
- \(C_{\text{warm}}(o)\)：预热开销。

请求 \(q\) 使用条目 \(o\) 的预测概率为：

\[
P_{\text{use}}(q,o)
=
Fresh(o)
\sum_{k=1}^{N}
p_{qk}\mathbf{1}[f_k=f_o]m_{qko}.
\]

该概率只用于预热和排序。Tool Call 到达后的实际参数、权限、scope 和 freshness 仍然是命中的硬条件。

### 5.1 端到端预热价值

设 \(T^{miss}_{2,q}\) 和 \(T^{hit}_{2,q}\) 分别为 Tool miss 与 hit 时请求 2 的预计启动时间。缓存命中的端到端节省为：

\[
\Delta T_{q,o}
=
[T^{miss}_{2,q}-T^{hit}_{2,q}]^+.
\]

条目 \(o\) 的预热价值为：

\[
V_{\text{tool}}(o)
=
\sum_q W_q(t)P_{\text{use}}(q,o)\Delta T_{q,o}
-C_{\text{warm}}(o).
\]

Scheduler 优先预热 \(V_{\text{tool}}(o)\) 较大的条目。

若 Tool Cache 的独立容量为 \(B_T\)，则选择：

\[
\max_{x_o\in\{0,1\}}\sum_o x_oV_{\text{tool}}(o)
\]

满足：

\[
\sum_o x_oSize(o)\le B_T.
\]

这是 Tool Cache 自身的容量约束，与 KV Cache 容量相互独立。

### 5.2 Tool Cache 驻留价值

历史统计可用后，条目的驻留价值可以定义为：

\[
V_{\text{res}}(o)
=
\sum_q W_q(t)P_{\text{hit}}(q,o)
(\tau^{miss}_q-\tau^{hit}_q)
Fresh(o)
-C_{\text{store}}(o).
\]

其中 \(P_{\text{hit}}\) 可以由已发生的命中、历史频率、当前 follower 和预测类型共同估计。预测只改变先验，实际命中结果必须覆盖预测。

## 6. Tool Call 1 到达：预测切换为事实

Tool Call 1 到达 Scheduler 时，已经知道实际的 Tool 名称、参数、调用 ID、scope 和 freshness 要求。此时进行真实 Tool Cache 查询：

\[
h_q\in\{0,1\}.
\]

### 6.1 Tool Cache 命中

若 \(h_q=1\)，Tool Result ready 时间为：

\[
\hat T_{\text{tool}}(q)
=
t_c+C_{\text{lookup}}+C_{\text{validate}}+C_{\text{deliver}},
\]

其中 \(t_c\) 是 Tool Call 到达 Scheduler 的时间。之前的执行时间预测不再用于该次 Tool ready 时间。

### 6.2 Tool Cache 未命中

若 \(h_q=0\)，Tool 由 Agent 本地执行。对实际 Tool 类型对应的候选 \(k^*\)，根据 SLO 紧迫度选择预测分位数：

\[
z_q(t)
=
\frac{U_j(t)-1}{U_{\max}-1},
\]

\[
\hat\tau_q
=
(1-z_q)\hat\tau^{50}_{qk^*}
+z_q\hat\tau^{90}_{qk^*}.
\]

于是：

\[
\hat T_{\text{tool}}(q)=t_c+\hat\tau_q.
\]

越接近 deadline，越偏向使用 P90 预测，使 KV 策略更保守。

如果实际 Tool 不在 Top-N 中，则退化到该 Tool 类型的历史 P90；如果没有可用历史，则使用不依赖预测的 wait-age 策略。

## 7. Tool Cache 与 KV Cache 的联合时间模型

设 Agent 收到 Tool Result 并形成请求 2 需要 \(C_{\text{agent}}\) 时间：

\[
\hat T_{\text{need}}(q)
=
\hat T_{\text{tool}}(q)+C_{\text{agent}}.
\]

对于 KV 动作 \(a\)，设 KV 可用时间为 \(T_{\text{KV}}(q,a)\)。请求 2 最早启动时间为：

\[
T_2(q,a)
=
\max\left(
\hat T_{\text{need}}(q),
T_{\text{KV}}(q,a)
\right).
\]

这就是两类 Cache 的联合点：

- Tool Cache 改变 \(\hat T_{\text{need}}\)；
- KV 动作改变 \(T_{\text{KV}}\)；
- 二者共同决定 \(T_2\)。

## 8. KV Cache 动作选择

KV 动作包括 KEEP、OFFLOAD、DROP 和 RESTORE。

### 8.1 KEEP

若 KV 一直保留：

\[
T_{\text{KV}}(q,\text{KEEP})=t_c.
\]

等待期间的机会成本为：

\[
C_{\text{KEEP}}
=
\mu_{\text{GPU}}Size_{\text{KV}}
(\hat T_{\text{need}}-t_c).
\]

这是 KV 自身占用 GPU 的机会成本，不是与 Tool Cache 的物理资源竞争。

### 8.2 OFFLOAD

设 offload 成本为 \(C_{\text{off}}\)，restore 时长为 \(R_q\)，restore 开始时间为 \(s_q\)：

\[
T_{\text{KV}}(q,\text{OFFLOAD})=s_q+R_q.
\]

理想 restore 开始时间为：

\[
s_q^*
=
\max(t_c,\hat T_{\text{need}}(q)-R_q).
\]

它尽量使 restore 在请求 2 需要 KV 时完成。

### 8.3 DROP

设 KV 重算时间为 \(M_q\)，则：

\[
T_{\text{KV}}(q,\text{DROP})
=
\hat T_{\text{need}}(q)+M_q.
\]

KV bytes、restore cost 和 rematerialization cost 必须来自推理引擎的真实遥测。Scheduler 不能根据 token 数自行虚构；不可用时应标记为 kv_telemetry=unsupported。

### 8.4 KV 动作决策

定义 KV 动作造成的额外等待：

\[
Delay_{\text{KV}}(q,a)
=
[T_{\text{KV}}(q,a)-\hat T_{\text{need}}(q)]^+.
\]

选择：

\[
a_q^*
=
\arg\min_a
\left[
W_q(t)Delay_{\text{KV}}(q,a)
+
C_{\text{action}}(q,a)
\right].
\]

其含义是：

- DAG 重要且 SLO 紧迫的请求，\(W_q\) 较大，更倾向 KEEP 或提前 RESTORE；
- Tool miss 且执行时间长时，KV 长时间等待，更可能 OFFLOAD；
- Tool hit 使 \(\hat T_{\text{need}}\) 提前，应立即提高 KV restore 优先级；
- restore 比重算慢或贵时，可以 DROP；
- 普通请求不会为了消除很小的 stall 长期占用 GPU KV。

## 9. 多请求的 KV Restore 排队

定义请求 \(q\) 的 restore laxity：

\[
L_q^{KV}(t)
=
\hat T_{\text{need}}(q)-t-R_q.
\]

它表示距离最迟启动 restore 还有多久。

Restore 优先级为：

\[
Priority_{\text{restore}}(q)
=
\frac{W_q(t)}
{\max(L_q^{KV}(t),0)+\epsilon}.
\]

若 \(L_q^{KV}\le0\)，说明已经错过理想恢复时机，应进入 overdue restore 队列。

## 10. 请求 2 的 LLM 调度

请求 2 只有满足以下条件后才能进入 LLM 队列：

\[
Ready(q_2)
=
DAGPredReady(q_2)
\land ToolResultReady(q_2)
\land KVPathSelected(q_2).
\]

其中 KVPathSelected 可以表示：

- KV 已在 GPU；
- KV restore 已完成；
- 已决定走 rematerialization/prefill 路径。

请求 2 到达后重新计算：

\[
W_{q_2}(t)
=
w_j\kappa_{q_2}(t)U_j(t),
\]

并使用：

\[
Priority_{\text{LLM}}(q_2)=W_{q_2}(t).
\]

请求 1 阶段的未来 Tool 预测不继续作为请求 2 的 DAG 权重。请求 2 到达后，预测已经完成了预热和 KV 时机规划的职责。

## 11. 完整三方调度算法

### 阶段 A：请求 1 到达

1. 根据 DAG 计算 \(\kappa_{q_1}\)。
2. 根据 deadline 计算 \(U_j(t)\)。
3. 得到 \(W_{q_1}=w_j\kappa_{q_1}U_j(t)\)。
4. 将请求 1 放入 LLM 队列。
5. 并行启动 Top-N Tool 类型和执行时间预测。
6. 按 \(V_{\text{tool}}(o)\) 对 Tool Cache 做索引、元数据或候选条目预热。

### 阶段 B：Tool Call 1 到达

1. 使用真实类型和参数查询 Tool Cache。
2. 命中时，使用 lookup、validate 和 deliver 延迟计算 \(\hat T_{\text{tool}}\)。
3. 未命中时，交由 Agent 本地执行，并使用 SLO 感知的预测分位数估计 \(\hat T_{\text{tool}}\)。
4. 计算 \(\hat T_{\text{need}}=\hat T_{\text{tool}}+C_{\text{agent}}\)。
5. 通过 \(a_q^*\) 选择 KEEP、OFFLOAD 或 DROP。
6. 如果选择 OFFLOAD，按 \(s_q^*\) 安排 RESTORE。

### 阶段 C：Tool 状态更新

1. Tool 进度变化时更新剩余时间；
2. 重新计算 \(\hat T_{\text{need}}\)；
3. 重新计算 KV restore laxity；
4. 必要时重新选择 KV 动作；
5. 设置 migration cooldown，避免预测波动造成频繁迁移。

### 阶段 D：Tool 完成和请求 2 到达

1. 使用实际 Tool 时长覆盖预测；
2. 更新 Tool Cache 的实际节省时间和命中统计；
3. 立即修正 KV restore 优先级；
4. Agent 构造并提交请求 2；
5. 请求 2 满足依赖后，按 \(W_{q_2}\) 进入 LLM 队列。

## 12. 联合优化的紧凑表达

每个调度周期可以将 Tool Cache 动作和 KV 动作写成联合选择问题：

\[
\begin{aligned}
(a_T^*,a_K^*)=\arg\min_{a_T,a_K}
\sum_q W_q(t)\Big(
&T_2(q,a_T,a_K)-t_q\\
&+\lambda_d[T_2(q,a_T,a_K)-D_q]^+
\Big),
\end{aligned}
\]

其中：

\[
T_2(q,a_T,a_K)
=
\max\left(
T_{\text{tool}}(q,a_T)+C_{\text{agent}},
T_{\text{KV}}(q,a_K)
\right).
\]

Tool Cache 使用自己的容量约束：

\[
\sum_o x_oSize(o)\le B_T,
\]

KV Cache 使用自己的 GPU、CPU、NVMe 容量和迁移约束。

不存在把 Tool Cache 与 KV Cache 放进同一个容量约束的项。它们的联合性只来自共同的 \(T_2\) 和 DAG/SLO 权重 \(W_q\)。

## 13. 三方职责总结

~~~text
DAG：
    决定请求在 workflow 中有多重要。

SLO：
    决定请求当前有多紧急。

Tool 类型和时间预测：
    在请求 1 推理期间提前准备 Tool Cache；
    在 Tool miss 后估计 Tool ready 时间。

Tool Cache：
    缩短 Tool Result ready 时间。

KV Cache：
    缩短 Tool Result ready 后请求 2 的恢复时间。

Scheduler：
    使重要且紧迫的请求尽可能同时满足 Tool ready 和 KV ready，
    从而最大化 SLO goodput。
~~~

最终优化的不是某一个缓存的命中率，而是：

> **在 DAG 重要性和 SLO 紧迫度加权下，最小化 Tool 与 KV 两个阶段共同造成的请求 2 启动延迟，并最大化按时完成的 workflow 数量。**
