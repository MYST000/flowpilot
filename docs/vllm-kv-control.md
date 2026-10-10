# vLLM KV 控制

核对日期：2026-09-27。FlowPilot 客户端见
[retention.py](../flowpilot/scheduling/retention.py)。
引擎实现位于本机 vLLM 0.29.0 基线
`98dff2a81d747d1dba01a47f939f48c3526d4206` 加未提交扩展：
[manager.py](../../../vllm/vllm/v1/kv_control/manager.py)、
[protocol.py](../../../vllm/vllm/v1/kv_control/protocol.py)、
[引擎接口说明](../../../vllm/docs/features/kv_control.md)。
这些跨仓库链接依赖当前工作区布局。

## 所有权和能力

FlowPilot 发送 KEEP/OFFLOAD/DROP，读取 prefix 观察和操作回执。
vLLM 负责物理 KV、lookup/acquire、分配、复制、引用以及恢复/重算。
后继请求走普通推理入口；没有外部 RESTORE、恢复队列、恢复优先级或 GPU-ready 门槛。
Tool cache 与 CPU/GPU KV 各自管理容量。

引擎默认关闭 KV control，开启配置放入 vLLM additional_config：

```json
{
  "kv_control": {
    "enabled": true,
    "finish_grace_ttl_ms": 250,
    "metadata_ttl_seconds": 300,
    "retention_preferences": true
  }
}
```

250 ms 是已有实验值；代码默认 finish_grace_ttl_ms=0，不建立物理 finish hold。
需要 APC、multiprocess EngineCore、
OffloadingConnector + CPUOffloadingSpec。
支持的控制布局为 full attention 和 Mamba align；PP/DP/DCP/PCP 必须为 1，
拒绝 speculative、canonical CPU layout 等未支持组合。
TP=4 是已有真实推理的验证点。
Qwen3.5 实验使用 dense checkpoint：
prefix_cache_retention_interval 为实际 Python None，字符串 "None" 不等价。

Capability 分别报告 descriptor query、finish grace、GPU preference、
CPU-backed preference、safe drop、CPU store、engine CPU reuse、hybrid、
transfer measurement、restore cost 和 continuation proof。
引擎 restore-cost estimate 与 continuation proof 仍为 false；新增 target_prefix_query、offload_gpu_reclaim。FlowPilot 的成本来自独立离线标定。
retention_preferences=false 时 KEEP/显式 OFFLOAD 返回 UNSUPPORTED，
原生自动 CPU offload 仍按自身配置运行。
标准 OpenAI-compatible vLLM 服务本身不保证存在这些扩展。

## 接口与请求关联

以下为 vLLM 引擎端路由，不能当作 FlowPilot 的 `/flowpilot/v1/kv`：

| 方法 / 路由 | 功能 |
| --- | --- |
| GET /v1/kv/capabilities | 分项能力与 engine epoch |
| POST /v1/kv/resolve | 用原始 CallBinding 解析完成 descriptor |
| POST /v1/kv/query | descriptor 的当前 prefix 观察 |
| POST /v1/kv/query-target | 真实 Chat/Responses 输入的目标 prefix 观察 |
| POST /v1/kv/apply | 提交策略 |
| POST /v1/kv/status | 查询异步 operation |
| POST /v1/kv/telemetry | metadata 事件、水位与容量计数 |

Wire schema_version=1。普通请求使用
`kv_transfer_params.kv_control_binding` 绑定 owner_scope、job、line、
request、llm_call、attempt、context_epoch。
engine parent/input/output choice 由真实 ingress 生成，不能从 provider response ID 猜测。
完成输出可以携带 kv_control_v1，流式结束后也可通过 resolve 领取。
resolve 状态为 PENDING、READY、DESCRIPTOR_EXPIRED、UNKNOWN_BINDING。
capabilities 公布 metadata_ttl_seconds，客户端据此限制后台 resolve 的重试寿命；单次 RPC timeout 不再决定整个解析过程的寿命。

resolve 顶层返回 observed_at_monotonic，handle 返回 expires_at_monotonic。
客户端必须先在引擎时钟域内相减，再以 RPC 本地开始时间换算保守的到期时间；
不同主机的 monotonic 绝对值不能直接比较。FlowPilot 按该时间设置本地清理定时器，
并消费匹配的 DESCRIPTOR_EXPIRED 事件，无需轮询全部 line。重复 resolve 不延长期限。
旧引擎缺少采样时间时会明确记录解析错误，普通推理仍可进行；启用此清理需同步更新两端。

动作包含 epoch、source call、expected tail、policy version、action ID 和幂等键。
相同键相同内容返回同一操作，冲突拒绝；ACCEPTED 表示仍有异步工作。
APPLIED、PARTIAL、FAILED、STALE、UNSUPPORTED 等是动作结果，
不能仅凭 HTTP 200 宣称动作成功。
查询/动作还会返回 403 owner mismatch、409 epoch/冲突、410 expired；
关闭控制面时返回 501。
owner_scope 是可信部署中的记录隔离标识，不是独立认证凭据。

## Finish、GRACE 与动作

descriptor 保存 hash/manifest、来源、已计算和候选范围，不保存整个 Request。
FlowPilot 按 line 维护当前 tail 引用；ID 跟随完成 request/output branch，每轮新建，
不是永远不变的 line ID。metadata 过期即撤销 KEEP，安全清理延后不延长有效需求。
正常结束先对有效 GPU 副本取得去重 GRACE 引用，再释放请求引用。
GPU 范围按 GPU block/checkpoint 枚举，包含不足一个 CPU chunk 的尾块。
空闲 EngineCore 按最近 deadline 唤醒处理到期；活跃时在安全点处理，
长 step 会延后实际释放。查询和重试不续期。

| 动作 | 当前语义 |
| --- | --- |
| KEEP | 登记软偏好并交接对应 GRACE，不增加长期 pin；压力下仍可淘汰 |
| OFFLOAD | 复用 READY、合并 PENDING，缺失部分走原生 CPU store；复制保护先于 GRACE 交接；全部 READY 后安全回收 GPU 映射，保护范围延迟重试 |
| DROP | 撤销本 owner 对应意图/GRACE；检查共享需求、请求引用和传输 fence 后定向清理；受阻范围登记延迟清理 |

OFFLOAD 完成后主动回收目标内无引用、DMA 或其他有效 KEEP 的 GPU 映射；CPU 副本仍可正常淘汰。回收使 KV 池内块可复用，不是 cudaFree。
KEEP/DROP 按 GPU 边界选择范围；OFFLOAD 仍遵循原生 CPU chunk/partial-tail 规则。
GRACE 按实际 GPU hash 覆盖交接，完整 CPU chunk 的复制保护可以接管其全部 GPU 源块，
未覆盖的尾块继续受剩余 GRACE 保护。
GPU 空闲块优先级为：无 hash=-1、普通缓存=2、有效 KEEP=3；不再另设 OFFLOAD 标签淘汰档位。同级保留 recency。
共享块汇总多个 descriptor 的有效需求，存在有效 KEEP 时保留该偏好。
同 line 的新 llm_call 入站使旧 descriptor 策略失效；
新请求结束后的策略可覆盖其共享 prefix 的旧意图。

原生 store 按已完成计算水位与 max_offload_tokens/offload_prompt_only 裁剪；
None 与 0 分别为不加上限和不新建 store。
未计算的最后一个采样 token 不应被发布为完整 KV。
common-prefix/cascade 按真实请求 block table 判断，GRACE/DMA 引用不计入请求成员。
必要 worker 全部停止访问后才能提交完整 CPU READY 或清理失败目标。
reset、generation 和映射版本用于阻止旧命令/回调伤及新 allocation。

## Prefix 查询

查询复用原生 GPU coordinator 和 CPU connector 匹配规则，
ID-only 查询不 tokenize、不 touch/pin、不触发复制。
目标查询重新渲染/tokenize 后只观察，不注册 descriptor 或提交推理；返回 TARGET_REQUEST、
engine epoch/state_version 和候选 CPU 对象 bytes。观察不提供驻留租约。
descriptor ID 在物理淘汰或 DROP 后仍可查询，直到 metadata 自身到期。
到期后查询仍返回 expired；如果 DROP 被引用或传输阻挡，引擎暂存其内部 descriptor
和原有清理索引，等安全回收或 generation 失效后释放，不延长外部有效期。

| 字段 | 含义 |
| --- | --- |
| prefix_token_count | descriptor 描述的范围 |
| gpu_ready_tokens | H_gpu，当前 GPU 可消费范围 |
| recoverable_tokens | H_all，按 backend 规则可用的 GPU/CPU 候选范围 |
| cpu_standalone_tokens | CPU 独立可恢复范围 |
| offload_object_bytes | 完整 offload 目标的去重对象字节总和，供 H2D 与 CPU 驻留估计 |
| offload_new_object_bytes | 目标中缺失 CPU 对象的新增 D2H 字节；目标含未完成 CPU 写入时为 null |
| 各组 resident/ready counts | 物理驻留计数，与连续可用前缀分开 |
| state_version / event_seq | 最佳努力观察水位，不提供驻留保证 |
| prefill_load | 当前运行中 decode 数、KV 长度总和、活跃 prefill 数及实际 budget/block/seq 配置 |

本地引擎 capability `prefill_cost_context=true` 时，target 与 descriptor 查询在
EngineCore 中附带只读 `prefill_load`，包含 epoch、identity 和引擎单调观察时间。
等待队列不计入 decode 负载；`num_computed_tokens` 含异步在途工作，每条 decode
的 C 为该值加当前一个 token。读取不调用 schedule、不获取引用、不启动复制。
这不是目标未来 batch；FlowPilot 的七特征冻结模型仅将 B/C 用于显式条件场景。
缺少此能力或负载时保留成本 unknown，不将缺失视为空闲。负载与 prefix 共用
本轮查询的本地 TTL，不比较跨主机单调时钟。

新增 D2H 字段按原生对象及 CPU replica 状态计算，不 touch LRU、不复制、不获取引用。
已完成 CPU 副本不重复计费；未完成写入的剩余时间无法由完整复制标定推断，保持 unknown。
FlowPilot 缺少新增字节字段时不能用完整目标 bytes 或 token 比例代替；完整 CPU prefix
覆盖目标时仍保留零新增 D2H。该字段也是最佳努力观察，不保证动作执行时副本仍驻留。

Hybrid 必须满足全部必需组、checkpoint 和 alignment，不能把跨层块并集当命中。
当前 OffloadingConnector 的 CPU lookup 从全部必需组共同命中的 GPU 边界开始，
与普通推理一致；其他组缺失时，不能用较深的 full-attention 单组命中推进起点。
PENDING lookup 的恢复字段可为 unknown。
ID-only 标记 DESCRIPTOR_ONLY，不计算后继 prefill；
提供 next_prompt_tokens 和 count_basis 仅得到 ASSUMED_CONTINUATION，
仍未证明真实输入内容，且受 N-1、prompt-logprobs、skip-cache 等规则限制。
真正请求入站重新验证并获取引用。

FlowPilot 使用真实 target prefix 与兼容成本作 W−K admission；任一候选成本未知
时整轮 FIFO。descriptor 查询用于首次 retention 的假设续接场景。
原生 transfer measurement 是复制测量；worker 求和时长不是墙钟恢复时间或排队 ETA。
成本未知不能写成 0，也不能用 tokens 推算 KV 字节。

## 2026-09-22 修复与剩余边界

以下问题已在本地引擎工作区修复，回归位于
[test_kv_control_manager.py](../../../vllm/tests/v1/engine/test_kv_control_manager.py)：

| 优先级 | 原缺陷 | 修复后的行为 |
| --- | --- | --- |
| P1 | GPU block=16、CPU chunk=32 时遗漏有效 GPU 尾块，KEEP/DROP 范围也被错误取整 | 16/48-token 范围受 GRACE；KEEP/DROP 可覆盖 GPU block 边界；OFFLOAD 只交接合法复制范围 |
| P2 | DROP 因引用暂缓，metadata TTL 到期丢失 intent | GPU/CPU 引用释放后继续安全清理；allocation/hash generation 检查仍防止旧 DROP 误删新映射 |
| P2 | KEEP 到期后 free heap 留下旧优先级 | 撤销策略后立即重索引受影响空闲块，保留原有 recency，并汇总其他有效共享需求 |

修复沿用已有 descriptor、deadline 和事件索引，没有新增独立老化服务。
FlowPilot 在 OFFLOAD 的 PARTIAL/FAILED 后按刷新周期重新查询、重新决策，并用新策略版本重试，见 [调度实现](scheduling.md)。丢失回执时仍幂等重发原命令。DROP 的延迟清理由引擎继续完成；PARTIAL/FAILED 均不计为成功。

同日重新执行了 [真实多轮对话实验](../../experiments/flowpilot/conversation-descriptor-20260922/REPORT.md)：
两个模型各 7 个会话、50 次 Chat Completions，模型实际回复原样进入下一轮历史。
缓存压力完全来自其他会话的正常请求，没有 token 填充、手动淘汰或 cache reset。
普通 attention 的 43 次续接中，15 次发生自然 GPU 前缀丢失；限制到新请求真实共享范围后，
GPU/CPU 查询均与实际命中相符。例如驻留块从 49 降至 42，连续 GPU prefix 从 784 降至 80。

该实验还发现了一个 hybrid 查询缺陷：真实账单测试会话第 4 轮，部分 GPU 组自然丢失后，
`recoverable_tokens=0`、`cpu_standalone_tokens=528`，正式请求实际从 CPU 恢复 528 tokens，
原生 load=68,812,800 bytes。查询没有遵循实际 connector 的 divergent-local-hit 能力边界，
CPU lookup 起点与正式请求不一致。此前
[底层诊断实验](../../experiments/flowpilot/descriptor-accuracy-20260922/REPORT.md)
也发现相同根因，但使用了 token-ID 输入和部分受控淘汰，不能替代多轮对话证据。

该缺陷已在后续修复中移除不适用于当前 OffloadingConnector 的单组 lookup 起点，
改用原生 coordinator 的共同 GPU 边界。只传 descriptor ID 的接口和字段语义保持不变：
报告旧前缀当前的 GPU、GPU/CPU 和 CPU 独立可用长度，不增加目标请求证明或恢复命令。
回归与独立目录的真实对话复跑见
[descriptor 修复验收](../../experiments/flowpilot/conversation-descriptor-fix-20260922/REPORT.md)。

修改后的有效性又通过
[独立多轮场景验证](../../experiments/flowpilot/conversation-scenarios-20260922/REPORT.md) 检查：
两个模型各执行 8 个新中文场景、每场景 8 轮；共 128 次真实请求、112 次续接，
其中 97 次共享范围非零。自然发生 GPU/CPU 前缀缩短后，限定真实共享范围的查询均无偏差。
Hybrid 两次 GPU prefix=0 的普通后继请求分别实际从 CPU 使用 1056/1584 tokens，
完成 load 为 86,114,304/103,415,808 bytes；CPU 也被自然淘汰时，查询归零与实际重算一致。
采集了 912 次当前 descriptor 快照，运行中未根据缓存读数改变问题、访问顺序或容量。

另一个真实会话现象是：无缓存丢失时，旧 descriptor 查询 432 tokens，下一轮只命中 192。
Qwen 模板在重渲染历史 assistant 消息时改变了 token 序列，真实 hash 共享范围只有 192。
因此 `prefix_token_count` 不是当前可用长度，旧 descriptor 的 H_all 也不是后继请求的命中保证；
提供 next_prompt_tokens 仍不能证明目标内容相同。

针对固定 `enable_thinking=false` 的 Qwen 文本会话，提供
[显式保留模板与启动配置](../examples/chat_templates/README.md)：历史 assistant
保留生成时已有的空 think 段及正文空白。Qwen3.5 模板同时处理字符串和 OpenAI
内容数组，避免数组路径上的 trim 改写原始回答。调用方仍原样追加普通 assistant
消息，descriptor 的 ID-only 接口与引擎恢复控制保持原有语义。
Qwen3.5-9B 最终模板的实机验证见
[非思考前缀验证](../../experiments/flowpilot/nonthinking-prefix-qwen35-20260922/REPORT.md)。
模板保留不等于缓存驻留保证；混合思考、工具序列化、多模态及文本重新编码的
一般情况不能仅凭这份文本对话证据宣称前缀永远一致。

回归结果、真实引擎实验与剩余验证见 [验证与证据](verification.md)。
