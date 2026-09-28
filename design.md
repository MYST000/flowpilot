# FlowPilot：OpenHands 与单个 vLLM 实例之间的网关、Tool 复用与请求调度

> 核对日期：2026-09-26。本文依据当前工作区代码更新，规定组件所有权并记录实际运行路径；明确标记的后续目标不代表已实现能力。
> FlowPilot 基线 `fd4b3e48062991e711aaa803075d467cbcdf7eb9`，OpenHands SDK 基线 `501f89bba6bd5a551bbfd7323e9087ed28bbcc37`，本地 vLLM 基线 `98dff2a81d747d1dba01a47f939f48c3526d4206`（0.29.0）；三者均包含本地未提交修改，因此仅检出上述提交不能复现本文全部行为。

## 0. 设计结论与当前范围

FlowPilot 接收 OpenHands 的完整 LLM 请求，代理到推理服务，并将回复交还对应 conversation。OpenHands 负责 agent loop、权威历史、安全决策和所有真实 Tool 执行；FlowPilot 负责身份、line-tail、复用与外部 admission；vLLM 负责推理、KV 存储和全部恢复/重算。

```text
OpenHands Agent / LocalConversation
    |  complete request + identity / local Tool telemetry
    v
FlowPilot gateway + frontier + optional reuse / DCS / admission / retention
    |  ordinary inference request / optional KV control v1
    v
one fixed vLLM instance
    |  response / SSE / engine-owned KV facts
    +---------------------> FlowPilot ---------------------> OpenHands
```

当前可选能力由独立开关控制。代码存在、默认启用、本地验证和生产效果是不同结论：

| 能力 | 当前实现 | 默认状态 |
| --- | --- | --- |
| Chat Completions / Responses 代理 | 身份注册、双向代理、SSE、取消与终端记录 | 启用；入口默认要求认证 |
| Frontier / Tool telemetry / trace | 五态 LineTail、显式依赖、实际 Tool 生命周期、元数据日志 | 启用 |
| Exact / in-flight Tool reuse | registry、历史缓存、leader/follower、可信发布 | 关闭 |
| Semantic reuse | 硬约束后的 query 向量匹配，shadow/candidate/active | 关闭；启用后的默认模式为 shadow |
| DCS | exact-only、显式 delegation、加密增量和分批 ACK | 关闭 |
| Forecast | 异步 envelope、超时/TTL/丢弃；可注入 adapter | 关闭；默认 NoOp |
| 合成 Tool 时延先验 | 事实 miss 后生成实验 ready-time 估计 | 关闭 |
| 单实例 admission | 加权分数、健康探测、配置化 credit | 关闭；启用后默认 limit=8 |
| KV retention | 本地 KV control v1 的 descriptor 查询与 KEEP/OFFLOAD/DROP | 关闭；需要 vLLM 扩展 |
| 目标 prefix / CPU restore 成本排序 | 全量排队请求查询与条件 prefill slack | 随 admission 开启；成本另需离线标定 |
| 多 worker / 多副本接管 | shared-state 契约存在，完整事务与恢复未接通 | 不支持 |

调度模式固定一个实例：admission 或 retention 任一开启，`Settings` 就拒绝多实例配置。基础代理仍保留旧多实例路由代码；这不构成当前调度设计的实例放置、请求迁移或 KV migration 能力。

当前请求队列默认按剩余 SLO 减去条件 prefill 成本排序，每轮全量查询所有排队请求的实际目标 prefix。旧加权策略保留为显式对照模式。已有 descriptor 观察用于已完成请求的 KV 去留；目标请求使用独立的真实渲染/hash 查询，旧 descriptor 不能证明新内容。

FlowPilot 的 KV 控制始终只有去留与观察：发送 KEEP/OFFLOAD/DROP，查询 prefix 和回执。不存在外部 RESTORE、恢复队列、恢复 deadline、H2D 预留或 GPU-ready 准入屏障。CPU-only 请求正常提交，vLLM 在该请求生命周期内自行恢复或重算。

组件细节见 [运行接口](docs/runtime.md)、[Tool 复用与 DCS](docs/tool-reuse.md)、[请求调度](docs/scheduling.md)、[本地 vLLM KV 控制](docs/vllm-kv-control.md) 和 [验证与证据](docs/verification.md)。这些文档包含早于本次核对的记录，配置与行为以本文所链接源码为核对依据。

## 1. 系统边界与组件所有权

### 1.1 三个所有者

| 组件 | 拥有的事实与执行 | 边界 |
| --- | --- | --- |
| OpenHands | conversation、agent loop、Action/Observation、安全策略、Tool executor、权威消息顺序、DCS 增量应用 | 复用结果仍由 Runtime 写入当前调用的 Observation |
| FlowPilot | 请求/响应代理、身份关联、frontier、依赖、缓存匹配与发布、DCS WAL、外部队列和去留策略 | 不执行 Tool，不控制引擎 batch、decode 或恢复顺序 |
| vLLM | tokenization、实际 prefix lookup/acquire、KV 物理对象、引用、复制、驱逐、恢复/重算 | 普通请求入站重新验证真实输入；观察不等于 pin |

Tool Cache 与 GPU/CPU KV 分属独立容量域。Tool reuse 会改变后继请求形成时间，进而影响 KV 去留；不能用 Tool bytes 抵偿 KV bytes，也不能据此声称共享容量收益。

### 1.2 OpenHands 接入

静态 `LLM.base_url` 负责代理地址，静态 headers 可承载固定凭据，但不足以提供每轮 request/call identity、tail version、context cursor 和 Tool 生命周期。当前实现已在 OpenHands SDK 接入默认关闭的 [FlowPilotRuntime](../../openhands/software-agent-sdk/openhands-sdk/openhands/sdk/flowpilot.py)，并在 LLM transport、Agent 和 LocalConversation 边界使用它。

启用时必须设置 `FlowPilotConfig(enabled=True, gateway_url=..., api_key=..., job_id=..., line_id=...)`，且 `tool_concurrency_limit == 1`。真实多 Tool 调用按 provider 顺序执行，每个调用保留独立 `tool_call_id` 和对应 Observation。辅助 LLM 默认不加入此路径（`include_auxiliary_llms=False`）。仅更改 base URL 不会自动注册身份。

### 1.3 非目标

不推测未来 Tool 来创建 DAG，不做 Tool speculative execution，不授权通用 Shell 结果复用，不在缺少 delegation 时隐藏 continuation；不预测 decode 或 vLLM 内部等待，不迁移已提交请求，不管理恢复优先级，也不声称求得完整 workflow 的全局最优调度。

## 2. Line-Tail Frontier 与身份

### 2.1 在线状态

[LineTailFrontier](flowpilot/frontier/store.py) 按 `(job_id, line_id)` 保存当前 tail；同线先后关系由 tail 替换表示，不存完整历史 DAG。完整对话历史属于 OpenHands。FlowPilot trace 仅保存 identity、digest、大小、时间和结果状态，**不保存完整请求、Tool 输入或结果正文**。

Frontier 的请求、上下文、Tool 生命周期和依赖分别存于内部表；这些表与 LineTail 同属进程内状态，但不是 LineTail 字段。DCS 的未确认 provider 消息另存独立加密 WAL。当前 tail 的轻量化不意味着整个服务状态均只占 `O(active_lines)`：缓存、审计窗口、Job 注册和 WAL 各有自己的生命周期。

### 2.2 LineTail 实际字段

```text
LineTail {
  job_id, line_id
  context_epoch, base_context_cursor
  version
  phase: EMPTY | ACTIVE | BLOCKED | READY | TERMINAL
  tail_request_id?
  delta_ref?, delegation_ref?
}
```

`RequestRecord` 保存 logical request、attempt、`llm_call_id`、model、arrival、instance/response 引用和 ToolCallSummary；`LineMetadata` 保存 conversation/parentage、deadline、weight 等注册事实。Tool resolution、forecast、DCS 事务与 KV descriptor 均有独立所有者，不复制进 LineTail。

### 2.3 唯一显式依赖

```text
waiter_line --DEPENDS_ON--> prerequisite_line
```

`DependencyUpdate` 以 version 原子替换 prerequisite 集合，校验同 Job、重复项和环；冲突时保留旧版本。parent/spawn 只是来源关系，不自动变成等待边。当前协议没有通用 ALL/ANY/quorum predicate 字段，Runtime 应将其消解为依赖集合更新。Tool Call 是 response 属性，不是 DAG 节点。

### 2.4 Tail 更新与失败

注册产生 EMPTY。接收合法完整请求后替换 tail 并进入 ACTIVE；获得完整回复后，存在未解决 Tool/依赖/上下文屏障时为 BLOCKED，否则为 READY。新请求仍需校验期望版本、上下文和当前线路条件。失败或取消通过 request backup 回滚未提交替换，并返回权威版本供下一 attempt 对齐。

普通无 Tool 回复使 frontier 进入 READY，不能自动视为整个 workflow 已结束。OpenHands 在显式 LocalConversation.close() 时，对已注册、无活跃本地工作且仍属于本 runtime 的 EMPTY/READY tail 发送 line finish；ACTIVE/BLOCKED 或被替换的 tail 不强制终止，关闭失败明确记录。run() 返回不会自动结束 line，保证同一会话可以继续交互。显式 line finish 或不可恢复的 context 冲突才具有终止语义；结束后按依赖关系回收线路状态。

### 2.5 结构重要性与投影

直接等待当前 line 的线路数来自真实依赖；`ProjectionCalculator` 还能遍历当前依赖计算 downstream depth，派生 DAG importance、SLO urgency 和 request weight。这些是短期诊断/ready-time 投影，不是 admission 的实际打分公式。

当前 admission 仅使用直接 `blocking_line_count` 的归一化释放项。不能把投影中的 DAG 深度、line weight 或 `estimated_inference_ms` 宣称为已参与实际队列排序。

### 2.6 Identity、conversation 与子 agent

```text
job / workflow
  +-- line / conversation
        +-- logical request / transport attempt / llm_call
              +-- provider tool_call / local Action / Observation
```

| 字段 | 语义与当前约束 |
| --- | --- |
| `job_id` | 一个 workflow；子 agent 继承，不因新增 line 增生 Job |
| `line_id` / `conversation_id` | 可独立推进的调度线路 / Runtime 对话，概念独立 |
| `parent_conversation_id` / `parent_line_id` / `spawn_id` | 来源、关联与审计；不隐含 DEPENDS_ON |
| `request_id` | logical request；重试保持不变 |
| `tail_request_id` | frontier 引用，与 logical request 字段分开 |
| `attempt` | 每次 transport retry 递增，从 1 开始 |
| `llm_call_id` | 每次新的 transport attempt 使用新 ID，标识一次 GatewayCall |
| `tool_call_id` | provider 消息 identity；复用交付也使用接收方自己的 ID |
| `action_id` / `execution_attempt` | OpenHands 本地真实执行身份，不能由缓存命中伪造 |
| `context_epoch/sequence/cursor/digest` | 上下文连续性与同步依据；不混入 provider messages |

先注册 Job 和 Line，再携带完整 `X-FlowPilot-*` 请求头。基础 wire version 为 `flowpilot-phase0-v2`，重复身份头、缺失字段和不匹配注册被拒绝。当前 canonical 协议拒绝旧 `tenant`/`tenant_id` 字段；部署认证在 workflow 层级之外，通过 `deployment_id/namespace_id` 参与别名和复用域校验。

root conversation 仅在稳定、唯一且具备对应 namespace 证据时可作为 Job 别名或直接 Job ID。FlowPilot 不猜测缺失身份。重试使用新的 `llm_call_id` 与递增 attempt；“重复幂等消息”和“新的上游推理 attempt”必须区分。

## 3. 端到端运行路径

### 3.1 请求 1：注册、代理与可选准入

1. 网关认证并解析身份、模型、上下文，创建 GatewayCall，原子更新 tail。
2. forecast 开启时启动后台任务；不等待预测结果再转发。
3. admission 开启时获取 cold token 工作量，将完整请求放入唯一队列；有健康 credit 才放行。出队后再次校验 tail version、request/call、ACTIVE 阶段与依赖，在锁外发送 HTTP。
4. retention 已协商成功时，在请求的 `kv_transfer_params.kv_control_binding` 附加关联字段；保留其他 transfer 参数。该字段由 FlowPilot 拥有，拒绝客户端覆盖。
5. 请求发给固定实例；vLLM 自行验证 prefix 并执行推理或 CPU 恢复。网关等待的是普通推理响应。

### 3.2 回复、SSE 与 reuse 决策

公开推理入口只有 `/v1/chat/completions` 和 `/v1/responses`。基础模式保留响应 body、SSE 顺序、usage、Tool fragments、状态码和适用的重复头。observer 只在完整 Tool Call 闭合后记录事实；错误、断开和取消都有独立 terminal 与上游关闭路径。

当前 `_drive_gateway_reuse()` 只驱动非流式完整响应。它通过响应 `flowpilot` 控制元数据向 OpenHands 交付每个 Tool 的复用决策。SSE 路径直接转发，不能描述为已在网关缓冲整轮并隐藏 Tool response；Runtime 侧仍有 Tool 边界 resolve 和专门的 DCS 路径，验证范围须区分。

对于非流式 gateway DCS，首批调用全部获得可延迟的 exact cached result 才在网关内部继续；首批含未就绪 follower、leader 或本地调用时交还 Runtime。已进入的隐藏循环可轮询后续 exact follower，但不能将所有 in-flight 场景概括为自动隐藏。

### 3.3 Tool 执行与请求 2

历史/在途复用结果由 adapter 构造成接收方的 Observation；需要真实执行时由 OpenHands 运行 Tool，发送 START 与 terminal telemetry，并在 Observation 提交后发布可信结果。FlowPilot 不调用搜索、浏览器或 Shell executor。

实际 Tool Call、复用结果和执行事件更新 `ToolResolutionStore`。Tool、依赖和上下文条件满足后，OpenHands 或获授权的 DCS 构造完整请求 2，再走相同准入路径。等待 Tool 的 continuation 本身不占 admission credit。

响应正常完成时，retention 在后台解析 descriptor 并做去留决策；它不延迟回复交付。credit 覆盖普通 GatewayCall，直到响应 terminal、取消或提交失败；descriptor resolve 和后续去留操作不继续占用这个 credit。

## 4. Tool 历史复用与在途合并

### 4.1 注册与适用工具

复用必须同时具备 FlowPilot registry 与 OpenHands `exact_reuse_enabled/reusable_web_tools` 授权。真实工具定义和执行器不因复用而改变。

| Tool family | 实际 adapter | 边界 |
| --- | --- | --- |
| Tavily Search | `tavily_search_mcp_v1` | exact；显式开启后允许受约束 semantic query |
| Tavily Extract | `tavily_extract_mcp_v1` | exact，URL 列表和实际参数参与匹配 |
| Tavily Crawl / Map | `tavily_crawl_mcp_v1` / `tavily_map_mcp_v1` | exact，根 URL、遍历/过滤/instructions 等参数保留 |
| 原生 Terminal curl/wget | `terminal_url_fetch_v1` | 只识别受限单 URL GET/HEAD 和 stdout 读取 |
| BrowseComp-Plus MCP search | `browsecomp_search_mcp_v1` | 显式 registry/profile；exact 与实验 semantic |
| 既有专用 curl/url_fetch | `curl_url_fetch_v1` | 兼容现有专用 adapter，不是 Terminal 接入依赖 |

其他普通注册项仍由 controller 的 registry/descriptor 规则处理，例如本地实验 `web_search`；这不意味着可以自动将任意新 Tool 视为可信只读工具。专用 adapter 映射见 [registry.py](flowpilot/reuse/adapters/registry.py)。

Tavily 固定接入版本为 `0.2.1`，实际 input schema 与 digest 存在 [tavily_schema.json](flowpilot/reuse/adapters/tavily_schema.json)。该 MCP 将结果格式化为文本，不完整暴露供应商失败列表；Crawl 每页返回 200 字符预览，Map 返回 URL 列表。因此缓存保存的是实际完整 MCP Observation，不声称完整网页或全部 URL 成功。`Title:/URL:/Content:` 不作为可靠条目边界，预算放不下完整 Observation 时拒绝复用。

Terminal parser 拒绝文件下载、认证/上传、POST、变量、管道和复合命令；不分析 Python/Node 脚本语义。只规范化 URL 的 scheme/host、默认端口和空路径，保留 query 顺序和路径字节；curl/wget family、选项及 timeout 参与 exact key。`url_exact` 开启两者，`curl_url_exact` 仅开启 curl。匹配不运行 embedding。

Terminal 发布要求真实 command/input digest、成功退出、非 timeout/is_error 与 Observation 关联。交付保留正文并绑定当前 command，清除 leader 的 pid/cwd/hostname 等本地元数据。成功退出不等于 HTTP 2xx；parser 不验证 curlrc、代理、别名或隐式 shell 状态。registry namespace/policy/version 必须对应兼容环境，复用不会重放 shell 历史和内部缓存。

BrowseComp 原 MCP `search(query: str)` 保留 query 原文，exact 不做大小写、标点或空白改写。硬约束包括实际 schema 及部署声明的不可变语料、检索器/模型、top-k、snippet/tokenizer profile；SDK scope 的 `browsecomp-search:<digest>` 必须一致。支持 JSON 数组或逐 hit 文本块，校验 docid/snippet/可选有限 score 后整体交付。声明摘要不证明远端索引实际内容；`get_document` 仍本地执行，此实验不启用 DCS 或额外 KV 调度。

### 4.2 匹配顺序与隔离

[ReuseService](flowpilot/reuse/service.py) 为 gateway resolve、Runtime resolve、poll、publish 和 DCS 统一解析权威 deployment/namespace，不能信任工具请求自报的 scope。controller 按以下顺序处理：

```text
exact history
  -> allowed semantic history
  -> exact in-flight
  -> allowed semantic in-flight
  -> register local leader
```

family、版本、schema、adapter、policy、语言/区域、safe-search、数据源和 freshness 是硬约束；只有允许的 query 内容参与软匹配，不额外建立 private/public query 分区。DCS 使用 exact-only 查找。

### 4.3 运行中 Tool 调用的匹配

当前已实现 in-flight 匹配，入口为 [WebReuseController.resolve](flowpilot/reuse/controller.py)。Exact 使用规范化 descriptor digest 查找 `_descriptor_bindings`；semantic 对相同 hard_scope 和 embedding index 的 running binding 计算相似度，active 模式达到阈值才加入 follower。shadow/candidate 只审计或返回候选，不阻止本地执行。

匹配与新 leader 注册在同一锁内完成。语义评分在锁外读取快照，回到锁内核验 binding generation、存活状态与 lease，并再次查询历史，处理评分期间 leader 已完成发布的竞争。匹配成功返回 `WAIT_AND_SYNC_REUSED_RESULT`，允许 deferred 时为 `DEFER_WAIT_FOR_INFLIGHT`；没有匹配才返回 `SYNC_AND_EXECUTE_AS_LEADER`。

这里的 running 是 binding 生命周期：leader 在 resolve 时注册，可能尚未收到真实 Tool START。因此能力准确地说是“对已登记、尚未完成的调用意图合并”，并非扫描或接管所有正在运行的本地进程。只有进入 registry 和 reuse 协议的调用参与匹配；in-flight 表在进程内，重启不会自动恢复。

Follower 通过 binding poll 等待真实发布，再校验 freshness、预算和自身身份；lease/失败/取消不能伪造成成功。现有回归包括 `test_exact_inflight_never_calls_embedding_worker`、`test_concurrent_semantic_misses_revalidate_snapshot_generation`，以及 BrowseComp 的 history/in-flight、candidate/active 参数矩阵。

### 4.4 真实执行来源与发布

LLM ToolCallRef 可以先于 Action 注册。START 绑定真实 Action 和 execution attempt；FINISH 核对输入/结果 digest、大小、终态及 adapter 格式，OpenHands 提交 Observation 后才能发布。MCP Action 使用原生 `to_mcp_arguments()`，不能把内部 `data` 包装当成 provider 参数。

SQLite 单事务提交 payload、origin execution evidence、索引和 publication receipt。重复的相同发布返回原 receipt，冲突不能成为可信结果。Follower 始终使用自己的 provider identity，不能接收 leader 的私有对话或 LLM 回复。

初始 TTL 从服务端接受 FINISH 的观察时间起算，默认 300 秒，实际窗口取发布请求 TTL（未指定时用 registry default）、registry max（未指定时用 default）和可选 scope max 的最小值。可缓存结果每次成功 history、poll 或 deferred 复用后滑动续期为 `命中时间 + 原有效 TTL`；每次使用同一窗口，持续命中可持续保留。原始 `observed_at` 和 publication receipt 不变，`reuse_entries.expires_at` 保存当前到期时间，交付 provenance 返回续期后的期限。查询候选、交付校验失败、发布重试和容量保护不续期；已过期/撤销结果不能复活，不可缓存结果仍按初始 FINISH TTL 交付给 follower。leader 失败、取消或 lease 到期有显式终态，不能用预测结果填充成功载荷。

### 4.5 Semantic 模式

wire version 分别为 `flowpilot-phase1-reuse-v3` 与 `flowpilot-phase3-reuse-v3`；数据库版本另算。Semantic 需 registry 显式允许；shadow/candidate 不替代真实执行，active 才允许语义结果交付。Tavily Search 只放宽 query，限定 general、非时间敏感搜索；Extract/Crawl/Map 和 Terminal URL 保持 exact。

[Qwen3Embedding](flowpilot/reuse/semantic.py) 使用本地模型，默认路径 `/docker/data/HF_MODELS/Qwen3-Embedding-0.6B`，向量默认 1024 维并 L2 归一化；运行时不下载权重。embedding 故障不使有效 exact 载荷失效。已有人工标签和 BrowseComp active 实验不构成语义等价、检索排名保持或生产质量证据。

### 4.6 Tool Cache 容量

默认独立库 `data/reuse-v4.sqlite`，schema v4；默认 committed payload 容量 512 MiB、后台维护间隔 60 秒。限额不是 SQLite 文件总大小，也不是 KV 容量。

先删除过期项，超容量时按以下键升序淘汰：

```text
ttl = initial_expires_at - observed_at
freshness = clamp((expires_at-now) / max(ttl, 0.001s), 0, 1)
value = max(0, measured_latency_ms) * (1+hit_count) * freshness / max(1, result_size)
eviction_key = (value, last_used_at, origin_id)
```

交付中的结果、完成 binding 尚待领取的结果受容量保护，但不突破 freshness。payload 按 origin 共享；删除联动 publication、向量和索引，不能通过旧 receipt 复活结果。维护在发布后、显式调用和后台执行；checkpoint 不保证文件立即缩小。源码见 [store.py](flowpilot/reuse/store.py)。

## 5. 请求成本与实际 admission

### 5.1 两类投影不可混用

`SchedulingProjection` 由 [ProjectionCalculator](flowpilot/scheduling/projection.py) 按需生成，包含 ready、`t_need`、DAG/SLO weight、age 和 tail version，用于诊断及 retention 的后继时间判断。协议中保留的可选 inference/critical-path 字段不表示已有在线预测器。

真正决定派发的是 [AdmissionQueue](flowpilot/scheduling/admission.py)。默认 `policy=prefill_slack`；snapshot 暴露 remaining SLO、prefill slack、GPU/CPU 条件成本、成本来源及 prefix 观察水位。旧 `weighted` 策略仅供显式对照。

### 5.2 时间与工作量

`CP_q=max(0,request_arrival-workflow_started_at)` 在到达时固定；`Age_q` 使用单调时钟增长。剩余 SLO 为 `R=deadline-workflow_started_at-CP_q-Age_q`，不重复累加过去 Tool 时间。子 line 继承 Job 起始时刻。

每轮对所有排队请求调用 `/v1/kv/query-target`，使用真实 Chat/Responses 渲染、input processor 和引擎 hash/coordinator/connector，得到 P、H_gpu、H_all 及候选 CPU 对象 bytes。不查询已派发请求或全部 line tail。无目标能力时，已有 tokenizer 结果仅作为 cold 工作量，未知值不冒充真实 0。

### 5.3 实际优先级

```text
C_gpu = calibrated_prefill(P, H_gpu)
C_cpu = calibrated_restore(actual_object_bytes) + calibrated_prefill(P, H_all)
C = min(C_gpu, C_cpu)                 # CPU 候选已知且已标定时
L = remaining_SLO - C               # 越小越先派发
```

CPU 候选不可估计时使用 C_gpu；没有兼容的标定时 C 保持 unknown，明确标记 `deadline_only:cost_unknown` 并按 deadline 排序。无 deadline 的请求排在有 deadline 的请求之后；同主键按可选 Job 在途数、blocking lines、年龄、FIFO 处理，没有强公平保证。

等价主键是 `deadline-C`，所有请求的剩余时间一起下降，不必每毫秒重查 KV。插入、heartbeat、credit 归还和依赖变化合并触发全量 sweep；同一 sweep 只查询一次各排队请求，原子更新仍在队列的项并可连续派发多个 credit。查询期间新增项进入下一 sweep，取消项不会重新占用 credit。

观察包含 engine epoch/state_version 和本地开始时间，默认 TTL=2 秒。epoch/身份不匹配或超时回退到显式 cold/unknown；sweep 返回时已经过期的估计不会用于 cache-aware 排序。TTL 和水位不能保证查询后不再失效：真正的 lookup/acquire 仍由普通引擎请求重新完成。本实现不锁住查询到的块，不等待 GPU-ready，也不在派发后重新 probe 阻塞请求。

### 5.4 单队列、健康与 credit

`SchedulingRuntime -> AdmissionQueue` 是生产调用链。一个 `asyncio.Lock` 保护 waiting/inflight，按 `(job_id,llm_call_id)` 管理 credit。健康且 `free=max(0,limit-inflight)>0` 时选出最小 slack 项并原子占用 credit，调用方在锁外发 HTTP；派发后不抢占或重排。

默认每 1 秒 GET 上游 `/health`，timeout=1 秒，TTL=5 秒。`limit=8` 是网关配置上限，不是引擎动态 batch 容量。健康失败或过期停止新派发，已接纳 GatewayCall 继续完成。terminal、取消和发送失败释放相应 key，重复 release 不增加额度。

插入、heartbeat、release 和依赖刷新触发全量排队请求查询与选择。查询在队列锁外执行，取消和 credit 归还无需等待 RPC。`queue_work_before_tokens` 是插入时已知前置完整 prompt 工作量的诊断快照，不是恢复时间或队列等待 ETA。
```text
Tool/context/dependency waiting (outside admission)
    -> complete request
    -> waiting map
    -> reserve credit under lock
    -> revalidate tail -> submit outside lock
    -> ordinary vLLM restore/recompute/inference
    -> GatewayCall terminal -> release credit
```

代码没有独立 DISPATCHING 枚举；从 waiting 移入 inflight 即完成预留。不存在 WAITING_KV 或 release-state 三档协议。forecast 不改变队列资格或分数。

## 6. Forecast、Tool Resolution 与 T_need

### 6.1 可选预测

[ForecastManager](flowpilot/scheduling/forecast.py) 异步接收版本化 `ForecastRequest/ForecastResult`，校验 request/tail、catalog/predictor version、Top-N、duration quantiles、confidence 和 TTL。默认 timeout=0.25 秒、TTL=30 秒、Top-N=3；可注入 adapter，仓库提供 NoOp 和 TraceReplay，没有内置生产预测器。

超时、取消、不兼容、低置信度、过期或事实 Tool 到达后的晚结果被丢弃，不改变转发、Tool 执行或上下文。当前 prewarm 回调只保存版本化 forecast metadata，没有真实 payload 预取，也不据预测修改物理缓存 LRU。

### 6.2 事实与实验先验

[ToolResolutionStore](flowpilot/scheduling/resolution.py) 保存 history/in-flight/local resolution、status、version、ready_at_estimate 和实际时延/大小。真实 Tool 名称、参数、命中、发布和本地生命周期覆盖预测；预测不能断言 cache hit，也不能产生 Tool Result。

`FLOWPILOT_SYNTHETIC_TOOL_DURATIONS=1` 是独立、默认关闭的实验开关。事实未命中后，名称含 `search` 的工具取 1000–2000 ms，其余取 100–200 ms，来源标为 `synthetic_factual_family_v1`；可用 seed 固定序列。它不 sleep、不延长真实工具时间，不能当作经过校准的性能测量。复用命中不套用该先验。

### 6.3 T_need 的实际含义

ProjectionCalculator 从当前 tail 的 resolution 读取估计。本地多 Tool 按 provider 顺序串行累计：已开始项使用预计剩余时间，尚未开始项累计完整 duration；in-flight follower 使用绝对 ready-time。未知项或未解决 line 依赖使 T_need 保持 unknown；`ready` 仍要求 frontier READY/EMPTY 且无未解决项。预测不是事实完成保证。

retention 使用 Tool gap、当前 prefix 的条件 prefill/transfer 成本和剩余 SLO；admission 只排序已形成的完整请求，不用 forecast 预测未来请求内容。

## 7. KV 去留、descriptor 与恢复所有权

### 7.1 本地引擎扩展

FlowPilot 客户端位于 [retention.py](flowpilot/scheduling/retention.py)，引擎实现位于 [vLLM KVControlManager](../../vllm/vllm/v1/kv_control/manager.py)。标准 OpenAI-compatible 接口不意味着具备这些能力。当前扩展基于本地 vLLM 0.29.0 加未提交修改，升级引擎需重新验证内部接口。

引擎需要 APC、multiprocess EngineCore 和 OffloadingConnector + CPUOffloadingSpec；支持 full attention 和 Mamba align、非 canonical CPU layout。PP/DP/DCP/PCP 均须为 1，不支持 speculative 等其他组合。TP=4 是已有 Qwen3.5-9B 验证点，不是其他配置的验收结论。

### 7.2 配置与能力

vLLM KV control 默认关闭；`finish_grace_ttl_ms` 的代码默认值为 **0**，因此默认不建立物理 finish hold。已有实验显式设为 250 ms，它不是生产推荐值。`retention_preferences=false` 时 KEEP/显式 OFFLOAD 返回 unsupported，原生自动 CPU offload 独立运行。

能力分别报告 descriptor query、finish GRACE、GPU preference、CPU-backed preference、safe DROP、CPU store、engine CPU reuse、hybrid 和 transfer measurement。引擎提供 `target_prefix_query` 和 `offload_gpu_reclaim`；引擎自身仍为 `restore_cost_estimate=false`、`continuation_proof=false`，FlowPilot 可使用独立离线成本模型。查询能工作不意味着成本模型或所有去留动作可用。

### 7.3 引擎接口与绑定

| 方法 / 路由 | 用途 |
| --- | --- |
| GET `/v1/kv/capabilities` | 分项能力、布局和 engine epoch |
| POST `/v1/kv/resolve` | 原 CallBinding 对应的已完成 descriptor，可能暂为 PENDING |
| POST `/v1/kv/query` | descriptor 当前可用 prefix 和水位 |
| POST `/v1/kv/query-target` | 真实目标请求渲染/hash 的 GPU/CPU prefix 观察 |
| POST `/v1/kv/apply` | KEEP/OFFLOAD/DROP 策略命令 |
| POST `/v1/kv/status` | 异步 operation 回执 |
| POST `/v1/kv/telemetry` | 有界事件、capacity 和测量计数 |

这是 vLLM 端 schema_version=1 的接口，不是 FlowPilot 的 `/flowpilot/v1/kv`。CallBinding 包含 owner_scope、job、line、logical request、call、attempt 和 context epoch；resolve 校验绑定，不能仅凭 HTTP response ID 猜 descriptor。owner_scope 是受信推理域中的关联边界，不是独立认证凭据。

### 7.4 GRACE 与 KEEP

正常 finish 时，已启用的 GRACE 在释放 request 引用前取得去重物理引用，保护当时仍存在的有效计算范围。TTL 由引擎单调时钟计量，空闲时也会处理到期；查询和重试不续期，不补回此前丢失的 checkpoint。

KEEP 先登记软偏好再解除对应 GRACE；本身不长期 pin，也不保证最低驻留时间。GRACE 到期无策略只解除剩余保护、回到正常缓存，不默认 DROP。共享前缀判断不能把 GRACE/复制引用错误当成活跃请求数。

### 7.5 OFFLOAD 与 DROP

OFFLOAD 复用 READY CPU 对象、加入已有 store 或启动原生复制；先取得复制引用/fence，再交接对应 GRACE。必要 worker 全部完成后才 READY；CPU 全部 READY 后主动回收目标范围内、无活跃引用/复制保护/其他有效 KEEP 的 GPU 映射；受保护块登记延迟 intent，在原生事件后重试。CPU 副本仍可正常淘汰。GPU 池内块变为可复用，不是 cudaFree。只交接合法 backend 恢复范围，未接管子集保持原 GRACE deadline。

DROP 解除本 owner 的需求与保护，由引擎核对其他 owner、request、compute、GRACE、transfer 后安全回收。暂不能删除时保留 intent；allocation/hash generation 防止旧 DROP 误删新映射。metadata 过期不破坏尚未完成的安全清理，外部 descriptor 仍按原 TTL 到期。

原生自动 store 与显式 OFFLOAD 都遵循真实 completed-token watermark、chunk/alignment 和 max_offload_tokens，不能把未计算的末 token 写为可恢复 KV。上述引用、复制和实际淘汰均由 vLLM 管理。

### 7.6 Prefix 观察的意义

| 字段 | 含义 |
| --- | --- |
| `prefix_token_count` | 旧 descriptor 的描述范围 |
| `gpu_ready_tokens` | H_gpu，按 backend 规则当前 GPU 可消费范围 |
| `recoverable_tokens` | H_all，兼容 GPU/CPU 加载路径候选范围；PENDING 时可未知 |
| `cpu_standalone_tokens` | CPU 独立可恢复范围 |
| group resident/ready counts | 物理对象计数，不能等同连续可用 prefix |
| `state_version/event_seq` | 最佳努力观察水位，不提供驻留保证 |

ID-only 查询不 tokenize；目标查询重新渲染/tokenize。两者均不 touch KV LRU、不 pin、不复制。Hybrid 需要全部必需组与有效 checkpoint；H_all 不能按对象并集或 GPU/CPU token 相加计算。当前 connector 从全部组共同 GPU 边界开始 CPU lookup，与普通推理保持一致。

FlowPilot 按 line 维护当前 tail 指向的 descriptor；引擎 ID 属于某次完成 request/output branch 的不可变 hash/manifest，下一轮生成新 ID。同 line 新 llm_call 撤销旧策略；metadata 过期即撤销 KEEP，即使旧复制使内部清理暂缓。descriptor 在物理淘汰、OFFLOAD 或 DROP 后仍可查询，直到其 metadata 到期。同一 ID 的可用长度可以缩短、归零或随兼容内容重新驻留而增长。

### 7.7 旧 descriptor 与目标请求

ID-only 为 `DESCRIPTOR_ONLY`，只观察旧内容；给出 next_prompt_tokens/count_basis 也仅为 `ASSUMED_CONTINUATION`，不证明新请求 token 内容相同。可消费范围还受 N-1、logits 重算、prompt-logprobs、skip-cache 和 hybrid checkpoint 约束。

即使原样追加 assistant 文本，chat template 重渲染也可能改变 token 前缀。固定非思考 Qwen 的 [显式保留模板](examples/chat_templates/README.md) 只覆盖已验证的文本场景，不能证明所有工具、多模态或混合思考历史均保持前缀。

### 7.8 当前 retention 策略

`choose_retention()` 对 TERMINAL/确认不可恢复 prefix 优先 DROP。存在匹配标定和已知 Tool gap 时，比较：KEEP 的残余 prefill、OFFLOAD 的 D2H + H2D + 残余 prefill、DROP 的 cold prefill；要求 D2H 能在 gap 内完成。先最小化相对 `remaining_SLO-gap` 的预计超支，再比较成本加驻留资源价格。

驻留价格是配置策略参数，不是测量值：默认 GPU=1 秒/GiB/秒、CPU=0.01 秒/GiB/秒；低 free capacity 时 GPU 价格乘 2。对象容量来自引擎，GPU bytes 是当前 descriptor 对象的去重容量，不是全局共享块的边际容量，因此这是启发式决策。未来 Tool 输出长度未知，使用已知 prefix 加一个后继 token 的 `ASSUMED_CONTINUATION` 成本，不宣称能保证整个 workflow SLO。

无兼容模型/未知 gap 时明确使用 `fallback_cost_unknown` 的已有 ready-time/容量规则：近端且不承压 KEEP，否则能力支持时 OFFLOAD，再退到 KEEP/unsupported。默认 horizon=1 秒，free<=128 为压力。unknown recoverability 不当作零。

### 7.9 后台刷新与回执

正常回复完成后后台 resolve；单次 RPC 默认 timeout=1 秒，PENDING、传输错误及可重试 5xx 按 refresh_seconds 间隔重试，整体受引擎 capabilities.metadata_ttl_seconds 约束。tail 替换、epoch 变化、明确 UNKNOWN_BINDING/EXPIRED 或取消结束重试；旧引擎缺少 TTL 字段时沿用单次 timeout 窗口，不猜测寿命。已确认支持绑定的引擎发生短暂能力 RPC 故障时仍携带 ingress binding，但 retention 动作暂停，直至重新协商成功；明确 unsupported 则停止绑定。retention 默认每 1 秒重新协商、读取 telemetry、轮询 pending operation 并用缓存观察计算决策。response resolve、Tool 生命周期和 line finish 标记相关 source；仅这些 source 或动作需要改变的 source 重新查 descriptor。不会每周期全量查询全部 line tail。

resolve 响应携带引擎 `observed_at_monotonic`，与 handle 的 `expires_at_monotonic` 属于同一时钟域。FlowPilot 用两者之差计算剩余 TTL，再加本次 RPC 的本地开始时间设置到期定时器；不直接比较不同主机的单调时钟，也不因传输或重试延长有效期。同一 descriptor 再次 resolve 保留更早的本地截止时间。

定时器只清理 FlowPilot 的 source 引用和策略跟踪，不依赖 RPC 锁、健康协商或新请求，也不发送 DROP。telemetry 中匹配 owner/engine epoch/descriptor 的 `DESCRIPTOR_EXPIRED` 事件可提前清除引用；事件缺口不触发全部 tail 查询，由本地 TTL 保证最终清理。晚到的查询/回执不会恢复已删除引用或触发后续策略；tail 替换、DROP 完成、引擎 epoch 变化及关闭时取消对应定时器。物理复制和 GPU/CPU 清理由引擎继续负责。

`/flowpilot/v1/scheduling/state` 的 retention source 暴露 `remaining_ttl_seconds`，`source_expirations` 区分 deadline 和 engine_event 清理。缺少 resolve 时钟字段的旧引擎响应明确报 resolution validation error，不猜测 300 秒或建立无限期本地引用；此协议补齐需要两端配套更新。未增加独立的提前续接超时，仍沿用引擎 metadata TTL（默认 300 秒）。

命令携带 epoch、descriptor、source call、tail/policy version、action_id/idempotency_key；客户端在执行前重查当前 tail。HTTP 响应丢失时重试同一保存命令。ACCEPTED 只表示异步处理中，APPLIED/PARTIAL/FAILED 分开记录，不能把成功 HTTP 传输视为策略成功。

OFFLOAD 的 PARTIAL/FAILED 保留真实失败状态，至少等待一个 refresh_seconds 后重新查询 descriptor；当前策略仍要求 OFFLOAD 时使用新 action_id 和新 policy_version 重试。未收到回执的传输重试仍复用原命令，避免重复执行。DROP 的 PARTIAL 由引擎延迟 intent 继续处理，不生成无意义的重复 DROP。事件刷新合并在途触发，RPC 期间新到事件不会丢失；某个 source 的传输或校验失败单独计数，不跳过其他 source。

### 7.10 离线成本模型

[OfflineCostModel](flowpilot/scheduling/cost.py) 支持按总 context 分桶的 `fixed + (P-H)*seconds_per_token`，以及分别标定 D2H/H2D 的 `fixed + actual_bytes*seconds_per_byte`。JSON 必须记录 source、version、带时区 measured_at、model、engine_identity_digest、measurement_basis 和各桶 uncertainty。加载方式和实测 CSV 拟合工具见 [调度文档](docs/scheduling.md)。没有内置伪造的生产标定值。

CPU 成本只表示引擎选择该恢复候选时的条件成本；不含 decode、内部排队、网络等价于 TTFT 的承诺。模型当前使用点估计排序；uncertainty 保存在配置中供校准审计，不自动折算为安全裕量。不能以 worker-summed time 冒充墙钟时间，也不能以 token 数推导 hybrid KV bytes。

### 7.11 Heartbeat 与目标查询

admission 的 `/health` + 配置 credit、排队请求 full target sweep、response-origin retention 三条链路分别工作。目标查询独立于 retention 开关，能力不足时明确退回 cold/unknown。未获 credit 的请求不会因 CPU-only 被阻塞于恢复屏障；已接纳请求的 credit 直到正常 terminal 才归还。

## 8. 独立状态机与 DCS

### 8.1 事件入口

Job/Line 注册、依赖替换、line finish 和 Tool telemetry 通过 `/flowpilot/v1` 控制 API 进入。LLM request/response 由代理产生，reuse 发布与 DCS ACK 由各自服务处理。不存在把所有细节塞入 LineTail.phase 的统一巨型状态机。

Tool telemetry 使用稳定 `event_id`、单调 sequence 和 execution_attempt；检查 START 到 FINISH/FAIL/CANCEL，另有 BLOCKED 表示拒绝/阻挡。重复事件幂等，冲突 terminal 和错误活跃 tail 被拒绝。telemetry 发送失败不得改写真实 Tool Observation。

### 8.2 GatewayCall 与 LineTail

GatewayCall phase 为 `active/routed/completed/provider_error/protocol_error/upstream_failed/cancelled/stream_error`。它与 LineTail 的 EMPTY/ACTIVE/BLOCKED/READY/TERMINAL 分开，记录 transport outcome、时间与权威 tail version；排队 credit 也由 AdmissionQueue 独立持有。

流式资源关闭、terminal 记录与 credit 归还须覆盖 EOF、malformed/incomplete Tool fragments、provider error、断连和取消。已提交响应不能因清理失败被当成未发生；未提交 tail 替换不能因失败永久前进。

### 8.3 DCS delegation 与容量

[DeferredContextManager](flowpilot/context/manager.py) 持有 request snapshot、provider-valid delta、receipt 和 ACK；OpenHands 是最终历史权威。DCS 依赖 exact reuse、显式 Tool 白名单/lease 和 Fernet 密钥，不接受 semantic resolution 作为 exact delta 来源。

当前 `DelegationPolicy` 的默认限制为 32 messages、1,000,000 bytes、8 次 internal continuation、delta TTL 300 秒；SDK 默认 lease 30 秒。没有 token 数上限字段，不能把 token cap 写成已实现功能。

续接只机械追加同线完整 assistant/tool 批次，保留每个 tool_call_id 和 request snapshot 的 system/tools/采样设置。本地执行、最终回复、容量、TTL、lease、故障和升级形成同步屏障。

### 8.4 同步、ACK 与恢复

DCS 状态为 `open/syncing/acked/aborted/diverged`。同步按完整 provider batch 分片，Runtime 原子应用每个完整 chunk 后 ACK；不能拆开 assistant/tool 批次。部分 chunk ACK 后仍为 syncing，全部 ACK 后才 acked 并允许继续。不是所有 chunk 必须一次全量提交的实现。

ACK 对齐 epoch、lease、cursor、sequence 和 chained digest；重复 ACK 返回幂等结果，冲突进入 diverged，禁止猜测或 merge。已确认 delta payload 删除，保留有限审计记录；不能把有界 ACK receipt window 宣称为无限期 exactly-once 网络交付。

WAL 实际 `PRAGMA user_version=4`，旧库显式拒绝。重启恢复还需 OpenHands 权威历史、恢复 manifest 与 Job/Line 重建，不是仅靠 WAL 自动恢复全部 workflow。当前 snapshot 的 `wal_schema_version` 仍硬编码为 3，属于未修复的观测字段不一致，见 §11。

## 9. 调度目标与策略边界

### 9.1 研究目标

优先优化 SLO 内完成的 workflow goodput，其次 weighted JCT、deadline lateness、重复 Tool 执行与引擎实际恢复/重算开销。命中率、吞吐和 GPU 利用率为辅助指标；当前启发式分数和 retention 分支没有被证明是这一目标的最优解。

### 9.2 当前 Job / Line / queue 策略

只有一条 admission queue。Job 关联 workflow start/deadline 与可选在途惩罚，line 提供显式阻塞事实；请求按 §5.3 排序。新增子 line 不创建新的 Job，但当前默认 fairness=0 也不提供 Job 份额保证。

内部 continuation 与 Agent 请求共享公式、固定实例和 credit。已经发送的请求不迁移、不抢占；CPU prefix 不影响可提交性。`gateway/router.py` 中旧 WeightedFairRequestQueue 和多实例策略不是 SchedulingRuntime 的 admission 实现，其测试不能替代实际队列验收。

### 9.3 在途等待与压力

兼容 follower 等待真实 leader，结束条件包括完成、失败、lease 或 Runtime 等待超时/取消。没有按预测值自动发起重复执行的 hard-SLO 策略；SLO 不放宽 freshness、scope 或 semantic threshold。

现有 backpressure 包括 admission credit、Tool payload 容量和 DCS 消息/字节/轮数/期限。尚无通用每 Job ready-line 配额、每 Agent context-sync 字节限流或完整多副本 drain orchestration，不能写成部署保证。

## 10. 正确性、隔离与可观测性

缓存命中保留来源 provenance，不能伪装为本地新执行。所有复用交付核验硬约束与 freshness；当前 provider identity 和消息顺序不因缓存来源改变。query 相似不等于结果/证据等价，active semantic 仍须独立质量验证。

入口认证与部署 namespace 是复用边界。FlowPilot 使用 `X-FlowPilot-API-Key`，向上游剥离私有 `X-FlowPilot-*` 头；provider Authorization 与控制 RPC 凭据分别处理。DCS payload 加密，Tool cache 的载荷存储不等于 trace；不能因为 trace 已脱敏就声称缓存已实现通用 PII 检测、加密或全系统删除治理。

[TraceRecorder](flowpilot/observability/trace.py) 只写元数据 JSONL，默认单文件 64 MiB、3 个备份。写入失败累加 failure/drop 并降低健康；本地 append/rotation 不提供跨进程 exactly-once 审计。不得记录 prompt、完整 Tool 输入/正文、凭据或 leader 私有上下文。

当前可观测入口包括 health、JSON/Prometheus metrics、GatewayCall、frontier、Tool resolution、forecast、reuse、DCS 和 `/flowpilot/v1/scheduling/state`。后者提供实际 prefill slack、条件成本/来源、兼容 score/contributions、credit、KV capability 和 receipt。`/tool-analysis` 是兼容的派生诊断入口，不是持久化的 ToolAnalysis owner。

外部 queue wait、上游首字节、完成时间可观察；内部 prefill/decode/restore 的归因需引擎数据。请求 2 的精确 prefix、恢复成本误差、生产 Job fairness 和长期 goodput 尚不能从现有 gateway 计数推导。

## 11. 故障语义与已知缺口

| 情况 | 当前行为 / 必须保持的边界 |
| --- | --- |
| 上游失败、取消或异常 SSE | 显式 GatewayCall terminal、关闭资源、回滚未提交 tail、归还 credit |
| heartbeat 失败或过期 | 停止新准入，保留已接纳请求；没有自动绕过队列或 FIFO 降级 |
| KV 扩展缺失/不兼容 | unsupported/unavailable，普通推理继续；不伪造 descriptor、bytes 或成本 |
| 新 tail / engine epoch | 旧 retention source 失效；动作前核验当前身份 |
| forecast 失效 | 丢弃提示，不改变事实 Tool 与控制流 |
| semantic 独有故障 | exact 独立保留；共享可信执行/发布约束不能绕过 |
| DCS 分叉或冲突 ACK | fail-closed，标记 divergence/终止，不自动拼接历史 |
| 进程重启 | frontier、bindings、queue/credit 等进程内状态丢失，不能靠 trace 自动重建 |
| trace 写失败 | 健康降级并记录失败/丢弃计数 |

当前仍存在的边界：

1. **成本证据缺口：** 全量目标查询、TTL 校验和离线模型排序已有实现；生产标定、并发干扰误差、查询成本与 SLO 收益仍需测量。观察不提供长期驻留保证。
2. **清理与重试边界：** OFFLOAD 失败会在有效 tail/TTL 内重新决策；DROP 的 PARTIAL 仍表示保护尚未解除，不能当作 APPLIED。控制面长期不可达时不保证完成保留策略；关闭 unresolved 会话不会越权强制终止。DCS snapshot 直接读取真实 PRAGMA user_version，当前为 4。
3. **部署/质量证据缺口：** shared-state 未接通完整多进程事务；生产 predictor、semantic active 质量、强公平与长期容量/恢复效果尚无充分证据。

这些限制不授权自动添加隐藏 fallback、模拟成功或绕开真实执行。控制面失效时不能悄悄建立 OpenHands 到 vLLM 的直连。具体错误与状态必须可观察。

## 12. 实际模块、接口与配置

### 12.1 源码布局

```text
flowpilot/
  app.py, config.py, protocol.py, identity.py
  gateway/       service.py, stream.py, call_state.py, router.py
  frontier/      store.py
  reuse/         service.py, controller.py, store.py, origin.py
                 contracts.py, command_line.py, semantic.py, maintenance.py
                 evaluation.py, adapters/
  context/       manager.py
  scheduling/    runtime.py, admission.py, retention.py
                 forecast.py, duration.py, resolution.py, projection.py
                 profile.py, metrics.py
  state/         shared.py
  observability/ trace.py
```

`app.py` 组装这些组件。没有已落地的独立 `control/request_store`、`kv_directory` 或 `prefix_cost_projection` 模块。OpenHands adapter 位于 SDK，KV 物理控制位于 vLLM 仓库；FlowPilot 无须也不应内置 Tool executor。

### 12.2 配置与 HTTP 入口

[Settings](flowpilot/config.py) 和 [app.py](flowpilot/app.py) 是实际配置/路由依据。网关默认 `0.0.0.0:9000`，ingress auth 默认开启，request timeout=120 秒；`create_app` 明确拒绝 workers!=1，即使提供 shared-state path 也不构成多 worker 服务。

| 配置 | 作用 |
| --- | --- |
| `FLOWPILOT_UPSTREAMS` / `FLOWPILOT_INSTANCES_JSON` | 上游地址；调度启用时恰好一个实例 |
| `FLOWPILOT_INGRESS_API_KEY` | 网关入口凭据 |
| `FLOWPILOT_UPSTREAM_CONTROL_API_KEY` | health/tokenize/KV RPC 的可选上游凭据 |
| `FLOWPILOT_REUSE_ENABLED` / `FLOWPILOT_WEB_TOOL_REGISTRY_JSON` | 开启 Tool reuse 并声明 registry |
| `FLOWPILOT_DCS_ENABLED` / `FLOWPILOT_DCS_ENCRYPTION_KEY` | exact reuse 基础上开启 DCS |
| `FLOWPILOT_FORECAST_ENABLED` | 开启可注入的 forecast side channel |
| `FLOWPILOT_SYNTHETIC_TOOL_DURATIONS` / `FLOWPILOT_SYNTHETIC_TOOL_DURATION_SEED` | 显式实验时延先验 |
| `FLOWPILOT_ADMISSION_JSON` | AdmissionConfig，默认 enabled=false |
| `FLOWPILOT_RETENTION_JSON` | RetentionConfig，默认 enabled=false |

控制路由按实际功能分组：

| 前缀或路由 | 功能 |
| --- | --- |
| `/flowpilot/v1/jobs`、`/flowpilot/v1/lines` | Job/Line 注册 |
| `/flowpilot/v1/lines/{line_id}/dependencies`、`/flowpilot/v1/lines/{line_id}/finish` | 依赖与结束 |
| `/flowpilot/v1/events/tools` | Tool 生命周期 |
| `/flowpilot/v1/reuse/*` | resolve、binding poll/progress/result/fail/cancel、semantic policy/audit、maintenance |
| `/flowpilot/v1/dcs/*` | delegation、delta、sync/next/ack、reconcile、continuation |
| `/flowpilot/v1/scheduling/state`、`/flowpilot/v1/scheduling/projections/{line_id}` | 实际队列/KV 状态与派生投影 |
| `/flowpilot/health`、`/flowpilot/metrics`、`/metrics` | 健康、JSON 和 Prometheus 指标 |

`/flowpilot/v1/*` 由统一认证中间件保护；health/metrics 不在该中间件前缀内。接口存在不表示对应可选组件已启用。完整 payload 见 [protocol.py](flowpilot/protocol.py)，公开推理 API 不含 `/v1/completions`。

### 12.3 存储与版本

| 状态 | 当前存储 |
| --- | --- |
| Frontier/request/context metadata、GatewayCall、in-flight binding、forecast、resolution、admission | 进程内，各有生命周期；不提供统一重启事务 |
| Tool payload/origin/index/publication/vector | SQLite schema v4，默认 `data/reuse-v4.sqlite` |
| DCS snapshot/delta/receipt/ACK | Fernet 加密敏感 payload，SQLite schema v4，默认 `data/flowpilot_dcs.sqlite` |
| Trace | metadata-only JSONL，独立轮转 |
| SharedStateBackend | SQLite CAS/fencing 契约，未替代完整进程内控制状态 |

旧 reuse/DCS 库不自动迁移、清空或兼容读取，应配置独立新库。wire versions、SQLite user_version 和 snapshot 展示字段是不同概念。reuse/DCS 的持久化不能代替引擎 KV 状态或 OpenHands 权威历史。

## 13. 组件组织与发布门槛

### 13.1 组件职责

Gateway 维护 transport fidelity；Frontier 维护 identity/tail/dependencies；Reuse 维护匹配、execution origin 和 publication；DCS 维护 delegation/delta/ACK；SchedulingRuntime 维护唯一 queue/credit 并连接 retention；vLLM extension 维护 KV 实体和动作安全。

OpenHands 当前需要已有 SDK adapter 及 Agent/Conversation/LLM hooks，而不是仅静态配置。基础推理代理不需要 KV 扩展；retention 和 descriptor query 必须由本地 vLLM control v1 提供，目标查询需要本次扩展，成本比较另需匹配的离线标定。

### 13.2 依赖关系

```text
M0 identity + gateway + telemetry
  +-> M1 exact / in-flight reuse -> M2 exact DCS
  |                           +-> M3 semantic reuse
  +-> M4 forecast envelope / factual Tool ready-time
  +-> M5 single-instance admission

factual readiness + real vLLM capabilities -> M6 retention
M5 + reliable target-prefix/cost support  -> M6 conditional cost-aware admission
```

M4 和 M5 可在 M0 后独立验证，不要求先开启 reuse/DCS。M6 去留与 M6 目标成本排序应分别报告，前者已有实现不代表后者完成。

### 13.3 阶段门槛与当前落点

| 阶段 | 当前落点 | 不能据此宣称 |
| --- | --- | --- |
| M0 | 双 API、身份、五态 tail、telemetry、终端清理已有代码和测试 | 任意重启/多副本恢复 |
| M1 | exact/in-flight、可信发布、Tavily/Terminal/BrowseComp adapter | 所有 Shell 环境等价或真实 Tavily 全链路验收 |
| M2 | exact DCS、加密 WAL、分批同步/ACK、恢复用例 | 无界历史、token cap、无限期 exactly-once 或统一隐藏 SSE |
| M3 | shadow/candidate/active 与向量后端 | active 生产质量或语义检索等价 |
| M4 | forecast contract、NoOp/replay、事实覆盖、实验先验 | 已部署/校准生产预测器 |
| M5 | 剩余 prefill slack、全量目标查询、单队列、健康、credit | 强公平份额或已证明 SLO 收益 |
| M6 | descriptor、成本留存、OFFLOAD 安全回收与本地 CPU reuse 证据 | 外部恢复控制或生产效果 |

### 13.4 现有回归入口

| 范围 | FlowPilot 测试 |
| --- | --- |
| Identity/frontier/dependencies | `test_identity.py`、`test_protocol.py`、`test_frontier.py` |
| Gateway/SSE/terminal/trace | `test_gateway.py`、`test_stream.py`、`test_app.py` |
| Exact/semantic/origin/publish | `test_reuse.py`、`test_phase3_api.py` |
| URL/BrowseComp adapters | `test_terminal_url_reuse.py`、`test_tavily_url_reuse.py`、`test_browsecomp_reuse.py` |
| 独立 Tool 容量 | `test_cache_retention.py` |
| DCS | `test_context.py`、`test_dcs_api.py` |
| Forecast / ready-time | `test_phase4.py` |
| 实际 admission | `test_admission.py` |
| KV client / 移除旧恢复接口 | `test_retention.py`、`test_kv_removal.py` |
| 旧路由/shared-state 契约 | `test_phase5.py`；不替代实际 admission 或多 worker 验收 |

OpenHands 的 `tests/sdk/test_flowpilot.py` 与 `test_flowpilot_mcp_arguments.py` 验证 adapter；[集成矩阵](integration/test_openhands_reuse.py) 覆盖 history/in-flight、gateway/direct、DCS/admission 配置组合。mock inference、受控 Tool fixture、真实 Terminal HTTP 和 GPU 推理证据必须分别说明。

## 14. 验证证据与后续实验

### 14.1 本次文档更新的验证口径

本次只更新 design.md，按当前源码核对模块、默认值、状态、版本、路由和测试入口，校验本地链接及 diff。定向回归结果单独报告，不把文档整理当作全部阶段重新验收；没有启动新的真实 GPU workflow 或生产负载实验。

2026-09-26 本次定向回归共 **211 passed**：第一组 94 项覆盖身份/frontier、admission、retention、DCS、forecast 和 BrowseComp，第二组 117 项覆盖通用 reuse 与 Terminal/Tavily 工具族。沙箱运行停滞后中止，以下结果来自获准的宿主环境；使用仓库现有虚拟环境与测试临时库：

```bash
.venv/bin/python -m pytest -q tests/test_identity.py tests/test_protocol.py tests/test_frontier.py tests/test_admission.py tests/test_retention.py tests/test_context.py tests/test_phase4.py tests/test_browsecomp_reuse.py
.venv/bin/python -m pytest -q tests/test_reuse.py tests/test_terminal_url_reuse.py tests/test_tavily_url_reuse.py
```

32 个本地链接、章节结构、代码块配对和 `git diff --check -- design.md` 检查通过。三仓库文件指纹核对仅 design.md 改变；本机旧 `workspace/tool-reuse` 目录不存在。本次没有重跑 SDK 集成矩阵、真实 GPU、Ruff/Pyright 或生产效果实验，下列命令保留为后续实现改动的验证入口。

常用验证入口：

```bash
uv run pytest -q
uv run ruff check flowpilot integration tests
uv run pyright flowpilot
git diff --check
```

具体 SDK/集成命令和历史运行记录见 [验证说明](docs/verification.md)。测试使用临时 SQLite；不能将旧业务库作为 smoke 数据源。

### 14.2 既有引擎证据

2026-09-22 的原生池回归和真实 vLLM 实验覆盖 GPU 尾块、GRACE 交接、延迟 DROP、KEEP 到期、hybrid query 起点，以及 GPU 淘汰后普通请求自主 CPU 恢复。直接引擎证据见 [修复验收](../experiments/flowpilot/kv-fixes-20260922/REPORT.md) 和 [descriptor 修复验收](../experiments/flowpilot/conversation-descriptor-fix-20260922/REPORT.md)。

[独立多轮场景](../experiments/flowpilot/conversation-scenarios-20260922/REPORT.md) 记录两个模型共 128 次真实请求、112 次续接，其中 97 次有非零真实共享范围；在实际共享范围内核对 query 与原生命中。它验证引擎观察与自然恢复，不直接证明 FlowPilot 调度收益。固定非思考模板的实机证据范围另见 [模板验收](../experiments/flowpilot/nonthinking-prefix-qwen35-20260922/REPORT.md)。这些是既有记录，本次未重新运行。

### 14.3 既有三组件联动

[2026-09-23 真实请求验收](docs/real-workflow.md) 使用 OpenHands Agent、FlowPilot、本机 Qwen3.5-9B 与真实本地 Tool：Terminal curl 访问本地页面，web_search 访问可重复的本地搜索端点，均不是外部 Tavily 服务。

8 个压力 Job 的一次运行记录 5 个 OpenHands 对话、18 次推理/admission，页面和搜索各实际请求一次，14 次 OFFLOAD:APPLIED、1 次 DROP:APPLIED；CPU KV load 增量 828,112,896 bytes。另一次 4 个压力 Job 运行记录 14 次推理，历史复用会话期间观察到 CPU load，但采样不能精确归因于其中的 Tool 后继请求。

独立并发 admission 补测以真实 HTTP GatewayCall 占满唯一 credit，观察到高紧迫度先于低紧迫度放行，最终 `inflight=0/free=1`。它证明队列竞争与 credit 行为，不是 OpenHands 并发 Tool workflow 或吞吐收益样本。

### 14.4 仍需的实验

需要对明确标识的 Tool 后继请求证明 CPU-only 入站、自主恢复、真实 bytes 与引擎结果一致；不能只凭 OFFLOAD 收据或进程累计 load 得出结论。本次目标查询与条件成本排序已经实现，仍需独立对照收益实验。

对比应固定模型、资源、CPU backend 和原生修复版本，分别测 APC 基线、retention-only、query/cost-only 与组合；加入无 forecast、无合成先验、忽略 restore-cost 等消融。分别记录 SLO goodput、weighted JCT、Job 公平性、request-2 形成/到达/派发/首 token、Tool 重复执行、KV 实际恢复与成本估计误差。

Semantic active 需要独立、代表性的真实标注；DCS 需要与 immediate delivery 对照；长期容量、故障恢复、多 worker 与多种模型/layout 需要各自证据。不能将本机短时成功外推为生产保证。

## 15. 研究贡献与完成状态的表述

当前可以描述的实现是：在 OpenHands 权威历史与本地 Tool 执行边界内，建立双向网关、可信 Tool 结果复用、exact DCS、事实 ready-time、单实例 admission，并通过独立能力接口控制已完成 KV 的去留。vLLM 始终拥有实际恢复/重算。

生产标定、经校准 Tool 预测、强 Job 公平和端到端 SLO 收益仍需后续工作。论文或报告应分别标注 `implementation complete`、`local verification complete` 和 `production evidence insufficient`，不能用一个阶段号或一次 smoke 替代这三类结论。
