# FlowPilot：协调 Tool 复用、KV 驻留与推理续接

> 设计更新：2026-10-10。调度与 KV 策略以 [最简成本方案](docs/simple_cost_model_for_flowpilot.md) 为依据，研究表述以 [无 SLO 框架](docs/flowpilot_paper_framing_without_slo.md) 为依据。本文将两者纳入当前权威设计；原 SLO 导向策略已撤销，源码尚待迁移，见 §16。
> 本次只更新设计与维护技能，不修改运行代码。历史实现核对基线：FlowPilot `fd4b3e48062991e711aaa803075d467cbcdf7eb9`，OpenHands SDK `501f89bba6bd5a551bbfd7323e9087ed28bbcc37`，本地 vLLM `98dff2a81d747d1dba01a47f939f48c3526d4206`（0.29.0）。这些基线及既有本地修改不是新策略已实现或已验证的证据。

## 0. 设计结论与当前范围

FlowPilot 接收 OpenHands 的完整 LLM 请求，代理到推理服务，并将回复交还对应 conversation。OpenHands 负责 agent loop、权威历史、安全决策和所有真实 Tool 执行；FlowPilot 负责身份、line-tail、复用与外部 admission；vLLM 负责推理、KV 存储和全部恢复/重算。

当前主线是协调 Tool reuse、KV retention 和 request admission，减少多轮 Agent 执行中的重复等待与推理启动开销。Tool reuse 改变后继完整请求的形成时间，KV 去留改变其启动成本，admission 决定推进顺序，并把实测排队等待反馈给后续轮次的驻留决策。

目标请求排序仅为 `score_r = W_r - K_r`，两项同为毫秒，分数越大越先派发。首次 KV placement 使用 `H_r = G_r + Q_hat` 估计下一次使用前的窗口，在合法动作中比较恢复、传输与驻留成本；选择后冻结。SLO/deadline、DAG importance、workflow 进度和 Job 在途惩罚均不参与这两个决策。`W−K` 是协调机制中的轻量准入启发式，不是端到端目标函数，也不提供关键路径优先或 Job 公平份额保证。

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
| 单实例 admission 基础设施 | 单队列、健康探测、配置化 credit；源码默认仍为旧 `prefill_slack` | 关闭；启用后默认 limit=8 |
| KV retention | 本地 KV control v1 的 descriptor 查询与 KEEP/OFFLOAD/DROP | 关闭；需要 vLLM 扩展 |
| 目标 prefix / CPU restore 成本 | 全量排队请求查询与离线标定已有实现 | 查询随 admission 开启；旧排序待替换 |
| `W−K` admission | 当前目标；尚未迁移源码 | 不能由旧配置启用 |
| `G+Q_hat`、无 SLO retention | 当前目标；沿用首次选择冻结与回执机制 | 排队反馈和成本比较待改写 |
| 多 worker / 多副本接管 | shared-state 契约存在，完整事务与恢复未接通 | 不支持 |

调度模式固定一个实例：admission 或 retention 任一开启，`Settings` 就拒绝多实例配置。基础代理仍保留旧多实例路由代码；这不构成当前调度设计的实例放置、请求迁移或 KV migration 能力。

当前源码仍含 `prefill_slack`、`slo_unexpired_first`、`weighted`、按过期状态分配的 best-effort 额度以及 retention 的 SLO 超支优先分支；它们是待移除的实现差距，不再是有效设计或新默认策略。旧实验只保留为历史记录。已有 descriptor 观察继续用于已完成请求的 KV 去留；完整目标请求使用独立的真实渲染/hash 查询，旧 descriptor 不能证明新内容。

FlowPilot 的 KV 控制始终只有去留与观察：发送 KEEP/OFFLOAD/DROP，查询 prefix 和回执。不存在外部 RESTORE、恢复队列、恢复 deadline、H2D 预留或 GPU-ready 准入屏障。CPU-only 请求正常提交，vLLM 在该请求生命周期内自行恢复或重算。

组件细节见 [运行接口](docs/runtime.md)、[Tool 复用与 DCS](docs/tool-reuse.md)、[请求调度](docs/scheduling.md)、[本地 vLLM KV 控制](docs/vllm-kv-control.md) 和 [验证与证据](docs/verification.md)。这些实现说明与旧提案可能尚未同步；设计以本文为准，已实现行为须核对源码，不能把旧文档中的策略带回新契约。

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

启用时设置 `FlowPilotConfig(enabled=True, gateway_url=..., api_key=...)`，且 `tool_concurrency_limit == 1`。未显式指定身份时，`LocalConversation` 根据自身 UUID 派生 `job-<conversation_id>`、`line-<conversation_id>` 和 root conversation 身份；恢复同一 conversation 时保持这些 ID。benchmark 采用此默认映射，`run_id/task_id/attempt_id` 仅用于实验记录，不再把整批独立任务注册为一个 Job。真实多 Tool 调用按 provider 顺序执行，每个调用保留独立 `tool_call_id` 和对应 Observation。辅助 LLM 默认不加入此路径（`include_auxiliary_llms=False`）。仅更改 base URL 不会自动注册身份。

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

`RequestRecord` 保存 logical request、attempt、`llm_call_id`、model、arrival、instance/response 引用和 ToolCallSummary；`LineMetadata` 保存 conversation/parentage 等注册事实。现有协议中的 deadline、weight 可作为兼容/评估元数据保留，但不参与新策略。Tool resolution、forecast、DCS 事务与 KV descriptor 均有独立所有者，不复制进 LineTail。

### 2.3 唯一显式依赖

```text
waiter_line --DEPENDS_ON--> prerequisite_line
```

`DependencyUpdate` 以 version 原子替换 prerequisite 集合，校验同 Job、重复项和环；冲突时保留旧版本。parent/spawn 只是来源关系，不自动变成等待边。当前协议没有通用 ALL/ANY/quorum predicate 字段，Runtime 应将其消解为依赖集合更新。Tool Call 是 response 属性，不是 DAG 节点。

### 2.4 Tail 更新与失败

注册产生 EMPTY。接收合法完整请求后替换 tail 并进入 ACTIVE；获得完整回复后，存在未解决 Tool/依赖/上下文屏障时为 BLOCKED，否则为 READY。新请求仍需校验期望版本、上下文和当前线路条件。失败或取消通过 request backup 回滚未提交替换，并返回权威版本供下一 attempt 对齐。

普通无 Tool 回复使 frontier 进入 READY，不能自动视为整个 workflow 已结束。OpenHands 在显式 LocalConversation.close() 时，对已注册、无活跃本地工作且仍属于本 runtime 的 EMPTY/READY tail 发送 line finish；ACTIVE/BLOCKED 或被替换的 tail 不强制终止，关闭失败明确记录。run() 返回不会自动结束 line，保证同一会话可以继续交互。显式 line finish 或不可恢复的 context 冲突才具有终止语义；结束后按依赖关系回收线路状态。

### 2.5 依赖事实与临时投影

真实依赖用于判定是否能形成/提交后继完整请求，并可用于执行后的路径归因。旧 `ProjectionCalculator` 中的 DAG importance、SLO urgency、request weight，以及 admission 的 blocking-line 释放项均退出策略。当前 frontier 不代表完整未来 DAG，不能据此宣称识别了剩余关键路径。

临时投影保留真实 readiness、`T_need/G_r`、tail version、队列进入时刻、`W_r`、真实 prefix 观察、条件 `K_r` 及来源。`Q_hat` 属于单实例 Scheduler 的实测统计；`H_r` 与采用的反馈快照属于 response 的首次 retention 决策。它们不成为 LineTail 字段或持久 request profile。

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

预测器沿用经注册验证的 OpenHands 身份。benchmark 的 `run_id` 是实验批次，与 Job 为一对多关系；`task_id/attempt_id` 通过采集日志关联根会话与 Job，不作为替代身份。`flowpilot_request_identity` 保存实验到请求身份的映射；DCS 返回 `flowpilot.final_identity` 后，`flowpilot_response_identity` 与真实工具 RTT 反馈使用最终调用身份，Job/Line/conversation 不变。

## 3. 端到端运行路径

### 3.1 请求 1：注册、代理与可选准入

1. 网关认证并解析身份、模型、上下文，创建 GatewayCall，原子更新 tail。
2. forecast 开启时启动后台任务；不等待预测结果再转发。
3. admission 开启时将完整请求放入唯一队列并记录单调时钟进入时间；查询真实目标 prefix、用兼容标定估计 `K_r`，按 §5 的 `W−K` 目标策略选择，有健康 credit 才放行。出队后再次校验 tail version、request/call、ACTIVE 阶段与依赖，在锁外发送 HTTP。排序源码尚待迁移。
4. retention 已协商成功时，在请求的 `kv_transfer_params.kv_control_binding` 附加关联字段；保留其他 transfer 参数。该字段由 FlowPilot 拥有，拒绝客户端覆盖。
5. 请求发给固定实例；vLLM 自行验证 prefix 并执行推理或 CPU 恢复。网关等待的是普通推理响应。

### 3.2 回复、SSE 与 reuse 决策

公开推理入口只有 `/v1/chat/completions` 和 `/v1/responses`。基础模式保留响应 body、SSE 顺序、usage、Tool fragments、状态码和适用的重复头。observer 只在完整 Tool Call 闭合后记录事实；错误、断开和取消都有独立 terminal 与上游关闭路径。

HTTPX 支持的压缩响应统一解码后转发，移除已解码的 `Content-Encoding`、旧长度及失效的实体校验头，非流式重新计算 `Content-Length`；不支持的编码保持声明。完整 Chat `[DONE]` 或 Responses `response.completed` 帧在交给客户端前完成 tail 提交和资源关闭，不依赖上游 EOF；不完整或错误的 Tool fragments 仍产生协议错误。身份校验或初次 tail 登记期间取消也记录 CANCELLED，后续 logical request retry 可使用递增 attempt 和新 call ID。

Responses 的 `failed/cancelled` 对象即使 HTTP 状态为 200，也记录为 GatewayCall `provider_error` 并回滚未提交 tail；SSE 的 `response.failed`、`response.cancelled` 与 `error` 帧在交付前完成回滚、资源关闭和 credit 归还，不等待 EOF。状态码和 provider 正文保持原样，失败回复不触发 Tool 复用或预测。`response.incomplete` 同样闭合流；结构合法的部分回复按正常回复提交，未闭合 Tool 参数仍按协议错误回滚。

当前 `_drive_gateway_reuse()` 只驱动非流式完整响应。它通过响应 `flowpilot` 控制元数据向 OpenHands 交付每个 Tool 的复用决策。SSE 路径直接转发，不能描述为已在网关缓冲整轮并隐藏 Tool response；缺少网关决策时 SDK 在实际 Tool 边界发起 resolve，已有显式决策则不重复查询。Runtime 侧另有专门的 DCS 路径，验证范围须区分。

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
| benchmark `search(query, top_k=5)` | `benchmark_search_v1` | Hotpot SQLite/RPC、BrowseComp SQLite；exact / 受约束 semantic query |
| benchmark `search(query)` | `benchmark_native_search_v1` | BrowseComp MCP 的本地包装；exact / 受约束 semantic query |
| benchmark `read_document` | `benchmark_read_document_v1` | Hotpot；doc_id、start_sentence、max_sentences 全部 exact |
| benchmark `get_document` | `benchmark_get_document_v1` / `benchmark_native_get_document_v1` | BrowseComp SQLite 按 docid/offset；MCP 按 docid；仅 exact |
| 原生 Terminal curl/wget | `terminal_url_fetch_v1` | 只识别受限单 URL GET/HEAD 和 stdout 读取 |
| 既有专用 curl/url_fetch | `curl_url_fetch_v1` | 兼容现有专用 adapter，不是 Terminal 接入依赖 |

其他普通注册项仍由 controller 的 registry/descriptor 规则处理，例如本地实验 `web_search`；这不意味着可以自动将任意新 Tool 视为可信只读工具。专用 adapter 映射见 [registry.py](flowpilot/reuse/adapters/registry.py)。

当前实验只选择 benchmark 的 `search/read_document/get_document`。Tavily 与旧 `browsecomp_search_mcp_v1` 已退出默认适配列表和当前实验配置；保留显式旧 adapter_id 的兼容实现、专用回归和历史实验资料。

benchmark registry 由 OpenHands 的 `benchmark_adapters.reuse_profile` 从同一 TOML 配置及实际 Action schema 导出。注册选择使用工具名、input schema digest 和 `required_data_source_constraints`；同名 `search` 可以有多个配置，匹配零项或多项都返回不适用，真实工具仍由 OpenHands 执行。不能根据调用中是否省略 top_k 猜后端。

`benchmark-retrieval:<policy_digest>` 绑定 benchmark、backend、不可变 corpus_revision、索引位置、top_k、snippet_chars、read_chars、Action 默认值和结果契约。RPC/MCP 还要求声明 `server_policy_revision`，覆盖服务端检索器/模型、k、snippet/tokenizer 和文档返回规则；声明不是远端资产证明，服务端配置改变必须换版本。SDK 对本地工具和 MCP 都传递真实输入 schema 摘要。schema、profile、deployment/namespace、语言/区域及 freshness 均为硬约束。

Terminal parser 拒绝文件下载、认证/上传、POST、变量、管道和复合命令；不分析 Python/Node 脚本语义。只规范化 URL 的 scheme/host、默认端口和空路径，保留 query 顺序和路径字节；curl/wget family、选项及 timeout 参与 exact key。`url_exact` 开启两者，`curl_url_exact` 仅开启 curl。匹配不运行 embedding。

Terminal 发布要求真实 command/input digest、成功退出、非 timeout/is_error 与 Observation 关联。交付保留正文并绑定当前 command，清除 leader 的 pid/cwd/hostname 等本地元数据。成功退出不等于 HTTP 2xx；parser 不验证 curlrc、代理、别名或隐式 shell 状态。registry namespace/policy/version 必须对应兼容环境，复用不会重放 shell 历史和内部缓存。

benchmark exact 保留 query/docid 原文，只补齐实际 Action 的默认参数并移除 SDK 已剥离的 summary/security_risk 注记。搜索仅 query 可软化，top_k 等仍精确一致。文档读取不进入 embedding；offset/句子范围或 read_chars/profile 变化不能命中原页。缓存校验、完整保存并交付 `RetrievalObservation`，包括 MCP 包装的 JSON 数组或逐 hit JSON 文本；不把它误当原生 `MCPToolObservation`，不裁剪正文。复用仍绑定当前 consumer 的 tool_call_id，只真实执行产生 RTT 学习反馈。

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

匹配与新 leader 注册在同一锁内完成。语义评分在锁外读取快照，回到锁内核验 binding generation、存活状态与 lease，并再次查询历史，处理评分期间 leader 已完成发布的竞争。匹配成功返回 `WAIT_AND_SYNC_REUSED_RESULT`，exact 匹配且允许 deferred 时为 `DEFER_WAIT_FOR_INFLIGHT`；没有匹配才返回 `SYNC_AND_EXECUTE_AS_LEADER`。

这里的 running 是 binding 生命周期：leader 在 resolve 时注册，可能尚未收到真实 Tool START。因此能力准确地说是“对已登记、尚未完成的调用意图合并”，并非扫描或接管所有正在运行的本地进程。只有进入 registry 和 reuse 协议的调用参与匹配；in-flight 表在进程内，重启不会自动恢复。

Follower 通过 binding poll 等待真实发布，再校验 freshness、预算和自身身份；lease/失败/取消不能伪造成成功。现有回归包括 `test_exact_inflight_never_calls_embedding_worker`、`test_concurrent_semantic_misses_revalidate_snapshot_generation`，以及 BrowseComp 的 history/in-flight、candidate/active 参数矩阵。

### 4.4 真实执行来源与发布

LLM ToolCallRef 可以先于 Action 注册。START 绑定真实 Action 和 execution attempt；FINISH 核对输入/结果 digest、大小、终态及 adapter 格式，OpenHands 提交 Observation 后才能发布。MCP Action 使用原生 `to_mcp_arguments()`，不能把内部 `data` 包装当成 provider 参数。

SQLite 单事务提交 payload、origin execution evidence、索引和 publication receipt。重复的相同发布返回原 receipt，冲突不能成为可信结果。Follower 始终使用自己的 provider identity，不能接收 leader 的私有对话或 LLM 回复。

初始 TTL 从服务端接受 FINISH 的观察时间起算，默认 300 秒，实际窗口取发布请求 TTL（未指定时用 registry default）、registry max（未指定时用 default）和可选 scope max 的最小值。可缓存结果每次成功 history、poll 或 deferred 复用后滑动续期为 `命中时间 + 原有效 TTL`；每次使用同一窗口，持续命中可持续保留。原始 `observed_at` 和 publication receipt 不变，`reuse_entries.expires_at` 保存当前到期时间，交付 provenance 返回续期后的期限。查询候选、交付校验失败、发布重试和容量保护不续期；已过期/撤销结果不能复活，不可缓存结果仍按初始 FINISH TTL 交付给 follower。leader 失败、取消或 lease 到期有显式终态，不能用预测结果填充成功载荷。

### 4.5 Semantic 模式

wire version 分别为 `flowpilot-phase1-reuse-v3` 与 `flowpilot-phase3-reuse-v3`；数据库版本另算。Semantic 需 registry 显式允许；shadow/candidate 不替代真实执行，active 才允许语义结果交付。当前 benchmark 仅 search 的 query 可参与非时间敏感语义匹配；read_document/get_document 和 Terminal URL 保持 exact。active 语义复用可与 deferred_context_enabled 同时开启：语义 history/in-flight 命中立即交付给 OpenHands，由其提交 Observation；DCS 隐藏续跑仍只接受 exact，语义命中不得进入 PendingContextDelta。

[Qwen3Embedding](flowpilot/reuse/semantic.py) 使用本地模型，默认路径 `/docker/data/HF_MODELS/Qwen3-Embedding-0.6B`，向量默认 1024 维并 L2 归一化；运行时不下载权重。开启原生语义复用时，服务启动先加载模型并完成一次预热，再开放请求和后台向量重建；加载失败直接使启动失败。单个 worker 串行处理不同文本，合并相同在途请求；等待者取消不会中断已开始的原生推理。运行中的 embedding 故障不使有效 exact 载荷失效。已有人工标签和 BrowseComp active 实验不构成语义等价、检索排名保持或生产质量证据。

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

## 5. 基于启动成本与累计等待的 admission（目标契约）

### 5.1 资格、状态与信息边界

只有真实 Tool 结果、依赖和上下文条件满足并形成完整输入的请求才能入队。预测 gap 归零不代表 ready。OpenHands 请求与获授权的 DCS continuation 共用一条队列、相同公式和固定实例；等待 Tool 的 continuation 不占队列或 credit。

[ProjectionCalculator](flowpilot/scheduling/projection.py) 提供事实 readiness 和 Tool gap；[AdmissionQueue](flowpilot/scheduling/admission.py) 拥有外部队列、时间、顺序和 credit；[OfflineCostModel](flowpilot/scheduling/cost.py) 与 target-prefix query 提供条件启动成本。Tool 命中已体现在请求形成时间、实际上下文和 KV 查询中，不再增加 Tool hit 分数，也不增加 `ToolCost`、DAG weight 或 Job fairness 排序项。

### 5.2 时间、prefix 与启动成本

`a_r` 是完整请求实际进入外部 admission queue 的单调时钟时间，`W_r = now - a_r`。不从 workflow start、网关预处理开始或 Tool START 累计；过去 Tool 时间不重复计入等待。workflow start 和旧 `CP_q` 若保留，只用于测量/兼容。

每轮对排队完整请求调用 `/v1/kv/query-target`，使用真实 Chat/Responses 渲染、input processor 和引擎 hash/coordinator/connector，得到 prompt tokens `P`、`H_gpu`、`H_all` 及真实候选 CPU 对象 bytes。这里 `P` 表示 token 数，排序分数统一写作 `score_r`，避免与来源文档的 `P_r` 混淆。GPU/CPU 重叠不重复计数；不查询已派发请求或全部 line tail。

```text
K_gpu = calibrated_prefill(P, H_gpu)
K_cpu = calibrated_H2D(actual_object_bytes) + calibrated_prefill(P, H_all)
K_r   = min(K_gpu, K_cpu)     # 两个候选均可评估时，沿用现有条件成本估计
K_r   = K_gpu                # CPU 候选不可评估，但 GPU/cold 路径成本有效时
```

没有可靠目标 prefix 时，只能在真实 token 数与兼容标定均可用时使用显式 cold 估计 `calibrated_prefill(P, 0)`；否则 `K_r=unknown`。CPU 未知不是零成本；仅有 OFFLOAD 回执不证明 CPU 恢复范围。GPU 完整/近完整命中也仍需遵循 backend/logits 等残余计算规则，不能一律置零。

`K_r` 只覆盖当前输入的条件恢复与残余 prefill，候选最小值不是 vLLM 实际执行计划或实际 TTFT 的保证。成本须记录来源、版本、观察时间、场景与不确定性。统一换算为毫秒后排序，不直接将 tokens/bytes 与时间相减；不包括 decode、引擎内部排队、剩余轮数、未来 Tool 或 workflow 总成本。

### 5.3 唯一排序公式与未知成本处理

```text
score_r = queue_wait_ms - kv_start_cost_ms       # 降序
QueueKey_r = (queue_entered_monotonic_ms + kv_start_cost_ms,
              enqueue_sequence)                # 等价升序，平分按入队序
```

第一版不加权重。无 deadline、deadline 已过或尚未到期完全同等处理；不使用 urgency、剩余预算、Job 在途数、blocking lines、DAG 深度或 workflow 进度作主键、桶或平分项。旧过期分流及其 best-effort 额度不进入新策略。

在同一选择时刻，最大化 `W−K` 等价于最小化 `a+K`。固定 `K` 时，两个已排队请求不会仅因时间流逝交换顺序；KV 状态改变后重算成本才可能改变它们的相对顺序。持续有 credit、成本有界且没有无限先到积压时，累计等待可抵消后来低成本请求的优势；这不是完整 workflow 的公平份额或最优 JCT 保证。

示例：A 的 `(W,K)=(200,20)`、B 为 `(200,300)`、C 为 `(600,300)` 毫秒，分数分别为 `180,-100,300`，顺序是 **C -> A -> B**。低启动成本不等于短 decode 或短 workflow。

**成本不可用时整轮 FIFO（用户确认的处理）：** 对本轮快照中的可提交候选，先刷新并校验成本；若至少一项仍缺失有效毫秒 `K_r`，该轮整个候选集合统一按 `enqueue_sequence` 派发，显式记录 `ordering_basis=fifo:cost_unknown` 与缺失原因。缺失项保留 `K_r=unknown`，已知项保留其估计，不将未知写成零，不将已知/未知成本分桶，不回到 deadline-only。健康和 credit 条件仍适用；后续轮次全部成本有效时恢复 `W−K`。若派发前观察失效且无法取得有效 cold 成本，重新确定本轮排序，不能沿用失效分数。

插入、heartbeat、credit 归还和依赖变化合并触发全量 sweep；同一 sweep 只查询一次各排队请求，原子更新仍在队列的项并可连续派发多个 credit。查询期间新增项进入下一 sweep，取消项不会重新占用 credit；sweep 的候选边界和 FIFO 处理原因须可观察。

观察包含 engine epoch/state_version 和本地开始时间，现有默认 TTL=2 秒。epoch/身份不匹配、水位失效或过期时重查或使用明确的 cold/unknown 路径；sweep 返回时已经过期的估计不能用于成本排序。不因 SLO 风险刷新。TTL/水位均不保证查询后不再失效，普通引擎请求仍执行真正的 lookup/acquire；不锁住查询到的块，不等待 GPU-ready，也不在派发后重新 probe 阻塞请求。

### 5.4 单队列、健康与 credit

`SchedulingRuntime -> AdmissionQueue` 是生产调用链。一个 `asyncio.Lock` 保护 waiting/inflight，按 `(job_id,llm_call_id)` 管理 credit。健康且 `free=max(0,effective_limit-inflight)>0` 时按 §5.3 选出请求并原子占用 credit，调用方在锁外发 HTTP；派发后不抢占或重排。

默认每 1 秒 GET 上游 `/health`，timeout=1 秒，TTL=5 秒。`limit=8` 是网关配置上限，不是引擎动态 batch 容量。健康失败或过期停止新派发，已接纳 GatewayCall 继续完成。terminal、取消和发送失败释放相应 key，重复 release 不增加额度。

可选 `admission.adaptive.enabled=true` 将 `limit` 作为硬上界，初始有效额度 24、下界 16（需配置 `limit>=24`；实验上界 32）。单独每 10 秒读取固定引擎 `/metrics` 的真实 running、waiting、preemption 累计量和完成请求累计量，以 30 秒窗口观察；不预测单请求 decode 或内部等待，不改变 vLLM 内部顺序。窗口平均 waiting>=2，且抢占增加或等待不下降，同时完成速率相比上一窗口改善不超过 5%，连续两窗口后额度减 4。waiting<2、无新增抢占、完成速率>0 且有额度需求，连续三窗口后加 1。上/下界、步长与窗口可配置；这些阈值是待实测的控制参数，不是最佳并发的测量结论。GPU KV 使用率本身不触发减额。

减额低于在途数时，停止补发并等待正常终态归还；不提前释放 credit。窗口更新和派发共用 queue 锁，prefix RPC 返回后重新检查最新有效额度。指标缺失、非有限值、多个同名引擎序列或传输失败显式记录 `metrics_unavailable`，保持最近额度并重建观察窗口，不把缺失数据当零负载。采样间隔超过三倍配置值或累计计数回退也重建窗口；`/health` 的独立健康门控仍有效。新策略不需要 OpenHands 核心或 vLLM 扩展修改，但启用自适应需要上述实测指标。snapshot 同时暴露配置上界 `limit`、`effective_limit`、样本年龄和调整原因。自适应 credit 是独立的已有可选能力；验证新排序时先固定额度，避免混淆效果。

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

### 5.5 实测 admission wait 反馈

Scheduler 维护固定实例近期完整请求的实测 admission wait 滑动平均 `Q_hat`。每次真正获准派发时记录一次 `dispatch_monotonic - queue_entered_monotonic`；不在轮询、重复 release 或请求终态时重复加样本。排队取消尚未派发的请求不产生完整 wait 样本，取消等待应另行统计。该口径不包含 Tool 时间、网关前处理、引擎 restore/prefill/decode，也不是 `queue_work_before_tokens` 除以假定吞吐。

当前队列为空且有健康可用 credit 时，首次 retention 可读取 `Q_hat=0`，来源明确标记为空闲快照；它不作为伪造观测写入均值。否则使用有效近期样本的均值，没有有效观测时保持 unknown。实现需显式记录滑动窗口、有效期、样本数、观察时刻和配置版本；窗口长度/有效期属于待标定参数，本文不伪造已有默认值。不得等待未来样本来阻塞回复或首次决策。

这是实例级粗略负载反馈，不是每请求等待 ETA；长 decode、负载突变和当前仍在等待的请求会使它滞后。该反馈只供后续首次 placement 读取，不进入 `W−K` 的额外奖励项、不修改已经冻结的 placement。

## 6. Forecast、Tool Resolution 与 T_need

### 6.1 可选预测

[ForecastManager](flowpilot/scheduling/forecast.py) 异步接收版本化 `ForecastRequest/ForecastResult`，校验 request/tail、catalog/predictor version、Top-N、duration quantiles、confidence 和 TTL。默认 timeout=0.25 秒、TTL=30 秒、Top-N=3；可注入 adapter，仓库提供 NoOp 和 TraceReplay，没有内置生产预测器。

超时、取消、不兼容、低置信度、过期或事实 Tool 到达后的晚结果被丢弃，不改变转发、Tool 执行或上下文。当前 prewarm 回调只保存版本化 forecast metadata，没有真实 payload 预取，也不据预测修改物理缓存 LRU。

独立的 response 侧 `tool_duration_adapter.on_response()` 接收已完成的非流式回复，只提交预测工作，返回覆盖本次调用的 awaitable；awaitable 完成表示适用估计已通过 resolution 的版本校验写入。它与 Tool Cache 匹配、后台 KV 查询并行。返回 None 表示没有待收集的预测，不允许以 None 代表仍在后台运行且需要本次 KV 决策等待的任务。预测器自行管理其 timeout；缺失、异常、超时或取消时，未解决本地 Tool 的预测为 unknown，使用既有显式 retention 规则完成一次选择。失败批次的部分估计不参与这次选择。仓库仍未提供生产 Tool 时长预测器，也未新增预测超时参数。SSE 保持原样转发，当前不调用此需要完整回复正文的 hook。

`on_resolution()` 是可选预测反馈。反馈异常或预测器自身取消只记录异常类型和 `tool_duration_resolution_failures`，不改变已登记的 Tool 事实、复用决定、telemetry 接收及正常回复；请求任务自身的取消继续向上传播。日志不包含预测器异常消息中的私有输入。

DCS 内部续接携带原 `x-flowpilot-predictor-context`，重新构造请求身份。`elapsed_ms` 统一从最外层请求进入网关计算，包含先前推理、复用和续接准备时间；预测器将其加到发送端提供的快照年龄，不能逐轮重新计零或重复累加。预测上下文仍由 `_forward_headers` 排除，不进入上游 provider 请求头。

### 6.2 事实与实验先验

[ToolResolutionStore](flowpilot/scheduling/resolution.py) 保存 history/in-flight/local resolution、status、version、ready_at_estimate 和实际时延/大小。真实 Tool 名称、参数、命中、发布和本地生命周期覆盖预测；预测不能断言 cache hit，也不能产生 Tool Result。

`FLOWPILOT_SYNTHETIC_TOOL_DURATIONS=1` 是独立、默认关闭的实验开关。事实未命中后，名称含 `search` 的工具取 1000–2000 ms，其余取 100–200 ms，来源标为 `synthetic_factual_family_v1`；可用 seed 固定序列。它不 sleep、不延长真实工具时间，不能当作经过校准的性能测量。复用命中不套用该先验。

### 6.3 T_need 的实际含义

ProjectionCalculator 从当前 tail 的 resolution 读取估计。本地多 Tool 按 provider 顺序串行累计：已开始项使用预计剩余时间，尚未开始项累计完整 duration；in-flight follower 使用绝对 ready-time。未知项或未解决 line 依赖使 T_need 保持 unknown；`ready` 仍要求 frontier READY/EMPTY 且无未解决项。预测不是事实完成保证。

历史缓存命中写入 READY 和当前 ready_at，对 KV 使用的剩余 Tool gap 贡献严格为 0；已有原始 duration 估计可以保留作记录，但 READY 项不参与串行累计，晚到预测也不能覆盖 READY。全部 Tool 命中且没有其他 line 依赖时，KV 的 Tool gap 为 0；部分命中只累计未解决的本地 Tool，不能取多 Tool 时长的最大值。in-flight follower 尚未拿到结果时仍需等待真实 leader，不视为零时长历史命中。

retention 在首次选择时使用 `H_r=G_r+Q_hat`、当前 prefix 的条件 prefill/transfer 成本与独立容量；Tool gap `G_r` 按上述串行事实与时长估计形成。任一必要项未知则 `H_r` 未知，未满足的依赖不能当作零等待；`H_r` 不代替 readiness 检查。admission 只排序已形成的完整请求，不直接消费 forecast 或预测未来请求内容。

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

### 7.8 无 SLO 的首次 retention 选择（目标契约）

未声明 gateway reuse policy 时，不能把 header 缺失当成缓存未命中；仍等待 SDK 对各 Tool 的 resolution 或实际 START/终态事件。仅在 policy 明确排除某工具时，才可直接认为该工具不参与复用。SDK 按串行边界逐个上报时，KV 选择也相应推迟到所需事实齐全，普通回复及 Tool 执行保持不阻塞。

每个有效 response 的 descriptor 只选择一次 placement。完成回复并登记事实 Tool Calls 后，并行收集 response 预测、reuse policy 中可复用调用的匹配结果及 KV descriptor/容量观察；必要信息收集完成后才调用 `choose_retention()`。缓存结果已就绪或只剩 in-flight follower 时，不再等待本地执行时长预测。DCS 每轮内部回复同样登记匹配事实和收集输入；SSE 的匹配由现有 SDK Tool 边界上报，后台 KV 等待不阻塞 SSE 或真实 Tool 执行。

等待预测期间，每次 Tool resolution 或真实执行事件都会重新检查当前事实。全部本地 Tool 已完成，或只剩 in-flight follower 时，取消已不需要的预测并继续首次去留决策；仍有未解决的本地 Tool 时继续收集预测，不因其中一个 Tool 完成而提前选择。取消预测不计作预测失败。

首次选择读取当时的 `G_r` 与 Scheduler 的 `Q_hat`，形成 `H_r=G_r+Q_hat`；选定 action、reason 后，将 gap、queue feedback、窗口、成本来源及容量快照一同冻结，不因后续 Tool 状态、排队均值或压力变化重新优化。信息收集沿用现有引擎 metadata TTL，过期或已被后继请求替换的 source 不再下发动作；正常 response 转发与 admission credit 归还不等待 KV 决策。`kv_retention_decision` 记录唯一选择，`kv_policy_receipt` 单独记录命令执行状态。line 结束时的 DROP 是需求释放，与 placement 选择分开。

`choose_retention()` 对 TERMINAL/确认不可恢复 prefix 优先按能力释放为 DROP；unknown recoverability 不等于零。其余情况先按真实 prefix、引擎能力和容量确定合法候选，再比较传输、恢复、残余 prefill 与驻留成本。删除 `remaining_slo_seconds`、`remaining_SLO-gap` 和预计预算超支这一首要比较项，不以 deadline 或 importance 给成本加权。

沿用现有成本/资源价格模型，将原来只用 Tool gap 的驻留窗口替换为 `H_r`。下式全部用秒与 GiB，排序前保持单位一致；`rho_gpu/rho_cpu` 的单位为秒/(GiB·秒)，因此驻留项是策略折算成本；`rho_gpu` 包含下述既有压力系数：

```text
H = G + Q_hat
J_KEEP    = residual_prefill_gpu + rho_gpu * gpu_GiB * H
J_OFFLOAD = D2H_new + H2D + residual_prefill_cpu + rho_cpu * cpu_GiB * H
J_DROP    = cold_prefill
selected_action = argmin J_action over legal, evaluable candidates
```

这是合法动作间的局部成本启发式，不是 workflow 总时长预测。沿用现有 OFFLOAD 的传输窗口判断时，使用估计的使用前窗口 `D2H_new <= H`；它只是候选成本假设，不证明未来请求到达时复制已经完成，也不增加恢复等待屏障。实际请求若提前到达或 CPU 副本已淘汰，vLLM 仍自主验证并决定恢复或重算。

引擎的 `cpu_standalone_tokens` 已覆盖整个 offload target 时，`D2H_new=0`，不要求新增卸载标定；仍计算真实对象 bytes 的 H2D、残余 prefill 和 CPU 驻留价格。仅有 OFFLOAD 回执、未知覆盖或部分 CPU 覆盖不能当作完整副本。GPU、CPU 容量与保护约束继续独立核验。

Tool hit 只使 `G=0`，不能使 `H=0`；即使 phase=READY，队列繁忙时仍使用 `H=Q_hat` 评估 KEEP/OFFLOAD/DROP。不得沿用旧代码中 `phase == READY` 将整个驻留窗口强制置零或无条件判定近端需求的捷径。没有其他等待且队列空闲、有健康 credit 时，`H=0` 才有明确依据。

驻留价格是配置策略参数，不是测量值：沿用现有 GPU=1 秒/GiB/秒、CPU=0.01 秒/GiB/秒及低 free capacity 时 GPU 价格乘 2 的启发式，需实验校准。GPU bytes 是 descriptor 对象的去重容量，不是全局共享块的边际容量，不宣称最优共享块分配。未来 Tool 输出长度未知，仍使用已知 prefix 加一个后继 token 的 `ASSUMED_CONTINUATION`，后继完整请求到达后以真实 target query 重新估计 `K_r`。

无兼容模型或 `H` 未知时，保留现有显式 `fallback_cost_unknown` 能力/容量处理，并记录具体缺失项：仅已知 `H` 足够近且不承压时使用近端 KEEP 分支，否则能力支持时 OFFLOAD，再到 KEEP/unsupported。沿用 horizon=1 秒、free<=128 为压力的现有参数；不把未知 `Q_hat/G` 或 READY 状态当作零窗口。回退不含 SLO，且不应在实验中算作已完成成本模型决策。该迁移仍待实现。

### 7.9 后台刷新与回执

正常回复完成后后台 resolve；单次 RPC 默认 timeout=1 秒，PENDING、传输错误及可重试 5xx 按 refresh_seconds 间隔重试，整体受引擎 capabilities.metadata_ttl_seconds 约束。tail 替换、epoch 变化、明确 UNKNOWN_BINDING/EXPIRED 或取消结束重试；旧引擎缺少 TTL 字段时沿用单次 timeout 窗口，不猜测寿命。已确认支持绑定的引擎发生短暂能力 RPC 故障时仍携带 ingress binding，但 retention 动作暂停，直至重新协商成功；明确 unsupported 则停止绑定。retention 默认每 1 秒重新协商、读取 telemetry、轮询 pending operation；输入就绪而尚未选择的 source 可以完成首次决策，已选择的 source 只处理回执、同一动作重试、过期和 line finish 清理，不重新比较 KEEP/OFFLOAD/DROP。不会每周期全量查询全部 line tail。

resolve 响应携带引擎 `observed_at_monotonic`，与 handle 的 `expires_at_monotonic` 属于同一时钟域。FlowPilot 用两者之差计算剩余 TTL，再加本次 RPC 的本地开始时间设置到期定时器；不直接比较不同主机的单调时钟，也不因传输或重试延长有效期。同一 descriptor 再次 resolve 保留更早的本地截止时间。

定时器只清理 FlowPilot 的 source 引用和策略跟踪，不依赖 RPC 锁、健康协商或新请求，也不发送 DROP。telemetry 中匹配 owner/engine epoch/descriptor 的 `DESCRIPTOR_EXPIRED` 事件可提前清除引用；事件缺口不触发全部 tail 查询，由本地 TTL 保证最终清理。晚到的查询/回执不会恢复已删除引用或触发后续策略；tail 替换、DROP 完成、引擎 epoch 变化及关闭时取消对应定时器。物理复制和 GPU/CPU 清理由引擎继续负责。

`/flowpilot/v1/scheduling/state` 的 retention source 当前暴露 `inputs_ready`、冻结的 `selected_action` 和 `tool_gap_seconds`，以及最近命令的 `action`/回执和 `remaining_ttl_seconds`；新契约还需暴露首次选择的 `queue_wait_estimate`、`retention_window`、反馈来源/样本数/时间和各候选成本，这些字段尚待实现。`source_expirations` 区分 deadline 和 engine_event 清理。缺少 resolve 时钟字段的旧引擎响应明确报 resolution validation error，不猜测 300 秒或建立无限期本地引用；此协议补齐需要两端配套更新。未增加独立的提前续接超时，仍沿用引擎 metadata TTL（默认 300 秒）。

命令携带 epoch、descriptor、source call、tail/policy version、action_id/idempotency_key；客户端在执行前重查当前 tail。HTTP 响应丢失时重试同一保存命令。ACCEPTED 只表示异步处理中，APPLIED/PARTIAL/FAILED 分开记录，不能把成功 HTTP 传输视为策略成功。

OFFLOAD 的 PARTIAL/FAILED 保留真实失败状态，至少等待一个 refresh_seconds 后重新查询 descriptor，并使用新 action_id 和新 policy_version 重试已选定的 OFFLOAD，不重新优化 placement。未收到回执的传输重试仍复用原命令，避免重复执行。DROP 的 PARTIAL 由引擎延迟 intent 继续处理，不生成无意义的重复 DROP。事件刷新合并在途触发，RPC 期间新到事件不会丢失；某个 source 的传输或校验失败单独计数，不跳过其他 source。

### 7.10 离线成本模型

[OfflineCostModel](flowpilot/scheduling/cost.py) 现支持七特征 cadence 冻结系数；当前唯一分发配置为 [Qwen3.5-27B / TP4 / seq256](examples/experiments/qwen35_27b_tp4/README.md)。旧 27B 分桶配置已替换，9B 参数及默认引用已移除；旧格式解析与外部历史测量证据保留。模型配置记录 source、version、带时区 measured_at、model、engine identity、测量口径与原始 fit hash，不在线重拟合。

vLLM 在 target/descriptor query 中只读提供 `prefill_load`：当前运行中 decode 数、KV 长度总和 C、活跃 prefill 数与真实 budget/block/seq 等配置。FlowPilot 使用七特征 `theta·[1,N_pre,B_dec,Σq²,C,Σqh,N_pre(C+Σh)]`，在 `candidate_prefill_frozen_decode` 条件场景中固定观察的 B/C、按原实验的 784-token 对齐规则累加候选残余 prefill。未来首 batch 和其他 prefill 的预算分配未知，不推测为事实、不控制 vLLM 调度。retention 的 prefix+1 场景延用当前负载，并随首次 placement 冻结；不是后继时刻负载预测。

负载不可用、身份/配置不匹配或非正模型输出保留 unknown，使用既有整轮 FIFO 与 retention unknown 路径。GPU 全命中仍需最后一个 token 的 logits 计算。prompt/特征超过标定范围显式标为外推；支持的上下文上限不等于实测覆盖。cadence 为引擎异步完成间隔，不是 GPU kernel time、TTFT 或内部等待 ETA。原生首 batch 预测器的 54.21% P90 误差不能作为派发前路径已验证精度。

restore 复用四特征实验中与当前 seq256 引擎 identity 完全一致的独立 H2D 冻结拟合：10 条单请求训练，6 条真实单请求恢复测试 P90 APE=27.99%，并发恢复竞争未验证。offload 使用同配置独立实测的冻结 D2H：`0.004039796055271255+2.044685376438999e-11*new_bytes` 秒，零新增字节为零成本；14 条训练、测试前冻结，12 条留出中位/P90 APE=8.61%/19.70%，最大 31.09%。测量范围 205324288–17058037760 bytes，空闲原生传输微基准含观测开销，未验证并发、部分副本组合或跨会话精度，范围外属于外推。

vLLM 只读查询提供 `offload_new_object_bytes`，按完整目标的实际缺失对象求和，已完成 CPU 副本不重复计费；有未完成 CPU 写入时返回 null，不估计在途剩余时间。FlowPilot 用该字段计算 D2H，用完整 `offload_object_bytes` 计算 H2D 与 CPU 驻留。完整 CPU prefix 覆盖仍免新增 D2H；旧引擎缺少新增字节字段时，其余 D2H 候选保持 unknown。只有实际 bytes 与适用标定均可用时才评估条件 CPU 候选；未知不记为零，CPU-only 请求仍可普通提交。传输接口保留 `fixed+actual_bytes*rate`；禁止 token-to-byte 推算及 worker-summed time。GPU/CPU 驻留价格是策略系数。模型采用点估计，不自动添加安全裕量。

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

网关续跑的 `context_sequence` 按委托起点的序号加累计 `delta_seq` 计算；每轮独立推进 request/LLM 身份与 tail version。同步后的普通请求仍须通过同一套上下文序号、cursor 和 digest 校验。

本实验链路的 SDK Chat（list/string）与 Responses 消息序列化均保留工具全文，不再按字符数裁剪；缓存结果和 DCS 增量也不裁剪。上下文窗口、输出 token 预算及 DCS 字节容量仍生效，不能通过截断伪造成功。网关构造 Chat 续接消息时省略响应中的空值字段和空白 assistant content，使其与 OpenHands 重建的历史一致；原始响应及非空字段保持保留，历史和摘要校验不放宽。

### 8.4 同步、ACK 与恢复

DCS 状态为 `open/syncing/acked/aborted/diverged`。同步按完整 provider batch 分片，Runtime 原子应用每个完整 chunk 后 ACK；不能拆开 assistant/tool 批次。部分 chunk ACK 后仍为 syncing，全部 ACK 后才 acked 并允许继续。不是所有 chunk 必须一次全量提交的实现。

lease 或 delta TTL 过期是停止委托续接的同步屏障。网关保留已完成的真实推理响应，只处理明确的 `DCSSyncRequired`，身份、cursor 和 digest 冲突仍拒绝。自动进入 syncing 后，首次 sync 在同一屏障原因下登记完整 envelope，之后仍严格校验重复请求；无 pending delta 的过期委托可以释放。

网关 DCS 元数据通过 `barrier_reason` 传递停止原因。如果当前响应及其完整缓存结果批次已写入 WAL，但下一次推理尚未获授权，`response_deferred=true` 表示 SDK 在同步中应用该批次后结束本次 step，下一步由 Agent 发出普通请求，避免再次处理同一响应。SDK 同步和异步路径使用相同规则；成功同步前不续跑，lease 不自动延长。

ACK 对齐 epoch、lease、cursor、sequence 和 chained digest；重复 ACK 返回幂等结果，冲突进入 diverged，禁止猜测或 merge。已确认 delta payload 删除，保留有限审计记录；不能把有界 ACK receipt window 宣称为无限期 exactly-once 网络交付。

WAL 实际 `PRAGMA user_version=4`，旧库显式拒绝。重启恢复还需 OpenHands 权威历史、恢复 manifest 与 Job/Line 重建，不是仅靠 WAL 自动恢复全部 workflow。当前 snapshot 的 `wal_schema_version` 直接读取真实 PRAGMA 值，与 §11 一致。

## 9. 调度目标与策略边界

### 9.1 研究目标

目标是在相同任务质量、固定资源和可比负载下，降低 mean/P95 workflow JCT，提高成功完成 workflow/s，并减少重复 Tool 执行和实际 KV 恢复/重算开销。请求命中率、token throughput、GPU 利用率及公平性分布为辅助诊断。SLO attainment 如需保留，只能作为可选离线统计，不能重新成为控制输入或首要优化目标。

`W−K` 只修正完整请求的 admission 次序，不直接减少总 prefill 工作，也不预测剩余 workflow。只有减少的 Tool/KV/排队等待影响实际完成路径时，才可能改善并行 workflow 的 JCT；分支等待不能简单相加成 JCT。长 decode 若主导 credit 占用，启动成本排序的收益可能有限，须由端到端实验确定。

### 9.2 Job / Line / queue 策略

只有一条 admission queue。Job 用于身份关联、统计和真实依赖校验，line 提供实际阻塞事实；完整请求只按 §5.3 排序。新增子 line 不创建新 Job；本版没有 Job 份额、在途惩罚或 line importance 调度。可报告各 Job 的等待分布，但不能把 request aging 称为 workflow 公平保证。

内部 continuation 与 Agent 请求共享公式、固定实例和 credit。已经发送的请求不迁移、不抢占；CPU prefix 不影响可提交性。`gateway/router.py` 中旧 WeightedFairRequestQueue 和多实例策略不是 SchedulingRuntime 的 admission 实现，其测试不能替代实际队列验收。

### 9.3 在途等待与压力

兼容 follower 等待真实 leader，结束条件包括完成、失败、lease 或 Runtime 等待超时/取消。预测值不自动触发重复执行；成本偏好不放宽 freshness、scope 或 semantic threshold。协议 lease、RPC timeout 和客户端取消仍是生命周期/故障机制，不是被撤销的 workflow SLO 调度预算。

现有 backpressure 包括 admission credit、Tool payload 容量和 DCS 消息/字节/轮数/期限。尚无通用每 Job ready-line 配额、每 Agent context-sync 字节限流或完整多副本 drain orchestration，不能写成部署保证。

## 10. 正确性、隔离与可观测性

缓存命中保留来源 provenance，不能伪装为本地新执行。所有复用交付核验硬约束与 freshness；当前 provider identity 和消息顺序不因缓存来源改变。query 相似不等于结果/证据等价，active semantic 仍须独立质量验证。

入口认证与部署 namespace 是复用边界。FlowPilot 使用 `X-FlowPilot-API-Key`，向上游剥离私有 `X-FlowPilot-*` 头；provider Authorization 与控制 RPC 凭据分别处理。DCS payload 加密，Tool cache 的载荷存储不等于 trace；不能因为 trace 已脱敏就声称缓存已实现通用 PII 检测、加密或全系统删除治理。

[TraceRecorder](flowpilot/observability/trace.py) 只写元数据 JSONL，默认单文件 64 MiB、3 个备份。写入失败累加 failure/drop 并降低健康；本地 append/rotation 不提供跨进程 exactly-once 审计。不得记录 prompt、完整 Tool 输入/正文、凭据或 leader 私有上下文。

当前可观测入口包括 health、JSON/Prometheus metrics、GatewayCall、frontier、Tool resolution、forecast、reuse、DCS 和 `/flowpilot/v1/scheduling/state`。后者目前仍含旧 prefill slack/score；目标需改为 `queue_wait_ms`、`kv_start_cost_ms`、`score_ms`、`ordering_basis`、成本来源/覆盖率、prefix 水位、`Q_hat` 及样本统计、冻结的 `G/H` 与 retention 候选成本，同时保留 credit、KV capability 和 receipt。字段存在与迁移完成须分开核对。`/tool-analysis` 是兼容的派生诊断入口，不是持久化的 ToolAnalysis owner。

外部 queue wait、上游首字节、完成时间可观察；内部 prefill/decode/restore 的归因需引擎数据。请求 2 的精确 prefix、恢复成本误差、Job 等待分布和长期成功完成 workflow/s 尚不能从现有 gateway 计数推导。

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

1. **成本证据缺口：** 全量目标查询、TTL 校验和离线模型排序已有实现；新 `W−K`/排队反馈/无 SLO retention 尚未实现，生产标定、并发干扰误差、查询成本与端到端收益仍需测量。观察不提供长期驻留保证。
2. **清理与重试边界：** OFFLOAD 失败会在有效 tail/TTL 内重试同一 placement；DROP 的 PARTIAL 仍表示保护尚未解除，不能当作 APPLIED。控制面长期不可达时不保证完成保留策略；关闭 unresolved 会话不会越权强制终止。DCS snapshot 直接读取真实 PRAGMA user_version，当前为 4。
3. **部署/质量证据缺口：** shared-state 未接通完整多进程事务；生产 predictor、semantic active 质量与长期容量/恢复效果尚无充分证据；强 Job 公平不在本版策略范围。

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
                 profile.py, metrics.py, prefix.py, cost.py, capacity.py
  state/         shared.py
  observability/ trace.py
```

`app.py` 组装这些组件。§5、§7.8 和 §16 中新增的公式、反馈字段与策略配置是迁移目标，不能按此段实际模块列表宣称已接通。没有已落地的独立 `control/request_store`、`kv_directory` 或 `prefix_cost_projection` 模块。OpenHands adapter 位于 SDK，KV 物理控制位于 vLLM 仓库；FlowPilot 无须也不应内置 Tool executor。

### 12.2 配置与 HTTP 入口

[Settings](flowpilot/config.py) 和 [app.py](flowpilot/app.py) 是实际配置/路由依据。网关默认 `0.0.0.0:9000`，ingress auth 默认开启，request timeout=120 秒；`create_app` 明确拒绝 workers!=1，即使提供 shared-state path 也不构成多 worker 服务。

| 配置 | 作用 |
| --- | --- |
| `FLOWPILOT_UPSTREAMS` / `FLOWPILOT_INSTANCES_JSON` | 上游地址；调度启用时恰好一个实例 |
| `FLOWPILOT_INGRESS_API_KEY` | 网关入口凭据 |
| `FLOWPILOT_UPSTREAM_CONTROL_API_KEY` | health/tokenize/KV RPC 的可选上游凭据 |
| `FLOWPILOT_HTTP_MAX_CONNECTIONS` | FlowPilot 自建共享 HTTP 客户端的最大连接数，默认 100；实验 profile 使用 `flowpilot.http_max_connections`。推理与 health/tokenize/KV RPC 共用此池；不改变 admission credit 或 vLLM 容量。注入客户端时由注入方管理，health 中标为 injected |
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

Gateway 维护 transport fidelity；Frontier 维护 identity/tail/dependencies；Reuse 维护匹配、execution origin 和 publication；DCS 维护 delegation/delta/ACK；Scheduler 维护唯一 queue/credit、`W−K` 和实测 `Q_hat`，SchedulingRuntime 向 retention 提供只读反馈；Retention 在首次选择时计算 `H` 与动作成本并冻结；vLLM extension 维护 KV 实体和动作安全。

OpenHands 当前需要已有 SDK adapter 及 Agent/Conversation/LLM hooks，而不是仅静态配置。基础推理代理不需要 KV 扩展；retention 和 descriptor query 必须由本地 vLLM control v1 提供，目标查询需要既有 target-query 扩展，成本比较另需匹配的离线标定。本次策略迁移主要在 FlowPilot 内，不要求增加 Tool executor hook 或引擎恢复控制接口。

### 13.2 依赖关系

```text
M0 identity + gateway + telemetry
  +-> M1 exact / in-flight reuse -> M2 exact DCS
  |                           +-> M3 semantic reuse
  +-> M4 forecast envelope / factual Tool ready-time
  +-> M5 single-instance admission

factual readiness + real vLLM capabilities -> M6 retention
M5 + reliable target-prefix/cost support  -> W-K admission
M5 measured queue wait + factual gap      -> M6 G+Q first retention
```

M4 和 M5 可在 M0 后独立验证，不要求先开启 reuse/DCS。query/cost、无 SLO retention 与排队反馈应分别报告；已有旧策略实现不代表新契约验收通过。缺少成本时按 §5.3 的显式整轮 FIFO 保持普通推理。

### 13.3 阶段门槛与当前落点

| 阶段 | 当前落点 | 不能据此宣称 |
| --- | --- | --- |
| M0 | 双 API、身份、五态 tail、telemetry、终端清理已有代码和测试 | 任意重启/多副本恢复 |
| M1 | exact/in-flight、可信发布、benchmark RetrievalObservation / Terminal adapter | 所有 Shell 环境等价或远端资产自动验证 |
| M2 | exact DCS、加密 WAL、分批同步/ACK、恢复用例 | 无界历史、token cap、无限期 exactly-once 或统一隐藏 SSE |
| M3 | shadow/candidate/active 与向量后端 | active 生产质量或语义检索等价 |
| M4 | forecast contract、NoOp/replay、事实覆盖、实验先验 | 已部署/校准生产预测器 |
| M5 | 单队列、健康、credit、全量目标查询已有；新门槛是 `W−K`/整轮 FIFO 与实测 `Q_hat`，待迁移 | Job 公平份额、关键路径优先或最优 JCT |
| M6 | descriptor、OFFLOAD 安全回收和本地 CPU reuse 已有；新门槛是无 SLO 成本比较、`G+Q_hat` 首次冻结，待迁移 | 新策略已实现、外部恢复控制或端到端效果 |

### 13.4 现有回归入口

| 范围 | FlowPilot 测试 |
| --- | --- |
| Identity/frontier/dependencies | `test_identity.py`、`test_protocol.py`、`test_frontier.py` |
| Gateway/SSE/terminal/trace | `test_gateway.py`、`test_stream.py`、`test_app.py` |
| Exact/semantic/origin/publish | `test_reuse.py`、`test_phase3_api.py` |
| benchmark / URL adapters | `test_benchmark_reuse.py`、`integration/test_benchmark_reuse.py`；保留 Terminal/Tavily/旧 BrowseComp 回归 |
| 独立 Tool 容量 | `test_cache_retention.py` |
| DCS | `test_context.py`、`test_dcs_api.py` |
| Forecast / ready-time | `test_phase4.py` |
| 实际 admission | `test_admission.py` |
| KV client / 移除旧恢复接口 | `test_retention.py`、`test_kv_removal.py` |
| 旧路由/shared-state 契约 | `test_phase5.py`；不替代实际 admission 或多 worker 验收 |

OpenHands 的 `tests/sdk/test_flowpilot.py` 与 `test_flowpilot_mcp_arguments.py` 验证 adapter；[集成矩阵](integration/test_openhands_reuse.py) 覆盖 history/in-flight、gateway/direct、DCS/admission 配置组合。mock inference、受控 Tool fixture、真实 Terminal HTTP 和 GPU 推理证据必须分别说明。

## 14. 验证证据与后续实验

### 14.1 本次文档更新与历史证据

2026-10-10 本次只更新设计及维护技能，核对旧源码中的排序、retention 和接口，进行文档一致性、链接、diff 与技能格式校验。没有实现 `W−K`、实测排队均值或无 SLO retention，没有重跑 GPU workflow，也不将旧测试通过数算作新策略验证。具体文档校验结果在本次交付报告中列出。

历史记录：2026-09-26 定向回归 **211 passed**，覆盖当时的身份/frontier、旧 admission/retention、DCS、forecast 和 Tool reuse。这些结果只支撑当时实现；旧 slack/预算超支用例必须改写，新门槛见 §16。旧链接检查、文件指纹和三组件 smoke 也不替代本次新算法证据。

常用验证入口：

```bash
uv run pytest -q
uv run ruff check flowpilot integration tests
uv run pyright flowpilot
git diff --check
```

具体 SDK/集成命令和历史运行记录见 [验证说明](docs/verification.md)。测试使用临时 SQLite；不能将旧业务库作为 smoke 数据源。

### 14.2 既有引擎证据

旧版文档记录：2026-09-22 的原生池回归与真实 vLLM 实验覆盖 GPU 尾块、GRACE 交接、延迟 DROP、KEEP 到期、hybrid query 起点及 GPU 淘汰后的普通请求自主 CPU 恢复。多轮场景记录两个模型共 128 次请求、112 次续接，其中 97 次有非零共享范围；另有固定非思考模板验证。这些属于引擎观察与自然恢复的历史证据，不能直接证明新调度收益。

以下四份历史报告在当前工作区的原路径不可用，本次无法复核；保留归档定位，不将缺失文件列为已校验链接：

| 历史报告 | 原路径（相对仓库根目录） |
| --- | --- |
| 修复验收 | `../experiments/flowpilot/kv-fixes-20260922/REPORT.md` |
| descriptor 修复验收 | `../experiments/flowpilot/conversation-descriptor-fix-20260922/REPORT.md` |
| 独立多轮场景 | `../experiments/flowpilot/conversation-scenarios-20260922/REPORT.md` |
| 模板验收 | `../experiments/flowpilot/nonthinking-prefix-qwen35-20260922/REPORT.md` |

### 14.3 既有三组件联动

[2026-09-23 真实请求验收](docs/real-workflow.md) 使用 OpenHands Agent、FlowPilot、本机 Qwen3.5-9B 与真实本地 Tool：Terminal curl 访问本地页面，web_search 访问可重复的本地搜索端点，均不是外部 Tavily 服务。

8 个压力 Job 的一次运行记录 5 个 OpenHands 对话、18 次推理/admission，页面和搜索各实际请求一次，14 次 OFFLOAD:APPLIED、1 次 DROP:APPLIED；CPU KV load 增量 828,112,896 bytes。另一次 4 个压力 Job 运行记录 14 次推理，历史复用会话期间观察到 CPU load，但采样不能精确归因于其中的 Tool 后继请求。

历史并发 admission 补测以真实 HTTP GatewayCall 占满唯一 credit，观察到旧紧迫度排序并最终 `inflight=0/free=1`。它只能支撑旧队列与 credit 行为，不是 `W−K` 验收、OpenHands 并发 Tool workflow 或吞吐收益样本。

### 14.4 新方案的实验与消融

需要对明确标识的 Tool 后继请求证明 CPU-only 入站、自主恢复、真实 bytes 与引擎结果一致；不能只凭 OFFLOAD 收据或进程累计 load 得出结论。已有 target query 和离线模型可复用，新排序与驻留反馈仍需独立实现和验证。

固定模型、任务集/质量、GPU/CPU 容量、并发/到达负载、CPU backend、原生修复版本与 credit 策略，至少完成以下对照：

| 对照 | 要隔离的作用 |
| --- | --- |
| 原生 APC + FIFO；Tool reuse only + FIFO | 重用本身减少的重复 Tool 工作 |
| Tool reuse + 无 SLO Tool-aware retention（只用 G）+ FIFO | Tool gap 对 KV 驻留的贡献 |
| 上一组 + `W−K`，仍不加 Q_hat | 启动成本排序相对 FIFO 的贡献 |
| 上一组 + `H=G+Q_hat` 首次 retention | 实测排队反馈的增量贡献 |
| retention-only、query/cost-only 与组合；无 predictor、无合成先验、忽略 restore-cost | 模块与估计各自的贡献和误差敏感性 |

主要结果为 mean/P95 workflow JCT 和成功完成 workflow/s，同时报告失败/取消率、任务质量与负载条件。机制证据包括 request-2 形成/入队/派发/首 token、实测 wait、K 误差与覆盖率、整轮 FIFO 触发比例、G/Q_hat/H、选定动作、实际 GPU/CPU 驻留及恢复/重算、重复 Tool 执行和 query RPC 开销。SLO attainment 可作附录统计，不用来解释新算法为何成立。

专门覆盖 Tool hit 但 queue 忙、长 Tool gap、CPU-only、unknown 成本/反馈、prefix 失效、多分支依赖及长 decode 占用 credit。并行 workflow 应按真实执行完成路径归因，不把重叠分支等待求和作为 JCT 改善。独立 predictor 的误差/overhead 结果只能支撑预测器选择，不能证明多模块协调带来端到端收益。

Semantic active 需要独立真实标注；DCS 需要与 immediate delivery 对照；长期容量、故障恢复、多 worker 与多种模型/layout 需要各自证据。不能将本机短时成功外推为生产保证。

## 15. 研究框架与论文改写

中心表述为：FlowPilot 协调 Tool-result reuse、KV retention 与 request admission，减少 Agent serving 的重复等待和推理续接开销。Tool reuse 改变推理状态何时被需要，FlowPilot 将这一时间变化传入 KV 去留，再用实际目标的启动成本与等待时间指导准入，并将实测排队等待反馈到后续首次 placement。

`W−K` 是该系统闭环的一个轻量策略。论文不再以 SLO、slack、remaining budget 或 critical-path-aware scheduling 为主线；critical path 只作为解释端到端生效条件与执行后归因的概念。不能从当前 frontier、aging 或公式推导最短 workflow 优先、全局最优 JCT、最大完成吞吐或强 Job 公平。

依据 [框架改写说明](docs/flowpilot_paper_framing_without_slo.md)，章节按以下方案调整；此处规定叙事目标，不表示已修改或重新核验原论文 PDF：

| 位置 | 新内容 |
| --- | --- |
| 标题、摘要、引言 | 高效多轮续接；解释 Tool 节省的时间为何可能被 KV 恢复与准入等待抵消 |
| Motivation / observations | 保留 Tool reuse shifts KV demand；补充相同 ready 状态下不同启动成本，以及 Tool hit 但队列繁忙 |
| Overview | `reuse outcome -> G -> H=G+Q_hat -> retention -> actual K -> W-K admission -> measured Q_hat` |
| KV management | 合法动作下的传输、恢复、驻留成本；删除预算超支优先分支，说明首次选择冻结 |
| Admission | Resumption-Cost-Aware Admission；解释 `W−K`、等价 `a+K`、真实 prefix 刷新和整轮 FIFO |
| Evaluation / limitations | mean/P95 JCT、成功完成 workflow/s、模块消融、完成路径归因与长 decode 限制 |

所有权与已有能力仍可描述为：OpenHands 保持权威历史及本地 Tool 执行，FlowPilot 提供双向网关、可信复用、exact DCS、事实 ready-time 与单实例队列，vLLM 始终拥有实际恢复/重算。新协调策略当前为设计完成、代码待迁移、端到端证据不足。报告必须分别说明 `implementation complete`、`local verification complete` 和 `production evidence insufficient` 的适用范围。

## 16. 代码迁移方案与验收（本次不实施）

按当前源码逐函数核对的实施细则见 [成本调度重构方案](docs/cost_based_refactor_plan.md)，包含队列时钟、整轮 FIFO、排队反馈接线、协议与配置迁移、测试替换和提交顺序。

按以下顺序改写，复用现有 query、离线标定、单队列/credit 和首次 placement 生命周期：

| 步骤 / 组件 | 具体改写 | 验收依据 |
| --- | --- | --- |
| 1. `scheduling/admission.py` 与实验配置 | 用 `W−K` 及 FIFO 对照替换旧策略；移除风险/预算/importance/Job 惩罚及过期 best-effort 额度。旧策略配置给出明确迁移错误，不能静默改义；具体新配置名随实现同步 | 同等待低成本优先；同成本 FIFO；C/A/B 示例；仅改变 deadline、weight、Job/line 结构不改变同一候选集合的顺序 |
| 2. `scheduling/prefix.py`、`cost.py`、队列 sweep | 复用真实 target query 与条件 K；统一毫秒，维持 TTL/epoch/取消处理；任一候选成本缺失则整轮 FIFO | GPU 残余非零、CPU 条件成本、cold、混合已知/未知、无标定、过期重查；成本固定时仅时间流逝不改变相对顺序 |
| 3. `scheduling/admission.py`、`runtime.py`、观测 | 派发时只记一次实际 wait；维护有来源和窗口的 Q_hat，向 retention 提供只读快照 | 重复轮询/release 不加样本，排队取消另计；空队列且健康 free credit 可用零，无有效样本 unknown；不混入 Tool/decode |
| 4. `scheduling/retention.py`、`projection.py` | 首次选择使用 G+Q_hat；去掉 remaining_slo 入参和超支比较、READY 强制零窗口；按合法动作的 J 比较并冻结 | Tool hit + busy queue；已有完整 CPU 副本零新增 D2H；未知 G/Q/标定；改变 deadline 不改变动作；后续反馈/压力不能重选 |
| 5. snapshot、trace、配置文档与技能 | 暴露 W/K/score、成本覆盖与整轮 FIFO 原因、Q 样本及冻结 G/H；移除活跃 SLO 策略说明，历史证据显式标记 | 字段单位、来源和生命周期一致；不将估计写成测量或 pin；兼容元数据不回流决策 |
| 6. 定向测试、真实路径与消融 | 改写旧排序/预算回归，运行实际 OpenHands -> FlowPilot -> vLLM 后继请求，再做 §14.4 对照 | 排序/feedback/retention 各自验证；质量固定；有真实 engine recovery 和端到端结果后再报告收益 |

重点修改现有 `test_admission.py`、`test_admission_capacity.py`、`test_prefix_cost.py`、`test_retention.py`、`test_response_retention.py` 和 `test_phase4.py`。保留 heartbeat 失效、原子 credit、取消/终态恰好归还一次、RPC 锁外执行、CPU-only 普通提交、冻结回执重试和独立容量回归。客户端/RPC timeout、descriptor/lease 到期继续保留；它们不属于已撤销的 SLO 调度。

此次策略改写不要求 OpenHands 新增控制逻辑，也不要求 vLLM 提供 RESTORE 接口或改变恢复顺序。真实 prefix/bytes/能力与离线标定仍是有成本排序的前提；缺失时按明确状态处理并记录覆盖率。排队均值的窗口和有效期需在实现时显式配置、用负载实验选定，不先承诺生产默认值或收益。
