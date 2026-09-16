# Tavily 与 URL 获取工具的 Tool Reuse 模块修复计划

本文是 FlowPilot Tool Reuse 单一模块的修复计划，目标是补全当前 exact reuse、trusted origin、TTL 维护、Tavily 与 URL 获取结果的复用，并为后续 semantic reuse 留出稳定接口。涉及 Gateway、frontier 和 OpenHands 的修改仅限于该模块所需的调用入口、身份关联、执行凭据和结果交付接口；不以重建整个 Gateway、Scheduler 或 DCS 为本轮交付。

本文以 [design.md](../design.md) 为架构与发布依据。URL 获取结果复用已纳入 design.md §§0、4 和 13，是本轮目标；OpenHands 执行 Tool、FlowPilot 负责复用的所有权保持不变。OpenHands 目录下早期 Phase 文档和历史大纲仅作为实现线索。

本次修订同时确认：Tavily 的输入、输出和可校验事实以当前实际接口提供的信息为准；不要求 Tavily 提供额外证明，不从纯文本反推已经丢失的结构；不兼容旧版 Tool Reuse 协议、数据库或缓存条目，新版本使用独立的新库冷启动。

## 1. 不变的系统边界

FlowPilot 与 OpenHands 的职责保持如下划分：

- OpenHands 持有 agent loop、权威对话历史、Action/Observation 顺序、安全策略和所有真实 Tool 执行。
- FlowPilot 持有 provider 请求代理、Tool Registry、canonical descriptor、exact/semantic historical lookup、in-flight leader/follower、freshness、provenance、审计和维护任务。
- FlowPilot 不执行 Tavily、curl、wget、Shell 或任何其他 Tool。
- 复用结果必须由 OpenHands 按当前 Action 的 observation_type 构造为当前 tool_call_id 对应的 Observation。
- 存在未确认 DCS 增量时，任何本地执行都必须发生在 context sync 已确认之后；M1 无缺失增量时走正常本地执行边界。复用结果不能被当作新的真实 origin 再次写回。

计划内的 Workstream 名称是开发工作流名称，不是发布门槛名称。发布能力必须映射到 design.md 的 M0 到 M3：

~~~text
M0 共享身份、事件、Gateway 和 metadata-only observability
M1 exact historical reuse 与 exact in-flight binding
M2 exact DCS；只处理 exact historical/follower
M3 conservative semantic historical/in-flight reuse
~~~

Tavily 和 URL exact 分别在各自 M1 验证后启用。Tavily semantic 只能在 M3 的离线证据和灰度条件满足后启用。URL 命令在拥有隔离执行证明前只能 shadow 或 execute_locally；这是执行接口前置条件，不能将 parser 完成或 shadow 运行报告为 URL active reuse 已交付。已有 DCS 路径需验证与本轮复用接口兼容，新增 DCS 能力不属于本轮修复。

## 2. 现有实现事实与改造切入点

计划必须围绕当前代码改造，不另造一套并行协议：

1. FlowPilot 当前通过 ToolTelemetryEvent 接收 START、FINISH、FAIL、CANCEL，并通过 reuse binding 的 result endpoint 接收 leader 结果。
2. 当前有两类 resolve 入口：GatewayService._drive_gateway_reuse 在完整 provider Tool Call 到达后直接调用 controller；OpenHands Agent._execute_action_event 在真实执行边界调用 FlowPilotRuntime.resolve_reuse，消费 Gateway 决策或在未配置 Gateway reuse 时调用 resolve endpoint。Gateway 已决策的调用不能再次登记另一份 binding。
3. Gateway 登记时尚无本地 Action，action_id=None；OpenHands 执行时才有 action_id。当前 leader 完成路径发送 Observation model_dump，必须补充 provider Tool Call 到本地 Action 的关联、START 输入关联和 FINISH/result digest 校验，不能直接比较这两阶段的完整 identity。
4. 当前 MCPToolDefinition 会透传 MCP annotations；没有 annotations 时，tool.annotations 为 None。当前 DCS eligibility 要求 readOnlyHint=True，因此 Tavily 的 DCS 能力必须由显式适配器或注册策略提供，不能假设 MCP server 自动声明。
5. 当前 TerminalTool 是有状态 Shell，readOnlyHint=False，输出还可能保存到文件。它不能直接被视为安全的 URL fetch executor。
6. 当前 ToolRegistryEntry 只有 curl_url_exact，ReuseScope 只有 locale、language、region、safe_search_policy、time_sensitivity_class 和 data_source_constraints。tenant 等旧字段被协议拒绝。新方案应扩展现有 FlowPilot contract，而不是把旧 tenant 字段重新塞回请求 JSON。
7. 当前 MCP executor 在调用供应商前会展开 conversation secret 引用，而 reuse resolve 发生在展开之前。Tavily query/urls 中的 $VAR、${VAR}、${VAR:-default} 等引用不能作为稳定字面量进入缓存键。
8. 当前 tavily-mcp@0.2.1 将 Search/Extract 响应经 formatResults 拼成 MCP 文本，不保留 failed_results，也没有无歧义的结果条目分隔符。MCPToolObservation 的类型校验不能证明供应商结果完整。

主责代码为 FlowPilot reuse/；必要的接口改造面包括 protocol.py、app.py、gateway/service.py、frontier/store.py，以及 OpenHands 的 flowpilot.py 和 Agent 执行/Observation 提交边界。MCP 实现和 Tavily 启动配置用于核实输入输出事实；不要求修改 Tavily MCP server 来提供它当前没有的信息。URL 执行适配由 OpenHands 提供，FlowPilot 只消费其结果和执行事实。

### 2.1 所有入口使用同一可信上下文

resolve endpoint、Gateway 直接调用、已有 DCS 的 resolve/poll/result 路径必须经过同一 ReuseService 上下文解析与校验，向 controller 显式传入 trusted_context。该上下文从认证入口、权威 job/line 记录、已验证 Tool/adapter 配置和当前授权策略取得，不由普通请求体构造。不能只修复 app resolve endpoint 而遗漏内部调用。

### 2.2 两阶段身份关联

复用请求先使用稳定的 provider 调用引用：

~~~text
ToolCallRef = (deployment_id, namespace_id, job_id, line_id,
               tail_request_id, llm_call_id, tool_call_id)
ExecutionRef = (ToolCallRef, action_id, execution_attempt, start_event_id)
~~~

Gateway 可以用 ToolCallRef 创建 leader/follower，action_id 暂空。受信 Runtime 接受该决策后，在真实执行 START 中带回 binding_id、当前 action_id、execution_attempt 和 canonical input_digest；frontier 接受 START 后，reuse owner 对该 binding 原子关联 ExecutionRef。校验当前 ToolCallRef、leader、Tool 名称、adapter/version 和输入 digest 均匹配；同一执行的重复关联幂等，冲突关联拒绝。action_id=None 只表示尚未关联，不能被实现成任意 Action 都匹配的通配符。

后续 FINISH/publish 必须匹配已关联的 ExecutionRef。binding 的 leader/follower 索引以 ToolCallRef 为稳定键，不能因补充 action_id 注册第二份调用。Gateway historical/follower 命中没有真实 START，由 OpenHands 在交付时关联当前 Action，保留自己的 tool_call_id，不创建伪造执行凭据。直接 resolve endpoint 路径即使已有 action_id，也复用同一契约。

## 3. 第一批支持范围

### 3.1 Tavily Search

固定为工具名 tavily-search、工具族 tavily_public_search、adapter_id 为 tavily_search_mcp_v1。第一版绑定当前 OpenHands 配置中的 tavily-mcp@0.2.1，并把 MCP input schema digest 写入 adapter metadata。MCP 包、schema 或默认值变化时必须升级 adapter version，旧条目不得自动复用。

当前 OpenHands trace 中实际出现的 Search 输入字段为：

~~~text
query: string, required
search_depth: basic | advanced, default basic
topic: general | news, default general
days: number, default 3
time_range: day | week | month | year | d | w | m | y, optional
max_results: number, default 10, range 5..20
include_images: boolean, default false
include_image_descriptions: boolean, default false
include_raw_content: boolean, default false
include_domains: string[], default []
exclude_domains: string[], default []
~~~

第一版不假设 Tavily API 或 MCP schema 中没有出现的字段，例如 start_date、end_date、country、include_answer。以后若实际 MCP schema 增加字段，必须通过新的 schema/adapter version 接入。

Exact descriptor 包含所有会影响结果的字段。Semantic descriptor 只把 query 作为 semantic text；search_depth、topic、days、time_range、max_results、图像/正文选项和域名过滤都是 hard filter。为了避免时间漂移，第一版只允许 topic=general、time_range 未设置且 time_sensitivity_class=standard 的 Search 进入 semantic，并保留当前时间敏感 query 的拒绝规则。执行链实际注入 days=3 时，不能仅因该字段存在就使 general 请求失去 eligibility；默认值是否等价补齐按 §7.2 验证。days 仍保留为 hard field，topic=news 和明确时间范围只做 exact。

### 3.2 Tavily Extract

固定为工具名 tavily-extract、工具族 tavily_public_extract、adapter_id 为 tavily_extract_mcp_v1。当前 schema 为：

~~~text
urls: string[], required
extract_depth: basic | advanced, default basic
include_images: boolean, default false
~~~

Extract 第一版只支持 exact。URL 列表顺序保留；每个 URL 做不改变资源身份的规范化。未知输入字段、输入 schema 校验失败或实际返回的 MCP/Observation envelope 损坏时不参与复用。供应商未暴露的完整性字段不构成缺失 envelope，具体结果契约见 §7.2。

### 3.3 URL 获取命令

第一版只实现 curl 的安全读取子集。wget、wget2、HTTPie、xh 必须各自拥有 parser、adapter_id、result validator 和独立测试后再加入 registry；不允许把不同 executable family 直接交叉复用。

对现有 TerminalTool 的支持分为两层：

- parser 和 descriptor 可以先以 shadow 运行，记录拒绝原因和候选结果；
- 只有 OpenHands 提供隔离、无状态、无 Shell 解释的 URL 执行证明后，才允许 active exact reuse。

URL 复用明确属于本轮模块能力。优先接入 OpenHands 隔离的一次性 argv GET executor；若当前没有该接口，可由 OpenHands 的 UrlFetchTool 提供最小执行适配，其实现归 OpenHands。若继续接入 TerminalTool，必须取得无环境、工作目录、前序进程、输出文件和输入流依赖的执行事实，否则只做 shadow/execute_locally。Tool Reuse 的 parser、descriptor、结果校验和 TTL 可先独立验证；URL active reuse 完成报告必须包含真实执行适配的联调证据。

## 4. Trusted origin：以已完成 Tool 事件为可信依据

### 4.1 信任边界

trusted origin 的含义是：FlowPilot 已经收到来自受信 OpenHands Adapter 的、与当前 leader binding 完全匹配的真实 Tool FINISH，并且待写入的结果可以由该 FINISH 事件和 adapter 校验。离线 trace、Forecast、semantic 命中、follower 回放和普通客户端自报结果都不是 trusted origin。

受信依据由三层组成：

1. 认证：请求来自当前 deployment/namespace 的 OpenHands Adapter。
2. 绑定：先按 §2.2 验证 ToolCallRef，再校验已关联的 ExecutionRef；不能要求 Gateway 登记时的空 action_id 与执行时的 Action 直接相等。
3. 事件：同一 identity 先有被 FlowPilot 接受的 START，再有 sequence=2 的 FINISH；FAIL、CANCEL、冲突事件或超时都会使该执行不能成为可缓存 origin。

API key 只解决调用方认证，不能替代事件关联。FlowPilot 仍须验证 FINISH 与 publish 的结果一致。

受信 OpenHands Runtime 是真实执行的信任根；本计划不新增独立签名服务，也不声称 digest 能证明远端内容真实。FINISH 只表示 executor 返回，不代表结果可复用。MCP is_error=True、实际暴露的失败状态、Terminal 非零 exit code 或 timeout 即使伴随 FINISH，也必须由 adapter 拒绝复用。Tavily 未暴露的部分失败不能伪装成已校验成功；按 §7.2 记录 upstream_completeness=unknown，复用对象是实际收到的完整 Observation。

### 4.2 事件与发布契约

扩展 ToolTelemetryEvent 的复用执行凭据，使用以下 metadata-only 字段；这些要求适用于可复用 leader，普通本地执行不承担 adapter/result digest 契约：

~~~text
binding_id（仅真实 reuse leader 的执行事件）
input_digest（可复用 leader 的 START 必填，并由 FINISH 关联同一输入）
result_digest
result_schema_version
adapter_id
adapter_version
executor_kind
final_url_digest（仅 URL fetch；只记录 digest，不记录完整 URL）
~~~

START/FINISH 继续使用各自稳定的 event_id；start_event_id 和 finish_event_id 是对这两个已接受事件的引用，不是独立产生的事件。input_digest 对无 secret 展开依赖、与 binding 使用相同 adapter canonicalization 的输入计算。result_digest 和 result_size_bytes 使用同一份经既有 secret policy 处理、adapter 规范化且未为 follower 截取的 Observation JSON，固定排序、UTF-8 编码和 Unicode 序列化规则。不能 FINISH 计算一种表示、publish 发送另一种表示。Telemetry 不携带结果正文。

扩展 LeaderResultPublish：

~~~text
binding_id
identity
start_event_id
finish_event_id
execution_attempt
input_digest
result_digest
result
cacheable
ttl_seconds
~~~

FINISH 在 executor 返回时上报；publish 在 OpenHands 接受并提交对应本地 Observation 后发送，作为本地结果已交付的通知。不能仅在 _execute_action_event 返回事件之前发布历史条目。该最小提交回调仍由 OpenHands 持有事件权威，复用侧不写 Agent 历史。

FlowPilot publish 使用以下顺序和提交契约：

1. 验证认证上下文、ToolCallRef 和请求身份，按登记的 adapter/version 从提交载荷重新计算 result digest/size 并核对声明值，再构造完整 publication fingerprint：包含 binding_id、ExecutionRef、adapter/schema、input/result digest、result size、cacheable 和请求的 TTL。省略值按固定协议默认值规范化；策略版本在 binding/已提交 receipt 中固定。
2. 先查持久化 publication receipt。相同 fingerprint 已提交时，返回原 origin_id、observed_at、expires_at 和提交状态，不能要求 binding 仍为 running，也不能重新延长 TTL。此分支只重放提交确认，不重新交付过期结果；即使 tail 已推进，也只允许经原身份和 namespace 验证的同一次提交重放。
3. 尚未提交时要求 binding 为 running、leader/ExecutionRef 匹配且执行 lease 有效。查找 frontier 已接受的匹配 START 和 FINISH，校验输入、结果 digest/大小、adapter/schema、成功终态以及未取消/未超时事实。
4. 调用 adapter.validate_result，检查实际可用的状态、Observation 类型、secret policy、大小和结果完整载荷。不可复用结果使 binding 显式失败并释放 follower；不能用 cacheable=false 将无可信事件或错误结果交给 follower。
5. 服务端生成 origin_id 和不可变 ExecutionRef，按首次接受的 FINISH 固定 observed_at、expires_at。一个短数据库事务写入 publication receipt、结果引用以及允许历史缓存时的 reuse_entries。唯一约束防止同一执行多次生成 origin；大载荷校验/暂存在事务外完成，提交前引用须已可读，未提交的暂存数据不可被 lookup 看见。
6. 持久化提交成功后才把内存 binding 标为 complete 并唤醒 follower。数据库提交记录是发布完成的权威；内存状态丢失时从该记录恢复终态引用，不能暴露“complete 但结果未落盘”的状态。follower 交付仍须重新执行 §6.2 的 freshness 与预算校验。

同一 publication key 的不同 fingerprint 是冲突，必须拒绝；若已有提交，撤销其后续复用资格并阻止新的 follower 交付，不能覆盖首次结果或回滚已经交付的 Agent 历史。相同 FINISH 和 publish 重试保留原 event_id/指纹；已有 DCS ACK 的幂等继续由 context owner 负责。

cacheable=false 只表示不写历史索引，仍要求可信且可交付的结果，并为在途 follower 保存有 expires_at 的短期结果引用。receipt 保存期覆盖约定的 publish 重试窗口及未释放引用；超过重试窗口返回明确过期状态，不能把旧提交作为新的执行。

事务失败时整体回滚，不能设置 complete 或返回伪成功；提交后响应丢失、进程在更新内存前退出时，相同 publish 依据持久化 receipt 重放。重启不恢复未经确认的 running 执行；只恢复已经提交的发布结果，其他 binding 按既有失败/重新匹配策略处理。

如果 telemetry、规范化、publish 或结果落盘失败，记录明确失败并释放或按 lease 终止 follower 等待，不写可信历史。leader 已获得的本地 Observation 必须正常提交和返回，不因复用侧失败重跑 Tool。重试只重发原事件/发布数据。

origin 模块只读取 frontier 已接受的事件 receipt，固定为不可变 execution reference；START/FINISH/FAIL/CANCEL 的可变状态机仍只有 frontier 一份。尚待发布的 receipt 不得被 frontier 清理提前回收；提交后保留不可变引用所需证据，按引用释放规则回收。

### 4.3 Origin 记录

新增 origin 元数据，至少包括：

~~~text
origin_id
job_id、line_id、tail_request_id、llm_call_id、tool_call_id
execution_attempt
start_event_id、finish_event_id
tool_name、canonical_tool_family
adapter_id、adapter_version、result_schema_version
input_digest、result_digest
execution_source=openhands_local
observed_at、expires_at
result_size、status
upstream_completeness（Tavily 当前为 unknown，不等同于执行失败）
~~~

origin_id、leader identity、Prompt 和完整结果正文不进入 follower 可见的 provider provenance。provider-visible provenance 只保留 reuse_type、observed_at、result_schema_version 以及必要的截取信息。

## 5. FlowPilot 作用域：deployment/namespace 为权威

### 5.1 不新增客户端 tenant 层级

canonical descriptor 不再要求客户端提交 tenant、tenant_id、project 或 experiment 字段。FlowPilot 以请求入口和权威 line 记录中的 deployment_id、namespace_id 作为缓存隔离边界：

- deployment_id 是部署级 hard partition；
- namespace_id 是租户、项目或其他业务隔离域在 FlowPilot 内的统一表示；
- 单部署可由 FlowPilot 服务端配置一个默认 namespace；若认证入口和服务端默认配置都不能确定 namespace，reuse 才 execute_locally；
- 原始 API key、Tavily API key、Authorization、Cookie 和其他凭据永远不进入 descriptor、scope digest 或向量。

ReuseScope 保持现有 Tool 硬约束字段；ToolReuseIdentity 通过 §2.2 分阶段关联 Action。所有入口由共享 ReuseService 从认证上下文和权威 job/line 记录解析内部 ReuseNamespace，再显式传给 controller；Gateway 和已有 DCS 的内部调用也走该路径。不能信任请求体中的同名 scope 字段覆盖服务端值。

### 5.2 project 与 experiment

project_id 只有在 FlowPilot 控制面已经把它映射到 namespace_id 时才参与作用域；不得把 OpenHands prompt 或 Tool 参数中的 project 字段当作隔离依据。

experiment_id 默认只作为 observability metadata。只有实验改变了 adapter、结果 schema、freshness policy、安全策略或 semantic index 时，才把一个不可逆的 policy_digest 纳入 hard scope。这样既能避免实验间错误复用，也不会把普通实验标签无限扩大 exact key。

hard_scope_digest 至少由以下服务端确定的信息组成：

~~~text
deployment_id
namespace_id
canonical_tool_family
tool_version
adapter_id、adapter_version
result_schema_version
locale、language、region、safe_search_policy
time_sensitivity_class
data_source_constraints
security_policy_id
freshness_policy_id
policy_digest（仅在改变结果语义或安全边界时）
~~~

## 6. Freshness、TTL 与 Tool Cache 维护

### 6.1 TTL policy 与 exact key

TTL 在 exact key 和条目元数据中承担不同作用：

- exact key 包含稳定的 freshness_policy_id 或 ttl_policy_version。只有 TTL 规则本身改变了复用有效性时，policy version 才变化；
- exact key 不包含当前时间、origin_id、observed_at 或绝对 expires_at；
- 条目元数据保存 observed_at、effective_ttl_seconds、expires_at 和 status；
- leader 上报的 ttl_seconds 只能被 family/scope policy 截断，不能由客户端延长。

每个 origin 按 FlowPilot registry/policy 计算：

~~~text
requested_ttl = publish.ttl_seconds 或 registry.default_ttl_seconds
effective_ttl_seconds = min(requested_ttl, family_policy.max_ttl, scope_policy.max_ttl)
expires_at = observed_at + effective_ttl_seconds
~~~

未配置的 scope cap 不参与 min。observed_at 在首次接受匹配 FINISH 时固定；客户端时间需要通过时钟偏差校验，否则使用服务端 receipt time。重复 publish、重新建向量和维护任务都不能延长 expires_at。

如果将来请求需要“至少新鲜到某个时间”的语义，可增加受信的 min_expires_at 或 max_age 字段。该字段用于 lookup hard filter，不把运行时 now 放进 key。当前版本没有 per-call freshness deadline，命中条件为 expires_at > now。

### 6.2 所有复用交付路径的校验

历史 exact/semantic、已完成 binding 的 follower poll、Gateway 交付和已有 DCS 消费结果前，都必须检查共同条件：

1. origin/publication 处于可复用成功状态、未撤销，ExecutionRef 和载荷引用完整；
2. expires_at 严格晚于当前时间；
3. adapter、schema、scope、security policy 和 freshness policy 匹配；
4. Observation schema、result digest、大小和载荷完整性通过；
5. 当前调用的 OpenHands delegation/safety policy 允许返回该结果。

向量维度、有限数值、normalization、model/index/pipeline version 和索引 namespace 仅属于 semantic 校验。Exact lookup 和 exact in-flight 不需要向量、embedding worker 或 semantic index；向量缺失或损坏不能删除仍然有效的 exact origin。

三类时间分别管理：执行 lease 决定 running leader 是否仍有效；terminal retention 决定 binding/receipt 保留多久；expires_at 决定结果是否还能首次交付给新的消费方。完成的 binding 必须引用与历史条目相同的 observed_at/expires_at，cacheable=false 也一样，不能用 terminal retention 替代 freshness。

向 Gateway/OpenHands 返回的复用决策携带控制面 expires_at、结果 digest 和关联引用，便于最终消费前校验；这些字段不混入 provider-visible provenance。过期响应不能在客户端被当作一次新命中接受。

查询时有效不代表交付时仍有效。候选评分、等待 leader、预算适配或排队后，在返回结果和现有 DCS 首次消费结果前再次检查 expires_at > now。延迟到过期后领取的 follower 解除结果绑定并重新进入匹配/本地执行；不能因它在过期前已 join 而交付陈旧结果。此变化不取消其他仍有效的 follower 或已经完成的真实执行。

已进入 Agent 权威历史，或已经被 DCS 合法消费并写入 provider-valid 增量的消息，之后同步/ACK 重放时保持原文，不因 TTL 到期篡改历史。尚未消费的过期结果不能用于新的 continuation；已有 DCS 按原同步屏障交还控制权。

### 6.3 Tool Cache 维护

新增 Tool Cache Maintenance，职责只包括：

- 周期性删除过期 reuse_entries；
- 级联删除 semantic_vectors、audit 派生索引和 result blob 引用；
- 清理失效 adapter/index namespace；
- 按独立的容量/价值策略回收可淘汰条目，为后续 Tool Cache 调度策略提供维护接口；
- WAL checkpoint、容量统计和受控 compaction；
- 记录删除数量、失败原因和耗时，不记录结果正文。

维护任务不能改变正在运行的 binding、lease 或 LineTail。容量淘汰与候选排序是两件事，不能用“只保留最新 N 条待打分”替代语义检索；仍被 active follower 或 delta 引用的 blob 必须在引用释放后才能物理删除，但引用保留不延长结果复用有效期。清理失效 semantic index 只清理向量/派生索引，不能连带删除有效 exact 条目。维护失败必须可观测，请求路径继续按 expires_at 拒绝过期复用。

## 7. Canonical descriptor 与 adapter 契约

### 7.1 统一接口

每个 adapter 实现：

~~~text
parse_tool_call(tool_name, arguments)
canonicalize_arguments(parsed)
build_exact_descriptor(canonical, trusted_context)
build_semantic_text(canonical)
validate_result(observation, execution_receipt)
adapt_result(observation, output_budget)
~~~

adapter 必须返回结构化 descriptor；controller 不理解 Tavily envelope、MCP 文本或 Shell 命令语法。validate_result 分别报告本地执行状态、载荷类型/完整性及实际可观察的供应商状态；无法观察的字段明确为 unknown，不补造事实。adapt_result 返回合法 Observation 或明确的 budget_exceeded，不返回伪造的成功摘要。

descriptor 的固定字段为：

~~~text
canonical_tool_family
tool_version
adapter_id
adapter_version
tool_schema_version
result_schema_version
hard_scope_digest
freshness_policy_id
canonical_arguments
~~~

JSON 使用固定排序、稳定 Unicode、无多余空格、禁止 NaN。禁止直接对原始 JSON、原始命令字符串、MCP envelope 或带凭据输入做 hash。

所有 adapter 在构建 descriptor、查历史、join in-flight 或生成向量之前，先检查输入是否依赖本地 secret/环境展开。Tavily query、urls 以及嵌套字符串中的 $VAR、${VAR}、${VAR:-default} 等按当前 MCP executor 实际支持的引用语法识别，命中即 execute_locally，并记录 secret_dependent_input 分类。不能先展开 secret 再发送、hash 或 embedding；这一规则不对普通 query 增加私有/公共分区。

对可复用的字面输入，Runtime 在 START 前使用同一 adapter 对实际执行参数计算 canonical input_digest，并与 binding 中的 digest 一致。Gateway 决策在消费时也必须核对 tool_call_id、原调用参数和 adapter；参数被 hooks、默认值处理或其他本地步骤改变后，应失效原决策并按实际非敏感输入重新解析，不能把新结果发布到原 key。

### 7.2 Tavily 参数规范化

Search：

- 缺失字段只有在固定版本的实际 OpenHands/MCP 执行链已验证同样默认值时才等价补齐；schema 中的 default 声明本身不证明 executor 已注入，未验证时保留“省略”语义；
- search_depth、topic、time_range 使用枚举校验；
- max_results 校验 5 到 20；
- domains 去重、大小写规范化并排序；若将来供应商声明顺序有意义，必须升级 adapter 而不是静默改变；
- days 只在 provider schema 接受的组合中保留；不自行把 days 转换成 time_range；
- unknown field、错误类型和不符合 MCP schema 的组合返回 execute_locally；
- Search 的 semantic text 只来自规范化 query，并附带固定 instruction version。

Extract：

- urls 逐个做 scheme/host/path/query 规范化，保留列表顺序；
- extract_depth 和 include_images 按上述实际执行默认值规则规范化；
- 空 URL、凭据 URL、未知字段和 schema 错误不进入 reuse。

#### 实际返回格式与校验能力

第一版按 tavily-mcp@0.2.1 实际返回的 MCP content 和 OpenHands MCPToolObservation 工作。该版本的 formatResults 将 Search/Extract 结果拼成文本；failed_results 等未透出的字段不能在 FlowPilot 恢复，正文中的 Title:/URL:/Content: 也不是可信结构分隔符。result_schema_version 标识 FlowPilot adapter 的 Observation 契约，不冒充供应商返回的 response version。

可校验的事实是：受信本地执行及 FINISH 关联、MCPToolObservation 的 tool_name/content/is_error、实际收到的内容块类型、规范化后的 digest/大小，以及 executor 明确报告的超时、错误或截断。is_error=True、明确执行失败、损坏的实际 envelope、不支持的内容块或超过存储大小限制时，不提供复用；不能仅因正文中出现“error”或“Title:”推断失败或新条目。

当前文本路径的 upstream_completeness 固定为 unknown；这不阻断对实际成功返回的完整 Observation 的复用，也不宣称搜索/提取覆盖率、文档数或所有 URL 成功。若未来已接入的版本实际提供结构化失败/完整性字段，再升级 adapter 按这些事实验证。第一版不要求改造 Tavily server，也不要求存在它没有提供的字段。

#### 预算与结果保真

当前 Tavily Observation 作为一个不可拆分结果整体保存和交付，保留原内容块及其顺序；不能按文本标签拆条目、猜测 URL 数量、重建原始 JSON 或插入模型摘要。整个结果连同必要的 provider provenance 能装入当前预算时才命中，truncation_policy=whole_observation。预算不足时返回 budget_exceeded，本次调用转向其他可交付结果或由 OpenHands 执行；已经完成的 leader 不因 follower 预算不足而重跑。

其他 adapter 只有在实际返回格式具有可靠条目/文档边界时才支持结构化截取，截取后仍需通过当前 Observation 类型校验。评估报告区分“收到的载荷完整”和“供应商结果完整性未知”，不能把后者统计成已证明没有 partial result。

### 7.3 URL 命令规范化

第一版只允许一个 literal URL 的 curl GET。允许的选项固定为：

~~~text
-4、-6、--compressed、--fail、--fail-with-body
-s、-S、--silent、--show-error
~~~

第一版拒绝 -L/--location。重定向需要单独的 redirect policy、最大跳数、协议升级/降级规则、目标 host policy、DNS rebinding 防护和最终 URL 证明，完成前不能加入 allowlist。

必须拒绝：

- |、;、&&、||、>、<、反引号、换行；
- $VAR、变量展开、命令替换、子 Shell、glob、反斜杠拼接、tilde 展开；
- env、sh -c、bash -c、timeout、sudo、python 或任何 wrapper；
- POST/PUT/PATCH、body、上传、header、cookie、认证、代理认证、配置文件；
- -o/-O、remote-name、文件下载、脚本执行、二进制输出、stdin；
- 多个 URL、空 URL、userinfo、控制字符和不可确定的环境依赖；
- loopback、link-local、RFC1918、IPv6 私网和其他非公共目标。域名解析与重定向的私网检查必须由实际 OpenHands executor 的网络策略再次执行。

当前 parser 必须新增回归测试，确保 curl https://example.com/$VAR、curl https://example.com/反斜杠形式和私网 URL 都被拒绝。URL query 保留键值和顺序，不排序重复参数；fragment 删除；scheme/host 小写；默认端口删除；空 path 为 /。percent-encoding、Unicode/IDNA、dot segment、重复 query 的处理规则必须写成固定测试向量。

URL origin 只有在 OpenHands executor 上报以下事实时才可缓存：

~~~text
exit_code=0
timeout=false
is_input=false
reset=false
stdout 为 UTF-8 文本且未写入文件
执行来自隔离的一次性 argv
实际最终 URL、响应状态和网络 policy 校验通过
~~~

URL executor 的配置、环境和调用方式必须由实际执行适配提供；一次性 argv 本身不证明 curl 没有读取默认配置或代理环境。adapter/version、url execution policy id 和执行事实必须与 descriptor 一致。TerminalObservation 的非零 exit code、软超时、full_output_save_dir、前序 session 依赖和 binary/截断不完整输出均拒绝复用；leader 已真实执行完成时只拒绝发布，不再次执行同一 Tool。

## 8. Exact reuse

exact_key 定义为：

~~~text
SHA256(canonical_descriptor_json)
~~~

key 必须包含 family、tool/version、adapter/version、schema、FlowPilot hard scope、freshness_policy_id 和 canonical arguments；不包含 wall-clock now、origin_id、leader identity、Tavily API key 或结果正文。

Exact lookup 顺序：

1. 从统一入口取得 trusted_context，执行 adapter parse/eligibility 和 secret 依赖检查；
2. 构造 descriptor 和 exact key；
3. 按 key 查询历史；
4. 验证 trusted origin metadata、expires_at、scope、schema、digest、安全策略；
5. 按 adapter 实际结构适配预算；当前 Tavily 只整体交付，不能靠文本标签截取；
6. 再次检查 freshness 和当前授权策略，返回当前调用的 Observation 和 provenance；不满足预算或有效期时不算命中。

Exact miss 后：

1. 在同一短事务内再次查询历史；
2. 查询 exact in-flight binding；
3. 兼容 binding 存在时注册 follower；
4. 否则注册 leader，并返回 sync_and_execute_as_leader；
5. leader 按 §2.2 关联真实 START/Action 和输入，完成并提交本地 Observation 后按 §4 发布 origin。

exact、semantic、in-flight 和 local execution 使用不同的 provenance 类型。任何 reuse 返回值再次被 publish 都必须被拒绝。

Exact lookup 的 descriptor 构造与查询不调用 embed，也不检查 semantic_vectors。只有 exact historical miss 后且 semantic 启用时才进入向量路径；semantic disabled/worker 失败时仍保留 exact historical 和 exact in-flight。真实交付成功后才记录命中和 touch，候选计算、join 和预算拒绝不更新命中计数。

## 9. Semantic reuse 与 Qwen3

### 9.1 Eligibility 与顺序

第一版 semantic 只开放 tavily_public_search 的 general、非时间敏感请求。顺序固定为：

~~~text
adapter eligibility
exact historical
semantic historical
exact in-flight
semantic in-flight
register leader
~~~

hard filter 至少包括 family、adapter/version、tool/schema、deployment/namespace、locale/language/region、safe_search、data_source_constraints、freshness、search_depth、topic、days/time_range、max_results、include flags、domains 和 security policy。hard filter 失败时不能降低阈值补偿。

### 9.2 候选检索和截断

候选数限制只能作用于完成打分后的 top-K，不能在打分前按 created_at 截取最新 N 条：

- SQLite 初版先完整扫描所有硬过滤通过且未过期的候选，再计算 cosine/IP；
- 后续引入 ANN 时，索引 namespace、model id、dimension、pipeline version 和 normalization 必须先匹配；
- in-flight binding 数量通常较小，必须遍历全部兼容 binding，再按 score、observed_at、origin/binding id 做稳定 tie-break；
- semantic_candidate_limit 不能用 break 截断尚未评分的候选；
- 低于 family threshold、向量损坏、模型不匹配或 freshness 失败的候选只记录拒绝原因。

必须有回归测试：一个较新的低分候选排在 limit 内，一个较旧的高分候选排在 limit 外，结果必须选择高分候选；in-flight 也必须覆盖同样场景。

### 9.3 Qwen3 provider

生产 semantic provider 固定为本地 Qwen3-Embedding-0.6B：

~~~text
model path=/docker/data/HF_MODELS/Qwen3-Embedding-0.6B
model id=qwen3-embedding-0.6b
dimension=1024
normalization=L2
similarity=cosine/IP
instruction version=web-query-equivalence-v1
local_files_only=true
~~~

Provider 接口：

~~~text
index_id
dimension
async embed(texts: list[str]) -> list[tuple[float, ...]]
~~~

模型只加载一次，批量编码，编码通过 worker thread 或独立 embedding worker 执行，不能阻塞 FlowPilot event loop，也不能持有 controller/index lock。每条向量记录 model id、dimension、instruction version、pipeline version、normalization 和 index namespace。Exact 结果发布不等待向量生成；异步建向量失败只记录 semantic 状态，不回滚有效 origin。候选评分在锁外读取版本化快照，join/register 前按当前版本、freshness 和策略原子复核，避免异步检索产生双 leader。

HashingEmbedder 改名为 TestHashingEmbedder，只能用于测试。生产环境不允许在 Qwen 不可用时静默回退 hash；Qwen 加载失败、维度不符、模型 metadata 不符时 exact 继续工作，semantic 明确降级为 disabled 或 shadow。

FlowPilot 应提供可选 embedding 依赖组或独立 worker 镜像，明确 torch/transformers 的版本与本地模型挂载，不在运行时联网下载。

### 9.4 Semantic mode 与审计

semantic_mode 有 shadow、candidate、active：

- shadow 只计算并记录候选，不改变执行；
- candidate 将候选交给上层策略，默认仍执行真实 Tool 验证；
- active 只有离线评估和灰度门槛通过后才能直接复用。

每次候选尝试记录 metadata-only audit：

~~~text
request/tail/trace correlation
origin_id 或 binding_id
match_kind
score、threshold
hard_filter digest 与拒绝分类
decision、reason
model/index/pipeline version
observed_at、latency
~~~

不得记录完整 prompt、凭据、授权头、leader context、完整 query 结果正文或 follower 不应看到的来源 identity。

## 10. 存储、新版本切换与删除

### 10.1 逻辑表

新版本数据库建立以下逻辑表，不从旧库导入：

~~~text
reuse_entries
  origin_id、family、adapter/version、canonical descriptor、exact key
  hard_scope_digest、freshness_policy_id、schema
  observed_at、expires_at、result_digest、result blob/ref
  execution source、status、created/updated timestamps
  upstream completeness、payload format

reuse_publications
  publication key、fingerprint、binding id、ExecutionRef
  origin id、result ref/digest/size、cacheable
  observed_at、expires_at、commit status、committed_at

semantic_vectors
  origin_id、semantic_text digest
  model id、pipeline version、dimension、normalized、index namespace
  float32 embedding BLOB、created_at

reuse_match_audit
  request/tail/trace correlation、origin/binding id
  match kind、score、threshold、hard filter result
  decision、rejection reason、model/index、latency、observed_at

origin_execution_refs
  origin_id、start/finish event id、execution attempt
  identity digests、input/result digest、不可变终态引用
~~~

semantic_text 如果需要保留用于离线评估，必须按数据分级策略加密或受控保存；审计表只保存 digest 和受控 metadata。结果 blob 不应复制到多个索引表。

reuse_publications 是发布事务和幂等重放的依据，不复制 frontier 的可变 Tool 生命周期，也不持久化一套完整 running binding 状态机。reuse_entries 与 publication 指向同一份规范化结果；semantic_vectors 可为空，由独立任务生成。

### 10.2 不兼容旧版，使用独立新库

本轮不实施 v3 migration、legacy_migrated、旧协议双读或兼容回退。使用新的 Tool Cache 路径和显式 schema version，例如 reuse-v4.sqlite / user_version=4，冷启动构建所有条目。新历史结果只能来自本版本接受的真实执行。

部署切换时停止向旧 reuse 实例登记新 binding，结束或显式释放未完成等待，再一起切换 FlowPilot reuse 接口与 OpenHands 最小适配。变更的 reuse/执行凭据 envelope 使用明确的新版本；不在旧版号下悄悄增加必填字段，不支持新旧 reuse 客户端混用。关闭 reuse 后正常的本地 Tool 执行仍可继续。

若新配置误指向旧 schema，返回明确的 schema mismatch 和新库路径配置要求；不能自动读取、转换或清空旧库。旧库不再参与服务，也不由新版本 maintenance 扫描或修改；是否删除旧文件不属于本次文档修订或兼容性要求。新版本故障时可关闭 reuse，不能用旧缓存兜底。

验证覆盖新库创建、schema/version mismatch、新旧路径隔离、WAL、busy timeout、publication 事务恢复、vector rebuild、maintenance 和磁盘满。

### 10.3 隐私与删除

写入前执行 adapter-specific secret/PII policy。删除 origin 时必须级联清理 result blob/ref、semantic vector、ANN 派生索引、不可变 origin 证据副本和 audit 中可关联的受控字段；frontier 的可变事件生命周期仍由其 owner 管理。同步失效 publication 的结果引用，不能由 publish 重试复活已删除的载荷。仍被有效同步事务引用的载荷按既有引用释放规则处理。日志只记录 digest、大小、状态、scope digest 和计数。

## 11. OpenHands 接入要求

### 11.1 Reuse resolve 与 Observation

FlowPilotRuntime 在 Agent._execute_action_event 的真实执行边界接收复用决策，遵循 §2 的两类入口：

- 已由 Gateway 决策时消费该决策，不再请求 resolve endpoint；没有 Gateway reuse 配置的直接路径才调用 endpoint。两条路径必须取得同样的 namespace、adapter、安全策略和 freshness 结果；
- 命中时核对当前 Tool Call 与参数、结果有效期，再使用当前 tool 的 observation_type.model_validate 构造 Observation，按 adapter 保留内容格式并附加白名单 provenance；不能把 MCP 文本反解析成不存在的结构；
- Gateway leader/follower 决策也必须被 Runtime 识别；leader 通过 START 关联当前 Action，follower 使用当前身份等待/取消，不能把合法决策作为未知类型抛弃或重建 binding；
- miss/leader 时在既有安全检查和必要 context sync 完成后执行原有 OpenHands Tool，START input_digest 与 binding 绑定；
- FINISH 上报 result_digest、结果大小和执行终态。本地 Observation 提交后发送含 start/finish_event_id、input/result_digest 和 execution_attempt 的 publish；失败重试只重发同一执行的数据；
- follower/historical replay 必须保留当前 action 的 action_id 和 tool_call_id；
- result adapter 不得把 leader 的 provider message identity 或 OpenHands prompt 带给 follower。

Tavily MCP result 需适配为当前 MCPToolObservation；URL fetch 需适配为当前或新增的 UrlFetchObservation。FlowPilot 不能只返回任意 dict 让 Agent 绕过 observation schema。

### 11.2 既有 read-only policy 与 DCS 接口约束

Tavily MCP server 当前可能不提供 annotations。OpenHands adapter 只有在以下条件全部满足时，才能为 tavily-search/tavily-extract 补充本地 read-only annotation：

- tool name、MCP package version 和 schema digest 与 registry 匹配；
- adapter 明确声明只读、无写入和无凭据结果；
- FlowPilot delegation policy 显式 allowlist 该 tool family；
- 当前请求满足 non-streaming、native tool calling、serial execution 和其他 design.md DCS 条件。

annotation 补充只影响 OpenHands 的安全 gating，不改变 MCP provider schema。没有明确 annotation 时 exact reuse 仍可在 M1 以同步结果路径工作，但不能启用 DCS。本节约束已有 DCS 接入；新增 annotation 策略或扩展 DCS 单列后续任务，不作为本轮 Tool Reuse 修复完成的条件。

### 11.3 多 Tool Call 与执行屏障

同一 assistant 回复的多个 Tool Call 必须作为一个 provider-valid batch 处理，当前 adapter 只支持 tool_concurrency_limit=1。混合 Tavily、URL 和不可复用 Tool 时，存在缺失消息就完整同步并等待 ACK，再按 provider 顺序为每个调用交付复用结果或执行本地 Tool。本轮不启用 semantic DCS，也不改变已有 DCS 的上下文权威、ACK 和 provider 顺序。

## 12. 模块和接口改造

建议的 FlowPilot 代码组织：

~~~text
flowpilot/reuse/adapters/base.py
flowpilot/reuse/adapters/tavily.py
flowpilot/reuse/adapters/url_fetch.py
flowpilot/reuse/adapters/registry.py
flowpilot/reuse/service.py
flowpilot/reuse/embedding.py
flowpilot/reuse/index/exact.py
flowpilot/reuse/index/semantic.py
flowpilot/reuse/origin.py
flowpilot/reuse/maintenance.py
flowpilot/reuse/audit.py
~~~

现有 flowpilot/reuse/controller.py 负责顺序编排和短事务，不负责供应商 envelope 解析、Shell 安全解析、向量模型加载或大结果写入。ToolRegistryEntry 至少增加：

~~~text
adapter_id、adapter_version
result_schema_version
reuse_mode
semantic_query_fields
semantic_threshold、candidate_limit
freshness policy/max ttl
url execution policy id（仅 URL family）
~~~

ReuseScope 保持 FlowPilot 当前 Tool 约束字段；deployment/namespace/policy digest 由服务端 trusted_context 派生并进入 hard scope，不通过普通 Tool 参数传递。service.py 统一所有入口，origin.py 管理不可变执行引用与发布事务，controller.py 管理匹配/绑定；frontier 和 context 继续持有自己的事件与同步状态。

## 13. 分阶段实施

### Workstream 0：模块接口和可信事件

基于已有 M0 事实接口准备 M1 修复。完成新 reuse/receipt 版本、统一 ReuseService/trusted_context、Gateway 与 endpoint 两类入口、ToolCallRef/ExecutionRef、START 输入关联、FINISH result digest、本地 Observation 提交回调和 publication receipt 契约。明确关闭配置，不将 reuse 能力归入 M0，也不重新实施整个 M0。

### Workstream 1：Tavily 实际 adapter

实现 tavily-mcp@0.2.1 Search/Extract 实际输入 schema、已验证的默认值、secret 引用拒绝、canonicalization、Observation envelope 校验和 exact descriptor。保存完整返回文本，upstream_completeness=unknown，预算不足时明确拒绝该结果交付。fixture 来自固定包实际输出，不能以原始 API JSON 冒充 MCP 输出；不在单元测试中使用真实 API key。

### Workstream 2：URL curl 安全子集

实现 literal argv parser、URL canonical descriptor、公共目标 policy、拒绝规则、结果 validator 和 isolated executor contract。联调由 OpenHands 提供的最小 URL 执行适配，验证实际执行输入与 result receipt。分别报告 parser/shadow 完成和 URL active exact 完成；没有隔离执行事实时不缓存 TerminalObservation，但不能将 URL 复用从本轮目标移除。

### Workstream 3：Exact cache、trusted origin 和 TTL maintenance

对应 M1。新库冷启动，完成 exact lookup、trusted FINISH gate、publication 幂等与原子提交、所有交付路径的 expires_at、真实格式下的预算适配、删除和维护任务。Exact 全链路不依赖向量。完成下一工作流的相关联调验收后，Tavily 与 URL exact 分别灰度启用。

### Workstream 4：复用接口联调与既有路径回归

验证 Gateway 与 endpoint 两类入口的 namespace/identity 一致性、真实 MCP/URL Observation、START/FINISH/提交/publish、幂等重试、迟到 follower、失败时本地结果保留和混合 Tool Call。对已启用 DCS 的接口做 freshness、预算及同步回归；不新增 DCS 状态机或要求未启用部署先实现 DCS。

### Workstream 5：Qwen3 semantic shadow

对应 M3 前置。接入 Qwen3 provider、模型 metadata、async batch、索引 namespace 和完整候选打分。semantic 永远从 shadow 开始，模型不可用只关闭 semantic。

### Workstream 6：Semantic in-flight 与 candidate

对应 M3。实现硬过滤后的 historical/in-flight semantic matching、top-K 后截取、审计、false-reuse feedback、kill switch 和 per-family threshold。不得启用 semantic DCS。

### Workstream 7：离线评估和灰度

使用脱敏 trace、人工正负例和真实 adapter fixture，评估 Recall@K、Precision@K、MRR、false reuse rate、freshness violation、embedding latency、controller latency、重复执行率、Observation schema 错误和预算拒绝。Tavily 未暴露的供应商完整性单独标为 unknown，不统计为已验证完整。按 namespace 从 shadow 到 candidate，再到 active；semantic 独有退化关闭 semantic 并保留经验证的 exact。若故障来自共享输入关联、发布或 TTL 路径，则关闭受影响 family 的复用并修复，不能因为 exact 模式而继续提供错误结果。

## 14. 验证矩阵

### FlowPilot 单元和集成测试

- deployment/namespace 不同但参数相同必须 miss；客户端伪造 tenant/project/experiment 不得改变 scope；
- Gateway、resolve endpoint 和已有 DCS 内部入口都经过同一 trusted_context 解析，不能缺省成共享 namespace；
- Gateway 以 action_id=None 登记 leader，受信 START 关联实际 Action 后可发布；相同关联幂等，其他 Action、输入、ToolCallRef 或 execution_attempt 的冲突关联拒绝；
- exact key 对 adapter、schema、policy digest、参数默认值变化敏感；
- 相同 $TOPIC/${TOPIC} 等原始输入在不同 conversation 会展开为不同值时，必须在 lookup/embedding 前拒绝复用；START 实际 canonical input_digest 与 binding 不同也拒绝发布；
- 同一条目 TTL 到期后 exact/semantic 历史、completed binding 的 follower poll、Gateway 交付和已有 DCS 首次消费都拒绝；覆盖 TTL=1 秒、terminal retention=30 秒、follower 第 2 秒领取；
- 查询/评分时未过期、交付时已过期不能命中；已消费的 provider 消息在同步/ACK 重放时不因 TTL 被改写；
- maintenance 删除条目时 payload、vector、索引和派生审计关联一起清理；
- START 缺失、FINISH/ExecutionRef 不匹配、input/result digest 或 result size 不匹配、FAIL/CANCEL 后首次 publish 均拒绝；
- 相同 publish 在 complete、响应丢失或 tail 已推进后重试返回原提交 receipt，不创建第二个 origin、不续 TTL；cacheable/TTL/结果变化的重试视为冲突；
- 在 payload 暂存后、事务写入中、提交后内存更新前、响应返回前注入失败：未提交结果不可见，已提交结果按 receipt 恢复，不产生 complete 而无载荷的 binding；
- cacheable=false 不写历史但仍需可信 ExecutionRef 和 follower freshness；telemetry 缺失不能通过该标志绕过校验；
- binding expiry、leader re-election 和 follower 取消保持身份隔离；旧执行不能覆盖新 leader 的结果；
- 新库冷启动可用；新配置指向 v3 库时明确 schema mismatch，不迁移、不读旧条目、不修改旧文件；新旧 reuse 协议混用明确拒绝；
- 未配置 embedding、无向量、向量损坏、模型不可用、index namespace 不匹配时，合法 exact historical/in-flight 仍可用；semantic 拒绝原因单独记录；
- semantic 候选在完整 hard filter 后打分，旧高分候选不能被最新 N 截断；
- in-flight semantic 同样不能按插入顺序提前 break；
- 异步评分期间 binding/policy/freshness 改变时，join/register 前重新校验；并发 miss 原子选择 leader；
- Qwen dimension/model/index mismatch、模型不可用和 worker 超时只禁用对应 semantic 路径，不回滚已提交 exact origin；
- audit 只含 correlation、digest、score、decision、reason、latency 和 model/index metadata。

### Tavily adapter 测试

- 当前 Search/Extract MCP schema 的默认值声明与实际执行默认值分别验证，覆盖枚举、范围、unknown field；
- Search 的 general semantic eligibility；
- news、days、time_range、include_images、include_image_descriptions、include_raw_content、domains 和 max_results hard negative；
- URL 列表顺序、URL 规范化、secret 引用、MCP error、实际 envelope 损坏和超大结果；
- 使用真实 formatResults 生成 fixture：加入未透出的 failed_results 后文本不变，必须保持 upstream_completeness=unknown，不声称检测到或排除了隐藏失败；
- 一篇正文含 Title:/URL:/Content: 分隔标记与两篇结果格式化后文本相同，adapter 都按完整 Observation 处理，不尝试重建条目；
- 不要求供应商不存在的 response_version/完整性字段；Tavily 以成功 envelope 返回的完整文本可 exact 复用；
- 整个 Observation 加 provenance 正好满足预算时可交付，预算不足明确 budget_exceeded，不能截断正文或伪造摘要；
- adapter/schema version 变化后旧 origin 不命中；
- MCP annotations 缺失时 exact 同步路径可用，DCS 路径保持关闭。

### URL 安全测试

- shell operator、变量展开、命令替换、wrapper、重定向、上传、cookie、auth、文件下载、binary 和 POST；
- literal URL、重复 query、fragment、默认端口、userinfo、非法编码、Unicode/IDNA、dot segment；
- localhost、RFC1918、link-local、IPv6 私网、DNS rebinding 和重定向到私网；
- nonzero exit code、timeout、is_input、reset、前序 session 状态、full_output_save_dir；
- 隔离 argv 的实际输入 digest、默认配置/环境影响、最终 URL 和网络 policy 执行事实必须与 registry/descriptor 匹配；
- curl/wget/httpie/xh 不跨 executable family 复用；
- 结果 Observation 能被当前 OpenHands observation_type 校验，并保留当前 tool_call_id。

### OpenHands 端到端测试

- Gateway 决策消费与直接 resolve endpoint 两条路径都覆盖历史命中、leader、follower，不双重 resolve、不遗留未关联 binding；
- Agent._execute_action_event 的历史命中跳过真实 Tool，保留当前 Observation 内容和身份；
- leader 的真实 Tool 执行产生 START、FINISH，本地 Observation 提交后产生一次逻辑 publish；网络重试沿用事件 id 和 publication fingerprint；
- FINISH telemetry、结果规范化、publish 或数据库失败时不写错误 origin，leader 原 Observation 正常交付且真实 Tool 执行次数仍为一；
- follower 收到自己的 tool_call_id 和当前 Observation；
- 延迟领取过期结果的 follower 重新匹配/执行，不影响 leader 已交付结果及其他 follower；
- 同一 assistant 消息中多个 Tool Call 的顺序、混合可复用/不可复用调用、context sync/ACK 和 local execution barrier；
- Tavily MCP schema/annotation 与 FlowPilot registry 不匹配时安全降级；
- DCS 只在 exact、serial、non-streaming、native tool calling 和显式 read-only policy 下开启。

## 15. 启用条件和证据报告

每个 workstream 报告三类结论：

1. implementation：模块代码、新 schema/协议、独立新库配置和必要接口是否完成；
2. local verification：单元、集成、OpenHands fixture 和失败路径是否通过；
3. production evidence：真实 trace/模型/网络环境下的指标和限制。

trace 只能用于离线 adapter 解析和 semantic 评估，不能自动提升为 origin。上线前必须明确当前启用的 adapter、实际 payload 格式、namespace policy、TTL、semantic mode、URL executor policy、既有 DCS 状态、unsupported capability 和未覆盖的接口路径。

完成报告单列 Tavily exact、URL exact 和 semantic 的状态：Tavily 按实际 Observation 信息报告能力，不能声称验证了隐藏的供应商完整性；URL active exact 需要真实隔离执行适配的证据，shadow 不算完成；semantic 故障不影响经过验证的 exact 路径。新库从零构建，旧版本不属于兼容或验收范围。
