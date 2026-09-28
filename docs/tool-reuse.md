# Tool 复用与 DCS

核对日期：2026-09-22。主要入口是 [ReuseService](../flowpilot/reuse/service.py)，
匹配和 binding 生命周期由 [WebReuseController](../flowpilot/reuse/controller.py)
负责；真实执行、Observation 提交和权威历史属于 OpenHands。

## 开启 exact

网关默认不复用 Tool。启用需要 registry、新库和 OpenHands adapter 的
`exact_reuse_enabled=True`、`reusable_web_tools` 白名单。
基础工具范围为 terminal、文件读写/搜索、任务管理，不包含浏览器。
以下配置直接适配原有 terminal；OpenHands 的 reusable_web_tools 包含 terminal，
无需注册 UrlFetchTool，也不修改 TerminalTool 的 schema 或执行器：

```bash
export FLOWPILOT_REUSE_ENABLED=true
export FLOWPILOT_REUSE_CACHE_PATH=data/reuse-v4.sqlite
export FLOWPILOT_WEB_TOOL_REGISTRY_JSON='[{
  "tool_name":"terminal",
  "canonical_tool_family":"terminal_url_fetch",
  "tool_version":"1",
  "result_schema_version":"1",
  "adapter_id":"terminal_url_fetch_v1",
  "command_line_reuse":"url_exact"
}]'
```

仅允许注册表声明的只读 Tool。当前专用 adapter：

| Tool | 约束 |
| --- | --- |
| tavily-search | tool_version=0.2.1；adapter_id=tavily_search_mcp_v1；固定 input schema digest |
| tavily-extract | tool_version=0.2.1；adapter_id=tavily_extract_mcp_v1；固定 input schema digest；exact |
| tavily-crawl | tool_version=0.2.1；adapter_id=tavily_crawl_mcp_v1；根 URL + 遍历/过滤/instructions；exact |
| tavily-map | tool_version=0.2.1；adapter_id=tavily_map_mcp_v1；返回站点 URL 列表；exact |
| terminal 中 curl / wget | terminal_url_fetch_v1；原生 TerminalObservation；exact |
| BrowseComp-Plus search | browsecomp_search_mcp_v1；实际 MCP schema + 检索 profile 摘要；exact / 显式 semantic 实验 |

Tavily 的 schema 与摘要从
[TAVILY_SCHEMAS / TAVILY_SCHEMA_DIGESTS](../flowpilot/reuse/adapters/tavily.py)
取得，不手工猜测版本。
[Terminal URL adapter](../flowpilot/reuse/adapters/terminal_url.py) 在 Scheduler 识别命令，
不向 Agent 暴露额外工具，也不在 Scheduler 执行请求。
历史的专用 url_fetch adapter 不再是基础工具 URL 复用的接入依赖。

### 基础工具中的 URL 读取

| 调用 | 当前处理 |
| --- | --- |
| `terminal: curl -sSL https://example.com` | 单 URL GET；支持声明的 flags、GET/HEAD、标准输出与超时参数 |
| `terminal: wget -qO- https://example.com` | 单 URL GET，必须输出到 stdout；`--output-document=-` 等价 |
| curl/wget 文件下载、POST、认证、上传、变量、管道、复合命令 | 原样交给 OpenHands 本地执行，不替代其副作用 |
| Python requests/httpx/urllib、Node fetch、git、包管理器 | 属于通用脚本/状态操作，本轮不做静态脚本语义推断 |
| 文件读写/搜索、任务管理 | 无独立 URL 获取操作，不登记为 URL 复用族 |

参数解析区分 shell 引号：带 query/`&` 的 URL 应使用引号。
短参数组合如 `-sSL`、`-qO-` 展开为对应选项，保留选项顺序；
URL 只规范化 scheme/host、默认端口和空路径，不排序 query、不改写路径字节。
GET/HEAD、重定向/压缩/输出选项、timeout 和 executable family 都进入 exact key。
`command_line_reuse=disabled` 不启用；`curl_url_exact` 仅启用 curl；
`url_exact` 同时启用 curl/wget。匹配不调用 embedding。

Action input_digest 按原生 TerminalAction 默认值计算，独立于 URL 匹配 descriptor，
SDK 无需重复实现 curl/wget parser。START/FINISH、实际 command、exit_code=0、
非 timeout、非 is_error 和完整 Observation payload 关联后发布。
退出码只证明工具完成，不能据此声称 HTTP 2xx 或上游完整性。
交付保留正文，command 绑定当前调用，移除 leader 的 pid/cwd/hostname、解释器及
本地输出文件目录；重算交付 digest，来源执行凭据仍指向不可变 origin。

注册的部署/namespace 与 policy/version 应对应兼容的可执行文件、配置、代理和环境。
terminal parser 不验证隐式 shell 状态，不会重放 shell 历史、`$?`、wget HSTS 等内部缓存。
环境差异应反映在既有隔离域或 policy_digest 中；这是内容结果复用的适用前提。
命令含 URL 并不自动满足此前提；配置本身不证明不同 shell 环境等价。
选项依据 [curl 官方手册](https://curl.se/docs/manpage.html) 和本机 wget 帮助核对。

### Tavily URL 工具

本机 `tavily-mcp@0.2.1` 的 `tools/list` 实际提供 Search、Extract、Crawl 和 Map。
四个原始 inputSchema 均保存在 tavily_schema.json，schema digest 仍为硬约束。
Extract 按 URL 列表提取内容；Crawl 从根 URL 抓取页面；Map 只返回发现的 URL 列表。
Crawl/Map 保留 max_depth/max_breadth/limit、instructions、select_paths、
select_domains、allow_external、categories 等所有实际参数；Crawl 还保留 extract_depth。
不把 instructions 当作语义 query，不擅自补齐 MCP 没有应用的默认值。

配置 Tavily 项时使用上述 adapter_id，填入 TAVILY_SCHEMA_DIGESTS[tool_name]，
并同时加入 OpenHands 的 MCP 配置与 reusable_web_tools 白名单。
注册表样例生成器见 [examples/url_reuse_registry.py](../examples/url_reuse_registry.py)。
0.2.1 的 Crawl formatter 将每页内容截为前 200 个字符再返回；缓存的是该 MCP Observation，
不是完整抓取页面。Extract/Crawl/Map 都不从文本标签推断每个 URL 的成功或完整性。

## 匹配、执行与发布

匹配顺序为 exact history、允许时的 semantic history、exact in-flight、
允许时的 semantic in-flight，最后注册新 leader。
namespace 来自权威 job/line 注册和部署配置，不信任 Tool 请求自报的隔离域。
Tool family、版本、schema、adapter、locale/language/region、freshness、
safe-search 和数据源等硬约束参与匹配；内容不另划 private/public 标签。

ToolCallRef 可先于 Action 注册。START 绑定实际执行身份，FINISH 校验实际
input/result digest、大小及 adapter；OpenHands 提交本地 Observation 后才发布。
SQLite 用单事务保存 payload、执行证据、索引和 publication receipt。
重复发布返回原 receipt，冲突发布不能继续作为可信来源。
Follower 使用自己的 tool_call_id，不复制 leader 的对话身份。

初始 TTL 从服务端接受 FINISH 的观察时间起算，默认 300 秒。
有效窗口取发布请求 TTL（未指定时用 registry default）、registry max
（未指定时用 default）和可选 scope max 的最小值。
可缓存结果成功 history、poll 或 deferred 交付后，将到期时间更新为
`本次命中时间 + 原有效 TTL`；持续命中可持续续期，窗口长度不会逐次增加。
原始 FINISH `observed_at` 和 publication receipt 保持不变，schema v4 中既有的
`reuse_entries.expires_at` 保存滑动到期时间，交付 provenance 返回更新后的期限。
已过期或撤销的结果不能续期；候选查询、交付校验/预算失败、发布重试和容量保护
不续期。不可缓存的 follower 结果仍使用初始 FINISH TTL。
Tavily payload 按真实 MCP 结构整体交付；不能从文本标签推断供应商截断状态。
当前适配器无法在预算内交付完整结果时拒绝该次复用，不盲目截断正文。

接口以 `/flowpilot/v1/reuse` 为前缀：

| 方法 / 后缀 | 用途 |
| --- | --- |
| POST /resolve | history / leader / follower 决策 |
| GET /bindings/{binding_id} | follower 查询 |
| POST /bindings/{binding_id}/result | 可信结果发布 |
| POST /bindings/{binding_id}/progress | leader 进度 |
| POST /bindings/{binding_id}/fail | 执行失败 |
| POST /bindings/{binding_id}/cancel | follower 取消 |
| PUT /semantic/policy | semantic 模式、阈值及停用配置 |
| POST /semantic/false-reuse | 错误复用反馈 |
| POST /maintenance | 显式缓存维护 |
| GET /flowpilot/v1/reuse | 当前状态（完整路由） |

Wire version 仍为 `flowpilot-phase1-reuse-v3` 和
`flowpilot-phase3-reuse-v3`；数据库 schema v4 是独立版本。

## Semantic 当前边界

BrowseComp-Plus 使用单独的 corpus-search family，具体入口和实验命令见
[OpenHands benchmark 适配](/home/liyachen/BrowseComp-Plus/docs/openhands-flowpilot.md)。
注册表通过 adapter_id 显式选择适配器，不能仅凭通用名称 search 启用复用。
policy_digest 与 SDK scope 中的 `browsecomp-search:<digest>` 必须匹配，
该 digest 代表部署声明的语料/索引、检索器/模型、k 和 snippet/tokenizer 配置。
配置声明不构成远端索引内容证明，索引变化时部署方必须更新 profile。
原 query 不做大小写、标点或空白归一化；支持 FastMCP JSON 数组及逐 hit 文本块，
校验 docid/snippet/可选数值 score 后整体保存和交付原生 MCPToolObservation。
query embedding 可参加现有 candidate/active 历史和在途匹配，其他约束不软化。
语义阈值尚无本 benchmark 的质量校准证据；get_document 不进入此适配器。
`GET /flowpilot/v1/reuse` 返回 registry 元数据，runner 在实验前核对实际配置，
避免将不同 schema、profile、mode 或 threshold 的运行混为一组。

registry 必须显式设置 semantic_reuse_enabled，并使用 phase3-reuse-v3。
模式默认 shadow；candidate 返回候选信息，active 才允许语义替代。
当前 Tavily Search 只软化 query，保留其余硬约束，限定一般、非时间敏感搜索；
Tavily Extract/Crawl/Map 和 Terminal URL 获取保持 exact。

[Qwen3Embedding](../flowpilot/reuse/semantic.py) 异步加载本地
Qwen3-Embedding-0.6B，默认 1024 维 L2 向量；通过
`FLOWPILOT_REUSE_EMBEDDING_MODEL_PATH` 指定目录。
可选依赖使用 `uv sync --extra dev --extra embedding` 安装。
运行时不下载权重，semantic 故障计为不可用，exact 路径独立保留。
6 组人工标签不构成 active 的生产质量依据，见 [验证说明](verification.md)。

## Tool Cache 容量

[ReuseCache](../flowpilot/reuse/store.py) 按 origin 保存一份结果。
默认容量 `FLOWPILOT_REUSE_MAX_PAYLOAD_BYTES=536870912`，限制 committed payload
字节，不是 SQLite 文件总大小。维护在发布后、手动调用和后台运行；
后台间隔 `FLOWPILOT_REUSE_MAINTENANCE_INTERVAL_SECONDS=60`。

先删除过期结果，再在超容量时按以下键升序淘汰：

```text
ttl = initial_expires_at - observed_at
freshness = clamp((expires_at-now)/ttl, 0, 1)
value = max(0, measured_latency_ms) * (1+hit_count) * freshness / max(1, result_size)
eviction_key = (value, last_used_at, origin_id)
```

时间分母使用至少 0.001 秒。缺少真实执行时延或不可缓存结果的 value 为 0。
交付中的结果和已完成 binding 尚有 follower 领取的结果受容量保护；
保护不延长 TTL。后台还处理向量版本、删除关联记录和被动 WAL checkpoint，
不保证数据库文件立即缩小。维护失败计入 reuse_maintenance_failures。
Tool payload、CPU KV、GPU KV 容量独立。

## DCS

DCS 默认关闭，仅支持 exact 的隐藏 continuation。开启条件：

```bash
export FLOWPILOT_DCS_ENABLED=true
export FLOWPILOT_DCS_WAL_PATH=data/flowpilot_dcs.sqlite
# 由部署提供有效 Fernet 密钥，不将密钥提交到仓库：
export FLOWPILOT_DCS_ENCRYPTION_KEY='<Fernet key>'
```

同时需开启 exact reuse；OpenHands 设置 deferred_context_enabled，并授予带
版本、期限、Tool 白名单、delta 容量和 continuation 次数限制的 delegation。
新增消息写入 [DeferredContextManager](../flowpilot/context/manager.py) 的加密 WAL。
多 Tool assistant 批次保持完整，按 provider 顺序匹配 Observation。

本地执行、最终回复、容量、delta TTL、lease 或失败形成同步屏障。
完整 ACK 后才确认交付；digest、cursor、sequence 或 provider batch 冲突按失败处理。
semantic 命中不进入 exact DCS。

接口均为 `/flowpilot/v1/dcs` 下的 POST：
`/delegations`、`/delegations/release`、`/reuse/resolve`、
`/reuse/bindings/poll`、`/deltas/append`、`/sync`、`/sync/next`、
`/sync/ack`、`/reconcile`、`/continuations`；根路径 GET 提供状态。
Wire version 为 flowpilot-phase2-dcs-v2，WAL 的 PRAGMA user_version 为 4。
恢复仍需 Runtime 权威历史与恢复 manifest；详见
[test_context.py](../tests/test_context.py) 和 [test_dcs_api.py](../tests/test_dcs_api.py)。
