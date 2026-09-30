# 运行与接口

核对日期：2026-09-22。配置以 [Settings](../flowpilot/config.py) 为准，
HTTP 路由以 [app.py](../flowpilot/app.py) 为准，载荷以
[protocol.py](../flowpilot/protocol.py) 为准。

## 最小启动

以下只启动基础网关，上游 URL 填服务根地址：

```bash
cd /home/liyachen/workspace/flowpilot
uv sync --extra dev
export FLOWPILOT_INGRESS_API_KEY=local-dev
export FLOWPILOT_UPSTREAMS=http://127.0.0.1:8001
export FLOWPILOT_HOST=127.0.0.1
uv run flowpilot
```

网关默认端口 9000。客户端 LLM base URL 为 `http://127.0.0.1:9000/v1`。
仅改 base URL 不会生成身份或注册线路；OpenHands 的
[FlowPilot adapter](../../../openhands/software-agent-sdk/openhands-sdk/openhands/sdk/flowpilot.py)
负责这些动态信息，并要求 `tool_concurrency_limit == 1`。
适配器默认为关闭；`FlowPilotConfig(enabled=True, gateway_url=..., api_key=...)`
随 LocalConversation 配置。未显式指定身份时，根据自身 conversation UUID 派生
`job-<conversation_id>` 和 `line-<conversation_id>`，恢复同一会话时保持身份。
benchmark 使用此默认方式，run_id 仅用于实验关联。完整接线见
[SDK 集成用例](../integration/test_openhands_reuse.py)。

显式 LocalConversation.close() 对仍由当前 runtime 持有的 EMPTY/READY line 发送 finish，撤销尾部保留需求并释放依赖；普通 run() 返回仍允许多轮续接。ACTIVE/BLOCKED、有活跃本地工作或已被替换的 tail 不强制结束，失败记录 warning，KV 仍遵守引擎 TTL。重复 close 不重复发送。

| 配置 | 默认值 / 作用 |
| --- | --- |
| `FLOWPILOT_UPSTREAMS` | 必填上游集合；调度模式使用一个 URL |
| `FLOWPILOT_INSTANCES_JSON` | 可替代上述变量，元素含 id、base_url、models |
| `FLOWPILOT_INGRESS_API_KEY` | 默认要求设置 |
| `FLOWPILOT_REQUIRE_INGRESS_AUTH` | true |
| `FLOWPILOT_HOST` / `FLOWPILOT_PORT` | 0.0.0.0 / 9000；上例显式限制为 loopback |
| `FLOWPILOT_REQUEST_TIMEOUT_SECONDS` | 120 |
| `FLOWPILOT_TRACE_PATH` | traces/flowpilot.jsonl |
| `FLOWPILOT_WORKERS` | 1；create_app 拒绝其他值 |
| `FLOWPILOT_UPSTREAM_CONTROL_API_KEY` | 可选，供 health/tokenize/KV 控制 RPC 使用 |

入口认证使用 `X-FlowPilot-API-Key`，不是把该密钥放入上游 Authorization。
网关剥离 `X-FlowPilot-*` 私有头；普通 provider Authorization 按代理规则处理。
上游控制密钥和推理请求的 provider 凭据分别配置。

## 请求身份

先 POST `/flowpilot/v1/jobs`，再 POST `/flowpilot/v1/lines`。
当前基础协议为 `flowpilot-phase0-v2`。完整请求需要以下 HTTP 头，
每个身份头只能出现一次：

| Header（均带 X-FlowPilot- 前缀） | 含义 |
| --- | --- |
| Protocol-Version | flowpilot-phase0-v2 |
| Job-Id / Line-Id / Conversation-Id | workflow、独立线路、Runtime 对话 |
| Request-Id / Tail-Request-Id | logical request 与 frontier 引用 |
| Request-Attempt / Llm-Call-Id | attempt >= 1，每次新 upstream 调用使用新 call ID |
| Tail-Version | 预期当前 tail 版本，>= 0 |
| Context-Epoch / Context-Sequence | epoch >= 1，sequence >= 0 |
| Context-Cursor / Context-Digest | 上下文游标和 64 位小写十六进制摘要 |

可选头包括 Parent-Conversation-Id、Parent-Line-Id、Spawn-Id、
Deployment-Id、Namespace-Id。它们须与注册事实一致。
Request-Origin 默认为 agent；scheduler_delegated 必须带 Delegation-Lease-Id。
当前协议拒绝旧 tenant identity 字段。

重试保持 logical request/tail 引用，递增 attempt，使用新的 llm_call_id。
子线路继承 job，拥有独立 line/conversation；parentage 不自动创建等待边。
依赖只能由显式、带版本的 dependencies 更新建立，执行同 job 校验和环检测。

## 网关与 frontier

公开推理入口为 POST `/v1/chat/completions` 和 POST `/v1/responses`。
FlowPilot 没有公开 `/v1/completions`，不能从 vLLM 支持该 API 推断网关也支持。
基础代理保留请求/响应 body、SSE 顺序、状态码、错误与适用的重复头；
启用复用或 DCS 后，按相应协议处理 assistant/tool 消息。
支持的压缩响应解码后转发，同时移除已解码的编码、原长度和失效的实体校验头；
非流式重新计算长度，未识别的编码保持声明。
Chat 的完整 `[DONE]` 或 Responses 的 `response.completed` 帧在交付前提交 tail 并关闭上游，
不等待传输 EOF；客户端收到终止帧后关闭不会再把成功请求改为取消。
身份校验或初次 tail 登记期间取消同样记录 CANCELLED，允许使用新 attempt/call ID 重试。
SSE 未携带网关复用决策时，SDK 在 Tool 边界查询；已有显式决策时不重复查询。
实现见 [gateway/service.py](../flowpilot/gateway/service.py)、
[stream.py](../flowpilot/gateway/stream.py)。

LineTail 只有 EMPTY、ACTIVE、BLOCKED、READY、TERMINAL 五种阶段。
GatewayCall 的排队、流式及 terminal 状态另存。
上游失败、断连和取消须进入明确 terminal 并清理资源；未提交的 tail 替换可回滚。
context 冲突按失败处理，不猜测或合并上下文。
Tool telemetry 使用 event_id、sequence、execution_attempt 去重并核对活跃 tail；
事件种类为 start、finish、fail、cancel、blocked。

| 方法 / 路由 | 用途 |
| --- | --- |
| POST /flowpilot/v1/jobs | 注册 JobRegistration |
| POST /flowpilot/v1/lines | 注册 LineRegistration |
| PUT /flowpilot/v1/lines/{line_id}/dependencies | DependencyUpdate |
| POST /flowpilot/v1/lines/{line_id}/finish | LineFinish |
| GET /flowpilot/v1/jobs/{job_id}/frontier | 查询线路与依赖 |
| POST /flowpilot/v1/events/tools | ToolTelemetryEvent |
| GET /flowpilot/v1/gateway-calls | 每次 GatewayCall 的 metadata 审计 |
| GET /flowpilot/v1/scheduling/state | 实际 admission 与 KV controller 状态 |
| GET /flowpilot/health | 上游、trace 与能力状态；不就绪返回 503 |
| GET /flowpilot/metrics | JSON 计数 |
| GET /metrics | Prometheus 文本 |

`/flowpilot/v1/*` 由统一中间件认证；health、metrics 不在该中间件路径内。
schema 可从本地 FastAPI OpenAPI 获取；路由存在不表示可选组件已经开启。

## 持久化与运行边界

| 状态 | 存储与重启行为 |
| --- | --- |
| Frontier、GatewayCall、in-flight binding、admission credit | 进程内；重启不能从 trace 自动恢复 |
| 已提交复用结果与 publication receipt | 独立 SQLite schema v4，默认 data/reuse-v4.sqlite |
| DCS request snapshot、delta、ACK | 独立 SQLite WAL schema v4，敏感 payload 使用 Fernet |
| Trace | metadata-only JSONL；默认单文件 64 MiB、3 个备份 |
| SharedStateBackend | SQLite CAS/fencing 契约实现；不构成多 worker 服务能力 |

Trace 写入失败增加失败/丢弃计数并降低健康状态；本地轮转和 append 不提供
跨进程 exactly-once 审计保证。Trace 不记录 prompt、完整 Tool payload 或凭据。
旧复用库及旧 DCS schema 不会被自动迁移、清空或兼容读取。

DCS 的恢复需要 Runtime 权威历史、恢复 manifest 与重建注册；
不能仅凭 WAL 存在就宣称任意未完成 workflow 可自动续跑。
相关验证入口见 [验证与证据](verification.md)。
