# Tavily / URL Tool reuse 实施与验证

验收日期：2026-09-16。依据当前 `design.md` 与
`tavily-url-tool-reuse-plan.md`。`phase*` 系列文档属于历史材料，
不作为本次实现及验收依据。本次按用户选择保留已有协议版本标识，
没有扩展重命名到调度、DCS 或 vLLM 接口。

## 结论与边界

- Tavily exact：已实现可信发布、历史及 in-flight 复用，并通过真实
  OpenHands MCP 类型与 Agent/Gateway 集成。载荷整体保留；上游供应商
  完整性为 `unknown`，没有从文本标签推断截断状态。
- URL exact：已实现 OpenHands `UrlFetchTool` 真实执行器；普通 Terminal、
  Shell、wget 不提升为可信执行器。公开网络单次 smoke 为 HTTP 200、
  559 字节正文、完整且网络策略验证通过。
- Semantic：本地 Qwen3-Embedding-0.6B 的异步、离线加载、1024 维 L2
  向量、历史与 in-flight Top-K、快照版本复核、索引重建和模式控制已实现。
  默认 `shadow`；当前质量证据不足以支持生产 `active`。
- FlowPilot 不执行真实 Tool；OpenHands 拥有 Action、Observation 及历史。
  未修改 vLLM；本次推理端集成证据为 mock-compatible，不是真实 vLLM 压测。

## 已修复的问题

1. 统一 `ReuseService` 从权威 job/line 注册和部署配置取得 namespace，
   所有解析入口使用同一控制器，不信任 Tool 请求自报隔离域。
2. ToolCallRef 注册不依赖尚未产生的 Action；接受的 START 绑定
   ExecutionRef，FINISH 验证实际 input/result digest、大小和 adapter 信息。
3. 本地 Observation 提交后才 publish。SQLite 单事务提交结果 blob、
   执行证据、索引与 publication receipt；重复发布返回原收据，冲突撤销后续复用。
   `cacheable=false` 仍需真实执行证据，且仅为已有 follower 保留结果。
4. TTL 以服务端收到 FINISH 的时间为起点，受 registry/scope 上限约束。
   交付与 DCS 首次消费再次检查有效期，不改写已提交的历史。
5. 固定 Tavily MCP 0.2.1 的实际 inputSchema；按真实 MCP Action 处理省略值、
   数字及 domains。环境/secret 引用在 lookup、hash、embedding 前拒绝复用。
6. Gateway 决策与 Runtime 直接 resolve 均消费 leader/follower；核对原始身份、
   输入和 schema。修复 OpenHands discriminator 校验原地删除 `kind` 导致的
   摘要错误，以及 Gateway 修改响应后未刷新 Content-Length 的真实 HTTP 故障。
7. Gateway DCS 使用原始请求快照，按当前 OpenHands provider 格式构造消息；
   缺少 MCP read-only annotation 时关闭 DCS，但 exact 同步路径仍可用。
8. 索引缺失、损坏、模型不可用只影响 semantic。完整候选打分后再截取 Top-K，
   编码与评分不持有 controller lock；并发注册前复核快照代次。
9. 新库维护包含过期清理、引用/向量级联、按访问时间容量回收、收据重试窗口、
   WAL checkpoint；启动后的周期维护失败单独计数。

## 本地验证

最新结果：FlowPilot 全量测试 148 项；OpenHands 定向测试 52 项；
真实 Agent 集成矩阵 18 项通过、6 项明确跳过。Ruff、Pyright、
编译导入及 OpenHands pre-commit 另行执行，不计入测试项数。

验证入口：

```bash
# FlowPilot 环境
.venv/bin/pytest -q
.venv/bin/ruff check flowpilot integration tests
.venv/bin/pyright flowpilot

# OpenHands 环境
UV_NO_SYNC=1 uv run pytest -q tests/sdk/test_flowpilot.py tests/tools/test_url_fetch.py
UV_NO_SYNC=1 uv run pre-commit run --files <本次修改的 OpenHands 文件>

# 同时可导入 OpenHands 的环境；推理端是本地 mock，Gateway 绑定 loopback
PYTHONPATH=/home/liyachen/workspace/flowpilot \
  /home/liyachen/openhands/software-agent-sdk/.venv/bin/python -m pytest -q \
  integration/test_openhands_reuse.py
```

Agent 矩阵覆盖 URL、tavily-search、tavily-extract；Gateway 与直接 resolve；
历史命中和并发 leader/follower；启用/关闭 Gateway DCS。
检查两条独立对话只执行一次真实 Tool 边界、当前 Tool Call 身份、
原 Observation 内容、provenance 和 metadata-only trace。
六个 Runtime-DCS 组合不在此矩阵重复运行，由 `tests/sdk/test_flowpilot.py`
专门覆盖；这些跳过项不是成功测试数。

部分 asyncio/thread 测试在受限沙箱内无法唤醒；获得授权后在沙箱外运行。
新 URL 执行器还测试 DNS 私网/回环/组播、rebinding、重定向、二进制、
环境与 cwd 隔离、禁用 curlrc、argv 固定和路径字节保留。
真实网络 smoke 暴露本机 curl 7.68 不支持 `%{json}`，已改用受控字段
write-out；没有绕过 DNS/最终 URL 检查。

## Qwen 小样本证据（非上线门槛）

使用仓库内 6 组人工正负例 `tests/fixtures/semantic_labels.json`，
不是生产 trace 标签集。CPU 执行，Torch 2.12.0、Transformers 5.9.0，
模型仅从 `/docker/data/HF_MODELS/Qwen3-Embedding-0.6B` 读取。
未安装/修改已有模型环境，未联网下载权重。

| 指标 | 结果 |
| --- | --- |
| Recall@1 / Precision@1 | 0.8333 / 0.8333 |
| MRR | 0.9167 |
| 阈值 | 0.97 |
| 阈值通过 / 错误匹配 | 1 / 0 |
| 包含冷启动的批编码耗时 | 20.58 秒 |
| 生产启用门槛 | 未通过：样本太小，缺独立测试集与灰度证据 |

复现命令为 `python -m flowpilot.reuse.evaluation tests/fixtures/semantic_labels.json`。
安装可选依赖使用 `uv sync --extra embedding`。运行时禁止下载模型。
首次模型加载可能超过在线等待时间；此时记录 semantic 不可用，
exact 继续工作，后台模型任务不会无限排队。生产前需预热和独立延迟评估。

## 配置与启用

新默认库为 `data/reuse-v4.sqlite`，带独立 schema 标记；旧库不迁移、不清空，
将旧路径误配置为新库会拒绝启动。参考 Tool-Reuse 仓库在本机不存在；
未发现待迁移的旧业务 SQLite，也没有修改旧业务库。测试库使用临时目录。
新库文件权限设为 0600；descriptor/semantic text 属于受控缓存数据，
不会写入 metadata-only trace。frontier 和未完成 binding 仍为进程本地状态；
重启后须重新建立权威 job/line 注册，已提交 publication receipt 可重复领取，
未提交执行不会被伪造为已完成 origin。

Tavily registry 需配置 `tool_version=0.2.1`、相应
`tavily_search_mcp_v1` / `tavily_extract_mcp_v1` adapter，以及
`flowpilot.reuse.adapters.tavily.TAVILY_SCHEMA_DIGESTS` 中的实际 schema 摘要。
OpenHands 必须同步部署本次 SDK 改动，并将明确允许复用的名称加入
`FlowPilotConfig.reusable_web_tools`。

URL registry 使用 `tool_name=url_fetch`、`adapter_id=curl_url_fetch_v1`、
`url_execution_policy_id=public-pinned-get-v1`；OpenHands 明确注册
`openhands.tools.url_fetch.UrlFetchTool`，不会替换普通 Terminal。

部署 namespace 默认值仅用于单一可信部署；多隔离域须通过权威 job/line
注册传入稳定 namespace。周期与容量配置为
`FLOWPILOT_REUSE_MAINTENANCE_INTERVAL_SECONDS`（默认 60）及
`FLOWPILOT_REUSE_MAX_PAYLOAD_BYTES`（默认 512 MiB）。
模型挂载由 `FLOWPILOT_REUSE_EMBEDDING_MODEL_PATH` 指定。

生产剩余证据：真实 Tavily 服务调用与稳定版本 pin、代表性脱敏 trace 标签、
独立质量集、namespace 灰度、超时/容量长期运行、控制器与模型延迟分位数。
这些不是用 mock 或小样本替代即可完成的门槛；默认不启用 semantic active。
