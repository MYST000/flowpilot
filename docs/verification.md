# 验证与证据

## 2026-09-27：descriptor 本地引用到期清理补齐

FlowPilot 全套 289 passed；vLLM manager/protocol 定向 54 passed；
OpenHands adapter 加一个已有 gateway/工具复用集成用例 37 passed。
新增回归覆盖不同 monotonic 时钟域、重复 resolve 不续期、传输延迟、
事件缺口、控制 RPC 阻塞、pending operation/回执丢失、查询途中到期、
过期事件 owner/epoch/descriptor 隔离，以及重启和关闭时取消定时器。
所有清理测试同时检查没有增加 descriptor 查询或策略命令。

主要命令为 FlowPilot `.venv/bin/python -m pytest -q --maxfail=1`；
vLLM `.venv/bin/python -m pytest -q tests/v1/engine/test_kv_control_manager.py tests/v1/engine/test_kv_control_protocol.py`；
OpenHands SDK 环境运行 `tests/sdk/test_flowpilot.py` 和 FlowPilot
`integration/test_openhands_reuse.py::test_agent_gateway_local_commit_then_history[False-True-True-False-curl]`。
Ruff、Pyright 和 compile 检查覆盖修改的实现。本次未启动真实 GPU 服务：
新增内容是 resolve 时钟元数据和本地引用生命周期；既有 GPU 回收证据见下节，
不将此前的实机结果报告为本次重跑。

## 2026-09-27：SLO 成本、全量排队查询与 OFFLOAD 回收

[本次报告](../../experiments/flowpilot/slo-prefix-20260927/REPORT.md) 记录精确范围与命令：
FlowPilot 277 passed，vLLM KV/connector/CPU manager 314 passed，
OpenHands adapter 与工具集成 112 passed、24 skipped；Ruff、定向 Pyright、compile 和 diff-check 通过。

真实 Qwen3.5-9B/TP=4 隔离测试中，OFFLOAD 后 GPU prefix=0、CPU recoverable=528，
单个普通后继实际加载 68,812,800 bytes。真实 OpenHands → FlowPilot → vLLM 工作流
完成 10 次推理/admission，全部 work 观察来自 TARGET_REQUEST。
该工作流未配置生产成本文件；离线模型拟合已有实现，实测标定误差与 SLO 收益仍待验证。
测试服务已停止，原有工作区修改保留。

## 既有证据索引

核对日期：2026-09-22。本页提供当前入口，并区分源码存在、单元/模拟验证、
真实引擎实验和生产证据。文档整理本身不代表重新通过全部发布门槛。

## FlowPilot

在 FlowPilot 根目录运行：

```bash
uv sync --extra dev
uv run pytest -q
uv run ruff check flowpilot integration tests
uv run pyright flowpilot
git diff --check
```

按改动范围选择测试；仅更新 Markdown 时校验链接、配置示例和版本即可。
测试使用临时路径，不将旧业务 SQLite 作为测试库。

| 范围 | 当前回归入口 |
| --- | --- |
| Identity/frontier/dependencies | test_identity.py、test_protocol.py、test_frontier.py |
| Gateway/SSE/terminal/trace | test_gateway.py、test_stream.py、test_app.py |
| Exact/semantic/可信发布 | test_reuse.py、test_phase3_api.py |
| Tool Cache 价值与保护 | test_cache_retention.py |
| DCS/context/ACK | test_context.py、test_dcs_api.py |
| Forecast/ready-time/projection | test_phase4.py |
| 实际 admission、heartbeat、credit | test_admission.py |
| KV client/capability/receipts | test_retention.py、test_kv_removal.py |
| 旧路由及 shared-state 契约 | test_phase5.py；不能据此宣称多 worker 已上线 |

这些文件均在 [tests](../tests)。
旧 `examples/openhands_e2e.py` 仍含 phase3-reuse-v2 registry，
与当前校验器不兼容；本次未修改该脚本，不作为现行 smoke 命令。

## OpenHands -> FlowPilot 集成

使用可导入 SDK、Tools 和 FlowPilot 的 OpenHands Python 环境，
从 FlowPilot 根目录运行：

```bash
PYTHONPATH=/home/liyachen/workspace/flowpilot \
  /home/liyachen/openhands/software-agent-sdk/.venv/bin/python -m pytest -q \
  integration/test_openhands_reuse.py
```

[集成矩阵](../integration/test_openhands_reuse.py) 参数覆盖原生 terminal curl/wget、
Tavily Search/Extract/Crawl/Map、
history/in-flight、gateway/direct、DCS 和 admission 开关。
Runtime-direct+DCS 组合明确跳过，由 SDK 专门用例覆盖。
推理服务为 mock，主矩阵的 Tool executor 为受控 fixture。
另外 4 项 test_real_terminal_http_reuse 使用真实 TerminalExecutor、curl/wget
和本地 HTTP 服务，分别验证 history/in-flight 时两个 Agent 调用只产生一次网络请求。
这些测试不证明真实 Tavily API 或 GPU 推理效果。

OpenHands 的定向测试在其根目录运行：

```bash
UV_NO_SYNC=1 uv run pytest -q tests/sdk/test_flowpilot.py
```

旧报告中的测试数量随代码版本变化；运行后记录实际 passed、failed、skipped，
不能把跳过项计为通过。

2026-09-22 URL 工具族修正的本地验证：

- FlowPilot 全套 243 passed，其中新增 Terminal/Tavily URL 定向回归 68 项。
- OpenHands FlowPilot adapter：36 passed；OpenHands 仓库没有代码修改。
- 集成矩阵及真实 terminal HTTP：76 passed、24 skipped。
- Ruff、Pyright（0 errors）、compile/import、registry 样例运行、git diff --check 通过。
- Tavily schema 从本机 tavily-mcp@0.2.1 的 build/index.js 提取，并逐项核对原有
  Search/Extract schema 不变；新增 Crawl/Map 保留其实际参数与 MCP 文本返回。
- sandbox 禁止本地 socket，并使跨线程 asyncio 唤醒测试停住；完整回归在获准的宿主环境通过。
- 所有测试 SQLite 均位于 pytest 临时目录；本机旧 workspace/tool-reuse 目录不存在。

本次状态为 implementation complete / local verification complete。
production evidence insufficient：尚无真实 Tavily API、多种实际 shell 环境及真实 vLLM 的联合负载证据。
Terminal 复用的适用前提是 registry namespace/policy/version 内执行环境兼容；
parser 本身不验证 curlrc、代理、别名或其他隐式环境事实。

## vLLM 既有证据与本次修复

真实引擎实验保留在
[kv-framework-20260917/REPORT.md](../../experiments/flowpilot/kv-framework-20260917/REPORT.md)。
该报告对应本地 0.29.0 扩展、Qwen3.5-9B、BF16、TP=4、Mamba align/dense checkpoint，
覆盖普通请求 CPU 加载、自然 GPU 淘汰、cascade、idle GRACE、
延迟 OFFLOAD 和 terminal-result 边界的必要 rank 失败注入。
它没有证明真实 GPU device crash 可恢复；小样本也未证明稳定吞吐收益。
不能把该引擎报告等同于 FlowPilot/OpenHands workflow 联动验收。

2026-09-22 修复前审查重跑 295 项既有测试通过，但额外 4 个断言失败，
确认 [KV 文档列出的 3 类缺陷](vllm-kv-control.md)。修复后：

- 同一组 control manager/protocol、offloading scheduler、CPU manager：308 passed。
- 新增 13 项回归覆盖 GPU 尾块、CPU chunk 交接、共享需求、GPU/CPU 延迟 DROP、
  metadata 到期后的新映射保护和 KEEP 到期后的分配顺序。
- 其中最初 10 项在修复前均失败、修复后通过；其他 3 项补充共享/映射安全验证。
- FlowPilot KV 客户端与旧接口移除测试：19 passed，客户端实现未修改。
- 两个修改的引擎 Python 文件通过 Ruff lint/format、compile；manager Pyright 0 errors。
- 沙箱无法初始化 NVML，原生 scheduler fixture 配置失败；308 项完整结果来自宿主环境。

本次宿主回归命令如下；临时依赖目录是本机已有隔离环境，其他机器需准备等价依赖：

```bash
cd /home/liyachen/vllm
PYTHONPATH=/tmp/flowpilot-kv-test-transformers HF_HUB_OFFLINE=1 \
  .venv/bin/python -m pytest -q --maxfail=1 --tb=short \
  tests/v1/engine/test_kv_control_manager.py \
  tests/v1/engine/test_kv_control_protocol.py \
  tests/v1/kv_connector/unit/offloading_connector/test_scheduler.py \
  tests/v1/kv_offload/cpu/test_manager.py
```

修复后重新运行现有 Qwen3.5-9B/BF16/TP=4 真实引擎脚本：
3 个压力请求后 H_gpu=0、H_cpu=1056；普通后继请求实际加载 86,114,304 bytes，
输出 token 与清空双层后的重算一致，选中 token 的最大 logprob 差异为 0。
必要 rank 的 terminal-result 故障注入返回 FAILED，新版本重试返回 APPLIED；
最终 pending jobs 和 active operations 均为 0。
见 [本次修复验收](../../experiments/flowpilot/kv-fixes-20260922/REPORT.md)。
此实验验证现有 hybrid 推理路径；GPU block=16/CPU chunk=32 的尾部缺陷、
长 metadata TTL 和分配顺序由原生池单元测试覆盖。

此前的 [descriptor 底层诊断](../../experiments/flowpilot/descriptor-accuracy-20260922/REPORT.md)
使用真实 GPU 推理，但包含 token-ID 输入与受控淘汰，不代表真实多轮对话验证。
它发现 hybrid `recoverable_tokens` 低估；随后按真实对话要求重新运行了
[多轮对话实验](../../experiments/flowpilot/conversation-descriptor-20260922/REPORT.md)：

- Qwen3-1.7B/TP=1 和 Qwen3.5-9B/TP=4，各 7 个会话、50 次真实 Chat Completions。
- 每轮实际模型回复原样进入历史；其他会话正常推理产生 GPU/CPU 压力，无手动淘汰、重置或 token 填充。
- 两组各 43 次跨轮续接，分别有 15/24 次自然 GPU 前缀丢失、10/23 次真实 CPU 恢复。
- 普通 attention 在真实 hash 共享范围内，所有 GPU/CPU query 均与实际命中相符。
- Hybrid 在当时复现 1 次低估：query H_all=0、CPU=528，实际恢复 528 tokens，
  原生 load=68,812,800 bytes；查询与正式请求的 CPU lookup 起点不同。
- 无压力对话也出现旧 descriptor=432、实际命中=192：Qwen 模板重渲染历史使 token 前缀分叉，
  并非缓存丢失。ID-only 查询不证明后继请求内容。
- 100 次 HTTP 缓存 usage 与原生 PrefillStats 一致，86 次续接历史摘要链一致；
  累计 load bytes 分别为 932,184,064 和 1,842,216,960，与 Prometheus 总量一致。

该原始实验只增加证据，没有修改查询实现；其记录保持不变。

### Descriptor 可用长度修复

后续 [修复验收](../../experiments/flowpilot/conversation-descriptor-fix-20260922/REPORT.md)
保持只传 descriptor ID 的接口，让当前 OffloadingConnector 的 CPU lookup
从所有必需组共同可用的 GPU 边界开始，移除错误的 full-attention 单组起点。

- 原生池回归新增 2 个部分驻留场景：修复前 H_all 分别误报 0/16，修复后均为 32，
  并与实际 Scheduler/connector lookup 对照。引擎相关套件共 310 passed。
- 修改文件 Ruff lint/format、manager Pyright、compile 和 diff whitespace 检查通过。
- FlowPilot KV/admission 测试 32 passed；OpenHands adapter 测试 36 passed；
  OpenHands -> FlowPilot 的 mock inference 集成 36 passed、12 skipped。
- 原脚本在新目录复跑 100 次真实 Chat Completions、86 次续接，
  GPU 与 GPU/CPU 查询在真实共享前缀范围内均无偏差，usage、摘要链及传输总量核对通过。
- 同一个 `hybrid-testing-4` 场景再次出现相同的部分 GPU 丢失：
  query H_all 从 0 修正为 528，与实际 CPU 恢复 528 tokens、68,812,800 bytes 一致。

实现与本地验证完成。真实 GPU 复跑直接连接 vLLM；OpenHands/FlowPilot 路径使用 mock
推理和受控 Tool fixture，两组证据不能合称真实三组件 workflow 全链路验收。

### 独立多轮对话场景验证

当前有效性证据进一步采用
[8 个独立新场景](../../experiments/flowpilot/conversation-scenarios-20260922/REPORT.md)：
社区活动、实验室预约、离线调查、图书馆流转、设备维护、数据协作、博物馆导览和课程安排。
每个模型各 64 次真实请求、56 次续接，回答实时生成并原样进入历史；
问题、访问顺序与预算在运行前固定，运行中不依据缓存状态调整。

- 128 次请求完成，112 次续接查询均无偏差，其中 97 次具有非零真实共享范围。
- 普通 attention/Hybrid 分别出现 6/19 次续接前 GPU 前缀缩短、5/15 次 CPU 前缀缩短，
  实际 CPU 恢复 1/2 次；Hybrid 两次均为 GPU prefix=0 的普通请求自主恢复。
- 对暂停会话持续采集 912 次 ID-only descriptor 快照；同一 ID 的描述范围保持不变，
  GPU/CPU 可用长度能随自然淘汰缩短。
- usage、原生 PrefillStats、历史摘要链与 worker/Prometheus 传输总量核对通过。
- Hybrid 有 7 次 length 结束，真实返回文本原样保留；没有补写或替换回答。

本次使用真实模型响应和正常请求产生的缓存压力，不重跑旧故障样本或人为选择淘汰块。
这是脚本提供问题的场景验证，不是生产人类会话回放。直接 vLLM 路径与本地短时结果
仍不代替真实三组件 workflow 或生产 SLO 证据。

### Qwen3.5-9B 非思考历史前缀

使用显式保留模板，固定 `enable_thinking=false`，在真实 Qwen3.5-9B/BF16/TP=4
上执行培训座位、午餐采购、值班交接三个新场景，每场景三轮。
最终模板9次生成均stop；6次续接全部保留上一轮完整实际token序列，
ID-only descriptor query与原生PrefillStats、HTTP usage一致。
其中2次续接实际命中528 tokens，另外4次旧序列未达到Hybrid可复用边界，命中为0。
没有CPU restore，也未将此实验计为缓存压力或答案质量评估。

模板保留历史assistant生成时已有的空think段及正文空白，兼容vLLM内容数组。
代码、静态检查范围、逐轮计数及运行快照见
[Qwen3.5实机报告](../../experiments/flowpilot/nonthinking-prefix-qwen35-20260922/REPORT.md)，
部署参数见[模板说明](../examples/chat_templates/README.md)。

## 2026-09-23 真实三组件联动

本机 Qwen3.5-9B、OpenHands、FlowPilot、真实本地 Tool 执行与 Tool Reuse
已完成联合请求验收；另以三个真实并发 GatewayCall 验证唯一 credit 下的
高低紧迫度排队与归还。命令、指标增量、trace 路径及未覆盖边界见
[本机真实请求验收](real-workflow.md)。本轮 FlowPilot 全套 244 passed，
OpenHands FlowPilot adapter 36 passed，真实 Terminal HTTP 集成 4 passed；
Ruff lint、改动文件的 format、Pyright、compile 和 diff whitespace 检查通过。
全仓 format 检查仍提示两个本轮未改动文件：
`flowpilot/gateway/call_state.py` 和 `flowpilot/reuse/store.py`。

## 尚缺的证据

Semantic active 需要代表性、独立标注的真实搜索数据，不能以
[6 组人工标签](../tests/fixtures/semantic_labels.json) 代替。
已有评估入口为
`uv run python -m flowpilot.reuse.evaluation tests/fixtures/semantic_labels.json`，
需要本地 embedding 模型与可选依赖；本轮未重跑模型评估。

真实链路已观察到 KV retention 收据、工作流负载中的 CPU KV 读回和
admission credit 生命周期；尚未证明特定 Tool 复用后继请求的 CPU-only
前缀提交与恢复，也未测出外部排序带来的收益。
目标 prefix 证明与恢复成本尚未接入队列，不能宣称已验证其调度收益。
SLO goodput、weighted JCT、Job 公平性、长期容量和元数据开销均需实际负载实验。

上述引擎问题已完成实现与本地回归；已有本地验证不代表生产证据充分。

2026-09-27 跨组件审查、控制重试与关闭清理修复、最终实机回归见 [审查报告](../../experiments/flowpilot/audit-20260927/REPORT.md)。该报告区分已修复缺陷、既有修复和仍待生产/质量验证的边界。
