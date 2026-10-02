# FlowPilot

FlowPilot 是 OpenHands 与一个固定 vLLM 实例之间的双向网关。
OpenHands 拥有 agent loop、权威历史和真实 Tool 执行；
FlowPilot 负责请求代理、身份与 frontier、可选 Tool 复用和外部请求排队；
vLLM 拥有 KV 存储以及全部恢复/重算执行。

[设计契约](design.md) 定义目标；
[当前实现文档](docs/README.md) 根据源码记录配置、行为和已知差距。
文档核对日期为 2026-09-22，不代表功能已在运行实例启用。

## 启动基础网关

```bash
cd /home/liyachen/workspace/flowpilot
uv sync --extra dev
export FLOWPILOT_INGRESS_API_KEY=local-dev
export FLOWPILOT_UPSTREAMS=http://127.0.0.1:8001
export FLOWPILOT_HOST=127.0.0.1
uv run flowpilot
```

网关默认端口 9000，推理入口为 `/v1/chat/completions` 和 `/v1/responses`。
请求需要注册 job/line、完整 `X-FlowPilot-*` 身份以及
`X-FlowPilot-API-Key`。OpenHands 默认关闭的 FlowPilot adapter
负责动态身份和 Tool telemetry，要求 `tool_concurrency_limit == 1`。
配置及完整接口见 [运行与接口](docs/runtime.md)。

## 可选能力

| 能力 | 默认状态 | 说明 |
| --- | --- | --- |
| Exact / in-flight Tool reuse | 关闭 | 注册只读 Tool，使用独立 reuse-v4.sqlite |
| Semantic reuse | 关闭 | 开启后的模式默认 shadow，生产 active 质量证据不足 |
| DCS | 关闭 | exact-only；显式 delegation、Fernet 和 schema v4 WAL |
| Forecast | 关闭 | 默认 NoOp，占位协议不代表已部署预测器 |
| Admission | 关闭 | 单实例加权排队、健康检查和配置化 credit |
| KV retention | 关闭 | 本地 vLLM KV control v1 的 KEEP/OFFLOAD/DROP |

Tool 复用、缓存容量和上下文同步见 [Tool 复用与 DCS](docs/tool-reuse.md)。
当前 benchmark 适配选择 `search`（exact / 受约束 semantic）和
`read_document/get_document`（exact）；从同一 benchmark 配置导出 registry，按后端和语料隔离同名工具。
实际排序公式、开关及策略分支见 [请求调度](docs/scheduling.md)。
admission 默认全量查询排队请求并按剩余 prefill slack 排序；成本需通过 `FLOWPILOT_COST_MODEL_PATH` 提供匹配的离线标定，无标定时明确退到 deadline-only。

KV descriptor、GRACE、共享偏好及已确认缺陷见
[vLLM KV 控制](docs/vllm-kv-control.md)。
普通请求由引擎自主恢复或重算；网关没有外部 RESTORE 和 GPU-ready 等待门槛。
Tool cache 与 KV cache 使用独立容量。
当前只支持单 worker；SQLite shared-state 模块不构成完整多进程事务能力。

四卡 Qwen3.5-9B 的完整实验参数、配置校验、服务入口和 OpenHands 接入见
[TP=4 实验配置](examples/experiments/qwen35_9b_tp4/README.md)。
该四卡配置已接入 prefill、KV offload/restore 的
[实测成本参数](examples/experiments/qwen35_9b_tp4/cost-model.json)，用于 admission 和 KV 去留成本比较。
该配置独立保存，不改变服务默认值；完整负载性能仍需实测。

当前 Qwen3.5-27B、TP=4、CPU KV 总预算 64 GiB 的
[实验入口与配置](examples/experiments/qwen35_27b_tp4/README.md) 默认接入独立的
[prefill / H2D 实测成本](examples/experiments/qwen35_27b_tp4/cost-model.json)。
独立 D2H 成本仍为 unknown；GPU 补测暂缓，完整工作流收益尚未验证。

## 验证

```bash
uv run pytest -q
uv run ruff check flowpilot integration tests
uv run pyright flowpilot
git diff --check
```

OpenHands 集成矩阵、既有真实 vLLM 实验和证据限制见
[验证与证据](docs/verification.md)。
旧 phase 文档和重复计划已从 docs 移除，历史内容保留在 Git 中。
