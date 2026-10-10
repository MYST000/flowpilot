# Qwen3.5-9B 四卡完整实验配置

冻结日期：2026-09-30；配置 ID：`qwen35-9b-tp4-benchmark-reuse-v3`（benchmark 检索工具复用；旧成本配置已移除）。
参数唯一来源为 [config.json](config.json)，所有入口均读取该文件。
这是完整实验的建议基线；已完成局部成本采样，完整 agent 工作负载仍未验收，不能称为最优配置。
配置落盘和 `--check` 均不会启动模型、发送推理请求或执行 Tool。

真实 prefill、KV CPU offload/restore 采样入口见 [成本测量说明](COST_MEASUREMENT.md)。
旧 9B 成本文件及其默认加载引用已经移除，cost_model_path=null。
当前唯一分发的七特征参数属于 27B，不能跨模型套用；9B 成本保持 unknown，
admission 整轮 FIFO，retention 使用既有 unknown 分支。历史测量保留在外部目录。

## 已固定的参数

| 部分 | 配置 |
| --- | --- |
| GPU / 模型 | GPU 0–3，4×24 GiB RTX 4090，Qwen3.5-9B，BF16，TP=4 |
| 引擎 | 当前本地 KV-control 扩展，PP/DP/DCP/PCP=1，eager，无 speculative/MTP |
| 上下文 / batch | 131072 tokens，max_num_seqs=4，batched_tokens=2048，chunked prefill |
| GPU KV | utilization=.80，无 block override；记录启动时实际 allocations/bytes |
| Hybrid prefix | APC、Mamba align、dense checkpoints（Python None）、非思考保留模板 |
| CPU KV | CPUOffloadingSpec，16 GiB 总预算，store_threshold=0，包含生成前缀 |
| KV control | KEEP/OFFLOAD/DROP，GRACE=250 ms，metadata TTL=1800 s |
| FlowPilot | 单实例、单 worker，推理 timeout=300 s，admission limit=4 |
| 查询 / 健康 | probe timeout=5 s，prefix TTL=5 s，heartbeat 间隔=1 s / TTL=10 s |
| 去留 | RPC timeout=5 s，refresh=.25 s，horizon=1 s，GPU reserve=128 allocations |
| 复用 | search/read_document/get_document exact history/in-flight；仅 search semantic=.92/shadow；静态语料 TTL=86400 s |
| DCS | 开启、仅 exact；OpenHands 同时启用 delegation，使用独立加密 WAL |
| 预测 | forecast、synthetic Tool duration 关闭，未知 gap 保持 unknown |
| OpenHands | 非流式，每 Agent Tool 串行，输出最多4096 tokens，每次 run 最多60 iterations |
| 负载 | 8 Job；扫描4/8/16；seed=41/42/43；SLO 倍率1.5，扫描1/1.5/2 |

GPU utilization 是模型执行器整体显存预算，不是 KV bytes。CPU KV、GPU KV、
512 MiB Tool payload 各自独立。不要用 smoke 的64 blocks或1000000的压力阈值替代主配置。
admission 使用 `policy=wait_cost`；缺成本时整轮 FIFO，显式 `fifo` 可作对照。
`wait_feedback.window_seconds=30` 是本次新增的待标定实验窗口；
retention 默认 `window_basis=tool_and_queue`，`tool_only` 可隔离 Q 的增量贡献。
SLO 倍率仅保留离线评估元数据，不影响顺序或去留。

## 输入与运行目录

- 从 OpenHands benchmark TOML 导出的 registry JSON（`python -m benchmark_adapters.reuse_profile --config ... --output ...`，支持重复 `--config`）。
  [profile.py](profile.py) 选择 search/read_document/get_document，保留同名 search 的多个后端及语料配置、schema 和 scope，应用本配置的模式和 TTL。
  文档读取始终 exact；think/finish 不参与复用。旧原生 BrowseComp runner 的导出不兼容此配置。
  配置导出、SDK 连接和远端策略版本的要求见 [工具复用说明](../../../docs/tool-reuse.md)。
- `FLOWPILOT_INGRESS_API_KEY`：网关与 OpenHands 共用的入口凭据。
- `FLOWPILOT_DCS_ENCRYPTION_KEY`：有效 Fernet 密钥；同一 WAL 重启时必须使用原密钥。
- 可选 `FLOWPILOT_COST_MODEL_PATH` 或 `--cost-model`：匹配本机TP/dtype/batch的真实标定 JSON。
  CLI 参数优先于环境变量，环境变量优先于 config 的 `workload.cost_model_path`。
  配置中的相对路径以 FlowPilot 仓库根目录解析；CLI/环境变量相对路径以工作目录解析。
- 独立实验目录，存放 trace、reuse-v4.sqlite、dcs-v4.sqlite 和 runner 输出。
  各实验组使用不同目录；复用同一目录会保留历史缓存/DCS状态，不会自动清空。
- 固定完整任务清单、原语料/MCP/search profile、基线时延文件，由现有 benchmark runner 提供。

密钥不写入此目录。Tool duration prior 和 baseline latency 仍为 null，表示尚未提供实测值。
成本文件已配置；路径无效或文件不合法会报错，不会静默忽略。将配置的
`workload.cost_model_path` 显式设为 null 且不提供 CLI/环境变量覆盖，可运行无标定对照：
admission 使用整轮 `fifo:cost_unknown`，retention 使用已有 `fallback_cost_unknown`。
即使加载成本文件，未知 Tool gap 或不兼容的引擎身份仍会走已有备用规则。
Tool 时延先验没有在此新增实现；需要接入真实测量/预测源后单独验证。

prefill 按总上下文分桶，再按剩余计算 tokens `P-H` 选择线性分段，涵盖冷请求、
部分命中及恢复后的残余计算样本。offload/restore 按真实对象 bytes 估算，分别约为
`6.36 + 29.82 × GiB` ms 和 `5.23 + 15.03 × GiB` ms，不能换成固定 token→bytes 比例。
这仍是本机单请求成本估计；未测并发干扰、未采样尺寸/命中比例和完整 workflow 的误差。

## 配置校验与服务入口

从 FlowPilot 仓库根目录运行。vLLM 与 SDK 使用各自已经安装依赖的虚拟环境；
SDK 环境通过 `PYTHONPATH` 引用本 checkout 的 FlowPilot。

```bash
cd /home/liyachen/workspace/flowpilot

# 只解析本地 vLLM 参数，不加载模型。
/home/liyachen/vllm/.venv/bin/python \
  -m examples.experiments.qwen35_9b_tp4.launch vllm --check

# 两个密钥从本次实验的凭据环境提供；registry 使用本次真实导出路径。
.venv/bin/python -m examples.experiments.qwen35_9b_tp4.launch gateway \
  --check --registry /absolute/path/current-registry.json \
  --run-dir /home/liyachen/workspace/experiments/flowpilot/full-tp4/run-001

PYTHONPATH=/home/liyachen/workspace/flowpilot \
  /home/liyachen/openhands/software-agent-sdk/.venv/bin/python \
  -m examples.experiments.qwen35_9b_tp4.launch openhands --check
```

实际启动时，在两个终端分别运行上面的 vLLM 和 gateway 命令并去掉 `--check`。
vLLM 监听 `127.0.0.1:18831`，FlowPilot 监听 `127.0.0.1:18832`。
入口使用 config 中的参数，不会混入其他 `FLOWPILOT_ADMISSION_JSON/RETENTION_JSON` 设置。
如需变体，复制 config 并通过 `--config /absolute/path/variant.json` 显式选择。
vLLM 启动器在导入 vLLM 前设置 GPU 0–3、EngineCore multiprocess 和本地模型离线加载。
JSON null 经 Namespace 赋值变成真正的 Python None，不传字符串 "None"。
请求/输出正文日志关闭；引擎启动与错误日志仍保留。

`openhands --check` 只校验 LLM 和 adapter 配置。这个目录不重新实现 benchmark executor；
并发数、任务集、seed循环、SLO倍率扫描由现有 runner 消费 `workload`。

## 在真实 OpenHands runner 中读取

runner 调用 `openhands_options()` 得到 LLM、Agent 参数和 LocalConversation 参数。
示例中的 `real_tools`、`workspace`、`task` 和 `baseline_seconds` 来自原 benchmark，
没有用 fixture 替代真实检索。先注册带 deadline 的 Job，再创建/运行 conversation：

```python
import os
from datetime import UTC, datetime
from uuid import uuid4

import httpx
from openhands.sdk import Agent, LocalConversation

from examples.experiments.qwen35_9b_tp4.profile import (
    job_registration,
    load_profile,
    openhands_options,
    service_url,
)

profile = load_profile()
api_key = os.environ["FLOWPILOT_INGRESS_API_KEY"]
conversation_id = uuid4()
job_id = f"job-{conversation_id}"
line_id = f"line-{conversation_id}"
options = openhands_options(
    profile,
    api_key=api_key,
    job_id=job_id,
    line_id=line_id,
    root_conversation_id=str(conversation_id),
    seed=profile["workload"]["seeds"][0],
)
payload = job_registration(
    profile,
    job_id=job_id,
    root_conversation_id=str(conversation_id),
    started_at=datetime.now(UTC),
    baseline_seconds=baseline_seconds,
)
response = httpx.post(
    service_url(profile, "flowpilot") + "/flowpilot/v1/jobs",
    headers={"X-FlowPilot-API-Key": api_key},
    json=payload,
    timeout=10,
)
response.raise_for_status()
conversation = LocalConversation(
    conversation_id=conversation_id,
    agent=Agent(llm=options["llm"], tools=real_tools, **options["agent"]),
    workspace=workspace,
    **options["conversation"],
)
try:
    conversation.send_message(task)
    conversation.run()
finally:
    conversation.close()
```

`baseline_seconds` 使用独立基线测量，不根据当前实验结果追改 deadline。
初次注册与 SDK 再注册必须使用同一 root_conversation_id/deployment/namespace；
不要改用 default_slo_ms，否则会与当前 SDK 的 Job 注册字段冲突。
子 Agent 继承 Job，通过已有 adapter 的 child() 传递 parent 元数据。
普通 run() 返回不等于 line finish，close() 的失败必须保留记录。

## 比较与证据

各组固定模型、实际GPU/CPU KV预算、原生CPU offload、GRACE、任务和到达序列。
原生自动 store 保持开启；不能把全部 store bytes 归因于 FlowPilot OFFLOAD。
复用冷启动与热启动分开；shadow 有真实 embedding 开销，需要在实验清单中记录。
语义active、DCS关闭、retention关闭、成本未知等变体单独保存配置，不能运行中换参。

记录任务正确率、失败/取消、mean/P95 workflow JCT、成功 workflow/s、实际Tool次数、目标prefix来源、
成本估计覆盖/误差、策略回执、credit和每条line的结束事件。
CPU恢复需要归因到普通后继请求及真实bytes，累计load计数不能代替单请求证据。
快照保留实际配置、三仓库commit与diff、模板/语料/registry/标定文件指纹，排除密钥和正文。

已有四卡CPU恢复证据见 [实机审查记录](../../../../experiments/flowpilot/audit-20260927/REPORT.md)。
配置语义以 [design.md](../../../design.md) 和本地源码为准。
本目录的参数校验不构成完整题集、128K×并发、DCS与shadow组合的实机验收。
