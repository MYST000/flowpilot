# Qwen3.5-27B TP=4 / CPU KV 64 GiB 成本 profiling

已完成 188 条主采样请求、156 条有效请求汇总，以及 prefill/H2D 拟合和留出验证。
结果见 [实验报告](/home/liyachen/workspace/experiments/flowpilot/cost-qwen35-27b-tp4-20261002/REPORT.md)。
独立 D2H、四条最长 prefix 共存/续接和目标查询开销尚未实测；用户已选择暂缓
GPU 补测。原 18851 实例在中断后已停止，当前不自动重新启动。

配置来源是 600 + 477 题的原生 benchmark。2026-10-02 的 16 GiB 容量核验
取得最长输入的真实 prefix 对象大小 17058037760 bytes（15.89 GiB）。用户随后
选择整个实例共 64 GiB 作为主实验预算，重点检查多个长 prefix 的保留。
16 GiB 核验位于实验根目录的 `extended/`，64 GiB 主实验另存 `cpu64/`。

实验使用 `/home/liyachen/vllm/.venv/bin/python` 和本机 FlowPilot vLLM
扩展，模型位于 `/docker/data/HF_MODELS/Qwen3.5-27B`。不使用或改动他人的
`qiul_host` 容器。vLLM/FlowPilot 端口分别为 18851/18852，编译缓存位于
`/home/liyachen/.cache/flowpilot-qwen35-27b/`；实验日志另存于自己的 experiments
目录。原生 benchmark 的版本为 `0.29.0+cu129`，本机扩展版本为
`0.29.0+precompiled`，必须分别记录源码身份，不能宣称两者执行栈完全相同。

## 保持的推理参数

GPU 0–3、4 × RTX 4090、TP=4、PP/DP/DCP/PCP=1、BF16、无权重量化、无
speculative decoding、262144 总上下文、max_num_seqs=4、每轮 2048 tokens、
显存预算 0.9、eager、APC 和 chunked prefill 开启、Mamba align，KV/Mamba dtype
均为 auto。使用相同的非思考模板；其 SHA256 为
`e8326ad42f41b61f24bf1ba9e07df5afa4b7380ada1b8806f75d011c66793d61`。

2048 是整个实例每个调度 iteration 的共享预算，不是单请求上下文长度，
也不是每卡 2048 或每条请求固定 512。原生命中部分不重新 prefill，未命中的
长输入分轮处理；decode 与 prefill 共享预算，缓存对齐也会影响实际 chunk。
单条无命中的 32768-token 输入至少需要 16 轮，实际可能更多。

原生采集器另有每题 60 iterations、160 次 Tool、100 次 LLM、14400 秒限制，
记录于 `profiling.native_reference.collector_limits`；本次合成输入成本采样
不执行该采集器，也不把配置文件中的未来 FlowPilot/reuse/DCS 设置称作已运行。

## CPU offload 配置与计时

| 配置 | 值与含义 |
| --- | --- |
| `cpu_offload_gb` | 0；模型权重仍在 GPU，此项与 CPU KV 缓存独立 |
| `kv_connector` / `kv_role` | `OffloadingConnector` / `kv_both` |
| `spec_name` | `CPUOffloadingSpec` |
| `cpu_bytes_to_use` | 68719476736 bytes，即整个 TP 实例共 64 GiB |
| `store_threshold` | 0，沿用已有实验的原生自动 store 配置 |
| `offload_prompt_only` | false |
| `kv_control.enabled` / `retention_preferences` | true / true |
| `finish_grace_ttl_ms` | 250；交接保护，不是复制时间 |
| `metadata_ttl_seconds` | 1800；descriptor 元数据生命周期，不保证 KV 驻留 |
| `prefix_cache_retention_interval` | Python `None`，沿用已有 FlowPilot 的 dense Mamba checkpoints；原生启动清单未显式设置此项 |

这里的 CPU KV 预算不包含 Python/引擎进程、模型加载和其他运行时内存。
容器独立不隔离物理资源：GPU 0–3、CPU、主机内存、PCIe 和磁盘仍与其他用户共享。
GPU 4–5 未被选择，但其他任务仍可能受到主机内存和 I/O 竞争影响。

prefill 取首次模型计算调度至首 token 返回 scheduler 的墙钟时间，包含 TP、
chunked prefill 和引擎开销。D2H/H2D 取复制 job 发出至全部必要 worker 完成的
墙钟区间；各卡 CUDA 时间单列，不把四卡时间之和当作延迟。真实 bytes 来自
worker 复制结果和引擎回执，不用 token 数换算。RPC 到 APPLIED、HTTP 完成时间
分别保存。H2D 必须来自 GPU 副本淘汰后的普通请求，测量过程没有 RESTORE 命令。

## 在框架中使用当前成本

27B 专用入口默认读取本目录 `config.json`，通过 `workload.cost_model_path` 加载
[cost-model.json](cost-model.json)。模型版本为 `offline-20261002T090029Z`，
admission 的目标 prefix 查询和 KV retention 共用同一个 `OfflineCostModel`。
该入口使用上述 TP=4、并发 4、2048 token budget 和 CPU KV 总预算 64 GiB 配置。

已有 benchmark registry 和网关凭据时，可只校验配置，不启动服务或创建数据库：

```bash
.venv/bin/python -m examples.experiments.qwen35_27b_tp4.launch gateway --check \
  --run-dir /absolute/path/to/new-run \
  --registry /absolute/path/to/benchmark-registry.json
```

凭据环境变量为 `FLOWPILOT_INGRESS_API_KEY`、`FLOWPILOT_DCS_ENCRYPTION_KEY`，
registry 沿用 benchmark 导出格式。启动输出应包含 `Qwen3.5-27B`、上述版本和
`H2D=calibrated D2H=unknown`。显式 `--cost-model` 优先于
`FLOWPILOT_COST_MODEL_PATH`，两者都未设置时使用本配置的 27B 文件。
通用 `python -m flowpilot` 入口仍需显式设置 `FLOWPILOT_COST_MODEL_PATH`；
使用旧 9B 入口时必须传本目录的 `--config`，否则仍加载 9B 配置。

| 决策中的成本 | 当前使用的数据 |
| --- | --- |
| admission 的 GPU 方案 | 实测分桶、分段 `F(P,H_gpu)`；cold、部分命中、近全命中 |
| admission 的 CPU 方案 | `R(actual_object_bytes) + F(P,H_all)`，与 GPU 方案取较小已知值计算 prefill slack |
| retention 的 KEEP / DROP | 同一份 `F`，分别计算残余 prefill / cold 重算 |
| retention 的已有完整 CPU 副本 | 零新增 D2H，加实测 H2D 和残余 prefill |
| retention 的新 D2H | `offload=null`；新复制的耗时仍未知，不按零成本加入已标定候选比较 |
| Tool 时长、查询开销、decode / 引擎排队 | 本轮没有相应模型；不以 prefill 或传输时间代替 |

GPU/CPU 驻留价格仍是策略系数 `1 / 0.01`，不把它们称为实测时间。
Tool gap 未知时 retention 仍明确走 `fallback_cost_unknown`，其中可能选择 OFFLOAD；
这属于已有能力/容量规则，不能解释为已经得到 D2H 预测。

例如 P=258048、H=257936、真实对象 bytes=17058037760 时，模型估算 cold
prefill 为 178.828 秒、残余 prefill 为 0.528532 秒、H2D 为 0.239082 秒，CPU
方案合计 0.767614 秒。这些是拟合值，不是单条请求实测，也不含 decode 或内部排队。
prefill 实测总输入覆盖 1024–258048；超过最高桶返回 unknown，较短输入和未采样
命中比例仍为近似。H2D 实测 bytes 覆盖 205324288–17058037760；当前线性模型
不强制 byte 范围，范围外为外推。四请求并发样本没有混入这个单请求拟合。

`tests/test_qwen27b_costs.py` 检查默认入口、实际 app 的共享模型接线、条件成本、
未知 D2H 分支，以及 CPU-only 请求正常提交并保持 credit 至响应结束。
这些是 CPU 上的受控回归验证；完整 OpenHands 工作流和实机并发收益尚未验证。

## 运行与采样范围

复用已有采样工具，需从 FlowPilot 仓库根目录运行。输出目录必须尚不存在：

```bash
/home/liyachen/vllm/.venv/bin/python -m examples.experiments.qwen35_9b_tp4.cost_serve \
  --config examples/experiments/qwen35_27b_tp4/config.json \
  --output /home/liyachen/workspace/experiments/flowpilot/cost-qwen35-27b-tp4-20261002/cpu64
```

在另一个终端执行：

```bash
.venv/bin/python -m examples.experiments.qwen35_9b_tp4.measure_costs \
  --url http://127.0.0.1:18851 --model Qwen3.5-27B --timeout 900 \
  --output /home/liyachen/workspace/experiments/flowpilot/cost-qwen35-27b-tp4-20261002/cpu64 \
  --contexts 1024,2048,4096,8192,16384,32768,65536,131072,196608,258048 \
  --repeats 3 --concurrency-probe --full-gpu-prefix \
  --sampling-json '{"temperature":1.0,"top_p":0.95,"top_k":20,"presence_penalty":1.5,"repetition_penalty":1.0,"min_p":0.0,"seed":41}'
```

使用合成 token IDs，三次重复；cold、部分 GPU prefix、近全 GPU prefix、CPU
恢复分别记录，实际命中以引擎事实为准。最大输入为 OpenHands 的 258048 预算。
采样覆盖到 258048，不把整个 262144 范围都称作实测。每次仅生成 1 token 并
设置 `ignore_eos=true` 以隔离 prefill；这与 benchmark 的 4096 输出上限不同，
不能作为完整 agent/decode 工作负载性能。四请求并发结果单列，不混入单请求拟合。

主采样后，在实例仍运行时执行 `measure_offload --url http://127.0.0.1:18851`
并传入输出目录和本轮 `--run-id`。只采纳 GPU 仍驻留而 CPU 副本已被原生 LRU
淘汰的当前 tail；没有新的复制条件会明确记录，不伪造零耗时。随后执行
`analyze_costs --include-residual-prefill`，使用已核对 H2D 完成早于计算开始的
恢复后残余 prefill，避免把恢复重复计费。拟合使用 `integration/fit_cost_model.py`
的 `--piecewise-prefill`，记录真实 engine digest、测量时间、范围和误差。

当前 `workload.cost_model_path` 指向本目录 [cost-model.json](cost-model.json)，
可用于当前配置的 prefill 与 H2D 条件成本估计，`offload=null` 表示独立 D2H
成本未知。现有 retention 代码只会在已证明 CPU 副本完整时采用零新增写回成本；
需要新复制时不能把未知值当零。本次没有启动 gateway 或验证完整工作流收益，
也没有复用旧 9B 成本系数。

主标定和空闲 D2H 测量完成后，使用同一实例另跑多 prefix 保留实验：

```bash
.venv/bin/python -m examples.experiments.qwen35_9b_tp4.measure_costs \
  --url http://127.0.0.1:18851 --model Qwen3.5-27B --timeout 900 \
  --output /home/liyachen/workspace/experiments/flowpilot/cost-qwen35-27b-tp4-20261002/cpu64 \
  --retention-only --retained-prefixes 4 --retained-prefix-length 258048 \
  --sampling-json '{"temperature":1.0,"top_p":0.95,"top_k":20,"presence_penalty":1.5,"repetition_penalty":1.0,"min_p":0.0,"seed":41}'
```

四条 line 使用互不共享的合成 prefix。每条计算完成后 OFFLOAD，并查询全部
已创建 descriptor 的 GPU/CPU 可恢复范围；全部创建后逐条通过普通推理续接。
`retained-prefixes-*.jsonl` 保存每次写入与续接前后的观察。新写入或恢复可以
触发原生 LRU，观察到旧 prefix 被淘汰属于结果，不把 64 GiB 声称为保留保证。
与基础成本标定使用不同 run_id，结果分开分析。

实例空闲时还可执行 `python -m examples.experiments.qwen35_27b_tp4.measure_query`
并传入输出目录，测量真实 Chat 渲染、分词、hash/lookup 和 HTTP 返回合计的
目标 prefix 查询开销。输入为无 Tool schema 的合成文本；长度采用返回的
实际 `prompt_tokens`，这些 RPC 时间单列，不加进引擎 prefill 拟合。

上述多 prefix、`measure_query.py` 和 `prepare_offload.py` 补测脚本目前尚未
实机执行。后者准备少量 GPU KEEP preference，供后续真实 CPU LRU 压力后测量
空闲 D2H；KEEP 不是驻留保证，必须观察实际 GPU/CPU 状态。重新启动必须使用
新的输出目录与 engine epoch；不同引擎的 transfer job ID 不能直接混合关联。

离线重新汇总已有数据：

```bash
.venv/bin/python -m examples.experiments.qwen35_9b_tp4.analyze_costs \
  /home/liyachen/workspace/experiments/flowpilot/cost-qwen35-27b-tp4-20261002/cpu64 \
  --run-id 03408abf5c2746b0af703681dccb4f55 --include-residual-prefill
.venv/bin/python -m examples.experiments.qwen35_27b_tp4.validate_costs \
  /home/liyachen/workspace/experiments/flowpilot/cost-qwen35-27b-tp4-20261002/cpu64 \
  --run-id 03408abf5c2746b0af703681dccb4f55
```
