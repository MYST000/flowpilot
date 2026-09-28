# Qwen3.5-9B TP=4 成本采样

使用本地 vLLM fork 的真实引擎和 CPUOffloadingSpec。`config.json` 保持原样，
`cost_serve.py` 只替换只读观测用的 scheduler/worker 子类。原生 async scheduler、
分块 prefill、自动 offload 和恢复选择仍由 vLLM 执行。
这里测的是 KV cache CPU offload，不是模型权重的 `--cpu-offload-gb`。

## 时间口径

| 数据 | 起止点 | 用途 |
| --- | --- | --- |
| engine prefill | 首次安排该请求的模型计算，到 scheduler 收到首 token | 含分块、TP 通信和原生调度开销；不是纯 CUDA kernel 时间 |
| engine transfer | scheduler 发出复制 job，到收齐所有必要 worker 的完成报告 | 包含引擎分发和完成轮询开销；多 job 使用最早开始到最晚完成 |
| worker CUDA | 原生复制 stream 上的 CUDA start/end event | 各卡复制时长，单列保留；相加不等于墙钟延迟 |
| HTTP | 客户端发送到完整响应 | 包含 HTTP、输入处理、引擎等待等；不用于 prefill 拟合 |
| OFFLOAD RPC | 发出策略命令到 APPLIED | 包含控制面、轮询及可能的 finish GRACE；不用于复制拟合 |

所有进程在同一主机，使用同一 monotonic 时钟。每次只生成一个 token，
排除多 token decode；仍包含首 token 的计算/采样及结果处理。CPU 恢复先发生，
随后才有普通 prefill 计算，因此分别记录二者。观测文件写入的开销包含在
测量中，不能宣称零开销的生产延迟。

bytes 来自 `SingleDirectionOffloadingHandler.get_finished()` 的实际复制结果，
逐 job 汇总各 worker，不从 token 数推导 hybrid 对象字节数。显式 OFFLOAD
的字节数还须与 `cpu_committed_bytes` 回执完全一致。

## 运行

在 FlowPilot 仓库根目录启动（输出目录必须尚不存在）：

```bash
/home/liyachen/vllm/.venv/bin/python -m examples.experiments.qwen35_9b_tp4.cost_serve \
  --output /absolute/path/to/new-run
```

模型就绪后，在另一个终端使用 FlowPilot 环境：

```bash
.venv/bin/python -m examples.experiments.qwen35_9b_tp4.measure_costs \
  --output /absolute/path/to/new-run --concurrency-probe
```

默认上下文为 1024、2048、4096、8192、16384、32768、65536、131071；最后一点
为最大 131072 长度留下一个输出 token。每点重复三次，先做两次 8K 预热。
每点测 cold、前缀 seed、GPU 命中及 CPU 恢复，实际命中量取 `PrefillStats`。
输入为确定性合成 token IDs，run 专属 cache salt 防止跨轮污染。只有元数据
和统计数落盘。四请求并发另测 4K、16K、32K，不混入单请求拟合。

`manifest-<run-id>.json` 记录引擎身份、时间和实验范围。采样完成后，在实例
仍运行时执行空闲 OFFLOAD 测量：

```bash
.venv/bin/python -m examples.experiments.qwen35_9b_tp4.measure_offload \
  /absolute/path/to/new-run --run-id RUN_ID
.venv/bin/python -m examples.experiments.qwen35_9b_tp4.analyze_costs \
  /absolute/path/to/new-run --run-id RUN_ID
```

这一阶段默认选用四请求并发样本中仍为当前 tail 的独立 line：它们仍有 GPU
副本，但 CPU 副本已被原生 LRU 淘汰。不要使用已被后继请求替换的旧 tail；
引擎会返回 STALE。`--phase cold` 仅适用于 cold 请求仍是当前 tail 的另行采样。
没有新复制条件的条目显式记为 `NO_NEW_COPY_CANDIDATE`，不会填成零成本。
复制期间没有其他推理请求。自动 offload 与 prefill 重叠，其原始 job 数据
保留，但不用于空闲 D2H 拟合。GPU eviction 后普通 inference 消费 CPU KV，
作为 H2D 样本；整个程序没有 RESTORE 命令。

## 产物和拟合

- `engine-*.jsonl`：原始 scheduler 和各 worker 数据。
- `requests-*.jsonl` / `controls-*.jsonl`：请求时间、usage、offload 回执。
- `request-costs.csv` / `summary.csv`：逐请求数据与重复样本中位数、范围。
- `transfer-jobs.csv`：逐 job 真实 bytes、四卡完成时间、CUDA max/sum 对照。
- `isolated-offload-costs.csv`：空闲 OFFLOAD 多 job 的总墙钟区间。
- `measurements.csv`：可供 `integration/fit_cost_model.py` 使用的数据。

拟合时使用 manifest 的真实 digest 和 measured_at，分桶上界设置为
`1024,2048,4096,8192,16384,32768,65536,131072`。首桶只采样了 1024 总长度，
更短输入属于未验证范围。三次重复不足以可靠估计尾延迟；uncertainty 是
样本残差，不是统计置信界。

模型只适用于本机、当前版本、四卡布局和本次配置。未采样的缓存命中比例、
并发干扰、CPU 内存/PCIe 竞争需要另行验证。identity digest 包含配置 hash，
加载前必须重新比对。不自动修改完整实验的 `cost_model_path`，避免把局部
成本标定等同于全流程 SLO/goodput 已验证。

2026-09-28 的首轮报告位于
[实验目录](/home/liyachen/workspace/experiments/flowpilot/cost-qwen35-tp4-20260928/REPORT.md)。
该轮确认了一个限制：128K 近全命中时，实际残余 prefill 约 0.197 秒，
由 cold/部分命中拟合的线性模型却预测约 1.508 秒。候选模型尚不适合直接
用于高命中率 agent follow-up；需要补充高命中率 GPU 样本并改善模型形式。
