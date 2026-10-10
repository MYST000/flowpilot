# Qwen3.5-27B TP=4：七特征 cadence 冻结成本

当前唯一随仓库分发的成本参数是 [cost-model.json](cost-model.json)，版本
`seven-feature-cadence-frozen-d2h-20261010`。[config.json](config.json) 和专用启动入口
默认加载它，admission 与 retention 共用同一个不可变模型，无在线拟合或滚动更新。
旧 27B 分桶参数已替换，9B 成本文件及默认引用已移除；历史实测日志仍保留在仓库外。

## 推理配置与来源

GPU 0–3、4×RTX 4090、Qwen3.5-27B BF16、TP4、PP/DP/DCP/PCP1，
max_model_len=262144、max_num_seqs=256、max_num_batched_tokens=2048，
gpu_memory_utilization=0.9；eager、prefix caching、chunked prefill、原生 async
scheduling 开启，Mamba align，实测 block=784。CPUOffloadingSpec 的整个实例
CPU KV 预算为 64 GiB，store_threshold=0、offload_prompt_only=false。
`max_num_seqs` 是引擎上限；FlowPilot admission credit 仍由自身配置决定，两者独立。

系数来自原生 vLLM 真实 BrowseComp/Hotpot/LiveCodeBench 并发回放：训练 137 条、
验证 41 条、按任务隔离，合计 9,749 个迭代。Ridge alpha=0.1，特征 RMS 缩放，
截距不惩罚；系数已经反变换为原始 token 单位，运行时直接点积，不能重复缩放。
[冻结拟合与数据身份](/home/liyachen/workspace/experiments/flowpilot/native-prefill-seven-feature-realcal-seq256-20261010/run-001/fit.json)
的 SHA256 为 `7073c07012e5bd90c9f4ab424191e2f1b4e8555055bb551fdc5da8bcd2434965`。

独立测试 235 条尝试中 234 条成功，1 条传输失败未进入引擎。基于首个实际 batch
的七特征 cadence 冻结预测，P90 APE=54.21%、WAPE=24.53%；
[完整误差报告](/home/liyachen/workspace/experiments/flowpilot/native-prefill-seven-feature-realcal-seq256-20261010/run-001/existing-results/report.html)
保留全部成功请求、启动尖峰和抢占样本。该数值不是 FlowPilot 派发前路径的已验证误差。

## 派发前条件估计

公式为 `theta · [1,N_pre,B_dec,Σq²,C,Σqh,N_pre*(C+Σh)]`，单位为秒与 tokens。
本地 vLLM 的 `/v1/kv/query-target` 和 descriptor `/v1/kv/query` 返回 `prefill_load`：
运行中 decode 请求数、已计算长度加当前 token 的总和 C、活跃 prefill 数，以及
实际 token budget、max_num_seqs、block、上下文上限、async/chunked/align 设置。
查询只观察 scheduler 状态，不提交目标、不调度 batch、不获取 KV 引用或执行恢复。
`num_computed_tokens` 包含已调度的在途 token；快照不是完成屏障或未来 batch。

FlowPilot 尚不知道目标未来的首个实际 batch，采用明确的
`candidate_prefill_frozen_decode` 场景：固定观察到的 B/C，候选独占剩余 prefill
预算 D=M−B；按 784-token 边界、部分 prefix 的下一边界和末尾整块边界拆 chunk。
每轮代入七特征后求和。其他活跃 prefill 数只作审计，不猜测其未来预算分配。
因此这不是此前使用首个真实 batch 的 native_aligned 预测器，也不是引擎内部等待 ETA。
未来 Tool 输出仍按原 retention 的“已有 prefix + 1 token”场景处理；当前负载被
条件性延用到后继请求，不承诺它在 G+Q 窗口后不变。

`prefill_estimates` 保存特征和、负载快照、场景和外推标志。标定 prompt 最大 96,965；
超出实测 prompt 或逐特征最大值标为 extrapolated，不伪装成实测，也不擅自截断。
262144 只是配置支持的上下文上限。完整 prefix 命中保留至少一个 token 的 logits
计算；不将近全命中强行置零。负预测不裁剪为零，保留 unknown。

引擎身份/epoch、budget、seq、block 或执行模式不匹配，以及负载不可用时，成本
明确 unknown；沿用 admission 整轮 FIFO 与 retention 的既有 unknown 分支。
负载随本轮 prefix 观察使用既有本地 TTL，原始 engine 单调时钟只作审计，不与
网关主机时钟直接比较。场景不包含额外异步首轮等待、HTTP TTFT 或未来 decode 时长。

## 传输成本与所有权

restore 复用四特征实验中独立拟合的 H2D 参数，和当前引擎 identity 完全一致；
两次实测的引擎启动参数仅端口不同。prefill 特征数变化不影响传输模型的复用。
[H2D 原始拟合](/home/liyachen/workspace/experiments/flowpilot/prefill-four-feature-seq256-20261010/fit.json)
SHA256 为 `25b54b4fdf9a0809144f58702d036aa9316ce5d259322565943be5b1ba3a4a0d`，
公式为 `T_H2D(s)=0.004506207082665055+1.6286942923456314e-11*actual_bytes`。
10 条单请求训练样本，bytes 范围 410845184–16646995968；零 bytes 仍为零复制成本。
4 条合成留出 P90 APE=12.39%，6 条真实 benchmark 单请求恢复 P90 APE=27.99%、
中位 APE=14.58%。未验证并发恢复竞争；最大训练绝对残差 0.105185 秒只作误差记录，
不是置信界，也不自动加入点估计。范围外是线性外推，不代表已测精度。

offload 使用同配置原生 vLLM 的独立 D2H 实机标定，
[冻结拟合](/home/liyachen/workspace/experiments/flowpilot/native-offload-seq256-20261010/fit.json)
SHA256 为 `2f14aa01f1c9df29110a4442c6da0ef5a7cbdbb7661a89a142ece1e7518d2836`。
公式为 `T_D2H(s)=0.004039796055271255+2.044685376438999e-11*new_bytes`，
零新增 bytes 为零成本，等效聚合带宽约 45.55 GiB/s。
14 条训练覆盖 7 个尺寸，范围 205324288–17058037760 bytes；用训练集留一尺寸
交叉验证选择相对平方误差线性拟合，并在测试采集前冻结参数。
7 条同尺寸留出中位/P90 APE 为 10.56%/23.39%，5 条未见尺寸为 6.67%/16.65%；
全部 12 条留出中位/P90 为 8.61%/19.70%，最大误差 31.09%（低估 159.15 ms）。
最大训练绝对残差 0.093198 秒仅作误差记录，不加到点估计，也不是置信上界。
[完整报告](/home/liyachen/workspace/experiments/flowpilot/native-offload-seq256-20261010/report.html)
保留原始逐项误差和计时诊断：大尺寸整体耗时的波动无法仅用 CUDA 复制带宽解释。
这是含观测开销的空闲原生传输微基准，使用合成 token ID、每次生成 1 token；
未验证并发推理竞争、部分副本对象组合或跨会话精度。范围外使用属于外推。

admission 比较 GPU 重算与 H2D+残余 prefill 的条件成本；retention 用引擎只读提供的
`offload_new_object_bytes` 计算新增 D2H，用完整 `offload_object_bytes` 计算 H2D
与 CPU 驻留成本。完整 CPU 副本免 D2H；部分副本按实际缺失对象字节计算，
不按 token 比例推算。目标含未完成 CPU 写入、或旧引擎缺少新增字节字段时，
需要复制的候选保留 `offload_new_bytes_unknown`，沿用既有未知成本处理。
成本估计不会阻止 CPU-only 请求普通提交；vLLM 仍自主决定恢复/重算。未增加
RESTORE 控制、恢复队列、GPU-ready 屏障或 token→byte 换算。
GPU/CPU 驻留价格 1/0.01 仍是原策略系数，不是测量成本参数。

## 使用与校验

原生引擎入口需要包含 `prefill_cost_context` 与 `offload_new_object_bytes`
只读查询扩展的本地 vLLM 源码；更新后需在下一次启动时加载。
配置校验不会启动服务、创建数据库或发出推理请求：

```bash
.venv/bin/python -m examples.experiments.qwen35_27b_tp4.launch gateway --check \
  --run-dir /absolute/path/to/new-run \
  --registry /absolute/path/to/benchmark-registry.json
```

registry 采用现有 benchmark 导出格式；凭据仍使用
`FLOWPILOT_INGRESS_API_KEY` 和 `FLOWPILOT_DCS_ENCRYPTION_KEY`。
输出应包含版本 `seven-feature-cadence-frozen-d2h-20261010`，以及 `H2D=calibrated D2H=calibrated`。
显式 `--cost-model` 优先于 `FLOWPILOT_COST_MODEL_PATH`，未设置时读取本目录参数。
通用网关入口可设置：

```bash
export FLOWPILOT_COST_MODEL_PATH="$PWD/examples/experiments/qwen35_27b_tp4/cost-model.json"
```

上述 D2H 实机标定已完成；参数接入及查询扩展使用 CPU 回归验证，没有再次启动推理。
七特征 prefill 与既有 H2D 系数保留。历史分桶、D2H/H2D 测量脚本保留用于复算外部
证据，不再提供其旧参数作为当前运行配置。
[2026-10-02 历史报告](/home/liyachen/workspace/experiments/flowpilot/cost-qwen35-27b-tp4-20261002/REPORT.md)
不代表当前 seq256 配置的传输精度或工作流收益。

## 成本策略迁移（2026-10-10）

当前配置为 `admission.policy=wait_cost`，等待从实际完整请求入队开始计算。
任一候选成本未知则整轮 FIFO；显式 `fifo` 用于对照。
`wait_feedback.window_seconds=30` 是待标定实验窗口，retention 默认
`window_basis=tool_and_queue`，首次选择用 G+Q 并冻结；`tool_only` 用于不含 Q 的消融。
既有 shared cost model、TP/容量和 RPC timeout 保留，原测量不证明新策略的 workflow 收益。
