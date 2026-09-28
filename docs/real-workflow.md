# 本机真实请求验收

核对日期：2026-09-23。该入口把真实 OpenHands SDK Agent、本机 FlowPilot 网关、
本机 Qwen3.5-9B vLLM 和 OpenHands 所属的本地 Tool 执行连在一起。
Terminal 使用真实 `curl` 请求本地 HTTP 页面；`web_search` 使用本地 HTTP
搜索端点。后者是可重复的本地搜索数据，不是外部搜索服务或生产流量。

## 启动

需要本机 Qwen3.5-9B 权重、具备 KV control 扩展的 vLLM 0.29.0 环境，
以及可导入 OpenHands SDK/TerminalTool 的环境。以下为当前机器上的命令：

```bash
cd /home/liyachen/workspace/flowpilot
HF_HUB_OFFLINE=1 PYTHONPATH=/home/liyachen/vllm \
  /home/liyachen/vllm/.venv/bin/python integration/real_vllm_server.py \
  --port 18801 --gpu-blocks 24
```

另开终端运行：

```bash
cd /home/liyachen/workspace/flowpilot
PYTHONPATH=$PWD NO_PROXY=127.0.0.1,localhost OPENHANDS_SUPPRESS_BANNER=1 \
  /home/liyachen/openhands/software-agent-sdk/.venv/bin/python \
  integration/real_workflow.py --vllm-url http://127.0.0.1:18801 \
  --pressure-jobs 8 --require-cpu-reuse
```

默认每次创建新的临时 work-dir，也可显式指定新的 `--work-dir`；脚本在其中创建 Tool Reuse SQLite 和
`trace.jsonl`，不会打开旧业务库。脚本验证五个 OpenHands 对话的真实 Tool
Observation、页面只实际抓取一次、搜索只实际请求一次、其余 Terminal 调用从
历史结果复用、网关请求均被 admission 接纳、KV retention 收据、最终 credit
归还，以及 trace 中没有 Tool 正文。可选的压力 Job 是真实 OpenHands 请求，
以不同提示词产生正常推理负载和自然缓存淘汰；不手动删除 KV。

Tool 时间采用显式开启、默认关闭的预测先验：名称含 `search` 的事实未命中
为 1–2 秒，其余为 100–200 毫秒。该值只更新 Tool ready-time 估计，
不向真实 Tool 执行添加等待。实际 Tool 时长由 OpenHands Tool 事件记录。
`--require-cpu-reuse` 检查本次运行前后 vLLM Prometheus CPU KV 读回字节
的**增量**，不把 `OFFLOAD:APPLIED` 收据或进程累计值当作物理读回证据。

## 并发 admission

OpenHands SDK 的同步会话在当前线程并发尝试中到达网关时呈串行，不能据此
宣称队列发生竞争。因此补充以下真实并发 GatewayCall；它们实际进入同一个
FlowPilot admission 队列并由本机 Qwen3.5-9B 推理：

```bash
cd /home/liyachen/workspace/flowpilot
PYTHONPATH=$PWD NO_PROXY=127.0.0.1,localhost \
  /home/liyachen/openhands/software-agent-sdk/.venv/bin/python \
  integration/real_admission_probe.py --vllm-url http://127.0.0.1:18801 \
  --trace-path "/tmp/flowpilot-real-admission-$(date +%s).jsonl"
```

脚本先占满一个 credit，然后按低、高紧迫度提交两个真实请求，并读取排队快照。
它要求队列顺序为高、低，实际放行顺序为占用者、高、低，且完成后
`inflight=0/free=1`。该补测使用原生 OpenAI 兼容 HTTP 客户端提交 GatewayCall，
不算作 OpenHands Tool 工作流样本。

## 本机结果与边界

2026-09-23 的干净 vLLM 进程、24 GPU blocks、8 个压力 Job：5 个真实
OpenHands 对话、18 次真实网关推理调用，18 次 admission 接纳；页面 HTTP
只执行 1 次，搜索 HTTP 只执行 1 次。KV retention 收据为
`OFFLOAD:APPLIED` 14 次、`DROP:APPLIED` 1 次。vLLM 指标在本次运行中
增加 1,655,046,144 CPU KV store bytes 和 828,112,896 CPU KV load bytes。
第二个历史复用会话期间的 load 增量为 0，因此这组数据证明工作流负载中
发生了真实 CPU KV 恢复，但不证明该特定 Tool 复用后继请求从 CPU 恢复。
验收 trace 位于 `/tmp/flowpilot-real-qwen35-clean-20260923/trace.jsonl`。

同日从另一个干净进程以固定的 4 个压力 Job 复跑：5 个 OpenHands 对话、
14 次真实推理与 admission，页面和搜索仍各只实际请求一次；
`OFFLOAD:APPLIED` 11 次。CPU KV store/load 在本次运行中分别增加
999,948,288 / 465,960,960 bytes，其中第二个历史复用会话期间 load
增加 189,923,328 bytes。该会话包含复用前和复用后的两次推理；当前采样
只能将读回归于这个会话，不能精确归于其中的 Tool 后续接请求。
trace 位于 `/tmp/flowpilot-real-qwen35-light-20260923/trace.jsonl`。

并发 admission 补测观察到等待队列为 `call-high, call-low`，实际放行顺序
为 `call-occupied, call-high, call-low`，最终 credit 全部归还。
最终复跑的 trace 位于 `/tmp/flowpilot-real-admission-final-20260923.jsonl`。

队列当前仍以 `tokenizer_cold` 工作量和 `COLD:no_target_proof` 为基准；
目标前缀证明、CPU restore 成本和公平资格尚未接入实际排序。这些本机短时
结果也不是生产 SLO、goodput 或长期缓存收益证据。
