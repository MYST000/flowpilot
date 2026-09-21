# 调度实现与验证

本轮实现接入三个实际路径：网关 request admission、Tool 结果发布/维护、vLLM KV control v1。以 `design.md` 为契约。旧 KV 框架仍已移除，旧路由返回 404；没有外部 RESTORE、恢复队列或 GPU-ready 门槛。

## 请求排队

只有一条外部队列，向一个固定实例发送。普通请求和 DCS 内部 continuation 共用队列。默认分数：

```text
score = 0.55*SLO + 0.35*Age + 0.05*Progress + 0.03*Release
        - 0.02*Cost - 0*JobConcurrency
```

分数降序，同分 FIFO。没有 RiskRank、风险分档或独立 cache tier 优先级；Job 公平性默认关闭。

| 分量 | 计算口径 |
|---|---|
| SLO | `S=max(deadline-workflow_start, 0.001s)`，`R=deadline-now`；`R>=0` 时为 `S/(S+R)`，否则 `1+min(1,-R/S)`；无 deadline 为 0 |
| Age | 自请求到达后等待秒数 / 5，持续增长，不截断 |
| Progress | `min(1,CP/S)`，CP 为请求到达前工作流已用时间，到达时冻结；无 deadline 时用 60 秒参考值 |
| Release | 真实阻塞 line 数 / (1 + 阻塞 line 数)，依赖更新时刷新 |
| Cost | cold prefill tokens / (4096 + tokens)；`/tokenize` 不可用或请求类型不支持计数时标记 unknown，没有成本加减分 |
| JobConcurrency | 同 Job 的当前在途数 / (1 + 在途数)，仅在显式设置非零公平权重时生效 |

`CP` 与 `Age` 独立，避免重复累计等待。原子选择请求并消费 credit 后，在锁外发送 HTTP。完成、发送失败、协议错误、流式取消及排队期间 HTTP 断连均清理额度；重复归还不增加 credit。流式响应保持原始 chunk 顺序。

健康来自 vLLM `/health` 的周期观测，TTL 过期停止新派发，已发送请求继续运行。最大在途数由部署配置提供，标为 `configured_gateway_limit`，不能当成引擎上报的 batch 容量。成本估计只涉及 prefill；不预测 decode 或引擎内部排队。

当前引擎没有目标 continuation proof。旧 descriptor 的 GPU/CPU 命中量不能证明新请求命中，因此当前请求排序始终使用明确的 cold 基线，不额外按 GPU_HOT/CPU_OFFLOADED 分级。CPU-only 请求正常提交，由 vLLM 自主恢复或重算。

## Tool cache

发布经过真实 Tool 执行回执与 freshness 校验后立即进行容量维护，后台也定期维护。过期结果先清理；无保护结果按下面的价值密度从低到高淘汰：

```text
value = measured_tool_latency_ms * (1 + delivered_hit_count)
        * remaining_freshness_fraction / max(1, payload_bytes)
```

这是基于事实的启发式，未声称具备校准的未来命中概率。未知耗时没有节省执行时间加分。LRU 只处理同值条目，新结果也参与竞争。不可进入历史缓存的结果在没有交付义务时价值为 0。

交付期间通过临时引用保护 payload；有 follower 的结果在绑定的既有重试期限内受保护，包括非历史缓存结果。没有交付 ACK，因此重复 poll 仍能取得结果；取消或重试期限结束后可回收。保护对象超过配置预算时报告 `over_capacity_bytes`，不删除等待交付的数据。过期仍禁止复用。

DCS WAL 持有独立完整结果，Tool cache 淘汰不会删除未 ACK delta。Tool cache 字节与 GPU/CPU KV 字节不交换配额。

## KV 保留

普通推理请求在协商成功后携带 `kv_transfer_params.kv_control_binding`。保留原有其他 transfer 参数，不修改 messages。响应完整结束后异步 `/resolve`，不等待 KV 控制请求才把推理结果交给 Agent。

| 当前事实 | 决策 |
|---|---|
| 显式 line finish，或 COMPLETE 查询确认没有可恢复 prefix | 支持 safe direct drop 时 DROP |
| 后继 READY 或 `T_need` 在 keep horizon 内，且 GPU 空闲 allocation 高于阈值 | 支持 GPU retention preference 时 KEEP |
| 等待较长/未知，或空闲 allocation 达到压力阈值 | CPU store、CPU reuse、offload preference 均支持时 OFFLOAD |
| OFFLOAD 不支持，但 GPU preference 支持 | KEEP，记录 CPU retention 不支持的原因 |
| 所需动作不支持 | 明确记录 unsupported，普通推理继续 |

默认 keep horizon 为 1 秒，GPU 空闲 allocation 阈值为 128，两者可配置。该阈值是引擎真实 allocation 数，不是 token 数，也不是推算的 GPU 百分比。CPU 配额、复制与共享引用安全由 vLLM 判定；未提供的 CPU 压力和恢复耗时保持未知。

每次策略刷新都重新 `/query`，不复用过期 prefix 观察。发送前检查源 GatewayCall、tail version、owner、engine epoch；新 tail 使旧决策失效。line 从 frontier 清理后保留短期完成记录，以完成安全 DROP。传输超时重试相同 idempotency key，`ACCEPTED` 通过 `/status` 继续查询；APPLIED、PARTIAL、FAILED 等回执分别记录，不把 HTTP 200 当作复制完成。

KEEP 是软偏好。OFFLOAD 先由引擎提交有效 CPU 副本，不代表立即回收 GPU。DROP 仍服从引擎安全引用；FlowPilot 不拥有物理 KV 对象。

## 启用与观测

默认关闭 admission 和 KV retention，保持 M0 透传行为。示例见 README 的 `FLOWPILOT_ADMISSION_JSON`、`FLOWPILOT_RETENTION_JSON`；引擎控制面有认证时设置 `FLOWPILOT_UPSTREAM_CONTROL_API_KEY`。这两个功能要求单实例、单 worker。

`GET /flowpilot/v1/scheduling/state` 返回排队分数、各项贡献、credit、成本依据和 KV 回执状态，需要网关控制面认证。trace 记录 `request_admitted` 与 `kv_policy_receipt`，不记录 prompt 或 Tool 正文。KV 扩展未启用/不可用时状态明确为 unsupported/unavailable，不重新启用旧协议。

## 验证边界

已验证内容包括连续评分、默认公平权重 0、等待老化、CP 冻结、heartbeat 过期、原子 credit、流式完成/取消、HTTP 断连、失败回收、Tool 价值淘汰与 follower 保护、KV 三种动作、ACCEPTED 后续查询、PARTIAL/FAILED、幂等重试、旧 tail 失效和 CPU-only 普通提交。

协议兼容性测试直接加载本地 vLLM `kv_control/protocol.py` 的 Pydantic 模型，检查绑定、查询、策略、状态和遥测请求。只加载独立协议模型，不初始化 vLLM 或 GPU。OpenHands 联动使用真实 SDK 和临时本地 FlowPilot HTTP 服务，上游推理为 MockTransport，包含开启/关闭 admission、历史复用、在途绑定和 DCS。

本轮按用户约束不启动 GPU 推理。真实 OFFLOAD 完成范围、CPU KV 被普通推理实际使用、GPU 压力下的回收效果、传输耗时和 SLO goodput 仍待 GPU 可用后验证；不能把协议与模拟测试视为生产性能或物理 KV 复用证据。


验证结果（2026-09-20）：

- FlowPilot：175 项通过，其中新增 31 项排队、缓存与 KV 控制测试。
- OpenHands FlowPilot 适配器：36 项通过。
- 真实 SDK → 本地网关 → 模拟推理服务：36 项通过、12 项既有条件跳过；admission 开启/关闭均覆盖。
- Ruff、生产代码与新增测试的定向 Pyright、compile/import、`git diff --check` 通过。
- 原有 vLLM 框架文档及历史 SQLite 样本未改动；没有修改 vLLM 或 OpenHands 代码，没有执行 GPU 测试。
