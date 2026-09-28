# 论文稿的源码对应与表述依据

本文是 [论文式框架描述](framework-paper.zh-CN.md) 的撰写依据，不属于论文正文。
核对日期：2026-09-28。依据为当前工作区的 [design.md](../design.md) 及三组件源码，包含已有未提交修改；不能仅以仓库 HEAD 代表本文核对版本。

## 关键机制对应

| 正文内容 | 直接源码依据 | 表述边界 |
| --- | --- | --- |
| 动态线路前沿、版本与显式依赖 | [frontier/store.py](../flowpilot/frontier/store.py)、[protocol.py](../flowpilot/protocol.py) | 有界的是每条线路的尾部；不声称整个服务状态均为 O(active lines) |
| 精确历史、语义历史、精确在途、语义在途的解析顺序 | [controller.py](../flowpilot/reuse/controller.py) 的 `resolve` | 未启用 semantic 时跳过相关步骤；在途是登记中的调用意图 |
| 真实执行、发布与接收方身份 | [controller.py](../flowpilot/reuse/controller.py) 的 `record_execution` / `publish`；[OpenHands flowpilot.py](../../../openhands/software-agent-sdk/openhands-sdk/openhands/sdk/flowpilot.py) 的 `observation_committed` | 来源验证依赖真实执行事实；复用不能伪装为接收方新执行 |
| 独立工具容量与滑动有效期 | [store.py](../flowpilot/reuse/store.py) | 成功交付续期不代表外部内容重新获取，不作固定绝对年龄上限承诺 |
| 工具串行时延与后继需求时间 | [projection.py](../flowpilot/scheduling/projection.py) 的 `for_line`；[resolution.py](../flowpilot/scheduling/resolution.py) | 本地未开始工具累加时延；依赖未解除时需求时间未知；不直接用该投影权重排序请求 |
| 可选预测 | [forecast.py](../flowpilot/scheduling/forecast.py)、[duration.py](../flowpilot/scheduling/duration.py) | NoOp / replay 和合成实验时延不等于生产预测器或实测校准 |
| KV 三动作及成本比较 | [retention.py](../flowpilot/scheduling/retention.py) 的 `choose_retention` / `_cost_retention` | 先比较后继预算超支，再比较总成本；D2H 必须能放入等待窗口；未知成本使用现有显式规则 |
| 条件计算成本 | [cost.py](../flowpilot/scheduling/cost.py) 的 `estimate_work`、`OfflineCostModel` | GPU 与 CPU 两个条件方案取较小成本，实际恢复决策仍属于引擎 |
| 目标查询及全量 sweep | [prefix.py](../flowpilot/scheduling/prefix.py)；[runtime.py](../flowpilot/scheduling/runtime.py) | 查询实际排队请求，独立于 retention 开关；观察不等于驻留保证 |
| 默认队列主键、次键与额度 | [admission.py](../flowpilot/scheduling/admission.py) 的 `_order` / `_dispatch_ready` | 默认 `prefill_slack`；`weighted` 为显式对照；默认 fairness=0；额度为配置的网关并发上限 |
| 真实目标 prefix 与安全物理操作 | [vLLM manager.py](../../../vllm/vllm/v1/kv_control/manager.py) 的 `query_target` / `apply`；[ingress.py](../../../vllm/vllm/v1/kv_control/ingress.py) | 本地扩展能力，不归因于标准 OpenAI-compatible 接口；混合模型不能将 GPU/CPU token 相加 |
| DCS 消息批次、同步与 ACK | [context/manager.py](../flowpilot/context/manager.py)；[gateway/service.py](../flowpilot/gateway/service.py) 的 `_drive_gateway_reuse` | exact-only、显式 delegation；非流式首批全部可延迟命中才进入网关内部续接 |

## 公式核对

保留策略中的 `u` 对应 `_cost_retention` 候选项的第二个元素，`v` 对应第三个元素。OFFLOAD 的 D2H 在等待窗口内完成，因此不进入后继阶段成本 `u`，但计入总体代价 `v`。GPU 压力通过提高容量持有价格表达。响应时刻的输入长度取已知 descriptor 前缀加一个 token，仅作为 `ASSUMED_CONTINUATION`。

请求排序中的 `d_q - C_q` 对应 `_projection` 的 `latest_prefill_start`。其余次键依次为可选 Job 在途数、负 blocking lines、负 age 和 sequence。当前实现没有把 `ProjectionCalculator` 中的完整 DAG importance、line weight 或 critical-path 字段纳入默认排序。

正文中的 SLO goodput 和加权 JCT 是研究评价目标，不是原型求得全局最优的声明。目标成本只涉及条件 prefill 与恢复传输；未加入 decode、引擎内部等待或未来完整工作流耗时。

## 证据口径

当前 [设计契约](../design.md) 已记录目标查询与成本排序接入；[旧实现文档索引](README.md) 仍保留“未接入”的旧行，稿件依据直接源码及较新的设计契约表述。论文任务未修改这些既有文档或实现。

本次工作完成源码核对与论文稿撰写，并对新增文档执行链接、结构和空白检查。未重新执行代码回归、SDK 集成矩阵或真实 GPU 性能实验，未添加实验结果、提升比例或新颖性优先权声明。现有局部运行记录可由 [设计中的验证证据](../design.md#14-验证证据与后续实验) 和 [真实工作流记录](real-workflow.md) 追溯，不在正文中外推为调度收益。

“实现已具备”“局部验证已有记录”和“生产效果证据充分”分别成立或待证。本文只将源码支持的机制写成当前方法，生产成本标定、主动语义复用质量、长期 SLO 收益和完整多副本恢复仍需独立证据。
