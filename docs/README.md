# FlowPilot 当前实现

核对日期：2026-09-22。本文档集依据当前源码和测试编写；
FlowPilot 核对基线为 `fd4b3e48062991e711aaa803075d467cbcdf7eb9`。
本地 vLLM 扩展包含未提交修改，不代表上游发行版能力。

[design.md](../design.md) 定义目标与所有权；这里记录实现、配置和差距。
当前成本方案的代码迁移入口为 [成本调度重构方案](cost_based_refactor_plan.md)，按 2026-10-10 源码给出函数级改动、配置迁移与验收顺序。该计划对应的 W−K、整轮 FIFO 与 G+Q 首次 retention 已落地，配置见[请求调度](scheduling.md)，验证状态见[证据](verification.md)。
代码存在、默认启用、本地验证通过、生产证据充分是不同结论。
没有检查运行中实例的配置，不能据此认定功能已经部署。

## 阅读入口

| 文档 | 内容 |
| --- | --- |
| [运行与接口](runtime.md) | 启动、身份、认证、网关、状态与存储 |
| [Tool 复用与 DCS](tool-reuse.md) | exact/in-flight/semantic、可信执行、容量与上下文同步 |
| [请求调度](scheduling.md) | 实际排序公式、credit、forecast、KV 策略选择 |
| [vLLM KV 控制](vllm-kv-control.md) | descriptor、GRACE、KEEP/OFFLOAD/DROP、共享前缀与修复边界 |
| [验证与证据](verification.md) | 当前测试入口、已有实验、尚未验证的能力 |

## 设计提案

| 文档 | 内容与状态 |
| --- | --- |
| [基于成本的请求调度与 KV 保留](cost-based-scheduling-proposal.zh-CN.md) | 复用现有模块的新算法提案：Job 份额内成本排序、分层 KV 增量价值、论文依据及验证方案；尚未实现 |
| [基于边际延迟成本的三者联合调度](joint-cache-scheduling-marginal-cost.zh-CN.md) | 请求延后成本、KV 驻留价值与 Tool Cache 到达时间反馈的统一模型、契约变更及验证计划；尚未实现 |

## 默认行为

| 能力 | 默认状态 | 当前实现边界 |
| --- | --- | --- |
| Chat Completions / Responses 网关 | 启用 | 要求注册 job/line 和完整请求身份 |
| Tool telemetry、frontier、trace | 启用 | 真实 Tool 始终由 OpenHands 执行 |
| Exact / in-flight 复用 | 关闭 | 开启需 registry 和新版 OpenHands adapter |
| Semantic 复用 | 关闭 | registry 开启后模式默认 shadow |
| DCS | 关闭 | 依赖 exact、显式 delegation 和加密密钥 |
| Forecast | 关闭 | 默认 adapter 为 NoOp，无内置生产预测器 |
| 单实例 admission | 关闭 | W−K / 整轮 FIFO；默认并发 credit 上限 8 |
| KV retention | 关闭 | 需要本地 vLLM KV control v1 扩展 |
| 目标 prefix / restore 成本参与排序 | 未接入 | 队列使用 cold prompt 工作量 |
| 多 worker 服务 | 不支持 | shared-state 模块尚未接入完整事务 |

## 文档维护

原来的 phase 文档、修复计划、重复算法说明和旧 RESTORE 实验清单已删除；
历史内容可通过 Git 查看。历史测试数量不作为当前通过数量。
新文档按组件维护，并链接到实现和测试，避免再维护另一套阶段完成清单。
外部实验产物与 vLLM 上游文档未删除。
