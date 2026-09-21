# FlowPilot 的 vLLM KV 管理框架

状态：设计提案，尚未实现。2026-09-17；采用请求结束后的短时 GRACE 保护与后续软保留策略，实施基线为本地 vLLM 0.29.0。

**本阶段范围：只设计和实现 vLLM 侧能力。FlowPilot 仓库中的 KV 相关代码均属于旧框架，一律不复用、不迁移，也不作为本计划的协议或验收依据。FlowPilot 侧将在下一阶段另行实现。** 本文提到的网关决策、准入与成本消费仅说明未来调用契约；本阶段用独立测试客户端验证 vLLM 接口，不依赖旧 FlowPilot adapter、directory、queue 或测试 fixture。

FlowPilot 的 KV 工作限定为两件事：决定当前 KV 的去留（KEEP/OFFLOAD/DROP），以及查看目标 request 的 prefix 情况。CPU restore 成本可以估算并纳入请求排序，但 restore 的触发、排队、资源分配、执行和恢复/重算选择全部由 vLLM 自主管理。FlowPilot 不发送 RESTORE 命令，不维护恢复队列，也不等待 GPU KV ready 才提交普通推理请求。

三类动作表达保留与回收意图：KEEP 让块正常参与 prefix cache，并相对已备份的 GPU 块倾向于保留；OFFLOAD 为选定恢复范围建立 CPU 副本，完成后让对应 GPU 副本更早成为驱逐候选；DROP 在不破坏运行请求、计算和传输的前提下尽早回收。KEEP/OFFLOAD 均不承诺最低存活时间，不增加保留租约或不可回收引用。实际淘汰时机、共享对象的偏好合并和资源分配始终由 vLLM 决定。

请求正常结束时另设一次短时 GRACE：引擎对结束时仍存在、已完成计算且纳入保护范围的 GPU KV 建立真实保护引用，等待放置策略接管。在策略完成交接或引擎侧 TTL 到期前，这些副本不能因正常缓存压力而被回收。GRACE 是策略交接窗口，不把后续 KEEP 改成硬保留，也不承诺补回推理期间已丢失的状态。具体保护范围、剩余时间和交接结果由引擎报告。

以 [design.md](../design.md) 的单实例边界、决策点 A/B 和 M5/M6 能力门槛为准。本提案细化引擎侧实现，不改变 OpenHands 的 agent loop，不迁移请求或 KV 到另一个实例，不改变推理消息。候选 descriptor 由引擎生成；相比 design.md §7.11 中网关计算 hash 的可选路径，新增的是引擎端登记和验证路径。

descriptor 的唯一职责是为某次已完成推理的前缀提供轻量 KV 查询入口：在产生 response 时建立，后续凭 ID 查询。它不是保留租约，也不是下一轮请求的 prepare ticket。查询不要求先上传完整 prompt 或执行 prepare。

部署不限定 TP=4。框架面向一个逻辑 KV 管理域，具体单卡、TP/PP 等布局由 backend capability 和 shard manifest 描述；是否支持某种布局需要对应实现与验证。实验的四卡配置仅是一个测试点。独立副本的 KV 域通过 replica/domain ID 区分，不因模型名相同就共享一个 descriptor 的位置事实。

## 1. 结论与证据边界

目标流程是“请求结束时登记 descriptor，建立短时 GRACE 并释放原 request 引用 → FlowPilot 表达 KEEP/OFFLOAD/DROP 意图 → vLLM 原子完成引用/策略交接 → 后继请求查询动态 prefix 并正常准入 → vLLM 自行验证、恢复或重算”。保护范围内的 GPU 副本在 GRACE 有效期间不能正常淘汰；未保护范围及 GRACE 到期后的状态仍按实际驻留处理。动作只报告真实可处理范围，不恢复已丢失对象来制造成功。

三个重要区别：

- 内容身份：这份状态对应哪一个 token 前缀及计算配置。
- 恢复能力：在哪些位置具备所有缓存组所需的模型状态。
- 驻留与使用：这些状态当前在哪些层、是否完整、是否仍被请求、计算或传输使用。

历史 Qwen3.5-9B 四卡实验使用 vLLM 0.18.0：关闭 thinking 的 5 个样本中，旧输入加输出完整匹配下一轮前缀；一次实际命中包含 20 个生成 token。开启 thinking 的块边界样本中，3168-token 输入只匹配 3167，按内容可匹配 2640，实际只命中 1584。这些结果提示 descriptor 需要覆盖实际已计算的 prefill/decode，并验证合法恢复点；不作为 0.29.0 的命中长度、策略效果或 CPU 复用证据。

实施基线固定为 `/home/liyachen/vllm`，标签 `v0.29.0`，commit `98dff2a81d747d1dba01a47f939f48c3526d4206`。该版本的 `OffloadingConnector` 已实现 `SupportsHMA`，具有多缓存组、Mamba 状态、异步复制及普通请求 CPU 加载路径；0.18.0 的单组断言不作为本计划的限制。第一条实现路径采用原生 `OffloadingConnector + CPUOffloadingSpec`，在其对象、分配与完成机制上扩展查询和策略。现有源码能力仍须在目标 Qwen3.5 模型、dtype、缓存模式和并行布局上验证。

配置需要显式记录 `offload_prompt_only`：0.29.0 原生默认值为 `true`。descriptor 可以描述已计算的 decode 范围，但不能因此宣称这些状态已经存入 CPU；若实验目标包含 decode 复用，需要启用相应存储配置并独立验证。查询、软保留偏好和定向 DROP 都是待实现的 vLLM 控制面扩展，原生 connector 的存在不等于这些能力已经具备。

### 1.1 对抗性代码审查结论（2026-09-17）

下列是原生 vLLM 代码与目标契约之间的缺口，不表示原生 vLLM 承诺过这些扩展能力。行号以以上固定 commit 为准；旧 FlowPilot KV 代码不纳入本阶段的可行性审查结论。

| 级别 | 已确认的问题与可观察失败 | 源码证据 | 本计划的补全与回归要求 |
|---|---|---|---|
| P1 | GRACE 增加的非请求引用会使不同请求被误判为共享 prefix；满足 cascade attention 选择条件时可能影响推理正确性 | `v1/core/single_type_kv_cache_manager.py:831–839`；`v1/worker/gpu_model_runner.py:2695–2789` | §4.3 将物理保护引用与请求共享关系分开，启用 GRACE 前修正 common-prefix 计算；覆盖不同/部分共享前缀、多 GRACE 及真实 cascade 输出 |
| P1 | 正常结束的原生 store 用输入加输出长度确定范围；decode offload 在块边界可能提交含未计算末 token 的块 | `offloading/scheduler.py:1337–1346`；`v1/request.py:264–290` | §5.2 在原生自动 store 与显式 OFFLOAD 中统一计算有效范围；步骤 1 先验证结束边界，再以 CPU-only 普通请求对照重算输出 |
| P1 | 延迟 OFFLOAD 不属于原生 store 构造的遍历范围；旧请求清理后完成回调也无法使用原状态 | `offloading/scheduler.py:1322–1337,1563–1570,1641–1662` | §5.1 共享 store 构造与原生任务账本，新增独立操作上下文；请求完全清理后仍能复制并收尾 |
| P1 | 原生复制失败直接触发断言，不能兑现计划中的可恢复失败/PARTIAL | `offloading/worker.py:262–292`；`common.py:76–105`；`scheduler.py:1641–1645` | §5.1 扩展现有完成 metadata、聚合及失败清理；单分片失败不能发布完整 CPU READY |
| P1 | GRACE 仅增加 ref_cnt 不能阻止定向 hash 撤销；只登记 deadline 也不能唤醒空闲引擎 | `v1/core/block_pool.py:749–765`；`v1/engine/core.py:1437–1457` | §4.1/§4.2 同时覆盖引用、索引/COW、空闲超时与 reset；压力和空闲场景分别验证 |
| P2 | hybrid partial-tail 的 `max_offload_tokens=0` 被当作未设上限；合法正上限低于边界时触发断言 | `offloading/scheduler.py:1244–1250` | §5.2 区分 None/0，正常过滤超限候选；覆盖自动和显式存储，不以断言处理合法配置 |
| P2 | `prepare_store()` 返回空集合不等于目标已 READY：可能已在复制或被阈值过滤 | `v1/kv_offload/cpu/manager.py:169–187` | §5.1 逐对象区分 READY/PENDING/MISSING，不以“无新增任务”判成功 |
| P2 | 用 CPU-backed 单块优先级可能切断原生可恢复前缀；一次 DROP 的 skipped 结果也不保证最终清理 | `block_pool.py:647–675,723–747`；`offloading/scheduler.py:_lookup_complete_chunks` | §5 按可消费恢复范围评估备份资格，事件驱动重查暂缓 DROP |
| P2 | 直接调用调度 lookup 有副作用；混合模型的本地候选命中不一定能独立执行 | `kv_cache_manager.py:228–340`；`offloading/scheduler.py:936–990`；推理 `scheduler.py:837–900` | §6.4 抽取共享只读匹配内核，覆盖跨组回退及 pending 状态 |
| P2 | 普通 JSON 的 KV metadata 回传不能证明 SSE 已收到 descriptor | `chat_completion/serving.py:1175`；`chat_completion/protocol.py:165` | §9.2 用既有 IPC 和绑定到实际 engine request 的查询侧通道，分别验证流式/非流式 |

结论是**可以沿原生 CPU offload 数据路径实施，但必须先修正原生 store 的完成范围与参数过滤，并在接入 GRACE 时修正共享前缀判断**。不能只在外部登记 descriptor 后直接调 worker copy，也不能只更换 CPU eviction 插件就实现 GPU 偏好。改动涉及原生 scheduler 的候选有效性、任务入口/生命周期、只读查询、请求共享关系、GPU 引用和候选顺序、控制 metadata；GPU/CPU allocator、传输执行器及普通请求的恢复路径继续复用。这些是 vLLM 内部工作，不需要修改 OpenHands。

## 2. 所有权与组件

```text
OpenHands
    | real request / response / tool result
    v
FlowPilot Gateway + admission queue + prefix/cost projection [next phase]
    | descriptor ID queries / versioned actions / ordinary requests
    v
vLLM API adapter                         [transport only]
    | EngineCore command/event channel
    v
EngineCore KV policy/query extension     [inside authoritative engine state]
    +-- Descriptor registry              [identity and state manifests]
    +-- Finish grace holds              [short TTL, protected GPU references]
    +-- Policy records                   [soft preference, owner, version]
    +-- Native GPU BlockPool             [lookup, references, eviction]
    +-- Native CPUOffloadingSpec/manager  [index, allocation, CPU eviction]
    +-- Native OffloadingConnector       [copy jobs and completion fences]
            |
            +-- required shard/worker 0
            +-- ...
            +-- required shard/worker N-1
```

FlowPilot 表达当前 KV 去留意图，查询目标 prefix 并将有依据的 CPU 恢复成本估计用于外部请求准入和排序。EngineCore 决定对象是否真实存在、是否可回收、何时淘汰，以及普通推理请求何时、从何处恢复或是否重算；恢复队列、目标分配和优先级属于引擎内部。Worker 负责数据复制及完成事件。查询与成本估计没有恢复副作用，也不下发引擎恢复 deadline。

不另建一套 CPU allocator 或传输完成账本。新增策略必须接入原生自动 store、CPU eviction、GPU block reuse、reset 和传输完成路径：已有 CPU 副本直接复用，原生在途复制与显式 OFFLOAD 合并，实际完成事实只有一个来源。原生自动存储可能早于 FlowPilot 动作发生；回执须区分本动作新增复制与先前已存在的副本，实验不能把后者全部计为 FlowPilot OFFLOAD 的收益。

不单独建立一个能修改 block map 的 hash 守护进程。独立进程最多是可重建的 metadata mirror，不能成为删除与引用获取的权威。查询后对象可以正常变化；正式请求仍由引擎重新 lookup 并取得引用。

所有 block lookup/acquire/evict、GRACE 建立/交接/到期、策略更新、action 提交和 transfer 完成在同一个 EngineCore 状态机串行提交。DMA 异步执行，不在 metadata 临界区等待。沿用原生 pending-work 机制处理空闲时的传输完成；另将最近 GRACE deadline 接入引擎空闲等待的唤醒/超时机制，控制命令与 metadata 回收也必须在无推理请求时得到处理。GRACE 到期不是 GPU 计算任务，不应仅为等待 TTL 让引擎持续执行空 batch。

### 2.1 原生接口复用清单

下表中的 `offloading/*` 均指 `vllm/distributed/kv_transfer/kv_connector/v1/offloading/`。以下方法大多是固定版本的内部接口，复用不代表上游承诺稳定的公共 API；升级需重跑契约测试。

| 职责 | 复用的本地接口 | 必须增加的薄层 |
|---|---|---|
| 控制命令进引擎 | `AsyncMPClient.call_utility_async`、`EngineCoreProc._handle_client_request` 的 UTILITY 分发 | 类型化 query/apply/status/resolve 方法，在 engine 线程串行执行；不新建能改缓存的服务进程 |
| 正常完成、metadata 回传 | 推理 scheduler `_free_request/_connector_finished`、`request_finished_all_groups`、`kv_transfer_params` | free 前登记 descriptor/GRACE，扩展输出 metadata；不能假定原生 connector 已使用传入的全部 block IDs |
| GPU 保护和释放 | `BlockPool.touch/free_blocks`、原生 compute 延迟 free、COW 与 transfer fence | 独立去重 GRACE 引用、deadline、generation；保留原始 request 释放顺序；非请求引用不得计入共享请求数 |
| 推理共享前缀 | `FullAttentionManager.get_num_common_prefix_blocks` 及 scheduler/model runner 消费路径 | 按真实请求 block-table 共享关系计算，替换总 `ref_cnt` 等于请求数的判定；验证 cascade attention |
| GPU 查询 | `KVCacheManager`/coordinator 的 GPU prefix 匹配算法 | 从入口中抽出无事件和无状态修改的匹配内核，返回跨组候选与最终可消费范围 |
| CPU 查询 | CPU manager `lookup`；connector `_lookup_complete_chunks/_lookup` 的匹配规则 | 只读视图、合法 checkpoint/chunk 约束；不调用 `prepare_load` |
| CPU 存储与回收 | CPU manager `prepare_store/complete_store`、原生 `CachePolicy` 和 block allocator | 先修正自动 store 的完成范围及 partial-tail 上限过滤，再复用到请求结束后的任务入口；定向 `evict_if_idle`（拟新增）；不在 FlowPilot 直接访问 `_policy` |
| 复制执行 | `GPULoadStoreSpec`、`TransferJob`、`OffloadingConnectorMetadata`、worker `prepare_store_kv/start_kv_transfers/get_finished` | 与原生任务合并及完成状态扩展，不新增另一套 CUDA copy/RPC |
| 源内存安全 | `_block_id_to_pending_jobs`、`jobs_to_flush`、原生写入/COW 同步 | 延迟任务在交接 GRACE 前立即登记 fence，不能等待已不存在的 request finish |
| GPU 软偏好 | `BlockPool.get_new_blocks/free_blocks` 和同一个 `FreeKVCacheBlockQueue` | 在原生候选维护点更新顺序，保持引用和队列唯一性；CPU `eviction_policy` 不控制 GPU |
| 空闲期任务推进 | connector `has_pending_push_work`、EngineCore 输入队列 | 实际待提交 store 纳入 pending；GRACE 只用 deadline 唤醒，不伪造推理任务 |

`ResidencyRecord` 是原生索引的带版本视图，不能成为另一套独立维护的可用性真值。CPU READY 来自原生 `complete_store`，GPU 状态来自原生 hash/COW/分配路径；状态变化在这些提交点同步更新，metadata mirror 不能凭未追平的事件批准删除或复制。

## 3. 五类记录

### 3.1 PrefixDescriptor：结束时建立的查询索引

以下为引擎内部记录，不要求每次返回完整内容或传送整条 hash 链。response 只需携带 `descriptor_id`、路由/epoch、所描述的前缀长度和可选访问凭据，以及本次 GRACE 的 ID、保护范围摘要和剩余时间观察；scheduler 留存这些少量元数据。GRACE 的动态状态独立于 descriptor 内容身份。

```text
schema_version
descriptor_id
instance_id, kv_domain_id, engine_epoch
scope_ref                         # 既有租户/部署/缓存命名空间
origin: job_id, line_id, request_id, llm_call_id, context_epoch
identity_digest                   # model revision, tokenizer/template,
                                  # adapters, hash/salt/extra keys, KV layout,
                                  # dtype, shard layout 等兼容身份
hash_chain_ref                    # 引擎已有 hash 链的引用
input_token_count
prefix_token_count                # C：该 descriptor 描述的 token 范围
computed_token_limit              # 实际已完成计算的范围，不用输出长度推定
candidate_token_limit             # 策略候选范围，可为 prefill-only 或含 decode
state_manifest_ref                # 各恢复点需要哪些缓存组对象
```

descriptor 不包含一个永久有效的 `tier=GPU/CPU`，也不把曾经存在的物理 block ID 当作长期身份。GPU→CPU 后 descriptor ID 不变；位置事实更新。不同 descriptor 可以引用同一份物理状态。

descriptor 元数据生命周期独立于物理 KV 驻留。自然淘汰、DROP 或 CPU eviction 后，只要登记记录仍有效，同一个 ID 查询应返回当前缩短/归零的可复用范围；不能因为块被清理就把查询身份当作另一份内容。兼容的相同内容后来重新进入缓存时，查询范围也可以增长。元数据自身被回收时显式返回 DESCRIPTOR_EXPIRED。访问凭据证明调用者有权查询，不自动证明未来请求的内容相同。

hash 链应覆盖本轮整个输入范围，包括已命中旧缓存的输入，以及已完成计算的生成块。不机械丢弃 prefill/decode 混合块。元数据可以引用父 descriptor 和追加链段，避免每轮复制完整历史；保留的链段必须独立于短命 Request 对象存活。

hash 身份不单独证明 KV 已计算或线性状态检查点存在。`state_manifest_ref` 需要标明 `ResumePoint(token_end, required_objects_by_group)`。对 full attention 可能需要前缀块序列；对线性注意力需要对应位置的完整状态；对其他模型还可能有窗口要求。manifest 引用缺失时，缩短可恢复范围，不伪造检查点。

### 3.2 ResidencyRecord：可变的位置事实

每个逻辑对象的 key 包含内容 hash、缓存组、有效范围/检查点身份和兼容布局。记录其 GPU 与 CPU 副本集合，允许跨层和同层多个副本同时存在。0.29.0 的 GPU hash map 允许同一内容对应多个物理 block，不能用单个 allocation 字段覆盖它们：

```text
logical_object_key
replicas[]:
    tier: GPU | CPU
    rank, group_id, allocation_id, allocation_generation
    state: COPYING | READY           # 无副本时集合为空
state_version, event_seq
real_bytes_by_rank_and_group
```

`COPYING` 不能计为可恢复命中。物理 block ID 再利用必须递增 generation，防止旧 DROP 或传输回调误操作新内容。CPU slab/extent 也适用。

generation 是待新增的分配序号，不是 0.29.0 的裸 block ID 已有属性。GPU 在 `get_new_blocks` 的实际重新分配点递增，CPU 在 `_allocate_blocks` 对 slot 的重新分配点递增；索引添加/撤销、partial hash alias 和 COW 的 `move_block_hashes` 另更新逻辑对象到 allocation 的映射版本。一个物理块的多个 hash alias 共用一次物理引用，不能重复保护或重复计算字节。operation 捕获 generation；提交和回调都检查 epoch/generation，失败清理只作用于该任务实际拥有的 allocation。

源与目标的检查时点不同：源在提交前必须匹配；原生 `jobs_to_flush` 已安全完成源读取后，GPU slot 可以被新内容重用，而其完成 metadata 尚未处理。此时不能仅因源 generation 已变化而否定有效 CPU 写入，也不能修改新 GPU allocation；按原生 job 完成事实及目标 generation 提交 CPU，清理本 job 的 fence/观察者。epoch/reset 的旧任务过滤继续沿用原生 `_stale_job_threshold`，补充的 allocation 版本不能替代它。

### 3.3 PolicyRecord：软保留意图

```text
owner_scope, descriptor_id
source_llm_call_id, expected_tail_request_id, expected_tail_version
policy_version, action: KEEP | OFFLOAD | DROP
target_manifest_ref, target_resume_token_end?
decision_ref
```

PolicyRecord 不拥有 KV 引用，不使对象退出可分配 free queue，也不保证 GPU/CPU 最低存活时间。新策略版本替换同 owner 的旧意图；已登记的后继请求、取消或终止事件使相应旧意图失效。共享对象上的意图由引擎按物理副本合并，不能复制进 `LineTail`。

短时保护引用由独立的 FinishGraceHold 持有。登记策略不等于已经完成交接，也不允许直接抹掉原 request、其他 owner 的 GRACE 或 DMA 所拥有的引用。

请求、计算、复制仍使用引擎的真实引用与 fence。传输中不能释放 DMA 访问的内存；这是操作安全约束，不是 FlowPilot 的保留租约。策略失效不允许提前解除这些安全引用，普通请求的引用获取与恢复始终归引擎。

### 3.4 ActionReceipt：操作事实

```text
action_id, idempotency_key, action: KEEP | OFFLOAD | DROP
descriptor_id, expected_engine_epoch, expected_policy_version
owner_scope, decision_ref, applied_policy_version
status: ACCEPTED | APPLIED | PARTIAL | FAILED | EXPIRED | STALE | UNSUPPORTED
operation_id, state_version, event_seq
grace_id?, grace_state?, grace_remaining_manifest_ref?
target_resume_token_end?, requested_manifest_ref
accepted_manifest_ref?, completed_manifest_ref?
gpu_ready_tokens, recoverable_tokens, cpu_standalone_tokens?
cpu_committed_bytes, cpu_reused_bytes, gpu_reclaimed_bytes
skipped_reasons, error
```

`ACCEPTED` 只说明任务已接收。KEEP 的 `APPLIED` 表示偏好已登记；OFFLOAD 的 `APPLIED` 要求终态快照中，请求指定目标的必要 CPU 对象均已提交且仍可用，相应 GPU 偏好已更新，之后仍可正常淘汰。只接受或完成原目标的一部分时返回 PARTIAL，不能缩小 accepted 范围后将原动作包装为全量成功；若需要较短目标，应在新动作中明确指定。此前提交的对象若在本操作结束前已被 CPU 淘汰，也须据实计入终态范围。

`cpu_committed_bytes` 只计本操作实际新增提交，`cpu_reused_bytes` 计复用的既有副本；它们是操作累计值，不等于当前驻留量或整个 prefix 的可恢复长度。`gpu_reclaimed_bytes` 只记录本操作实际新增的可分配容量，OFFLOAD 不主动驱逐 GPU 时可为零，后续自然淘汰单独记事件。

幂等键重复且载荷相同返回同一操作及已提交结果；同键不同载荷拒绝。历史成功回执不代表当前驻留，当前位置通过 query 查询。策略版本与动态位置版本分离，避免无关缓存事件令所有动作 CAS 失效。

### 3.5 FinishGraceHold：请求结束到放置策略之间的短时保护

```text
grace_id, descriptor_id, engine_epoch
owner_scope, source_llm_call_id
protected_manifest_ref
held_gpu_replicas[]                # allocation_id/generation、group、必要 shard
started_at_monotonic, deadline_monotonic
state: ACTIVE | HANDED_OFF | EXPIRED | CANCELLED | INVALIDATED
state_version
```

GRACE 按引擎侧 `finish_grace_ttl_ms` 配置建立，每次正常完成只创建一次，使用单调时钟。时长暂不写死，实施时依据 finish 到策略接管的延迟分布和不可回收 GPU 字节开销配置；重复查询、命令重试和重复 finish 事件均不续期。剩余时间是响应生成时的观察，不依赖网关与引擎时钟同步。

保护范围包括本次结束时仍有效的已计算 GPU KV、命中的共享前缀及已登记的合法混合模型状态，按物理副本去重取得所需引用；不包含未计算输出、null block 或已经消失的历史检查点。范围不能只依赖 request block table，需合并 descriptor manifest 中仍存活的相关状态。GPU 备份可代表对应逻辑对象，但不能以只保护一个组替代完整混合模型状态。CPU-only 对象保持原生 CPU 驱逐规则，不因本 GPU 交接窗口获得隐含 CPU 租约。

未能建立声明范围的保护时必须如实报告，不能将整个 descriptor 标成已受保护。有效 GRACE 对已保护副本提供正常运行期间的硬回收约束，内存压力不能静默提前撤销；引擎重启、明确 reset 或对应 owner 的显式取消则使其失效并报告原因。TTL 到期只解除本记录尚持有的保护引用，不删除 prefix hash，也不隐式选择 OFFLOAD/DROP。

## 4. 请求结束：建立查询索引和短时 GRACE，再释放 request 引用

普通 prefix cache 中，引用归零的块仍可被命中，也可被重新分配。因此仅设置 metadata TTL 不足以保证等待策略期间的驻留；GRACE 必须拥有真实引用。KEEP 在交接完成后恢复正常的可淘汰缓存语义。

建议在 EngineCore 正常 request finish 的释放钩子中执行：

```text
on_request_finished(req):
    identify completed, restorable state and its object manifest
    register immutable descriptor from existing token/hash metadata
    atomically acquire deduplicated GRACE references before request release
    set a one-shot engine-monotonic deadline; enqueue its expiry
    follow native connector finish and compute/transfer fence handling
    release this request's original block references exactly once when safe
    emit descriptor and GRACE metadata correlated with llm_call_id
    finish response normally; do not await scheduler policy
```

正常结束时按已处理的计算完成事件判定有效状态，先取得 GRACE 引用再走原生 request free，二者之间不能出现零引用窗口。可以复用 `BlockPool.touch()/free_blocks()` 的引用机制，但必须同时完成 §4.3 的共享前缀修正，独立记录 GRACE 拥有的去重副本与 generation，不保留整个 Request 充当计时器。取消/异常路径不自动建立成功结束 GRACE；只发布已确认完成的部分，未完成写入继续由原生计算 fence 保护。

GRACE 有效期间禁止对其保护副本做自然淘汰、无权的定向 DROP 或破坏已登记状态的原地写入；共享推理按原生引用/COW 规则继续。过期或未保护范围内的对象已经丢失时，策略返回实际范围或零命中，不为执行 KEEP/OFFLOAD 而恢复或重算。

即使当前没有可复用状态，也可以返回一个后续可查询且命中为零的 descriptor。结束时复用已有 hash 链，登记未完成 KV 计算的输出尾部不能将其标记为可复用。

流式场景也在引擎结束点建立 descriptor。描述信息通过 engine output 的扩展 metadata 或可靠 side channel 交给网关，网关原样转发模型内容，不写入 tool arguments 或对话消息。回复不等待策略送达或复制完成。终止回复若无需后继复用，scheduler 可发出 DROP 意图，由引擎安全处理。

保留现有 request 引用、计算延迟释放和传输 fence 机制；只有 GRACE 和实际请求/计算/复制持有各自明确记账的引用，PolicyRecord 不增加 `ref_cnt`。若为复制新增安全引用，必须与原生任务合并并恰好释放一次。特殊 null block 不参与保护、保留偏好或定向回收。descriptor/receipt 的 metadata TTL、查询有效期和 GRACE 的物理保护 TTL 分别管理，前两者不提供驻留保证。

### 4.1 策略接管与 TTL 到期

| 事件 | 原子交接规则 |
|---|---|
| KEEP 生效 | 先登记当前范围的软偏好，再释放对应 GRACE 引用；随后可以正常淘汰 |
| OFFLOAD：CPU 副本已 READY | 验证对应目标状态、登记偏好与回执，再释放所接管范围的 GRACE 引用；CPU 后续仍可淘汰 |
| OFFLOAD：需要复制或合并在途任务 | 先使原生复制任务取得源保护或确认已有有效 fence，再解除对应 GRACE；复制跨过原 TTL 时仍由传输引用保护 |
| DROP 生效 | 撤销本 owner 的对应 GRACE，随后检查其他请求、GRACE 和传输引用并尽早安全清理 |
| 策略只处理子集 | 只解除已完成交接子集的 GRACE；其余保持原 deadline，并在回执中报告 |
| STALE、UNSUPPORTED、拒绝或尚未完成交接 | 不因命令收到/排队就解除 GRACE；仍遵守原 deadline |
| TTL 到期而策略尚未接管 | 释放剩余 GRACE 引用，回到正常 prefix cache；不默认 DROP，也不无限等待网关 |

`ACCEPTED` 不能单独作为释放 GRACE 的依据。交接与到期按 EngineCore 实际处理时的单调时钟串行裁决：处理时已到期则先结束 GRACE，迟到命令重新检查当前对象；合法命令仍可对剩余状态执行，但不能续期或追认过期保证。复制任务已经取得的安全引用不随 GRACE 到期撤销。

使用 deadline heap 或等效有序索引跟踪到期；空闲的 `input_queue.get()` 必须能在最近 deadline 唤醒，在引擎线程处理释放。活跃时在引擎安全处理点检查到期，不在计时器线程直接改 block pool。到期释放延迟需要计量；不能依赖下一次推理请求到来，也不能把只剩 GRACE 的请求计入运行请求或继续占用 FlowPilot admission credit。

GRACE 会实际增加短时不可回收 GPU 容量，必须记入容量观察，并测量并发请求结束时对分配、排队和原生抢占的影响。保留时长依据策略接管延迟配置，不因容量压力将已经承诺的硬保护静默改成软偏好。

### 4.2 GRACE 接入的源码边界

在推理 scheduler `_free_request` 调用 `_connector_finished` 和释放 request blocks 前取得已完成状态；继续执行 connector 原有 `request_finished` 和延迟 free。不能用 `request_finished` 的 `delay_free=True` 一直保留整个 Request 来代替独立 hold，也不能复用 `finished_sending` 充当 TTL 通知：原生 offload store 使用 job metadata/fence 收尾，并不通过它报告 store 完成。

`BlockPool.touch` 可以阻止自然重新分配，但 `evict_blocks` 即使 `ref_cnt>0` 也会撤销 hash。扩展的定向 DROP 必须先经过统一安全检查；原生完整性失效（例如已确认的损坏）仍须撤销不可用状态，并将对应 GRACE 标为 INVALIDATED，不能为了 TTL 保留错误 KV。显式 reset 要先协调 GRACE 与原生 transfer drain，再按 reset 范围使记录失效；不能仅增加 epoch 就直接清空仍被 DMA 访问的内存。

混合模型检查点采用原生完成/COW 状态：`take_pending_boundary_state_offloads` 的状态未必仍在 request block table，`move_block_hashes` 也不单独证明 GPU copy 已完成。只有在对应写入完成后才发布 READY；等待完成期间沿用计算/复制保护，不能把 hash 存在或引用增加误认为有效快照。首版应复用原生已捕获的 aligned-boundary/partial-tail 对象，不新增一套推测性 Mamba 快照流程。

空闲循环将最近 deadline 转为 `input_queue.get(timeout=...)` 的等待上界；超时后在引擎线程处理到期，再重新计算是否有真实 engine work，无任务则继续阻塞等待。活跃路径在既有安全点处理到期；时间推进与控制命令都不启动额外推理。测试须测实际延迟，而非假定长时间 GPU step 中仍能准时执行 Python 回调。

### 4.3 保护引用不等于请求共享关系

原生 `FullAttentionManager.get_num_common_prefix_blocks()` 使用 `block.ref_cnt == len(req_to_blocks)` 判断所有已分配请求是否共享块。引入 GRACE 后该等式不再成立：A、B 使用不同块，A 的块有一个请求引用和一个 GRACE 引用，总数恰好为 2，原生方法就会误报共同前缀。该结果进入 scheduler output 并供 cascade attention 使用，属于推理正确性问题；独立保存 GRACE 账本但不修改这个消费者仍然有错。

物理 `ref_cnt` 继续负责阻止回收，保持原生 free queue 和写入/COW 安全语义；请求共享关系以引擎实际 `req_to_blocks` 为准。首版 common-prefix 计算逐位置比较各已分配请求的 block table，只有对应位置均为同一非 null 物理块才延长共同前缀，任一缺失或不同即停止；按原生口径包含已分配但本步未调度的请求。相同内容 hash 的不同物理副本不能替代这项判定。GRACE、计算延迟释放和复制保护均不增加请求成员，不创建虚假 Request，也不通过给原等式只减去 GRACE 数量来遗漏其他非请求引用。

这项修正与 GRACE 在同一实施步骤完成，不以全局关闭 cascade attention 掩盖误判。回归覆盖完全不共享、部分共享、全部共享、多个 GRACE、COW/复制保护与请求释放后的成员变化；只增减保护引用时 common-prefix 结果必须不变，回收安全仍由总引用保证。真实推理用满足 backend 选择条件的 batch 确认执行了 cascade 路径，并与不使用 cascade 的相同请求比较输出数值。逐位置比较的热路径开销单独计量，后续若改用成员索引，仍以 block-table 对照验证结果。

## 5. KEEP、OFFLOAD 和 DROP

### KEEP：正常缓存，表达相对保留偏好

完整块通常已在推理过程中登记 prefix hash，不需要在结束时做一次物理“转换”。

KEEP 不持有额外引用，不设置固定存活时间，也不承诺下一轮命中。它让当前可用的块正常参与 prefix cache，并向引擎表达相对保留偏好；生效时按 §4.1 解除对应 GRACE，此后有压力时 vLLM 仍可淘汰 KEEP 块。已丢失的块不会因 KEEP 重新出现，已位于 CPU 的对象不会因 KEEP 触发 H2D。

在已经满足引擎安全回收条件、且没有其他线路更高保留需求的候选中，建议优先级为：

```text
DROP candidate -> CPU-backed GPU replica -> KEEP candidate
         earlier eviction ----------------> later eviction
```

这是在 vLLM 内新增的驱逐偏好，不是 0.29.0 原生 free queue 已识别的标签。实际选择仍由引擎结合共享需求、最近访问与容量完成。只有兼容 CPU 副本已完整提交、且满足以下恢复范围条件的对象才具有优先驱逐资格；CPU 副本被淘汰或失效后需更新该事实，不能永久相信一次 OFFLOAD 回执。

单块 CPU READY 不足以证明淘汰代价小。例如 GPU 有 1–6、CPU 只有 3，优先淘汰 GPU 3 会使原生可恢复前缀从 6 降到 3；淘汰 GPU 6 则还可用到 5。首版将 CPU-backed 优先级限定为**所属目标合法恢复点的必要对象已构成完整 CPU READY 集合，并经 backend 独立 CPU 查询确认可消费**；部分备份保留对象事实，但不自动给散落的 GPU 块打低优先级。完整备份被 CPU eviction 破坏时撤销相关优先级。将来若支持更细的跨层边界优化，需要证明其查询规则与真实加载一致。

GPU 侧复用原有 free queue，维护副本到有效策略/恢复 bundle 的反向索引。在引用归零入队、策略交接、CPU 完成/淘汰和共享需求变化时，更新受影响零引用候选的顺序；同等偏好沿用原生顺序，保留无缓存块优先复用的规则。不持引用的 policy 不减少 `get_num_free_blocks`，不能在另一张表中“预留”块。批量分配前重新核验候选对应 bundle 的有效版本，避免 CPU 备份已消失却仍按旧顺序选择；不在每次分配时遍历全体 descriptor。

### OFFLOAD：建立 CPU 副本，GPU 随正常策略逐步淘汰

```text
select a valid target resume point and required objects
    -> reuse existing CPU replicas / merge native in-flight stores
    -> reserve CPU space for missing objects and establish native copy fences
    -> hand off the corresponding GRACE holds only after copy protection exists
    -> copy available GPU state after source writes finish
    -> commit completed objects across their required ranks/groups
    -> report CPU READY objects and actual recoverable range
    -> release copy-only references/fences
    -> lower eviction preference for corresponding CPU-backed GPU replicas
    -> vLLM may later evict GPU replicas when needed
```

OFFLOAD 不主动删除 GPU 副本，不要求它们立即离开 GPU，也不把全体对象从 GPU 切换为 CPU 的原子迁移。正常状态可以是 GPU_ONLY、GPU_AND_CPU、CPU_ONLY 或部分对象已丢失；不同对象可处在不同状态。CPU 副本仍受原生 CPU 容量与驱逐策略管理，没有 KEEP_CPU 硬租约。

CPU 空间不足、源已丢失或必要分片复制失败时，返回失败或明确的部分结果，不发布完整可恢复命中。失败对象不能标成 CPU-backed，也不因为失败主动删除唯一 GPU 源。尚未交接且未到期的 GRACE 继续有效；已经交接给复制任务的源，在复制结束并释放安全引用、且无其余保护后才可正常淘汰。取消也必须等 DMA 不再访问相关内存后才能释放引用。

共享 GPU 块可能仍被其他请求使用、处于其他 owner 的 GRACE，或被其他线路表达 KEEP 偏好。前两者按真实引用保护，后者由引擎合并偏好后选择；单个 owner 的 OFFLOAD 不撤销其他 owner 的 GRACE，也不强制压低其保留意图。CPU 复制成功与 GPU 实际回收分别统计，不能把“复制成功”当成“GPU 显存已回收”。

Qwen3.5 的 offload bundle 必须覆盖恢复点所需的 attention 和线性状态，以及当前布局的全部必要分片。只搬 attention KV 不算可恢复。通过 backend 返回的 required-shard manifest 决定完成条件，不在协议中写死卡数或 TP 大小；CPU 恢复沿用兼容布局。尚未满足 bundle 的部分成功数据可以保留为对象级事实，但不能把整个恢复点提前标为 READY。

### OFFLOAD 的目标范围与部分完成

第一版按选定合法恢复位置组织 OFFLOAD：目标是在 CPU 保存该位置所需的完整对象集合，而不是无条件复制本次请求产生过的所有物理块。目标范围、实际完成对象和当前可恢复范围分别报告；传输可以分批，容量不足可以部分完成，不能把 PARTIAL 当成全量备份。

允许 CPU 只保存部分对象、其余依赖 GPU 驻留，但这种组合没有独立 CPU 恢复保证。以普通 full-attention 等长块为例，GPU 保留块 1–3、CPU 保留块 4–6 时，当前 backend 若支持可恢复到 6；GPU 的块 1 丢失后，CPU 没有它，恢复范围会缩短。若 CPU 已完整保存块 1–6，GPU 副本则可逐步淘汰而不因这些 GPU 淘汰本身破坏 CPU 备份；CPU 副本后续也可能被淘汰。

不能用“已复制块数 / 总块数”替代可恢复前缀。优先形成完整合法恢复点，避免只留下无法利用的碎片；哪些片段能被实际 backend 组合使用由 §6.2 的规则决定。

### 5.1 延迟 OFFLOAD：复用原生 store 流水线的具体做法

新增的 `enqueue_store_for_descriptor` 是 **EngineCore 内部的拟议入口**，不是 0.29.0 已存在的 API。descriptor 登记和提交 store 可以在同一 engine 调用中连续执行，但前提是已有有效 OFFLOAD 决策；自动 GRACE 本身不授权替 FlowPilot 选择 OFFLOAD。延迟决策使用同一个入口，不重新创建推理 Request。

原生 `_build_store_jobs` 只处理 scheduled/finished request IDs，并按 `next_stored_chunk_idx` 前进。该游标无法描述“已结束请求的一个 CPU 副本后来被淘汰，需要重存”，也不能覆盖所有仅在 GPU 命中的共享前缀。实施时先完成 §5.2 的原生候选修正，再抽取对象检查、`prepare_store`、spec 构造、job/fence 登记为共享内部 helper：原生自动 store 继续提供增量候选；descriptor 入口从当前 manifest 和原生索引提供目标候选，两者使用同一有效计算范围与配置过滤规则。aligned-boundary 与 partial-tail store 的布局/COW 规则同样复用，不能把所有组强行走 full-attention block 列表。

执行顺序固定为：

1. 校验 descriptor、owner、epoch、policy version 和原始目标，解析原生 `OffloadKey`、group/chunk 及当前可用源副本；不沿用过期的 request block IDs。保留原始 requested manifest，不能在容量不足时改写它。
2. 用 CPU manager 当前状态将目标分为 READY、PENDING、MISSING。READY 记录复用；PENDING 关联现有原生 job 的完成观察；MISSING 只有在合法 GPU 源仍存在时才尝试存储。请求命中的共享 GPU 前缀也必须检查，不能假定原来一定已有 CPU 副本。
3. 通过同一 manager 的 `prepare_store` 取得真实 CPU 目标及 `keys_to_store`，随后构造原生 `GPULoadStoreSpec/TransferJob`。在该引擎提交段内登记源保护和任务，再交接对应 GRACE；构造失败必须用原生失败收尾归还本次取得的 CPU 目标和临时引用。
4. 将任务并入既有 `OffloadingConnectorMetadata.store_jobs`，经既有 worker、复制流和同步点执行。实际待提交任务也使 `has_pending_push_work` 返回 true，防止无推理时永不执行；只有 GRACE/descriptor 元数据时不保持 engine stepping。
5. 通过同一个 `_jobs` 账本与 worker metadata 聚合完成，调用 `complete_store`，再更新操作回执、可恢复范围及 GPU 偏好。原生自动 store 与显式操作不能分别提交同一 key，也不能各释放一次相同 fence。

在原生 job 状态上补充最小 `ReqContext`、来源类型、operation 观察者与源 allocation/generation。完成处理先按 job 持有的 context 清理；原有 request 的 `transfer_jobs/finished_signaled` 仅在该 job 属于仍被跟踪的 request 时更新。descriptor 生命周期保留必要 hash/布局与传输参数，不保存完整 Request、prompt、sampling state。首版仅面向 CPUOffloadingSpec；其无请求资源的 context 可以复用，不能由此宣称任意 tiering backend 的 `on_new_request/on_request_finished` 生命周期也兼容。

建立 `OffloadKey -> native job ID` 的反向索引以合并 PENDING，引用已有 `_jobs` 的唯一事实，不新增第二套 transfer 状态机。共享 operation 是完成观察者；某一策略取消不能取消另一 request/operation 仍依赖的复制。复制 fence 要在新任务登记时生效，不能再依赖未来的 request finish；GRACE 解除后的物理块若被重分配，必须仍经过原生 `jobs_to_flush` 在覆写前完成相关源读取。副本也可能仍被其他请求使用，源写入/COW 条件依旧必须满足。

`prepare_store` 空结果可能来自 READY、PENDING 或 `store_threshold` 过滤，必须回查分类。首个原生复用基线显式配置 `store_threshold=0`；启用非零阈值时，未被接纳对象报告 `STORE_THRESHOLD`，不绕过配置、不伪报 APPLIED，也不靠反复重试同一动作累加访问次数。`offload_prompt_only`、可存储 checkpoint、chunk 对齐与 `max_offload_tokens` 同样进入 accepted 范围和 skipped reason。

一次 `prepare_store` 对传入集合的保护不延续到整个多批次 operation。后续批次或原生自动 store 可淘汰较早完成的 CPU 对象；不引入额外 CPU 保留租约。容量不足直接报告当前部分结果/失败，仍在执行的 native job 则先等待其安全终态；不无限等待容量变化或尚未出现的 GPU 源。`APPLIED` 前重查**原始目标**是否全 READY；若已经失去部分对象，返回 PARTIAL，并分别报告累计复制量与终态驻留量。

失败通道需要修改 `OffloadingWorkerMetadata` 及聚合路径：继续使用原生 job ID 和必要 worker 完成数，补充成功/失败、错误及停止访问的终态。包括非 writer worker 的完成确认，不能简单把 TP 数当成实际复制副本数。可恢复失败须等待所有相关 DMA 已完成或安全终止后，调用 `complete_store(..., success=False)` 清理本 job 未提交目标并释放 fence；一个 job 的部分 rank 成功不能提交整组 key。若需对象级 PARTIAL，应拆为原生可独立提交的 jobs。CUDA fatal error/worker 丢失走原生引擎失败和 epoch 失效流程，不能把它包装成可继续推理的普通 PARTIAL，也不能只删除 `assert success` 后默认成功。

### 5.2 原生 store 的计算范围与配置过滤

步骤 1 先修正原生自动 store，再将同一规则接入显式 OFFLOAD。0.29.0 的 `_build_store_jobs` 对正常结束请求使用 `req.num_tokens`；该长度包含已采样但尚未进行 KV 计算的最后一个 token。例：full-attention 的 block/chunk 长度为 16，prompt 为 15 tokens，生成 1 token 后结束；`offload_prompt_only=false` 时，原生方法会将 16-token hash 对应的块提交给 `prepare_store`，实际 KV 只计算到 15。仅截断 descriptor 的 `computed_token_limit` 不能修正已经进入原生 CPU 索引的对象，后继普通请求并不依赖 descriptor 才会命中它。

正常结束和取消后的 store 候选以引擎已确认完成且仍有效的计算范围为上界，并结合 speculative 接受/回退、group/chunk 对齐和实际 checkpoint 存在性过滤。不得以采样后的总长度或已有 hash 代替计算完成事实，也不能无条件把异步调度中可能提前推进的 `num_computed_tokens` 当作完成水位。活跃请求仍允许沿原生流水线为已安排计算的范围构造 store，但源读取必须在对应写入完成后发生；最终未被安排计算的采样尾部不属于该范围。自动 full-chunk、aligned-boundary、partial-tail 与 descriptor store 共用有效性规则；不能把正常调度流水线改成等待 FlowPilot 的串行存储。

配置过滤统一显式区分 `max_offload_tokens=None`（不额外限制）和 `0`（不新建任何 store），正值与 `offload_prompt_only`、有效计算范围共同约束候选边界。原生 partial-tail 的 `max_offload_tokens or num_prompt_tokens` 会忽略 0；当合法正上限低于 partial boundary 时，`assert boundary <= max_boundary` 又会触发断言。应在构造任务和申请 CPU 空间前正常过滤超限候选，显式动作记录跳过原因并按原始目标返回实际完成范围；自动 store 使用同一过滤语义。合法配置裁剪不作为内部完整性错误，真正无效的状态仍显式报错。上限只约束新存储，不删除原先存在的 CPU 副本。

回归必须覆盖 `offload_prompt_only` 两种配置、结束位置在 block/chunk 边界前/恰好边界/边界后、正常结束与取消、同步与异步调度以及已支持的 speculative 模式；检查候选和提交对象均不越过其有效计算范围。hybrid partial-tail 另覆盖 `None`、0、低于/等于/高于 boundary 的合法上限，以及非块对齐的上限。普通请求 CPU 复用验证应使用包含旧输出且继续增长的后继输入，清除 GPU 缓存后确认真实 CPU load，并与重算基线比较数值；不能仅查询 descriptor 或检查 store 回执。

### DROP：撤销本 owner 的保留意图，尽早安全回收

默认 DROP 作用于该 descriptor 授权范围的 GPU 和 CPU 副本，表示本 owner 不再要求保留。OFFLOAD 后的 GPU 自然淘汰不等于 DROP，也不应连带删除 CPU 副本。DROP 不是删除所有相同 hash 的全局请求历史。

原子处理步骤：

1. 校验 scope、engine epoch、policy version、对象 generation 和本次决策来源。
2. 用 DROP 替换发起者对目标范围的保留意图，释放该 owner 对应的 GRACE 引用；不释放其他 owner、请求或传输的引用。
3. 对每个物理副本检查其余 GRACE 与原生 request/compute/transfer 引用和 fence，并合并其他 owner 的有效需求。
4. 对已可安全清理且没有其他保留需求的副本立即撤销缓存索引及对应恢复点可用性，使其尽早被 allocator 复用；无需等待 LRU 自然选中。
5. 返回已清理范围/实际新增可分配字节，以及 SHARED_REQUEST、OTHER_GRACE、OTHER_DEMAND、IN_FLIGHT 等跳过原因。已可分配的零引用块可以撤销 hash 并调整 free queue 复用顺序，不虚报新增释放容量。

已有 vLLM `evict_blocks()` 能撤销指定块的 prefix hash，但它本身不是这套安全 DROP：不能裸调它替代引用、共享需求和版本检查。不能重复调用 free 导致 ref_cnt 负数，也不能重复把对象放入 free queue。GPU 池通常是预分配的，回收供后续推理复用不等于 CUDA 释放给操作系统，也不意味着 nvidia-smi 必然下降。

若 A/B 共享前缀，A 的 DROP 不撤销 B 的有效需求，也不打断 B 的推理。首轮未清理部分返回 PARTIAL，并在该版本 PolicyRecord 下保存待重查的**原 allocation/generation 集合**。最后引用释放、DMA 完成、其他 GRACE 结束或共享需求失效时，engine 通过反向索引自动重查；不依赖网关再次发送命令才能尽早回收。新策略、后继需求接管、epoch 或 allocation generation 变化立即使对应旧 intent 失效，之后相同 hash 重新缓存不被旧 DROP 追杀。首次回执保留其操作快照，后续清理用递增序号事件和动态查询体现；不能把历史 PARTIAL 静默改写为当时已成功。

CPU 侧拟新增 manager-owned `evict_if_idle(keys, expected_generations)`：只有 READY 且无 load/store 引用、无其他有效需求的对象才可删除，内部复用原生 policy remove、block free、计数和 removal events。`prepare_load` 会持 CPU 引用，必须等 `complete_load` 后再重查；写入中的目标等待 `complete_store` 收尾。不能从 API 层直接 `_policy.remove`，不能用 `reset_cache` 代替定向 DROP。GPU 安全撤销 hash 后只调整已在 free queue 中的零引用块顺序，不再次 `free_blocks`。共享保留偏好仍然是软偏好，vLLM 在容量压力下可以按自己的规则回收无安全引用的对象。

## 6. 主路径：response 时建索引，下一轮用 ID 查询

```text
request 1 finishes
    -> vLLM registers D1 using existing hashes and completed-state metadata
    -> engine establishes short GRACE holds before releasing request references
    -> response metadata carries D1.id and GRACE observation
    -> request references are released through the native finish path
    -> FlowPilot retains D1.id; policy application hands off GRACE atomically
    -> if no policy takes over before the deadline, GRACE expires into normal cache

request 2 reaches FlowPilot
    -> query_descriptor(D1.id, optional request metadata)
    -> engine resolves D1 and looks up current GPU/CPU residency
    -> response contains reusable-prefix and prefill-work facts
    -> FlowPilot estimates prefix/prefill and optional CPU restore cost
    -> FlowPilot queues and dispatches ordinary request 2 with admission credit
    -> vLLM validates actual tokens and acquires matching state internally
    -> vLLM decides and executes restore or recompute
```

查询不接收完整 prompt，不套 chat template，不 tokenize，不重算旧前缀 hash，不要求 prepare ticket。它不是 LLM 推理请求。注册表按 ID 定位 descriptor 后，在引擎权威位置表中读取状态。内部仍可能需要遍历缓存块/恢复点，不能把“网络报文小”宣称成查询计算恒为 O(1)。

### 6.1 查询输入和输出

```text
query_descriptor(
    descriptor_id,
    access_credential?,             # 权限凭据，与内容延续证明不同
    next_prompt_tokens?,            # N，可选；必须标明 exact/estimated 来源
    continuation_proof?             # 可选；绑定下一轮实际输入或严格构造契约
)

-> descriptor_id, kv_domain_id, engine_epoch, event_seq, observed_at
   prefix_token_count               # C
   gpu_ready_tokens                 # H_gpu，符合 backend GPU 匹配规则的范围
   recoverable_tokens?              # H_all，backend 支持的跨层恢复范围
   cpu_standalone_tokens?           # 不依赖现存 GPU 副本时的可恢复范围
   cpu_extension_tokens             # H_all - H_gpu，不是 CPU 物理对象总量
   prefix_uncovered_tokens          # C - H_all
   gpu_resident_blocks_by_group     # 当前真实驻留数量，不等于连续前缀
   cpu_ready_blocks_by_group        # 已提交数量，不包含 COPYING
   prefill_without_restore_tokens?  # N - H_gpu
   prefill_after_restore_tokens?    # N - H_all
   reuse_basis                      # DESCRIPTOR_ONLY / ASSUMED_CONTINUATION /
                                    # BOUND_CONTINUATION / TOKEN_VERIFIED
   count_basis                      # UNKNOWN / ESTIMATED / EXACT
   lookup_basis                     # backend/config、边界规则与查询假设
   lookup_state                     # COMPLETE / PENDING / UNSUPPORTED
   grace_state?, grace_remaining_ms?, grace_protected_manifest_ref?
   resume_points_ref, restore_bytes?, effective_policy_ref?, in_flight_operations
   restore_cost_estimate_ms?         # conditional estimate, not ready time
   restore_cost_basis?               # engine samples / calibrated model
   cost_model_version?, cost_observed_at?, cost_uncertainty?
```

这些字段是本提案新增协议。只传 ID 时，查询该 descriptor 内容在当前 backend 规则下的可用范围，以及该旧范围中多少未被缓存覆盖；下一轮总 prefill 两项必须为 null，因为 request 1 结束时不知道未来工具结果有多少 token。`lookup_basis` 标明所用 backend/config 和未绑定未来请求的边界假设；缺少真实查询能力时返回 unsupported，不能把未知范围伪装为实测零命中。

块数量按 group 记录，必要时附 rank/layout；物理副本按 allocation 去重，多份相同内容不能增加 prefix token 长度。混合模型各组块大小与状态语义可能不同，不能把这些数量相加后乘一个 block size。`cpu_standalone_tokens` 必须按真实 backend 规则评估；未提供这种查询时为 unknown，不从 CPU 块数量推导。

GRACE 字段报告引擎在观察时刻已建立的短时保护范围与剩余时间；它不是查询新建的 pin。查询不续期，网络返回时 TTL 可能已到期，策略也可能已提前完成交接。不得将部分 GRACE 范围或剩余时间观察扩大成整个 descriptor 在未来派发时的命中保证。

上面两条 `N-H` 公式适用于 request 2 延续 descriptor 前缀且 N 不小于 C 的情况。未证明延续时，添加 N 只能得到明确标记的条件估计；如果 N 本身也是估计，计数也必须标记 ESTIMATED。若实际请求短于 C 或只匹配 L<C，引擎按真实匹配范围和合法恢复位置重新截断 H，再计算 N-H，不能机械套用旧 H。

`reuse_basis=BOUND_CONTINUATION` 需要能绑定实际提交内容的延续契约，`TOKEN_VERIFIED` 需要真实 token 验证。只提供签名的 descriptor ID、同一个 conversation ID 或 context_epoch，均不能自动升级为这两种状态。

返回范围明确限定为该 descriptor 所覆盖的前缀；如果引擎全局 APC 在其以外还命中其他请求的缓存，实际 prefill 可更少。`N-H` 应解读为“仅使用这份 descriptor 覆盖范围、若引擎采用查询所描述的 prefix 时的条件 prefill 工作”，不是整个引擎所有缓存的全局最优命中承诺。查询时刻的事实也不等于派发时的保留承诺，不能据此指令引擎采用某个恢复方案。

### 6.2 部分驻留与 backend 可用前缀

`gpu_ready_tokens` 要求恢复所需对象在 GPU READY，且满足 backend 的 GPU prefix 匹配规则。`recoverable_tokens` 可以依赖 CPU READY 对象，但要求实际 backend 能沿正常加载路径使用这些对象；不等于此刻 GPU 已能执行。COPYING 不计为 READY，CPU/GPU 重叠和同层重复副本不重复增加命中长度。

令 Required(n) 为位置 n 所需状态对象集合，L 在 ID-only 查询时为 C，在实际请求绑定后为已证明匹配的范围。查询完成并能给出 H_all 时：

```text
H_coverage = max valid n <= L with Required(n) present in GPU READY or CPU READY
H_gpu      = BackendGpuPrefixLookup(descriptor, L, ready_replicas, lookup_basis)
H_all      = BackendRecoverablePrefixLookup(descriptor, L, ready_replicas, lookup_basis)
0 <= H_gpu <= H_all <= H_coverage <= L
```

`H_coverage` 仅是状态覆盖上界，不是请求成本公式采用的命中值。不能直接把 GPU/CPU 集合并集映射为 `H_all`。0.29.0 原生 OffloadingConnector 从本地 GPU 命中边界继续查 CPU，并按各缓存组、块/chunk 对齐及恢复点约束收敛；它不是任意交替拼接散落 GPU/CPU 块的通用恢复器。只读扩展应复用这些匹配规则，但剥离原生调度查询中的 touch、请求状态更新、统计写入及可能的加载副作用。

下面以普通 full-attention、等长完整块且每 chunk 一块为例；假设目标请求长于第 6 块，内容匹配且没有其他请求选项限制。混合模型还需检查各组恢复点：

| 当前 GPU 块 | 当前 CPU READY 块 | H_gpu（块） | H_all（块） | 解释 |
|---|---|---:|---:|---|
| 1–3 | 1–6 | 3 | 6 | CPU 已覆盖完整范围；GPU 副本可逐步淘汰 |
| 1–3 | 4–6 | 3 | 6 | 当前可以续接，但依赖 GPU 起始前缀 |
| 2–3 | 4–6 | 0 | 0 | 第 1 块两层均缺失，其余块数不能冒充前缀 |
| 1–2、4–6 | 3 | 2 | 3 | 并集覆盖 6 块，原生路径不能据此宣称命中 6 块 |
| 1–2、4–6 | 无 | 2 | 2 | GPU 实际剩 5 块，连续前缀只有 2 块 |

descriptor 的内容身份保持不变，以上淘汰和复制完成只改变查询结果。后续查询可以看到范围缩短、归零，或因兼容内容重新缓存而增长；不能把自然淘汰报成 descriptor 身份失效。查询只保证本次引擎状态快照，不 pin 对象、不承诺派发时仍相同，正式请求必须重新 lookup 并获取引用。

目标请求实际可消费的命中还需服从末 token/logits 重算、块对齐和跳过 prefix-cache read 等请求选项。ID-only 的 `lookup_basis` 必须披露未绑定这些选项的假设；请求绑定后重新截断范围。恢复字节依据 backend 实际需加载的对象集合计算，不能简单用 `H_all-H_gpu` 推导，也不能默认所有零散 GPU 副本都可免除复制。

例如 C=8192、N=9000、H_gpu=4096、H_all=8192，在前缀延续成立、长度准确且满足 backend 消费规则时：不读取 CPU 的工作为 4904 tokens；若引擎使用 CPU prefix，则为 808 tokens 加 CPU 恢复开销。仅传 ID 时可以查到 4096/8192，但不知道 N=9000；查询不保证引擎最终使用 CPU 副本。

### 6.3 CPU 恢复成本只作估计

恢复字节来自所需对象的真实布局和副本情况；FlowPilot 可以使用引擎历史测量，或真实字节与兼容布局下实测复制速率的校准模型估算 CPU 恢复成本。估计必须带来源、版本、观测时间和适用条件，缺失时为 unknown。复制服务时间、实际资源等待与引擎内部排队是不同量；不能把估计冒充实测值、硬性的 GPU ready 时间或精确 TTFT。

有可比较的 prefill 时间估计时，可以把“引擎采用 CPU prefix 的剩余 prefill 成本 + CPU restore 估计”作为条件请求成本；只有 token 工作量时，不直接与毫秒相加。旧 descriptor 与当前请求尚未证明匹配时，恢复估计同样是条件值。正式请求仍正常提交，实际命中、恢复或重算事件用于后续校准；FlowPilot 不据此生成 RESTORE、恢复优先级或恢复 deadline。

### 6.4 无副作用查询的复用边界

不直接用 `get_num_new_matched_tokens` 作为 query：它修改 RequestOffloadState、hit 统计并调用 `_touch`；内部 `_maximal_prefix_lookup` 也会记录 lookup 事件。GPU `get_computed_blocks` 在特定配置下会发送缓存事件。新入口应抽取供普通推理与 descriptor 查询共同调用的只读匹配内核，传入不可变 hash/长度/选项及只读索引，副作用保留在推理入口外层，避免维护两份逐渐分叉的匹配算法。

GPU-only 的 H_gpu 使用各组协调后的合法本地命中；CPU-assisted 的 H_all 则复用 `_get_local_prefix_cache_hit` / `get_computed_blocks_for_connector` 对混合组的候选处理，再按 connector 的 `_lookup_complete_chunks`、partial-tail、SWA/chunk 对齐规则求解。若 `hit_diverged` 且没有有效外部扩展，必须像原生 scheduler 一样回退到全组协调的 GPU 命中；不能把 full-attention 的较深候选直接报成 H_gpu。`cpu_standalone_tokens` 使用同一内核在无 GPU 命中的条件下评估。

首版 CPUOffloadingSpec 的 manager `lookup` 读取现有 policy，不调用 `touch` 或 `prepare_load`；只读能力只对已经核实的 backend 宣告。原生 lookup 遇到 HIT_PENDING/RETRY 可能返回 `None` 要求稍后调度；查询应报告 `lookup_state=PENDING`、在途事实以及可独立确认的 GPU 范围，H_all 及依赖它的 N-H/成本为 unknown，不把 None 当成零命中或复制完成。查询自身不等待 DMA，也不创建加载任务。

验证在同一冻结缓存状态、相同 hash/选项下比较只读内核与普通请求 lookup 的结果，覆盖各组命中不一致、缺口、末 token/logits、partial-tail、chunk 大于一块和在途 CPU 写入。另比较查询前后引用、free queue 顺序、CPU LRU、请求/job 状态及事件计数，证明查询不会改变缓存生命周期。真实请求仍执行原生分配与 `update_state_after_alloc -> prepare_load`，这些步骤不移入 query。

## 7. 身份证明与实际输入验证的放置

### 7.1 默认路径不增加一次完整 prepare

scheduler 持有 descriptor ID 即可做 §6 的轻量查询。普通 OpenAI-compatible 请求仍在实际推理入站时完成标准 render/tokenize/hash，并验证实际前缀；这项必要工作不属于 descriptor 查询开销。若只依赖“不压缩、关闭 thinking”的经验条件，查询结果标为 ASSUMED_CONTINUATION；不能把旧 descriptor 的 H 当作目标请求的已验证命中。基线可靠命中取 0，条件成本观察另行记录。正式请求中的实际失配由引擎正常计算并回报，用于校准后续估计，不能把更新投影说成撤回已提交请求。

### 7.2 需要入队时精确工作量的两种证明

一种方式是由可信 renderer/adapter 为当前不可变请求签发或登记 token 前缀匹配凭据，绑定 descriptor ID、实际请求摘要、匹配长度、完整 token 数及模板/hash 身份。凭据只能来自已经做过的实际验证，不能只给旧 descriptor 的签名换个名称。查询阶段可只传固定大小的凭据，但生产凭据的 tokenization/验证成本仍需单独计量。

另一种方式是显式的 canonical continuation 协议：提交语义本身定义为“引擎登记的不可变 token 前缀 + 已确定编码的增量”。实际推理必须消费这个构造，因此不必再次重算旧前缀来证明同一性。该能力需要保留/获取 canonical token 前缀及正确的消息边界与增量编码，不能只存 hash 就重建内容；普通 JSON messages 的 append 和独立 tokenize(增量文本) 不自动满足该契约。新增工具消息可以只传增量，但与标准 messages API 的等价性需验证。

两种方式都允许让查询保持轻量。没有这些能力时，应返回观察值或条件估计，不强制为了 ID 查询而重新上传完整 prompt。

### 7.3 prepare ticket 仅作为可选的工程实现

如果部署希望把正式输入预处理提前到入队前，可以使用 `prepare(request, descriptor_id)` 返回 prepared_request_id，并让后续 dispatch 消费同一结果。完整请求只传入一次，后续查询仍传 descriptor ID。prepare 不执行推理、不加入 vLLM 内部运行队列、不提前占用 inflight credit；ticket 绑定不可变请求及配置，变更即重新准备，过期/取消时释放。

这是一种生产 TOKEN_VERIFIED 凭据并避免重复预处理的方法，不是 descriptor 的职责，也不是查询接口的前置条件。未实现该能力时维持 §7.1 的普通请求路径。prepare/proof 只是查询精度与预处理复用的可选优化，不是 M6 或引擎自主恢复的前置条件，不附带恢复资源预留。

## 8. 普通推理路径中的引擎自主恢复

FlowPilot 的边界止于：查询目标 prefix、估算成本、按正常 admission credit 提交完整请求。CPU prefix 请求无须提前变成 GPU ready，不进入外部 WAITING_KV 状态，不依赖 FlowPilot 的 restore 调用、恢复计划、DISPATCH lease 或 `acquire_for_dispatch` 协议。

普通请求到达 vLLM 后，由引擎完成真实输入验证与权威 lookup，并在自己的状态机中获取 request 引用。查询是较早时刻的观察，不能替代这个 lookup+acquire；vLLM 必须让请求获取、DROP、allocation generation 与传输完成遵循同一引擎内的一致性规则。

若 vLLM 选择使用 CPU 副本，backend 自行选择合法恢复点，保护必要的 GPU/CPU 源，分配目标、安排复制并等待所有必要 rank/group 完成后供推理使用。若选择较短命中或重算，则正常执行并报告实际结果。源/目标引用、完成 fence、部分失败、取消和资源不足均由引擎处理；FlowPilot 不决定其顺序、deadline、H2D 预留或恢复/重算选择。

这些是可恢复 CPU backend 的内部正确性要求，不是新增的外部恢复控制面。若目标模型/布局尚无兼容 backend，只能报告相应 CPU 复用能力 unsupported；不能因为 OFFLOAD 保存了字节就声称普通推理已经可以使用它。

FlowPilot 在正常 dispatch 时消费 credit；引擎内部恢复等待属于同一次已提交 GatewayCall，credit 直到响应、取消、提交失败或上游终止才恰好归还一次。取消普通请求时，由引擎处理正在进行的 DMA 和引用释放，网关不另行取消一个恢复任务。

若 OFFLOAD 尚未完成而普通后继请求已来，vLLM 可以使用仍在 GPU 的匹配副本。CPU 提交后只改变可回收 GPU 副本的保留偏好，正常淘汰仍检查新 request 引用和原生 fence；形成 CPU 副本不允许删除正在被推理使用的 GPU 状态。FlowPilot 仅观测实际命中、restore/recompute 及成本，不要求引擎采用查询时预想的方案。

## 9. 最小控制面

对外 KV 接口服务于“当前去留”和“目标 prefix 查询”两类工作；普通推理沿用既有 infer/取消接口。

| 提议接口/事件 | 作用 | 对物理 KV 的影响 |
|---|---|---|
| `capabilities()` | 查询、去留动作、兼容 CPU backend、布局及成本观测能力 | 无 |
| `REQUEST_KV_DESCRIPTOR_READY` | response 时生成查询 ID，并报告 GRACE 范围与剩余时间 | finish hook 已建立独立短时保护引用 |
| `resolve_finished_descriptor(call_binding)` | 流式或 metadata 遗失时，解析已绑定 engine request 的完成 descriptor | 只读、不等待策略、不创建 GRACE |
| `query_descriptor(id, N?, proof?)` | 动态 prefix、驻留数量、内容依据和条件成本 | 无 touch、pin 或复制 |
| `apply_policy(KEEP/OFFLOAD/DROP, ...)` | 版本化软保留、CPU 存储和安全回收意图 | 按动作执行并交接 GRACE，不续期为长期保留 |
| `operation_status(operation_id)` | 查询去留动作的异步终态及部分结果 | 无 |
| `KV_STATE_CHANGED / GRACE_ENDED / POLICY_INVALIDATED / ENGINE_RESET` | 带序号的已提交 metadata 事实 | 报告交接、到期或失效等事实 |
| `ENGINE_PREFIX_USAGE`（可选遥测） | 正式请求的实际命中、引擎 restore/recompute 和成本 | 无；不是恢复控制接口 |

GRACE 由正常 finish 自动建立，期限由引擎配置；不提供外部保留 lease 的 acquire/renew 或无限延长接口，释放由策略交接、到期或明确失效事件驱动。也不暴露 `restore(...)`、恢复队列排序、恢复 deadline 或外部 `acquire_for_dispatch(...)`。§7 的 proof/prepare 若实现，仅用于查询精度和输入预处理复用，不是最小 KV 控制面的依赖。OpenHands delegation、DCS 和 Tool in-flight 的既有租约不属于本次变更。

KEEP/OFFLOAD/DROP 携带源 llm_call 和 expected tail/version 的决策引用。网关发送前校验自己的权威 tail；vLLM 校验对象 generation、owner 与 policy version，并通过已收到的新请求/取消/版本事件使旧意图失效。引擎不能声称知道尚未送达的网关 tail 更新；跨通道乱序可能减少缓存收益，不能破坏已取得的请求引用。实际推理请求的 KV 接管由引擎内部串行处理，不增加外部 dispatch lease 或 GPU-ready 屏障。

capability 分项报告 descriptor query、finish grace（含配置时长与保护范围语义）、GPU retention preference、CPU-backed eviction preference、safe direct drop、CPU store、engine CPU reuse、hybrid state、shard layout、restore-cost observation 和可选 continuation proof/prepare。`engine CPU reuse` 表示普通推理内部可消费 CPU 状态，不代表接受 RESTORE 命令。原生 APC 可运行不代表已实现 GRACE 或偏好控制；未知成本不影响已有查询或 KEEP/DROP 能力，缺失动作返回 UNSUPPORTED，不能只返回全局 enabled。

事件断序时，网关 mirror 标记 stale 并重新查询。引擎重启/缓存重置更新 epoch 或使相关对象失效，旧 descriptor/ticket/policy/action 不得误操作新 allocation；旧 CPU 内容只有引擎验证兼容后才能使用。控制面事件只含身份、hash/引用、位置、字节和时间等 metadata；原生 KV event 可能含 token IDs，不能直接转发或持久化为 FlowPilot trace。

### 9.1 vLLM 对外契约与独立验收

定义独立版本的 vLLM KV 控制面 schema，按本文件的 descriptor、GRACE、PolicyRecord、分项 capability 和异步 receipt 冻结字段；不兼容或继承旧 FlowPilot KV 协议。首版仅声明 CPUOffloadingSpec 及已验证布局的能力，不顺带承诺 NVMe/多级存储。

vLLM 提供有明确类型的 query/apply/status/resolve 入口，动作只包括 KEEP/OFFLOAD/DROP，未知动作或载荷明确报错；ACCEPTED、部分完成、失败、过期等状态保持完整语义。普通 OpenAI 推理不依赖控制面可用。HTTP 超时不等于引擎未接收：同一 action/idempotency key 的重试返回同一操作，不能重复复制或续期 GRACE。

本阶段用独立测试客户端驱动真实 vLLM server/EngineCore，验证接口及其物理效果；可以复用 vLLM 自身测试 fixture，不导入 FlowPilot 旧 KV 类型、adapter、mock 或调度逻辑。交付给下一阶段的是版本化协议、能力清单、调用示例、真实推理证据和已知限制；FlowPilot 新实现不属于本计划的代码改动或完成门槛。

### 9.2 传输与流式 descriptor 交付

沿用 `call_utility_async` 的 UTILITY IPC，在 EngineCore 定义类型化的 query/apply/status/resolve 方法；API adapter 只负责鉴权、布局路由、参数校验和序列化，不在 API 进程读取 GPU BlockPool。apply 短调用返回 ACCEPTED，实际任务由原生 connector 推进；query/status 不持锁等待 worker。utility 返回的 Future/回调只能交付结果，不能在其他线程修改引擎状态。对外暴露有限的 KV 方法，不将通用 method-name dispatcher 直接映射为 HTTP API。

非流式优先复用 `EngineCoreOutput -> RequestOutput.kv_transfer_params` 的 metadata 通路，使用有版本的扩展字段，保留 connector 原有字段。已核实 Chat Completions 的非流式响应会复制该字段，而原生 `ChatCompletionStreamResponse` 无此字段；不能只改 finish hook 就声称流式网关已获取 descriptor。

首版为流式路径提供只读 `resolve_finished_descriptor(call_binding)`，与非流式共用 engine 完成注册表。API adapter 在普通请求入站时绑定 owner、FlowPilot `llm_call_id`/attempt 与实际 engine request ID；兼顾 `chatcmpl-` 前缀、多输入后缀和多输出 child IDs，返回明确的 choice/child 到 descriptor 映射，不用 session_id 或单个 provider ID 猜测物理身份。完成事件可加速送达，按绑定查询负责遗漏后的恢复，未完成/未知绑定/元数据已过期分别返回明确状态。

vLLM 正常完成模型 SSE，不等待客户端查询 metadata 或提交策略。独立测试客户端在 terminal response 后解析 descriptor、延迟发送策略；若 metadata 在 GRACE 到期后才取得，仍可查询和操作剩余状态，但报告真实过期结果。把完成到解析再到策略接管的总延迟纳入 TTL 实验。Chat/Responses、流式/非流式分别验收，不能以其中一种 API 的成功替代其余路径。下一阶段网关按此契约接入，响应转发和 admission credit 不等待这些 metadata 操作。

## 10. 关键竞态和处理结果

| 场景 | 必须保证的行为 |
|---|---|
| 策略未到、GRACE 有效且出现 GPU 压力 | 已保护副本不能提前淘汰；容量观察计入 GRACE 占用 |
| KEEP 已接管、原 GRACE 已解除后出现 GPU 压力 | vLLM 可正常淘汰 KEEP 块；KEEP 无最低存活时间，descriptor 查询缩短 |
| GRACE 到期后策略才到达 | 回报真实剩余范围；不重建 GRACE、不恢复对象来制造成功 |
| 策略命令与 GRACE 到期并发 | 引擎按 deadline 和原子交接裁决；不出现引用空窗或重复释放 |
| OFFLOAD 已取得复制保护，随后原 TTL 到期 | 仅结束尚存的 GRACE 引用，不能释放 DMA 所持的源保护 |
| DROP 与新 request 命中同一块 | lookup+acquire 和 DROP 串行；已被新请求引用的对象不能回收 |
| DROP 与 D2H/H2D 同时发生 | 不提前释放 DMA 内存；原生完成与策略版本原子协调，旧回调不覆盖新意图 |
| A/B 共享相同 prefix | A 的 DROP/GRACE 到期不撤销 B 的 GRACE 或需求；其余安全引用保持有效 |
| 同一 hash 有多份 GPU allocation | 按物理副本检查引用、generation 和字节；查询长度不重复累加 |
| CPU 容量不足/某必要分片复制失败 | 不发布完整恢复范围，不主动删除唯一 GPU 源；仍检查未结束的 GRACE 和其余安全引用 |
| CPU 副本提交成功而 GPU 无压力 | GPU_AND_CPU 合法，OFFLOAD 不主动驱逐 GPU |
| CPU 副本后来被淘汰 | 撤销对应 CPU-backed 事实并更新偏好，重算 descriptor 的真实范围 |
| 前缀中间缺块、后段仍驻留 | 分别报告驻留数量与合法前缀，不把对象并集当作 backend 命中 |
| engine 无推理请求 | deadline 唤醒仍处理 GRACE 到期；原生 pending-work 处理传输，metadata 回收不依赖新推理请求 |
| 旧 block ID 已重用 | generation 不同即拒绝旧操作，不凭裸 block ID 删除 |
| 复制中的线性状态随后会被原地更新 | 使用引擎合法快照/COW 和写入 fence；引用存在本身不防止内容被写坏 |

## 11. 开销控制和验证顺序

descriptor、GRACE、策略记录和 receipt 的生命周期按引用与有效版本回收，避免每步扫描全部 KV block；共享对象按真实物理副本记账。登记、GRACE 引用增减、查询、偏好更新和去留动作分别计时，记录每 shard/group 的实际字节。ID 查询可以使用事件维护的可用范围索引，但需要实测索引维护与查询成本。父链段、manifest、GRACE、policy、operation 和幂等 receipt 的回收关系须明确；metadata 回收不能遗失未释放的 GRACE，也不能删除仍被后继引用的内容身份或续期物理保护。

按以下依赖实施，查询、成本估计和去留动作分别报告完成状态：

1. **冻结 0.29.0 基线、修正原生 store 并验证 CPU 路径。** 固定本地 commit、模型、dtype、并行布局、缓存模式及 `offload_prompt_only`。先按 §5.2 补充并修复结束块计算范围、hybrid partial-tail 上限的原生回归，记录补丁版本，再验证 `OffloadingConnector + CPUOffloadingSpec`。明确 GPU 缓存已清除、CPU 状态仍在，再提交普通请求，以真实加载事件证明 CPU 被使用，并在包含旧输出的后继输入上对照重算数值；输出正确但全部重算不能算 CPU 复用成功。目标单卡/多卡配置分别验证，TP=4 仅是一个测试点。若目标布局存在缺口，在 vLLM backend 内修复，不用外部 RESTORE 代替。
2. **冻结 vLLM 控制面契约及最小测试客户端。** 独立定义 descriptor、FinishGraceHold、PolicyRecord、分项 capability、部分完成 receipt 与成本来源；不读取、迁移或复用 FlowPilot 旧 KV 协议/实现。明确复用的原生 index/allocator/store/load/fence 接口及对外调用样例。测试客户端只提交动作和普通请求、查询状态，不拥有 allocator、复制任务或恢复控制权。
3. **请求共享关系、descriptor、短时 GRACE 与动态 prefix 查询。** 先按 §4.3 将 common-prefix 判断改为真实请求 block-table 共享关系，验证额外保护引用不会制造共享命中，再接入 GRACE；不能先启用 hold、后补推理消费者。response 时登记引擎已有 hash/恢复点，先取得去重保护引用再正常 free request；接入单调 deadline 和空闲唤醒。按原生规则实现无副作用的 ID 查询，区分驻留块数、backend 可用前缀、独立 CPU 恢复范围、GRACE 范围与目标请求匹配依据。超时释放后同一 ID 能继续查询，查询不 tokenize、不上传完整 prompt、不 touch/pin、不续期、不触发复制。本步骤包含真实 cascade 路径的数值验证。
4. **软 KEEP、OFFLOAD 与安全 DROP。** 将版本化偏好接入原生 GPU 候选选择；实现 §4.1 的原子 GRACE 交接。OFFLOAD 复用原生复制和 CPU 管理，补齐独立于旧 Request 生命周期的操作上下文，复制取得保护后才解除对应 GRACE，完成后只降低 GPU 保留优先级。实现完整目标与部分完成回执、CPU eviction 后偏好失效、共享需求合并及安全 DROP。与原生 lookup/acquire、计算 fence 和 DMA 完成协调，验证 GRACE 到期/交接后 KEEP 可淘汰、推理可推进，不新增网关 dispatch KV 协议。
5. **vLLM 接口联调与证据交付。** 通过独立客户端验证 descriptor 交付、延迟动作、普通请求 CPU 复用、取消/重启及真实压力回收；提供真实对象字节、传输测量和有来源的可选成本估计。比较原生 APC、相同预算的原生 APC+CPU offload、仅 GRACE、GRACE 加偏好及查询开销。完成后交付下一阶段 FlowPilot 新实现使用；本阶段不实现请求投影、admission queue 或网关成本排序。

proof/prepare 是可独立评估的查询精度优化，不阻塞上述普通请求路径。需要时分别测凭据生产与查询开销，验证预处理结果确实被正式请求复用，不附加恢复屏障。

步骤 1 显式记录 `store_threshold` 和逐请求 `max_offload_tokens`。在原生请求/传输已收尾且没有 GRACE 的测试状态，可用引擎 `reset_prefix_cache(reset_connector=False)` 清除 GPU 索引而保留 connector CPU 状态，核实 CPU READY 后再提交普通请求；还要覆盖真实压力淘汰。步骤 3 验证 GRACE 建立/交接/到期和 COW/复制保护只改变物理保护，不改变真实请求的 common-prefix 关系。步骤 4 必须完成 §5.1 的独立 context、PENDING 合并、源 fence、worker 失败聚合与 manager 清理，才能宣告异步 OFFLOAD 能力；GPU 优先驱逐须通过完整 CPU 恢复范围检查，暂缓 DROP 须能自动重查。

§5.2 的原生正确性修复及 §4.3 的 common-prefix 修复在各性能比较组中使用相同版本；GRACE/偏好按实验组配置启用。未修复的固定 commit 用于保留反例证据，不能将修复错误数据复用或合法参数断言产生的差异计作 FlowPilot 策略收益。报告分别列出原生修复、控制面实现和真实推理验证状态。

必要功能证据：GRACE 在 request 引用释放前建立，压力下保护范围不被淘汰；无请求且网关失联时也能按 deadline 释放，重复事件不续期；KEEP 先登记再解除 GRACE，之后不增加保留引用且可正常淘汰；OFFLOAD 先取得复制保护再交接，DMA 跨过原 TTL 仍安全，子集处理不会错误解除其余保护；DROP 不伤其他 owner 的 GRACE、共享请求、计算和 DMA，不重复 free 或虚报容量。CPU 副本提交后 GPU 可继续驻留，但在相同有效需求下优先于 KEEP 候选回收；COPYING 或已失效 CPU 副本不能产生虚假的备份优先级。同一 ID 在 GRACE 到期、自然淘汰、部分 OFFLOAD、CPU eviction 和 DROP 后返回真实范围。覆盖 §6.2 的前缀缺口、跨层片段及同 hash 多副本案例，验证剩余块数不冒充前缀。

查询与普通请求证据：未知 N 不伪造 prefill 数；不同请求长度、前缀失配、末 token 重算和请求选项正确约束命中；访问凭据不冒充内容证明；查询无 tokenizer、touch、引用获取或复制副作用；CPU-only 普通请求无外部恢复指令、无 GPU-ready 等待地进入 vLLM；真实 CPU 加载的输出与重算基线在规定数值容差内一致；必要分片失败不发布完整恢复点；超时、重启、取消无引用泄漏或重复释放。

性能分别测 finish hook、登记、GRACE 引用操作与到期处理、common-prefix 计算、查询、驱逐偏好维护、动作送达及真正接管、引擎实际 CPU 搬运、TTFT、请求完成时延和吞吐。记录策略接管延迟分布、GRACE 超时比例、实际保护字节高水位、到期释放延迟及对原生分配/抢占的影响。成本估计与实测分开，报告覆盖率、误差、布局和测量时间；不能把整体请求延迟充当纯复制成本。固定引擎恢复策略与资源预算，比较无 GRACE、仅 GRACE、GRACE 加偏好和带相同正确性修复的原生基线，同时报告活跃推理延迟及长期 metadata 开销，不把短时保护的收益全部归因于后续策略。FlowPilot 外部排序、Job 公平性和 workflow JCT 的联动收益留到下一阶段验证。现有 0.18.0 的实验不能代替 0.29.0 的引擎策略或 CPU 复用证据。

### 11.1 回归用例落点

优先扩展 vLLM 已有 fixture 和测试文件；下列是实施必须补齐的行为测试，不是本次已通过的功能声明。`offloading_connector/` 测试路径的前缀为 `tests/v1/kv_connector/unit/`。

| 行为 | 主要用例与原有测试落点 |
|---|---|
| 原生 store 的完成边界 | prompt 15、生成 1、block/chunk 16 的未计算尾部反例；边界前/上/后、两种 `offload_prompt_only`、取消、async 及受支持的 speculative；CPU-only 后继请求对照重算数值。`offloading_connector/test_scheduler.py`，另做真实 CPU 复用实验 |
| hybrid partial-tail 上限 | `max_offload_tokens=None/0`、低于/等于/高于 boundary、非块对齐上限；合法过滤不触发断言、不申请超限 CPU 目标；自动/显式入口一致。`offloading_connector/test_scheduler.py` |
| GRACE 与 common-prefix/cascade | 不同请求首块 + 一个 GRACE 不得报共享；部分/全部共享、多 GRACE、COW/复制与释放；物理回收保护保持有效；真实 batch 确认执行 cascade 并对照数值。`tests/v1/core/test_single_type_kv_cache_manager.py`、`tests/v1/e2e/general/test_cascade_attention.py` |
| 延迟 store 与自动 store 合并 | 清空旧 `_req_status` 后提交；共享 GPU 命中、CPU 已淘汰重存；两个操作合并同一 PENDING；无推理仍完成。`offloading_connector/test_scheduler.py` |
| CPU 接纳与部分结果 | READY/PENDING/阈值过滤三种空结果；容量不足；后批次淘汰前批次；终态按原始目标判断。`tests/v1/kv_offload/cpu/test_manager.py` |
| 复制失败及安全清理 | 单 worker/rank 失败、非 writer 确认、晚到完成、reset 后旧 job；所有 DMA 停止后才释放源和目标。`offloading_connector/test_worker.py`、`test_worker_metadata.py`、`test_scheduler.py` |
| GRACE 引用与 GPU 回收 | finish/free 无零引用窗口、同 hash 多 allocation/alias、共享 GRACE、到期交接幂等、定向 hash 撤销、generation 复用、GPU 偏好不降低可分配数。`tests/v1/core/test_prefix_caching.py`、`test_deferred_block_free.py`、`prefix_cache/test_partial_prefix_cache_primitives.py` |
| GRACE 空闲和长 step | 无输入时按 deadline 释放、控制请求唤醒、不因 TTL 空转 GPU、记录长 step 导致的实际到期延迟。`tests/v1/engine/test_engine_core.py` |
| hybrid/COW 查询 | 各组命中分歧、Mamba 检查点、partial-tail、chunk 对齐、CPU pending；只读结果对照正常 lookup，索引/LRU/ref/event 不变。`offloading_connector/test_scheduler.py`、`tests/v1/core/test_single_type_kv_cache_manager.py` |
| DROP 的最终清理 | load/store/共享请求结束后自动重查；新策略和 generation 使旧 intent 失效；相同 hash 新副本不被误删。上述 CPU manager、core scheduler 和 prefix-cache 测试 |
| 协议、交付与普通推理 | 分项能力、异步完整状态、幂等重试、SSE 后 resolve、child/attempt 关联；提交普通请求后由引擎自行加载 CPU。`tests/v1/engine/test_engine_core_client.py`、vLLM 现有 OpenAI API 测试及独立控制面客户端 |

原生 CPU 复用必须另做目标模型的真实 GPU/CPU 推理实验，验证恢复字节、group/shard 覆盖、输出数值正确性和压力下的收益；unit/fixture 不能替代它。

### 11.2 本次审查的实际验证

- 在 vLLM 仓库运行 `.venv/bin/python /tmp/flowpilot-kv-native-api-review-20260917/reproduce.py --native-only`，从固定 vLLM 源码抽取方法并注入最小 stub：确认无 scheduled/finished request 不建 store、CPU pending 可返回空 store、复制失败断言、带引用块仍可撤销 hash，共 **4 类反例**。该模式不导入 FlowPilot；这是隔离源码行为验证，不是完整 vLLM runtime 测试。
- 补充审查通过 vLLM 仓库的 `.venv/bin/python -` 抽取原生方法，使用最小 stub 确认另外 **3 类缺口、4 个反例**：不同请求块增加一个 GRACE 引用后 common-prefix 从 0 误变为 1；仅计算 15 tokens、总长 16 时仍向 `prepare_store` 提交完整块；partial-tail 上限为 0 仍提交候选；boundary 24、合法上限 16 时触发断言。store 探针在候选接纳处停止，没有伪造复制成功。这些仅证明方法级行为，不证明真实 GPU 输出已经错误，也不证明本文要求的修复已经实现。
- 此前检查过的 FlowPilot 旧 KV 代码和测试已排除出本阶段依据；其结果不能证明本计划的任何新能力。
- 本次仅修改设计文档；未修改 FlowPilot/OpenHands/vLLM 生产代码，未运行新的 GPU 推理、混合模型恢复或多卡故障实验，生产可行性仍需上述证据。

## 12. 核对来源

- [vLLM 0.29.0 BlockPool](https://raw.githubusercontent.com/vllm-project/vllm/v0.29.0/vllm/v1/core/block_pool.py)：同 hash 多副本、free queue、free_blocks 与定向 evict。
- [vLLM 0.29.0 Scheduler](https://raw.githubusercontent.com/vllm-project/vllm/v0.29.0/vllm/v1/core/sched/scheduler.py)：结束钩子、计算延迟 free 与 connector pending-work。
- [vLLM 0.29.0 OffloadingConnector](https://raw.githubusercontent.com/vllm-project/vllm/v0.29.0/vllm/distributed/kv_transfer/kv_connector/v1/offloading_connector.py)：SupportsHMA、普通请求存取与 worker 完成路径。
- [vLLM 0.29.0 OffloadingConnectorScheduler](https://raw.githubusercontent.com/vllm-project/vllm/v0.29.0/vllm/distributed/kv_transfer/kv_connector/v1/offloading/scheduler.py)：跨组 prefix 查询、store jobs、原生 transfer fence 与部分状态。
- [vLLM 0.29.0 CPU manager](https://raw.githubusercontent.com/vllm-project/vllm/v0.29.0/vllm/v1/kv_offload/cpu/manager.py)：CPU 对象提交、引用及驱逐。
- [vLLM 0.29.0 Offloading worker](https://raw.githubusercontent.com/vllm-project/vllm/v0.29.0/vllm/distributed/kv_transfer/kv_connector/v1/offloading/worker.py)：attention/Mamba 状态布局与复制任务。
- [vLLM 0.29.0 配置说明](https://raw.githubusercontent.com/vllm-project/vllm/v0.29.0/docs/features/kv_offloading_usage.md)：CPU backend、块/chunk 粒度和 `offload_prompt_only`。
- [vLLM 0.29.0 KVCacheManager](https://raw.githubusercontent.com/vllm-project/vllm/v0.29.0/vllm/v1/core/kv_cache_manager.py)：GPU lookup、副作用和 hybrid connector 候选。
- [vLLM 0.29.0 SingleTypeKVCacheManager](https://raw.githubusercontent.com/vllm-project/vllm/v0.29.0/vllm/v1/core/single_type_kv_cache_manager.py) 与 [GPU model runner](https://raw.githubusercontent.com/vllm-project/vllm/v0.29.0/vllm/v1/worker/gpu_model_runner.py)：总引用计数下的 common-prefix 判定及 cascade attention 消费路径。
- [vLLM 0.29.0 EngineCore](https://raw.githubusercontent.com/vllm-project/vllm/v0.29.0/vllm/v1/engine/core.py) 与 [core client](https://raw.githubusercontent.com/vllm-project/vllm/v0.29.0/vllm/v1/engine/core_client.py)：现有 UTILITY 通道及空闲等待。
- [vLLM 0.29.0 worker metadata](https://raw.githubusercontent.com/vllm-project/vllm/v0.29.0/vllm/distributed/kv_transfer/kv_connector/v1/offloading/common.py)：原生 job 完成聚合。
- [Qwen3.5 历史实验报告](../../experiments/flowpilot/kv-roundtrip-20260917/REPORT.md)：0.18.0 的内容一致性、块边界反例、GPU 命中与证据限制。

本文件仅为设计产物；本次未修改 OpenHands 或 vLLM 生产代码，也未执行新的推理/迁移实验。
