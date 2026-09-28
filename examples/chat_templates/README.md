# Qwen 非思考模式的历史前缀

这两个模板基于本地 Qwen3-1.7B 和 Qwen3.5-9B 的原始模板，供固定
`enable_thinking=false` 的文本 Chat Completions 会话使用。

原模板在生成 assistant 前插入 `<think>\n\n</think>\n\n`，新 user 消息到来后
却省略历史 assistant 的这段前缀。保留模板在历史 assistant 中输出相同前缀，并保留
回答正文的原始空白。请求者仍发送正常 messages；descriptor 仍只传 ID 查询。
选择模板即启用这一行为，不修改模型目录、权重、vLLM KV 管理代码或默认部署。

Qwen3-1.7B 的 vLLM 启动参数增加：

```bash
--chat-template /home/liyachen/workspace/flowpilot/examples/chat_templates/qwen3-preserve-prefix.jinja \
--default-chat-template-kwargs '{"enable_thinking":false}'
```

Qwen3.5-9B 使用：

```bash
--chat-template /home/liyachen/workspace/flowpilot/examples/chat_templates/qwen3.5-preserve-prefix.jinja \
--default-chat-template-kwargs '{"enable_thinking":false}'
```

也可以逐请求设置 `chat_template_kwargs={"enable_thinking": false}`。会话各轮保持
同一设置和模板，并将返回的 assistant.content 原样追加到 messages，不做 strip、
重写或摘要，不把空 think 段手动加进 content。由模板恢复生成时已有的空段。

启用后的顺序为：

```text
第一轮：用户1 -> assistant -> 空 think 段 -> 回答1
第二轮：用户1 -> assistant -> 空 think 段 -> 回答1 -> 用户2 -> assistant -> 空 think 段
```

这里保留的是关闭思考时的空标记，没有生成或伪造思考正文。

模板在 `enable_thinking=true` 或未定义时沿用原模型分支；该模式不在本次前缀
一致性保证范围内。验证范围是文本、完整回答、原样历史；工具调用参数的重新序列化、
多模态、混合思考模式、历史压缩和 OpenHands 全链路需要单独验证。

模板一致仍须检查实际 token IDs：生成 token 解码后再编码可能出现不同切分，
特殊停止方式也可能改变回传的正文。缓存淘汰、块对齐和 hybrid 检查点还会影响
实际命中，保留模板不建立 KV 驻留保证，也不改变 ID-only descriptor 的观察语义。

Qwen3.5-9B 的真实多轮证据见
[验证报告](../../../experiments/flowpilot/nonthinking-prefix-qwen35-20260922/REPORT.md)。
