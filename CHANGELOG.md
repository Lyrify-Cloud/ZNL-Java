# Changelog

## 0.6.2

- 对齐 Node.js ZNL 0.6.2 协议与公开 API 语义。
- 新增 `push` 控制帧、`PUSH(topic, payload)` 单向上报能力与 `onPush` 事件回调。
- 为 `publish/subscribe/unsubscribe` 补充 `PUBLISH/SUBSCRIBE/UNSUBSCRIBE` 大写别名，便于对齐 Node.js 示例。
- 更新 Node.js 互通测试，覆盖 Java Slave -> Node Master PUSH 与 Node Slave -> Java Master PUSH 的加密场景。
- 同步 `lmm-core` 与 `lmm-edge` 到 `znl-java 0.6.2` 依赖，并补充插件侧 PUSH 接入。

## 0.6.0

- 对齐 Node.js ZNL 0.6.0 协议与恢复语义。
- 新增 `heartbeat_ack` 心跳确认链路与 `isMasterOnline()` 在线状态语义。
- Slave 在心跳确认超时后会重建 Dealer，取消旧 pending，请求不再在主节点恢复后回灌。
- 加密场景支持重建后重新绑定新的 `masterNodeId`。
- 补齐 `authKeyMap`、`heartbeatTimeoutMs`、`enablePayloadDigest`、`maxPending` 默认值对齐。
- 认证失败事件新增失败原因与加密状态上下文。
- 新增 Node.js 互通测试、主节点掉线恢复测试、加密场景主节点切换恢复测试。
