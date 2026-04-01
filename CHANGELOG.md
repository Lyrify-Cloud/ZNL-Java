# Changelog

## 0.6.6

- 对齐 Node.js ZNL `v0.6.6` 的内部 service 通道协议：新增 `svc_req` / `svc_res` 控制帧解析与收发。
- 新增 Java 侧 service API：`registerService` / `unregisterService` 与 `SERVICE(identity, service, payload, timeoutMs)`。
- 新增 `ZmqEvent.service` 字段和 `onServiceRequest` / `onServiceResponse` 事件回调。
- 对齐 Node.js service 安全语义：对 service 请求/响应使用 `svc:<service>:<requestId>` 参与签名与加密上下文。
- 对齐默认配置：`enablePayloadDigest` 默认值从 `true` 调整为 `false`。
- 增加 Node.js 互通测试，覆盖 Java Master -> Node Slave 与 Node Master -> Java Slave 的 service 双向加密调用。

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
