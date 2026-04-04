# Changelog

## 0.7.3

- 对齐 Node.js ZNL `v0.7.3` 的下载死循环防护：
  - `master.fs.download()` 在未到 EOF 时收到空分片会立刻失败。
  - `slave.fs` 下载分片读取在未到 EOF 但 `bytesRead=0` 时立刻失败。
- 与 `v0.7.3` 保持一致，继续使用精简测试输出策略（Java 侧无 runner 输出模式开关差异）。

## 0.7.2

- 对齐 Node.js ZNL `v0.7.2` 的安全与底层行为更新：
  - `SecurityUtils.verifyTextSignature()` 改为固定长度缓冲比较，降低长度差异时序暴露。
  - `fs` 传输会话 ID 生成改为纯 `UUID.randomUUID()`。
  - 节点停止时会丢弃并回调未发送队列任务，避免请求悬挂。

## 0.7.1

- 对齐 Node.js ZNL `v0.7.1` 的 `kdfSalt` 能力：
  - `ZmqClusterNodeOptions` 新增 `setKdfSalt(byte[])` / `setKdfSalt(String)`。
  - `SecurityUtils.deriveKeys(...)` 新增可选 salt 重载，空 salt 回退默认盐。
  - `ZmqClusterNode` 在构造主密钥、`addAuthKey()` 与 `authKeyMap` 派生路径统一使用 `kdfSalt`。

## 0.6.8

- 对齐 Node.js ZNL `v0.6.7` / `v0.6.8` 的内建 `fs` 更新。
- 新增 `master.fs.create(slaveId, path, recursive, overwrite, timeoutMs)`，支持创建空文件。
- 新增 `master.fs.mkdir(slaveId, path, recursive, existOk, timeoutMs)`，支持创建目录。
- 对齐 `upload(remotePath)` 目录语义：支持 `.` / `./` / `.\`、结尾斜杠路径、已存在目录自动解析为目录目标并拼接源文件名。
- 继续加固上传完成阶段，明确拒绝“已存在目录被文件覆盖”的危险写入。
- 新增 Node Master -> Java Slave 与 Java Master -> Node Slave 的 `create/mkdir/upload-dir` 互通与回归测试。

## 0.6.6

- 对齐 Node.js ZNL `v0.6.6` 的内部 service 通道协议：新增 `svc_req` / `svc_res` 控制帧解析与收发。
- 新增 Java 侧 service API：`registerService` / `unregisterService` 与 `SERVICE(identity, service, payload, timeoutMs)`。
- 新增 `ZmqEvent.service` 字段和 `onServiceRequest` / `onServiceResponse` 事件回调。
- 对齐 Node.js service 安全语义：对 service 请求/响应使用 `svc:<service>:<requestId>` 参与签名与加密上下文。
- 对齐默认配置：`enablePayloadDigest` 默认值从 `true` 调整为 `false`。
- 增加 Node.js 互通测试，覆盖 Java Master -> Node Slave 与 Node Master -> Java Slave 的 service 双向加密调用。
- 新增 Java Slave 侧 `fs` 服务最小实现与 `fs().setRoot(..., policy)` / `getPolicy()` 能力。
- 对齐 `v0.6.6` `fs` 安全策略关键语义：`readOnly`、`allowedPaths`、`denyGlobs`、路径越权与 `symlink/junction` 拦截。
- 增加 Node Master -> Java Slave 的 `fs` 策略互通测试，覆盖允许读与拒绝写/越权场景。
- 补齐完整 `fs` 功能移植：`list/stat/get/delete/rename/patch/upload/download`（含 `init/resume/chunk/ack/complete` 与 `download_init/download_chunk/download_complete` 会话流程）。
- 新增 Java Master -> Node Slave 与 Node Master -> Java Slave 的完整 `fs` 双向互通测试覆盖。

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
