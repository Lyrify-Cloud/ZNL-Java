# ZNL Java

ZNL (ZeroMQ Node Link) 的 Java 实现版本。这是一个基于 ZeroMQ (JeroMQ) 的轻量级、高性能 RPC、Pub/Sub、PUSH 与 Service 通信库，当前与 Node.js [ZNL v0.6.6](https://github.com/Lyrify-Cloud/ZNL) 的核心协议语义保持对齐。

## 特性

- 🚀 **基于 ZeroMQ (ROUTER/DEALER)**：单端口即可实现双向异步通信。
- 🔒 **端到端加密**：支持 AES-256-GCM 载荷透明加密，HMAC-SHA256 签名认证。
- 🛡️ **防重放攻击**：支持 `maxTimeSkewMs` 与 `replayWindowMs` 配置。
- 🔑 **多从节点独立密钥**：Master 支持 `authKeyMap`，可按 `slaveId` 分配不同密钥。
- 📡 **双端支持**：可作为 Master (ROUTER) 或 Slave (DEALER) 运行。
- 🔄 **异步 API**：基于 `CompletableFuture`，非阻塞设计。
- 📬 **单向 PUSH**：Slave 支持 `PUSH(topic, payload)` 向 Master 无回包上报。
- 🧩 **独立 Service 通道**：支持 `svc_req/svc_res` 内部服务请求响应，不与业务 RPC 串流。
- 🔧 **Service API**：支持 `registerService/unregisterService` 以及 Master 侧 `SERVICE(...)` 调用。
- 📁 **FS Slave 服务**：支持 `slave.fs().setRoot(root, policy)` 与 `getPolicy()`，可与 Node `master.fs` 互通。
- 💓 **心跳确认链路**：Slave 使用 `heartbeat -> heartbeat_ack` 维护链路状态。
- ♻️ **断线自动恢复**：`heartbeat_ack` 超时后会重建 Dealer、取消旧 pending 并重新注册。
- 📶 **在线状态可观测**：Slave 提供 `isMasterOnline()` 用于读取最近一次链路确认状态。
- 🧪 **跨语言互通测试**：包含 Java/Node 双向 PUSH、RPC 与恢复相关互通测试。

## 依赖

本项目基于 Maven 构建，依赖以下库：
- `jeromq` (ZeroMQ Java 实现)
- `slf4j` (日志门面)
- `junit-jupiter` (仅测试)

## 快速开始

### 1. 引入依赖
将编译后的 jar 包引入到您的项目中，或者在您的项目中直接引用源码。

### 2. 作为 Master 启动

```java
import com.znl.*;
import java.util.concurrent.CompletableFuture;

ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
options.setRole("master");
options.setId("master-01");
options.setAuthKey("your-secret-key");
options.setEncrypted(true);
options.setHeartbeatInterval(3000);
options.setHeartbeatTimeoutMs(0);
options.setMaxPending(1000);
options.getEndpoints().put("router", "tcp://0.0.0.0:6003");

ZmqClusterNode master = new ZmqClusterNode(options);

master.ROUTER(event -> {
    String payload = new String(event.getPayload());
    System.out.println("收到请求: " + payload);
    return CompletableFuture.completedFuture("处理完成".getBytes());
});

master.start().join();
System.out.println("Master 启动成功");
```

### 3. 作为 Slave 启动

```java
import com.znl.*;
import java.util.concurrent.CompletableFuture;

ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
options.setRole("slave");
options.setId("slave-01");
options.setAuthKey("your-secret-key");
options.setEncrypted(true);
options.setHeartbeatInterval(3000);
options.setHeartbeatTimeoutMs(0);
options.getEndpoints().put("router", "tcp://127.0.0.1:6003");

ZmqClusterNode slave = new ZmqClusterNode(options);
slave.start().join();

try {
    CompletableFuture<byte[]> future = slave.DEALER("Hello Master!".getBytes(), 5000);
    byte[] reply = future.join();
    System.out.println("收到回复: " + new String(reply));
} catch (Exception e) {
    e.printStackTrace();
}
```

### 4. 广播与订阅 (Pub/Sub)

```java
// 在 Master 广播消息
master.PUBLISH("news", "服务器即将重启".getBytes());

// 在 Slave 订阅消息
slave.SUBSCRIBE("news", event -> {
    System.out.println("收到广播: " + new String(event.getPayload()));
});

// 在 Slave 单向上报消息给 Master
slave.PUSH("metrics", "{\"online\":42}".getBytes()).join();
```

## 0.6.6 对齐项

- `svc_req/svc_res`：新增内部 service 控制帧与事件语义。
- `registerService/unregisterService`：支持在节点侧注册/注销 service 处理器。
- `SERVICE(identity, service, payload, timeoutMs)`：Master 可通过 service 通道请求指定 Slave。
- `onServiceRequest/onServiceResponse`：新增 service 事件回调。
- `ZmqEvent.service`：事件携带 service 名称。
- `enablePayloadDigest` 默认值更新为 `false`，与 Node.js 最新默认行为一致。
- `slave.fs().setRoot(root, policy)` / `getPolicy()`：支持 `readOnly`、`allowedPaths`、`denyGlobs` 等策略。
- `fs` 路径安全：拦截 root 越权访问与路径链路中的 `symlink/junction`。

## 0.6.2 对齐项

- `authKeyMap`：Master 可为不同 Slave 分配不同认证密钥。
- `heartbeatTimeoutMs`：显式配置心跳确认超时时间。
- `enablePayloadDigest`：可按需启用或关闭 payload 摘要校验。
- `isMasterOnline()`：Slave 可读取当前最近一次链路确认状态。
- `PUSH(topic, payload)`：Slave 可向 Master 发送无回包单向消息。
- `onPush`：Master 可接收 Slave 的 PUSH 事件。
- `PUBLISH/SUBSCRIBE/UNSUBSCRIBE`：补充 Node.js 风格大写别名。
- `auth_failed` 上下文：认证失败事件可读取失败原因与加密状态。
- `heartbeat_ack` 恢复：心跳确认超时后自动重建 Dealer，避免旧消息在主节点恢复后回灌。

## 恢复语义

- Slave 发出一条 `heartbeat` 后，会等待合法的 `heartbeat_ack` 或合法业务帧确认链路可达。
- 若在超时时间内未收到合法确认，`isMasterOnline()` 会变为 `false`。
- 进入恢复流程后，旧 Dealer 会被直接关闭，旧 pending 请求会异常结束，未发送的旧任务会被丢弃。
- 随后会创建新的 Dealer、重新注册到 Master，并恢复后续心跳。
- 加密模式下，Slave 在重建后可重新绑定新的 `masterNodeId`，适配主节点重启或主备切换。

## 版本说明

- 当前库版本：`0.6.6`
- 底层 ZeroMQ Java 绑定：`jeromq 0.6.0`
- `isMasterOnline()` 表示最近一次链路确认状态，不代表实时网络探测结果。

## 测试与构建

使用 Maven 构建和运行测试（包含与 Node.js 原版库的互通测试）：
```bash
mvn clean install
mvn test
```

## 协议兼容性
本项目与 Node.js 版本的 [ZNL v0.6.6](https://github.com/Lyrify-Cloud/ZNL) 保持核心协议兼容，并对齐了 `heartbeat_ack`、`masterOnline`、`push`、`svc_req/svc_res`、断线重建 Dealer、认证失败事件上下文等语义。
