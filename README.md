# ZNL Java

ZNL (ZeroMQ Node Link) 的 Java 实现版本。这是一个基于 ZeroMQ (JeroMQ) 的轻量级、高性能 RPC 和 Pub/Sub 通信库，完美兼容原版的 Node.js [ZNL](https://github.com/Lyrify-Cloud/ZNL) 库。

## 特性

- 🚀 **基于 ZeroMQ (ROUTER/DEALER)**：单端口即可实现双向异步通信。
- 🔒 **端到端加密**：支持 AES-256-GCM 载荷透明加密，HMAC-SHA256 签名认证。
- 🛡️ **防重放攻击**：基于 nonce 和时间戳的严格防重放保护。
- 📡 **双端支持**：可作为 Master (ROUTER) 或 Slave (DEALER) 运行。
- 🔄 **异步 API**：基于 `CompletableFuture`，非阻塞设计。
- 💓 **自动心跳**：内置心跳与断线检测机制。

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

ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
options.setRole("master");
options.setId("master-01");
options.setAuthKey("your-secret-key"); // 设置相同的密钥
options.setEncrypted(true);            // 开启加密
options.getEndpoints().put("router", "tcp://0.0.0.0:6003");

ZmqClusterNode master = new ZmqClusterNode(options);

// 处理来自 Slave 的请求
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
options.setAuthKey("your-secret-key"); // 必须与 Master 一致
options.setEncrypted(true);            // 开启加密
options.getEndpoints().put("router", "tcp://127.0.0.1:6003");

ZmqClusterNode slave = new ZmqClusterNode(options);
slave.start().join();

// 发送请求给 Master
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
master.publish("news", "服务器即将重启".getBytes());

// 在 Slave 订阅消息
slave.subscribe("news", event -> {
    System.out.println("收到广播: " + new String(event.getPayload()));
});
```

## 测试与构建

使用 Maven 构建和运行测试（包含与 Node.js 原版库的互通测试）：
```bash
mvn clean install
mvn test
```

## 协议兼容性
本项目与 Node.js 版本的 [ZNL v0.5.x](https://github.com/Lyrify-Cloud/ZNL) 完全兼容。
