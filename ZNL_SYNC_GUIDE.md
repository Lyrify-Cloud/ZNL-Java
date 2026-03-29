# ZNL-Java 同步 Node.js 库更新流程与问题解决方案

> 本次同步目标版本：Node.js ZNL `v0.6.2`  
> 当前 Java 对齐版本：`znl-java 0.6.2`

## 1. 背景与目标
ZNL 是 Lyrify-Cloud 的底层通信核心，原始版本使用 Node.js 实现。为了保证 `znl-java` 与 `Lyrify-Cloud/ZNL` 的协议和特性完全对齐，需要定期将 Node.js 版本的更新同步到 Java 移植版中。由于语言特性和底层 ZMQ 绑定的差异，直接的代码映射并非总是可行。

## 2. 更新同步流程
标准的更新同步流程可以划分为以下几个阶段：

1. **获取最新代码**：拉取最新的 Node.js 版 ZNL 仓库。
2. **变更对比 (Diffing)**：将当前已参考的 Node.js 源码与最新源码进行 `git diff`，提取出所有的修改点（特别是协议层、加密层、配置项）。
3. **Java 代码适配 (Porting)**：
   - 将 Node.js 中增加的类属性、方法同步到 Java 的对应类中（如 `ZmqClusterNodeOptions`, `ZmqClusterNode`）。
   - 处理 Java 特有的多线程并发和强类型要求。
4. **互通性测试 (Integration Testing)**：启动最新的 Node.js Master/Slave 节点，运行 Java 对端节点，通过 `mvn test` 验证双方能够正常完成握手、RPC 请求/响应、PUB/SUB 广播以及 PUSH 单向上报。

## 3. 本次更新遇到的典型问题与解决方案

### 3.0 0.6.2 版本变更摘要
**本次同步的关键点**：
- 新增 `heartbeat -> heartbeat_ack` 心跳确认语义，Slave 不再只依赖单向心跳。
- 新增 `masterOnline / isMasterOnline()` 在线状态语义。
- 当 `heartbeat_ack` 超时时，Slave 会重建 Dealer、取消旧 pending、丢弃未发送的旧队列任务，再重新注册。
- 加密模式下，Slave 在重建后支持重新绑定新的 `masterNodeId`，适配主节点重启或切换。
- 新增 `PUSH(topic, payload)` 单向上报语义与 `push` 控制帧。
- Java 侧补充 `onPush` 事件以及 `PUBLISH/SUBSCRIBE/UNSUBSCRIBE` 大写别名，便于与 Node.js 示例保持一致。
- 默认参数继续与 Node.js 0.6.x 对齐：`maxPending=1000`、`heartbeatInterval=3000`。
- 新增互通测试，覆盖 Java -> Node 与 Node -> Java 的 PUSH 加密场景。

### 3.1 `diff` 在 Windows 下输出编码导致的乱码问题
**问题描述**：在 Windows PowerShell 下执行 `git diff --no-index` 时，如果直接重定向输出到文本文件，可能会由于默认编码（UTF-16LE）导致后续使用 Python 或其他工具读取时出现乱码或无法解析的问题。
**解决方案**：推荐使用 Python `subprocess` 将标准输出直接以二进制写入文件，或者在 PowerShell 中强制使用 `utf8` 编码 `Out-File -Encoding utf8`。我们将在自动化脚本中使用统一的 Python 提取逻辑。

### 3.2 密码学（SecurityUtils）与缓冲区处理的差异
**问题描述**：
- Node.js 中可以直接对 Buffer 数组进行简单的迭代哈希。本次 Node.js 取消了把整个 Payload `Buffer.concat()` 再做 SHA-256 的操作，而是改为增量 `update`。
- Java 中需要手动维护 `ByteBuffer`，特别是在处理 `frames == null` 或空帧情况时。
**解决方案**：在 Java `SecurityUtils.java` 的 `digestFrames` 中，严格对齐 Node.js 中头部的长度占位符（4字节小端/大端需确认，ZNL 默认处理方式）并使用 `MessageDigest.update` 进行增量摘要，防止 OutOfMemory。

### 3.3 协议变更带来的方法签名重构
**问题描述**：
- 新版 Node.js 支持了为每个 Slave 分配不同的 `authKey` (`authKeyMap`)。这意味着加密密钥 (`encryptKey`) 和签名密钥 (`signKey`) 不再是全局单例，而必须在每次发包时动态根据 `slaveId` 进行解析。
- Java 中 `sealPayloadFrames` 和 `createAuthProof` 原本没有传递独立密钥的参数。
**解决方案**：
1. 为相关方法追加 `byte[] encryptKeyOverride` / `byte[] signKeyOverride` 参数，允许回退到默认密钥。
2. 在 `ROUTER` / `publish` 遍历 Slave 列表发包时，动态调用 `resolveSlaveKeys(slaveId)` 获取当前目标节点的密钥。

### 3.4 单元测试环境与 Node.js 依赖问题
**问题描述**：为了进行互通测试，Java `NodeJsIntegrationTest.java` 依赖于一个启动 ZNL Node.js Master 的测试脚本 `test-server.js`。拉取新版 Node.js 源码后，如果没有在 Node 目录执行 `npm install`，将导致 `zeromq` 模块缺失，从而使 Java 互通测试卡死或失败。
**解决方案**：在每次执行同步测试前，必须确保目标 Node.js 源码目录下执行过 `npm install`。

## 4. 自动化更新脚本
为了简化上述流程，我们提供了一个 PowerShell 脚本 `sync-znl.ps1`，该脚本将自动拉取最新的仓库、生成 diff 文件，并尝试执行构建与测试。

详见 `sync-znl.ps1`。
