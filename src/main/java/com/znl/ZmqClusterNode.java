package com.znl;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZmqClusterNode {

    private static final Logger logger = LoggerFactory.getLogger(ZmqClusterNode.class);

    private static final String DEFAULT_TOPIC = "";
    private static final int DEFAULT_TIMEOUT_MS = 5000;
    private static final byte[] EMPTY_BUFFER = new byte[0];
    private static final String CONTROL_PREFIX = "__znl_v1__";
    private static final String CONTROL_REQ = "req";
    private static final String CONTROL_RES = "res";
    private static final String CONTROL_SVC_REQ = "svc_req";
    private static final String CONTROL_SVC_RES = "svc_res";
    private static final String CONTROL_AUTH = "__znl_v1_auth__";
    private static final String CONTROL_HEARTBEAT = "heartbeat";
    private static final String CONTROL_HEARTBEAT_ACK = "heartbeat_ack";
    private static final String CONTROL_REGISTER = "register";
    private static final String CONTROL_UNREGISTER = "unregister";
    private static final String CONTROL_PUB = "pub";
    private static final String CONTROL_PUSH = "push";
    private static final String SECURITY_ENVELOPE_VERSION = "__znl_sec_v1__";

    private final ZmqClusterNodeOptions options;
    private final ZContext context;

    private ZMQ.Socket routerSocket;
    private ZMQ.Socket dealerSocket;
    private ZMQ.Socket wakePairRecv;
    private ZMQ.Socket wakePairSend;

    private Thread ioThread;
    private volatile boolean running = false;

    private final ConcurrentLinkedQueue<QueuedTask> taskQueue = new ConcurrentLinkedQueue<>();
    private final Map<String, PendingRequest> pending = new ConcurrentHashMap<>();
    private String authKey;
    private final byte[] kdfSalt;

    private boolean secureEnabled;
    private SecurityUtils.Keys keys;
    private SecurityUtils.ReplayGuard replayGuard;
    private String masterNodeId = null;
    private volatile boolean masterOnline = false;
    private volatile long lastMasterSeenAt = 0L;
    private volatile boolean heartbeatWaitingAck = false;
    private volatile boolean dealerRestarting = false;

    private final Map<String, String> authKeyMap = new ConcurrentHashMap<>();
    private final Map<String, SecurityUtils.Keys> slaveKeyCache = new ConcurrentHashMap<>();

    private ZmqRequestHandler routerAutoHandler = null;
    private ZmqRequestHandler dealerAutoHandler = null;
    private ZmqMultipartRequestHandler routerAutoMultipartHandler = null;
    private ZmqMultipartRequestHandler dealerAutoMultipartHandler = null;
    private final Map<String, ZmqMultipartRequestHandler> serviceHandlers = new ConcurrentHashMap<>();

    private final List<ZmqEventListener> listeners = new CopyOnWriteArrayList<>();
    private final Map<String, Long> slaves = new ConcurrentHashMap<>();
    private final Map<String, java.util.function.Consumer<ZmqEvent>> subscriptions = new ConcurrentHashMap<>();
    private FsService fsService;
    
    private Timer heartbeatTimer;
    private Timer heartbeatAckTimer;
    private Timer heartbeatCheckTimer;

    private static class PendingRequest {
        CompletableFuture<List<byte[]>> future;
        long expireTimeMs;
        long timeoutMs;
        String requestId;
        String identityText;
    }

    private static class QueuedTask {
        private final Runnable task;
        private final Runnable onDrop;

        private QueuedTask(Runnable task, Runnable onDrop) {
            this.task = task;
            this.onDrop = onDrop;
        }
    }

    public ZmqClusterNode(ZmqClusterNodeOptions options) {
        if (!"master".equals(options.getRole()) && !"slave".equals(options.getRole())) {
            throw new IllegalArgumentException("role must be 'master' or 'slave'");
        }
        if (options.getId() == null || options.getId().isEmpty()) {
            throw new IllegalArgumentException("id cannot be empty");
        }
        this.options = options;
        this.context = new ZContext();
        this.authKey = options.getAuthKey() != null ? options.getAuthKey() : "";
        this.kdfSalt = options.getKdfSalt();
        
        if ("master".equals(options.getRole()) && options.getAuthKeyMap() != null) {
            for (Map.Entry<String, String> entry : options.getAuthKeyMap().entrySet()) {
                if (entry.getValue() != null) {
                    this.authKeyMap.put(entry.getKey(), entry.getValue());
                }
            }
        }
        
        this.secureEnabled = options.isEncrypted();
        if (this.secureEnabled) {
            boolean hasAuthMap = "master".equals(options.getRole()) && options.getAuthKeyMap() != null;
            if (this.authKey.isEmpty() && !hasAuthMap) {
                throw new IllegalArgumentException("authKey or authKeyMap cannot be empty when encrypted=true");
            }
            try {
                if (!this.authKey.isEmpty()) {
                    this.keys = SecurityUtils.deriveKeys(this.authKey, this.kdfSalt);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to derive keys", e);
            }
            this.replayGuard = new SecurityUtils.ReplayGuard(options.getReplayWindowMs());
        }
    }

    public void addListener(ZmqEventListener listener) {
        listeners.add(listener);
    }

    public void removeListener(ZmqEventListener listener) {
        listeners.remove(listener);
    }

    public synchronized ZmqClusterNode addAuthKey(String slaveId, String authKey) {
        if (!"master".equals(options.getRole())) {
            throw new IllegalStateException("addAuthKey() can only be called on master");
        }
        if (slaveId == null || slaveId.isEmpty()) {
            throw new IllegalArgumentException("slaveId cannot be empty");
        }
        if (authKey == null || authKey.isEmpty()) {
            throw new IllegalArgumentException("authKey cannot be empty");
        }

        this.authKeyMap.put(slaveId, authKey);
        
        if (this.secureEnabled) {
            try {
                SecurityUtils.Keys derived = SecurityUtils.deriveKeys(authKey, this.kdfSalt);
                this.slaveKeyCache.put(slaveId, derived);
            } catch (Exception e) {
                logger.error("Failed to derive keys for slave " + slaveId, e);
            }
        }
        return this;
    }

    public synchronized ZmqClusterNode removeAuthKey(String slaveId) {
        if (!"master".equals(options.getRole())) {
            throw new IllegalStateException("removeAuthKey() can only be called on master");
        }
        if (slaveId == null || slaveId.isEmpty()) {
            throw new IllegalArgumentException("slaveId cannot be empty");
        }

        this.authKeyMap.remove(slaveId);
        this.slaveKeyCache.remove(slaveId);

        if (this.slaves.containsKey(slaveId)) {
            this.slaves.remove(slaveId);
            emitSlaveDisconnected(slaveId);
        }
        return this;
    }

    private SecurityUtils.Keys resolveSlaveKeys(String slaveId) {
        if (slaveKeyCache.containsKey(slaveId)) {
            return slaveKeyCache.get(slaveId);
        }
        String key = authKeyMap.get(slaveId);
        if (key != null) {
            try {
                SecurityUtils.Keys derived = SecurityUtils.deriveKeys(key, this.kdfSalt);
                slaveKeyCache.put(slaveId, derived);
                return derived;
            } catch (Exception e) {
                logger.error("Failed to derive keys for slave " + slaveId, e);
                return null;
            }
        }
        return this.keys;
    }

    public synchronized CompletableFuture<Void> start() {
        if (running) return CompletableFuture.completedFuture(null);
        logger.info("[ZNL] Starting node. Role: {}, ID: {}", options.getRole(), options.getId());
        running = true;
        masterOnline = false;
        lastMasterSeenAt = 0L;
        heartbeatWaitingAck = false;
        CompletableFuture<Void> startFuture = new CompletableFuture<>();

        ioThread = new Thread(() -> {
            try {
                initSockets();
                startFuture.complete(null);
                runLoop();
            } catch (Exception e) {
                running = false;
                startFuture.completeExceptionally(e);
                emitError(e);
            } finally {
                closeAllSockets();
            }
        });
        ioThread.setName("ZmqClusterNode-IO-" + options.getId());
        ioThread.start();
        
        startFuture.thenRun(() -> {
            if ("master".equals(options.getRole())) {
                startHeartbeatCheckTimer();
            } else {
                sendRegister();
                startHeartbeatTimer();
            }
        });
        
        return startFuture;
    }

    public synchronized CompletableFuture<Void> stop() {
        if (!running) return CompletableFuture.completedFuture(null);
        logger.info("[ZNL] Stopping node...");
        if ("slave".equals(options.getRole())) {
            sendUnregister();
            wakeUp();
            try { Thread.sleep(50); } catch (InterruptedException ignored) {}
        }
        running = false;
        
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel();
            heartbeatTimer = null;
        }
        if (heartbeatAckTimer != null) {
            heartbeatAckTimer.cancel();
            heartbeatAckTimer = null;
        }
        if (heartbeatCheckTimer != null) {
            heartbeatCheckTimer.cancel();
            heartbeatCheckTimer = null;
        }
        masterOnline = false;
        lastMasterSeenAt = 0L;
        heartbeatWaitingAck = false;

        wakeUp();
        CompletableFuture<Void> stopFuture = new CompletableFuture<>();
        new Thread(() -> {
            try {
                if (ioThread != null) {
                    ioThread.join();
                }
                stopFuture.complete(null);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stopFuture.completeExceptionally(e);
            }
        }).start();
        return stopFuture;
    }

    private void initSockets() {
        String wakeAddr = "inproc://wake-" + UUID.randomUUID().toString();
        wakePairRecv = context.createSocket(SocketType.PAIR);
        wakePairRecv.bind(wakeAddr);
        wakePairSend = context.createSocket(SocketType.PAIR);
        wakePairSend.connect(wakeAddr);

        String endpoint = options.getEndpoints().get("router");
        if (endpoint == null) endpoint = "tcp://127.0.0.1:6003";

        if ("master".equals(options.getRole())) {
            routerSocket = context.createSocket(SocketType.ROUTER);
            routerSocket.bind(endpoint);
            logger.info("[ZNL-Master] ROUTER socket bound to {}", endpoint);
        } else {
            dealerSocket = createDealerSocket(endpoint);
        }
    }

    private ZMQ.Socket createDealerSocket(String endpoint) {
        ZMQ.Socket socket = context.createSocket(SocketType.DEALER);
        socket.setIdentity(options.getId().getBytes(StandardCharsets.UTF_8));
        socket.setImmediate(true);
        socket.setLinger(0);
        socket.setSendTimeOut(0);
        socket.setReconnectIVL(200);
        socket.setReconnectIVLMax(1000);
        socket.connect(endpoint);
        logger.info("[ZNL-Slave] DEALER socket connected to {}", endpoint);
        return socket;
    }

    private void closeAllSockets() {
        discardQueuedTasks(new Exception("Node stopped, queued request cancelled."));
        for (PendingRequest req : pending.values()) {
            req.future.completeExceptionally(new Exception("Node stopped, pending request cancelled."));
        }
        pending.clear();
        closeSocket(routerSocket);
        closeSocket(dealerSocket);
        closeSocket(wakePairRecv);
        closeSocket(wakePairSend);
        routerSocket = null;
        dealerSocket = null;
        wakePairRecv = null;
        wakePairSend = null;
        slaves.clear();
        masterNodeId = null;
        masterOnline = false;
        lastMasterSeenAt = 0L;
        heartbeatWaitingAck = false;
    }

    private void closeSocket(ZMQ.Socket socket) {
        if (socket == null) return;
        try {
            socket.close();
        } catch (Exception ignored) {
        }
    }

    private void runLoop() {
        Poller poller = null;
        int wakeIdx = -1, routerIdx = -1, dealerIdx = -1;
        ZMQ.Socket registeredRouterSocket = null;
        ZMQ.Socket registeredDealerSocket = null;

        long lastCheckTime = System.currentTimeMillis();

        while (running && !Thread.currentThread().isInterrupted()) {
            if (poller == null || registeredRouterSocket != routerSocket || registeredDealerSocket != dealerSocket) {
                poller = context.createPoller(3);
                wakeIdx = poller.register(wakePairRecv, Poller.POLLIN);
                routerIdx = -1;
                dealerIdx = -1;
                registeredRouterSocket = routerSocket;
                registeredDealerSocket = dealerSocket;
                if ("master".equals(options.getRole())) {
                    if (registeredRouterSocket != null) {
                        routerIdx = poller.register(registeredRouterSocket, Poller.POLLIN);
                    }
                } else if (registeredDealerSocket != null) {
                    dealerIdx = poller.register(registeredDealerSocket, Poller.POLLIN);
                }
            }

            int rc = poller.poll(100);
            if (rc == -1) break;

            QueuedTask task;
            while ((task = taskQueue.poll()) != null) {
                try {
                    task.task.run();
                } catch (Exception e) {
                    emitError(e);
                }
            }

            if (registeredRouterSocket != routerSocket || registeredDealerSocket != dealerSocket) {
                continue;
            }

            if (poller.pollin(wakeIdx)) {
                wakePairRecv.recv(ZMQ.DONTWAIT);
            }

            if (routerIdx != -1 && poller.pollin(routerIdx)) {
                handleRouterFrames(recvFrames(registeredRouterSocket));
            }

            if (dealerIdx != -1 && poller.pollin(dealerIdx)) {
                handleDealerFrames(recvFrames(registeredDealerSocket));
            }

            long now = System.currentTimeMillis();
            if (now - lastCheckTime > 50) {
                lastCheckTime = now;
                checkTimeouts(now);
            }
        }
    }

    private void wakeUp() {
        if (wakePairSend != null && running) {
            synchronized (wakePairSend) {
                try {
                    wakePairSend.send(new byte[]{1}, ZMQ.DONTWAIT);
                } catch (org.zeromq.ZMQException e) {
                    if (e.getErrorCode() != 156384765) { // Context was terminated
                        throw e;
                    }
                }
            }
        }
    }

    private void submitTask(Runnable task) {
        submitTask(task, () -> {});
    }

    private void submitTask(Runnable task, Runnable onDrop) {
        taskQueue.offer(new QueuedTask(task, onDrop));
        wakeUp();
    }

    private List<byte[]> recvFrames(ZMQ.Socket socket) {
        List<byte[]> frames = new ArrayList<>();
        do {
            byte[] data = socket.recv(ZMQ.DONTWAIT);
            if (data != null) {
                frames.add(data);
            }
        } while (socket.hasReceiveMore());
        return frames;
    }

    private void checkTimeouts(long now) {
        Iterator<Map.Entry<String, PendingRequest>> it = pending.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, PendingRequest> entry = it.next();
            PendingRequest req = entry.getValue();
            if (now >= req.expireTimeMs) {
                it.remove();
                String msg = req.identityText != null ?
                        "Request timeout: requestId=" + req.requestId + ", identity=" + req.identityText :
                        "Request timeout: requestId=" + req.requestId;
                req.future.completeExceptionally(new Exception(msg));
            }
        }
    }

    private void ensurePendingCapacity() {
        if (options.getMaxPending() > 0 && pending.size() >= options.getMaxPending()) {
            throw new RuntimeException("Request overloaded: pending=" + pending.size() + ", maxPending=" + options.getMaxPending());
        }
    }

    private String pendingKey(String requestId, String identityText) {
        return (identityText != null && !identityText.isEmpty()) ? identityText + "::" + requestId : requestId;
    }

    public boolean isMasterOnline() {
        return "slave".equals(options.getRole()) && masterOnline;
    }

    public ZmqClusterNodeOptions getOptions() {
        return options;
    }

    public String getRole() {
        return options.getRole();
    }

    public synchronized FsService fs() {
        if (fsService == null) {
            fsService = new FsService(this);
        }
        return fsService;
    }

    // --- Security Helpers ---

    private String createAuthProof(String kind, String requestId, List<byte[]> payloadFrames, byte[] signKeyOverride) {
        if (!secureEnabled) return "";
        byte[] signKey = signKeyOverride != null ? signKeyOverride : (keys != null ? keys.signKey : null);
        if (signKey == null) return "";
        
        SecurityUtils.Envelope envelope = new SecurityUtils.Envelope();
        envelope.kind = kind;
        envelope.nodeId = options.getId();
        envelope.requestId = requestId != null ? requestId : "";
        envelope.timestamp = System.currentTimeMillis();
        envelope.nonce = SecurityUtils.generateNonce(16);
        try {
            envelope.payloadDigest = options.isEnablePayloadDigest() ? SecurityUtils.digestFrames(payloadFrames) : "";
            return SecurityUtils.encodeAuthProofToken(signKey, envelope);
        } catch (Exception e) {
            logger.error("Failed to create auth proof", e);
            return "";
        }
    }

    private List<byte[]> sealPayloadFrames(String kind, String requestId, List<byte[]> rawFrames, byte[] encryptKeyOverride) {
        List<byte[]> safeFrames = rawFrames != null ? new ArrayList<>(rawFrames) : Collections.emptyList();

        if (!secureEnabled) return rawFrames;
        byte[] encryptKey = encryptKeyOverride != null ? encryptKeyOverride : (keys != null ? keys.encryptKey : null);
        if (encryptKey == null) throw new RuntimeException("Encryption key not initialized");
        
        try {
            String aadStr = "znl-aad-v1|" + kind + "|" + options.getId() + "|" + (requestId != null ? requestId : "");
            byte[] aad = aadStr.getBytes(StandardCharsets.UTF_8);
            SecurityUtils.EncryptedResult res = SecurityUtils.encryptFrames(encryptKey, safeFrames, aad);
            
            List<byte[]> out = new ArrayList<>();
            out.add(SECURITY_ENVELOPE_VERSION.getBytes(StandardCharsets.UTF_8));
            out.add(res.iv);
            out.add(res.tag);
            out.add(res.ciphertext);
            return out;
        } catch (Exception e) {
            throw new RuntimeException("Failed to encrypt payload", e);
        }
    }

    private List<byte[]> sealPayloadFrames(String kind, String requestId, byte[] payload, byte[] encryptKeyOverride) {
        return sealPayloadFrames(kind, requestId, singlePayloadToFrames(payload), encryptKeyOverride);
    }

    private List<byte[]> openPayloadFrames(String kind, String requestId, List<byte[]> payloadFrames, String senderNodeId, byte[] encryptKeyOverride) throws Exception {
        if (!secureEnabled) return payloadFrames;
        byte[] encryptKey = encryptKeyOverride != null ? encryptKeyOverride : (keys != null ? keys.encryptKey : null);
        if (encryptKey == null) throw new Exception("Encryption key not initialized");
        
        if (payloadFrames.size() != 4) {
            throw new Exception("Invalid encrypted envelope format: expected 4 frames");
        }
        
        String version = new String(payloadFrames.get(0), StandardCharsets.UTF_8);
        if (!SECURITY_ENVELOPE_VERSION.equals(version)) {
            throw new Exception("Encrypted envelope version mismatch");
        }
        
        byte[] iv = payloadFrames.get(1);
        byte[] tag = payloadFrames.get(2);
        byte[] ciphertext = payloadFrames.get(3);
        
        String aadStr = "znl-aad-v1|" + kind + "|" + senderNodeId + "|" + (requestId != null ? requestId : "");
        byte[] aad = aadStr.getBytes(StandardCharsets.UTF_8);
        
        return SecurityUtils.decryptFrames(encryptKey, iv, ciphertext, tag, aad);
    }

    private static class VerifyResult {
        boolean ok;
        String error;
        SecurityUtils.Envelope envelope;
    }

    private VerifyResult verifyIncomingProof(String kind, String proofToken, String requestId, List<byte[]> payloadFrames, String expectedNodeId, byte[] signKeyOverride) {
        VerifyResult res = new VerifyResult();
        if (!secureEnabled) {
            res.ok = true;
            return res;
        }
        byte[] verifyKey = signKeyOverride != null ? signKeyOverride : (keys != null ? keys.signKey : null);
        if (verifyKey == null) {
            res.ok = false; res.error = "Sign key not initialized"; return res;
        }
        if (proofToken == null || proofToken.isEmpty()) {
            res.ok = false; res.error = "Missing auth proof token"; return res;
        }
        
        try {
            SecurityUtils.Envelope env = SecurityUtils.decodeAuthProofToken(verifyKey, proofToken, options.getMaxTimeSkewMs(), System.currentTimeMillis());
            
            if (!kind.equals(env.kind)) {
                res.ok = false; res.error = "Proof kind mismatch: expected " + kind + ", got " + env.kind; return res;
            }
            if (!Objects.equals(requestId != null ? requestId : "", env.requestId != null ? env.requestId : "")) {
                res.ok = false; res.error = "Proof requestId mismatch"; return res;
            }
            if (expectedNodeId != null && !expectedNodeId.isEmpty() && !expectedNodeId.equals(env.nodeId)) {
                res.ok = false; res.error = "Proof nodeId mismatch: expected " + expectedNodeId + ", got " + env.nodeId; return res;
            }
            
            if (options.isEnablePayloadDigest()) {
                String currentDigest = SecurityUtils.digestFrames(payloadFrames);
                if (!currentDigest.equals(env.payloadDigest)) {
                    res.ok = false; res.error = "Payload digest mismatch"; return res;
                }
            }
            
            String replayKey = env.kind + "|" + env.nodeId + "|" + (env.requestId != null ? env.requestId : "") + "|" + env.nonce;
            if (replayGuard.seenOrAdd(replayKey, System.currentTimeMillis())) {
                res.ok = false; res.error = "Replay detected"; return res;
            }
            
            res.ok = true;
            res.envelope = env;
            return res;
            
        } catch (Exception e) {
            res.ok = false; res.error = "Proof validation failed: " + e.getMessage(); return res;
        }
    }

    // --- Heartbeat & Registration ---

    private void startHeartbeatTimer() {
        if (options.getHeartbeatInterval() <= 0) return;
        scheduleNextHeartbeat(0);
    }

    private void scheduleNextHeartbeat(long delayMs) {
        if (!running || !"slave".equals(options.getRole()) || options.getHeartbeatInterval() <= 0) return;
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel();
        }
        heartbeatTimer = new Timer("ZmqClusterNode-Heartbeat-" + options.getId(), true);
        heartbeatTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                sendHeartbeat();
            }
        }, Math.max(0L, delayMs));
    }

    private void markMasterOnline() {
        masterOnline = true;
        lastMasterSeenAt = System.currentTimeMillis();
    }

    private void markMasterOffline() {
        masterOnline = false;
        lastMasterSeenAt = 0L;
        heartbeatWaitingAck = false;
        if (heartbeatAckTimer != null) {
            heartbeatAckTimer.cancel();
            heartbeatAckTimer = null;
        }
    }

    private void confirmMasterReachable() {
        markMasterOnline();
        if (!heartbeatWaitingAck) {
            return;
        }
        heartbeatWaitingAck = false;
        if (heartbeatAckTimer != null) {
            heartbeatAckTimer.cancel();
            heartbeatAckTimer = null;
        }
        scheduleNextHeartbeat(options.getHeartbeatInterval());
    }

    private long resolveHeartbeatAckTimeoutMs() {
        if (options.getHeartbeatTimeoutMs() > 0) {
            return options.getHeartbeatTimeoutMs();
        }
        return Math.max(options.getHeartbeatInterval() * 2L, 1000L);
    }

    private void onHeartbeatAckTimeout() {
        if (!running || !"slave".equals(options.getRole()) || !heartbeatWaitingAck) {
            return;
        }
        markMasterOffline();
        restartDealer("heartbeat_ack_timeout");
    }

    private void startHeartbeatCheckTimer() {
        if (options.getHeartbeatInterval() <= 0) return;
        heartbeatCheckTimer = new Timer("ZmqClusterNode-HeartbeatCheck-" + options.getId(), true);
        heartbeatCheckTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                checkDeadSlaves();
            }
        }, options.getHeartbeatInterval(), options.getHeartbeatInterval());
    }

    private void sendHeartbeat() {
        submitTask(() -> {
            if (dealerSocket == null || heartbeatWaitingAck) return;
            String proof = secureEnabled ? createAuthProof("heartbeat", "", Collections.emptyList(), null) : "";
            heartbeatWaitingAck = true;
            if (heartbeatAckTimer != null) {
                heartbeatAckTimer.cancel();
            }
            heartbeatAckTimer = new Timer("ZmqClusterNode-HeartbeatAck-" + options.getId(), true);
            heartbeatAckTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    onHeartbeatAckTimeout();
                }
            }, resolveHeartbeatAckTimeoutMs());
            try {
                dealerSocket.sendMore(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                if (proof != null && !proof.isEmpty()) {
                    dealerSocket.sendMore(CONTROL_HEARTBEAT.getBytes(StandardCharsets.UTF_8));
                    dealerSocket.sendMore(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                    dealerSocket.send(proof.getBytes(StandardCharsets.UTF_8));
                } else {
                    dealerSocket.send(CONTROL_HEARTBEAT.getBytes(StandardCharsets.UTF_8));
                }
            } catch (Exception e) {
                onHeartbeatAckTimeout();
            }
        });
    }

    private void sendRegister() {
        submitTask(() -> {
            if (dealerSocket == null) return;
            String token = secureEnabled ? createAuthProof("register", "", Collections.emptyList(), null) : "";
            
            dealerSocket.sendMore(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
            if (token != null && !token.isEmpty()) {
                dealerSocket.sendMore(CONTROL_REGISTER.getBytes(StandardCharsets.UTF_8));
                dealerSocket.sendMore(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                dealerSocket.send(token.getBytes(StandardCharsets.UTF_8));
            } else {
                dealerSocket.send(CONTROL_REGISTER.getBytes(StandardCharsets.UTF_8));
            }
        });
    }

    private void sendUnregister() {
        submitTask(() -> {
            if (dealerSocket == null) return;
            dealerSocket.sendMore(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
            dealerSocket.send(CONTROL_UNREGISTER.getBytes(StandardCharsets.UTF_8));
        });
    }

    private void checkDeadSlaves() {
        long now = System.currentTimeMillis();
        long timeout = options.getHeartbeatTimeoutMs() > 0 ? options.getHeartbeatTimeoutMs() : options.getHeartbeatInterval() * 3L;
        Iterator<Map.Entry<String, Long>> it = slaves.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> entry = it.next();
            if (now - entry.getValue() > timeout) {
                String deadId = entry.getKey();
                it.remove();
                logger.warn("[ZNL-Master] Slave {} timeout, removed.", deadId);
                emitSlaveDisconnected(deadId);
            }
        }
    }

    private void rejectPendingRequests(Exception error) {
        Iterator<Map.Entry<String, PendingRequest>> it = pending.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, PendingRequest> entry = it.next();
            it.remove();
            entry.getValue().future.completeExceptionally(error);
        }
    }

    private void restartDealer(String reason) {
        if (!running || !"slave".equals(options.getRole()) || dealerRestarting) {
            return;
        }
        dealerRestarting = true;
        submitTask(() -> {
            try {
                markMasterOffline();
                masterNodeId = null;
                rejectPendingRequests(new Exception("Dealer restarted: " + reason));
                discardQueuedTasks(new Exception("Queued task discarded: " + reason));
                String endpoint = options.getEndpoints().get("router");
                if (endpoint == null) {
                    endpoint = "tcp://127.0.0.1:6003";
                }
                ZMQ.Socket oldDealer = dealerSocket;
                dealerSocket = null;
                closeSocket(oldDealer);
                dealerSocket = createDealerSocket(endpoint);
                sendRegister();
                startHeartbeatTimer();
            } finally {
                dealerRestarting = false;
            }
        });
    }

    private void discardQueuedTasks(Exception error) {
        QueuedTask queuedTask;
        while ((queuedTask = taskQueue.poll()) != null) {
            try {
                queuedTask.onDrop.run();
            } catch (Exception e) {
                emitError(e);
            }
        }
    }

    private void ensureSlaveOnline(String identityText) {
        boolean existed = slaves.containsKey(identityText);
        slaves.put(identityText, System.currentTimeMillis());
        if (!existed) {
            logger.info("[ZNL-Master] Slave connected: {}", identityText);
            emitSlaveConnected(identityText);
        }
    }

    private void sendHeartbeatAck(byte[] identity, String identityText, byte[] signKeyOverride) {
        if (routerSocket == null) return;
        String proof = secureEnabled ? createAuthProof("heartbeat_ack", "", Collections.emptyList(), signKeyOverride) : "";
        try {
            routerSocket.sendMore(identity);
            routerSocket.sendMore(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
            if (proof != null && !proof.isEmpty()) {
                routerSocket.sendMore(CONTROL_HEARTBEAT_ACK.getBytes(StandardCharsets.UTF_8));
                routerSocket.sendMore(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                routerSocket.send(proof.getBytes(StandardCharsets.UTF_8));
            } else {
                routerSocket.send(CONTROL_HEARTBEAT_ACK.getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            logger.debug("[ZNL-Master] Failed to send heartbeat_ack to {}", identityText, e);
        }
    }

    private void handleRouterFrames(List<byte[]> frames) {
        if (frames.isEmpty()) return;
        byte[] identity = frames.get(0);
        String identityText = new String(identity, StandardCharsets.UTF_8);

        List<byte[]> bodyFrames = frames.size() > 1 ? frames.subList(1, frames.size()) : new ArrayList<>();
        ParsedFrames parsed = parseControlFrames(bodyFrames);
        
        if (CONTROL_HEARTBEAT.equals(parsed.kind)) {
            byte[] ackSignKey = null;
            if (secureEnabled) {
                SecurityUtils.Keys sKeys = resolveSlaveKeys(identityText);
                if (sKeys == null) {
                    emitAuthFailed(new ZmqEvent("router", frames, "heartbeat", null, parsed.authProof, Collections.emptyList(), EMPTY_BUFFER, identity, identityText, null), "No authKey configured for slave");
                    if (slaves.remove(identityText) != null) emitSlaveDisconnected(identityText);
                    return;
                }
                VerifyResult v = verifyIncomingProof("heartbeat", parsed.authProof, "", Collections.emptyList(), identityText, sKeys.signKey);
                if (!v.ok) {
                    emitAuthFailed(new ZmqEvent("router", frames, "heartbeat", null, parsed.authProof, Collections.emptyList(), EMPTY_BUFFER, identity, identityText, null), v.error);
                    return;
                }
                ackSignKey = sKeys.signKey;
            }
            ensureSlaveOnline(identityText);
            sendHeartbeatAck(identity, identityText, ackSignKey);
            return;
        }
        
        if (CONTROL_REGISTER.equals(parsed.kind)) {
            if (secureEnabled) {
                SecurityUtils.Keys sKeys = resolveSlaveKeys(identityText);
                if (sKeys == null) {
                    emitAuthFailed(new ZmqEvent("router", frames, "register", null, parsed.authKey, Collections.emptyList(), EMPTY_BUFFER, identity, identityText, null), "No authKey configured for slave");
                    if (slaves.remove(identityText) != null) emitSlaveDisconnected(identityText);
                    return;
                }
                VerifyResult v = verifyIncomingProof("register", parsed.authKey, "", Collections.emptyList(), identityText, sKeys.signKey);
                if (!v.ok) {
                    emitAuthFailed(new ZmqEvent("router", frames, "register", null, parsed.authKey, Collections.emptyList(), EMPTY_BUFFER, identity, identityText, null), v.error);
                    return;
                }
            }
            ensureSlaveOnline(identityText);
            return;
        }
        
        if (CONTROL_UNREGISTER.equals(parsed.kind)) {
            if (slaves.remove(identityText) != null) {
                logger.info("[ZNL-Master] Slave unregistered: {}", identityText);
                emitSlaveDisconnected(identityText);
            }
            return;
        }

        List<byte[]> finalFrames = parsed.payloadFrames;
        byte[] payload = EMPTY_BUFFER;
        
        if ("request".equals(parsed.kind) || "response".equals(parsed.kind) || "push".equals(parsed.kind) || "service_request".equals(parsed.kind) || "service_response".equals(parsed.kind)) {
            if (secureEnabled) {
                SecurityUtils.Keys sKeys = resolveSlaveKeys(identityText);
                if (sKeys == null) {
                    emitAuthFailed(new ZmqEvent("router", frames, parsed.kind, parsed.requestId, "request".equals(parsed.kind) ? parsed.authKey : parsed.authProof, parsed.payloadFrames, EMPTY_BUFFER, identity, identityText, parsed.topic), "No authKey configured for slave");
                    if (slaves.remove(identityText) != null) emitSlaveDisconnected(identityText);
                    return;
                }
                String proof = "request".equals(parsed.kind) ? parsed.authKey : parsed.authProof;
                String proofKind = "service_response".equals(parsed.kind) ? "response" : parsed.kind;
                if ("service_request".equals(parsed.kind)) {
                    proofKind = "request";
                }
                String requestKey;
                if ("push".equals(parsed.kind)) {
                    requestKey = parsed.topic;
                } else if ("service_request".equals(parsed.kind)) {
                    requestKey = "svc:" + parsed.service + ":" + parsed.requestId;
                } else if ("service_response".equals(parsed.kind)) {
                    requestKey = "svc:" + parsed.service + ":" + parsed.requestId;
                } else {
                    requestKey = parsed.requestId;
                }
                VerifyResult v = verifyIncomingProof(proofKind, proof, requestKey, parsed.payloadFrames, identityText, sKeys.signKey);
                if (!v.ok) {
                    emitAuthFailed(new ZmqEvent("router", frames, parsed.kind, parsed.requestId, proof, parsed.payloadFrames, EMPTY_BUFFER, identity, identityText, parsed.topic), v.error);
                    return;
                }
                ensureSlaveOnline(identityText);
                
                if (options.isEncrypted()) {
                    try {
                        finalFrames = openPayloadFrames(proofKind, requestKey, parsed.payloadFrames, identityText, sKeys.encryptKey);
                    } catch (Exception e) {
                        emitAuthFailed(new ZmqEvent("router", frames, parsed.kind, parsed.requestId, proof, parsed.payloadFrames, EMPTY_BUFFER, identity, identityText, parsed.topic), "Payload decrypt failed: " + e.getMessage());
                        return;
                    }
                }
            } else {
                ensureSlaveOnline(identityText);
            }
        }
        
        payload = payloadFromFrames(finalFrames);
        ZmqEvent event = new ZmqEvent("router", frames, parsed.kind, parsed.requestId, parsed.authKey, finalFrames, payload, identity, identityText, parsed.topic, parsed.service, null, false);
        event = emitMessage(event);

        if ("request".equals(parsed.kind)) {
            emitRequest(event);
            if (routerAutoMultipartHandler != null) {
                try {
                    routerAutoMultipartHandler.handle(event).thenAccept(replyFrames -> {
                        replyTo(identity, parsed.requestId, replyFrames);
                    }).exceptionally(ex -> {
                        emitError(ex);
                        return null;
                    });
                } catch (Exception e) {
                    emitError(e);
                }
            } else if (routerAutoHandler != null) {
                try {
                    routerAutoHandler.handle(event).thenAccept(replyPayload -> {
                        replyTo(identity, parsed.requestId, replyPayload);
                    }).exceptionally(ex -> {
                        emitError(ex);
                        return null;
                    });
                } catch (Exception e) {
                    emitError(e);
                }
            }
        } else if ("response".equals(parsed.kind)) {
            String key = pendingKey(parsed.requestId, identityText);
            PendingRequest req = pending.remove(key);
            if (req != null) {
                req.future.complete(finalFrames);
            }
            emitResponse(event);
        } else if ("push".equals(parsed.kind)) {
            emitPush(event);
        } else if ("service_request".equals(parsed.kind)) {
            emitServiceRequest(event);
            ZmqMultipartRequestHandler handler = serviceHandlers.get(parsed.service);
            if (handler != null) {
                try {
                    handler.handle(event).thenAccept(replyFrames -> {
                        replyServiceTo(identity, parsed.requestId, parsed.service, replyFrames);
                    }).exceptionally(ex -> {
                        emitError(ex);
                        replyServiceTo(identity, parsed.requestId, parsed.service, buildServiceErrorPayload(ex));
                        return null;
                    });
                } catch (Exception e) {
                    emitError(e);
                    replyServiceTo(identity, parsed.requestId, parsed.service, buildServiceErrorPayload(e));
                }
            }
        } else if ("service_response".equals(parsed.kind)) {
            String key = pendingKey("svc:" + parsed.service + ":" + parsed.requestId, identityText);
            PendingRequest req = pending.remove(key);
            if (req != null) {
                req.future.complete(finalFrames);
            }
            emitServiceResponse(event);
        }
    }

    private void handleDealerFrames(List<byte[]> frames) {
        if (frames.isEmpty()) return;
        ParsedFrames parsed = parseControlFrames(frames);
        
        List<byte[]> finalFrames = parsed.payloadFrames;
        byte[] payload = EMPTY_BUFFER;
        
        if (CONTROL_HEARTBEAT_ACK.equals(parsed.kind)) {
            if (secureEnabled) {
                VerifyResult v = verifyIncomingProof("heartbeat_ack", parsed.authProof, "", Collections.emptyList(), masterNodeId, null);
                if (!v.ok) {
                    emitAuthFailed(new ZmqEvent("dealer", frames, "heartbeat_ack", null, parsed.authProof, Collections.emptyList(), EMPTY_BUFFER, null, null, null), v.error);
                    return;
                }
                if (masterNodeId == null && v.envelope != null) {
                    masterNodeId = v.envelope.nodeId;
                }
            }
            confirmMasterReachable();
            return;
        }
        
        if ("publish".equals(parsed.kind) || "request".equals(parsed.kind) || "response".equals(parsed.kind) || "service_request".equals(parsed.kind)) {
            if (secureEnabled) {
                String proof = "request".equals(parsed.kind) ? parsed.authKey : parsed.authProof;
                String reqId;
                String verifyKind;
                if ("publish".equals(parsed.kind)) {
                    reqId = parsed.topic;
                    verifyKind = "publish";
                } else if ("service_request".equals(parsed.kind)) {
                    reqId = "svc:" + parsed.service + ":" + parsed.requestId;
                    verifyKind = "request";
                } else {
                    reqId = parsed.requestId;
                    verifyKind = parsed.kind;
                }
                VerifyResult v = verifyIncomingProof(verifyKind, proof, reqId, parsed.payloadFrames, masterNodeId, null);
                if (!v.ok) {
                    emitAuthFailed(new ZmqEvent("dealer", frames, parsed.kind, parsed.requestId, proof, parsed.payloadFrames, EMPTY_BUFFER, null, null, parsed.topic), v.error);
                    return;
                }
                if (masterNodeId == null && v.envelope != null) {
                    masterNodeId = v.envelope.nodeId;
                }
                if (options.isEncrypted()) {
                    try {
                        String expectedNodeId = "publish".equals(parsed.kind) ? v.envelope.nodeId : masterNodeId;
                        finalFrames = openPayloadFrames(verifyKind, reqId, parsed.payloadFrames, expectedNodeId, null);
                    } catch (Exception e) {
                        emitAuthFailed(new ZmqEvent("dealer", frames, parsed.kind, parsed.requestId, proof, parsed.payloadFrames, EMPTY_BUFFER, null, null, parsed.topic), "decryption failed: " + e.getMessage());
                        return;
                    }
                }
            }
            confirmMasterReachable();
        }

        payload = payloadFromFrames(finalFrames);
        ZmqEvent event = new ZmqEvent("dealer", frames, parsed.kind, parsed.requestId, parsed.authKey, finalFrames, payload, null, null, parsed.topic, parsed.service, null, false);
        event = emitMessage(event);

        if ("request".equals(parsed.kind)) {
            emitRequest(event);
            if (dealerAutoMultipartHandler != null) {
                try {
                    dealerAutoMultipartHandler.handle(event).thenAccept(replyFrames -> {
                        reply(parsed.requestId, replyFrames);
                    }).exceptionally(ex -> {
                        emitError(ex);
                        return null;
                    });
                } catch (Exception e) {
                    emitError(e);
                }
            } else if (dealerAutoHandler != null) {
                try {
                    dealerAutoHandler.handle(event).thenAccept(replyPayload -> {
                        reply(parsed.requestId, replyPayload);
                    }).exceptionally(ex -> {
                        emitError(ex);
                        return null;
                    });
                } catch (Exception e) {
                    emitError(e);
                }
            }
        } else if ("response".equals(parsed.kind)) {
            String key = pendingKey(parsed.requestId, null);
            PendingRequest req = pending.remove(key);
            if (req != null) {
                req.future.complete(finalFrames);
            }
            emitResponse(event);
        } else if ("publish".equals(parsed.kind)) {
            emitPublish(event);
            java.util.function.Consumer<ZmqEvent> sub = subscriptions.get(parsed.topic);
            if (sub != null) {
                try { sub.accept(event); } catch (Exception e) { emitError(e); }
            }
        } else if ("service_request".equals(parsed.kind)) {
            emitServiceRequest(event);
            ZmqMultipartRequestHandler handler = serviceHandlers.get(parsed.service);
            if (handler != null) {
                try {
                    handler.handle(event).thenAccept(replyFrames -> {
                        replyService(parsed.requestId, parsed.service, replyFrames);
                    }).exceptionally(ex -> {
                        emitError(ex);
                        replyService(parsed.requestId, parsed.service, buildServiceErrorPayload(ex));
                        return null;
                    });
                } catch (Exception e) {
                    emitError(e);
                    replyService(parsed.requestId, parsed.service, buildServiceErrorPayload(e));
                }
            }
        }
    }

    private static class ParsedFrames {
        String kind = "message";
        String requestId = null;
        String authKey = null;
        String authProof = null;
        String topic = null;
        String service = null;
        List<byte[]> payloadFrames = new ArrayList<>();
    }

    private ParsedFrames parseControlFrames(List<byte[]> frames) {
        ParsedFrames parsed = new ParsedFrames();
        if (frames.size() >= 2) {
            String prefix = new String(frames.get(0), StandardCharsets.UTF_8);
            if (CONTROL_PREFIX.equals(prefix)) {
                String action = new String(frames.get(1), StandardCharsets.UTF_8);
                
                if (CONTROL_HEARTBEAT.equals(action)) {
                    parsed.kind = action;
                    if (frames.size() >= 4 && CONTROL_AUTH.equals(new String(frames.get(2), StandardCharsets.UTF_8))) {
                        parsed.authProof = new String(frames.get(3), StandardCharsets.UTF_8);
                    }
                    parsed.payloadFrames = Collections.emptyList();
                    return parsed;
                }

                if (CONTROL_HEARTBEAT_ACK.equals(action)) {
                    parsed.kind = action;
                    if (frames.size() >= 4 && CONTROL_AUTH.equals(new String(frames.get(2), StandardCharsets.UTF_8))) {
                        parsed.authProof = new String(frames.get(3), StandardCharsets.UTF_8);
                    }
                    parsed.payloadFrames = Collections.emptyList();
                    return parsed;
                }
                
                if (CONTROL_UNREGISTER.equals(action)) {
                    parsed.kind = action;
                    parsed.payloadFrames = Collections.emptyList();
                    return parsed;
                }
                
                if (CONTROL_REGISTER.equals(action)) {
                    parsed.kind = action;
                    if (frames.size() >= 4 && CONTROL_AUTH.equals(new String(frames.get(2), StandardCharsets.UTF_8))) {
                        parsed.authKey = new String(frames.get(3), StandardCharsets.UTF_8);
                    }
                    parsed.payloadFrames = Collections.emptyList();
                    return parsed;
                }
                
                if (CONTROL_PUB.equals(action)) {
                    parsed.kind = "publish";
                    if (frames.size() >= 3) {
                        parsed.topic = new String(frames.get(2), StandardCharsets.UTF_8);
                        int payloadStart = 3;
                        if (frames.size() >= 5 && CONTROL_AUTH.equals(new String(frames.get(3), StandardCharsets.UTF_8))) {
                            parsed.authProof = new String(frames.get(4), StandardCharsets.UTF_8);
                            payloadStart = 5;
                        }
                        parsed.payloadFrames = frames.size() > payloadStart ? frames.subList(payloadStart, frames.size()) : Collections.emptyList();
                    }
                    return parsed;
                }

                if (CONTROL_PUSH.equals(action)) {
                    parsed.kind = "push";
                    if (frames.size() >= 3) {
                        parsed.topic = new String(frames.get(2), StandardCharsets.UTF_8);
                        int payloadStart = 3;
                        if (frames.size() >= 5 && CONTROL_AUTH.equals(new String(frames.get(3), StandardCharsets.UTF_8))) {
                            parsed.authProof = new String(frames.get(4), StandardCharsets.UTF_8);
                            payloadStart = 5;
                        }
                        parsed.payloadFrames = frames.size() > payloadStart ? frames.subList(payloadStart, frames.size()) : Collections.emptyList();
                    }
                    return parsed;
                }

                if ((CONTROL_SVC_REQ.equals(action) || CONTROL_SVC_RES.equals(action)) && frames.size() >= 4) {
                    String requestId = new String(frames.get(2), StandardCharsets.UTF_8);
                    String service = new String(frames.get(3), StandardCharsets.UTF_8);
                    if (!requestId.isEmpty() && !service.isEmpty()) {
                        int payloadStart = 4;
                        if (frames.size() >= 6 && CONTROL_AUTH.equals(new String(frames.get(4), StandardCharsets.UTF_8))) {
                            parsed.authProof = new String(frames.get(5), StandardCharsets.UTF_8);
                            payloadStart = 6;
                        }
                        parsed.kind = CONTROL_SVC_REQ.equals(action) ? "service_request" : "service_response";
                        parsed.requestId = requestId;
                        parsed.service = service;
                        parsed.payloadFrames = frames.size() > payloadStart ? frames.subList(payloadStart, frames.size()) : Collections.emptyList();
                        return parsed;
                    }
                }
                
                if (frames.size() >= 3) {
                    String requestId = new String(frames.get(2), StandardCharsets.UTF_8);
                    
                    if ((CONTROL_REQ.equals(action) || CONTROL_RES.equals(action)) && !requestId.isEmpty()) {
                        int payloadStart = 3;
                        
                        if (CONTROL_REQ.equals(action) && frames.size() >= 5 && CONTROL_AUTH.equals(new String(frames.get(3), StandardCharsets.UTF_8))) {
                            parsed.authKey = new String(frames.get(4), StandardCharsets.UTF_8);
                            payloadStart = 5;
                        }
                        if (CONTROL_RES.equals(action) && frames.size() >= 5 && CONTROL_AUTH.equals(new String(frames.get(3), StandardCharsets.UTF_8))) {
                            parsed.authProof = new String(frames.get(4), StandardCharsets.UTF_8);
                            payloadStart = 5;
                        }
                        
                        parsed.kind = CONTROL_REQ.equals(action) ? "request" : "response";
                        parsed.requestId = requestId;
                        parsed.payloadFrames = frames.size() > payloadStart ? frames.subList(payloadStart, frames.size()) : Collections.emptyList();
                        return parsed;
                    }
                }
            }
        }
        parsed.payloadFrames = frames;
        return parsed;
    }

    private byte[] payloadFromFrames(List<byte[]> frames) {
        if (frames == null || frames.isEmpty()) return EMPTY_BUFFER;
        return frames.get(0); 
    }

    private List<byte[]> singlePayloadToFrames(byte[] payload) {
        List<byte[]> frames = new ArrayList<>();
        if (payload != null) {
            frames.add(payload);
        }
        return frames;
    }

    // --- Public API ---

    public ZmqClusterNode registerService(String service, ZmqMultipartRequestHandler handler) {
        if (service == null || service.isEmpty()) {
            throw new IllegalArgumentException("service cannot be empty");
        }
        if (handler == null) {
            throw new IllegalArgumentException("service handler cannot be null");
        }
        serviceHandlers.put(service, handler);
        return this;
    }

    public ZmqClusterNode unregisterService(String service) {
        if (service == null || service.isEmpty()) {
            throw new IllegalArgumentException("service cannot be empty");
        }
        serviceHandlers.remove(service);
        return this;
    }

    public CompletableFuture<Void> publish(String topic, List<byte[]> payloadFrames) {
        if (!"master".equals(options.getRole())) {
            throw new IllegalStateException("publish() can only be called on master");
        }
        final String finalTopic = topic == null ? DEFAULT_TOPIC : topic;
        byte[] t = finalTopic.getBytes(StandardCharsets.UTF_8);
        
        CompletableFuture<Void> f = new CompletableFuture<>();
        submitTask(() -> {
            try {
                if (routerSocket == null) throw new IllegalStateException("ROUTER socket not ready");
                if (slaves.isEmpty()) {
                    f.complete(null);
                    return;
                }
                
                List<String> currentSlaves = new ArrayList<>(slaves.keySet());
                for (String slaveId : currentSlaves) {
                    SecurityUtils.Keys sKeys = null;
                    if (secureEnabled) {
                        sKeys = resolveSlaveKeys(slaveId);
                        if (sKeys == null) {
                            if (slaves.remove(slaveId) != null) emitSlaveDisconnected(slaveId);
                            continue;
                        }
                    }
                    
                    List<byte[]> sealedPayloadFrames = sealPayloadFrames("publish", finalTopic, payloadFrames, sKeys != null ? sKeys.encryptKey : null);
                    String proof = secureEnabled ? createAuthProof("publish", finalTopic, sealedPayloadFrames, sKeys != null ? sKeys.signKey : null) : "";
                    
                    List<byte[]> frames = new ArrayList<>();
                    frames.add(slaveId.getBytes(StandardCharsets.UTF_8));
                    frames.add(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                    frames.add(CONTROL_PUB.getBytes(StandardCharsets.UTF_8));
                    frames.add(t);
                    if (proof != null && !proof.isEmpty()) {
                        frames.add(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                        frames.add(proof.getBytes(StandardCharsets.UTF_8));
                    }
                    frames.addAll(sealedPayloadFrames);
                    
                    for (int i = 0; i < frames.size(); i++) {
                        if (i == frames.size() - 1) routerSocket.send(frames.get(i));
                        else routerSocket.sendMore(frames.get(i));
                    }
                }
                f.complete(null);
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        });
        return f;
    }

    public CompletableFuture<Void> publish(String topic, byte[] payload) {
        return publish(topic, singlePayloadToFrames(payload));
    }

    public CompletableFuture<Void> PUBLISH(String topic, byte[] payload) {
        return publish(topic, payload);
    }

    public CompletableFuture<Void> PUSH(String topic, List<byte[]> payloadFrames) {
        if (!"slave".equals(options.getRole())) {
            throw new IllegalStateException("PUSH() can only be called on slave");
        }
        final String finalTopic = topic == null ? DEFAULT_TOPIC : topic;
        byte[] t = finalTopic.getBytes(StandardCharsets.UTF_8);

        CompletableFuture<Void> f = new CompletableFuture<>();
        submitTask(() -> {
            try {
                if (dealerSocket == null) throw new IllegalStateException("DEALER socket not ready");

                List<byte[]> sealedPayloadFrames = sealPayloadFrames("push", finalTopic, payloadFrames, null);
                String proof = secureEnabled ? createAuthProof("push", finalTopic, sealedPayloadFrames, null) : "";

                List<byte[]> frames = new ArrayList<>();
                frames.add(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_PUSH.getBytes(StandardCharsets.UTF_8));
                frames.add(t);
                if (proof != null && !proof.isEmpty()) {
                    frames.add(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                    frames.add(proof.getBytes(StandardCharsets.UTF_8));
                }
                frames.addAll(sealedPayloadFrames);

                for (int i = 0; i < frames.size(); i++) {
                    if (i == frames.size() - 1) dealerSocket.send(frames.get(i));
                    else dealerSocket.sendMore(frames.get(i));
                }
                f.complete(null);
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        }, () -> f.completeExceptionally(new Exception("PUSH task discarded before send")));
        return f;
    }

    public CompletableFuture<Void> PUSH(String topic, byte[] payload) {
        return PUSH(topic, singlePayloadToFrames(payload));
    }

    public CompletableFuture<Void> push(String topic, byte[] payload) {
        return PUSH(topic, payload);
    }

    public void subscribe(String topic, java.util.function.Consumer<ZmqEvent> handler) {
        if (!"slave".equals(options.getRole())) {
            throw new IllegalStateException("subscribe() can only be called on slave");
        }
        subscriptions.put(topic, handler);
    }

    public void SUBSCRIBE(String topic, java.util.function.Consumer<ZmqEvent> handler) {
        subscribe(topic, handler);
    }

    public void unsubscribe(String topic) {
        if (!"slave".equals(options.getRole())) {
            throw new IllegalStateException("unsubscribe() can only be called on slave");
        }
        subscriptions.remove(topic);
    }

    public void UNSUBSCRIBE(String topic) {
        unsubscribe(topic);
    }

    public List<String> getSlaves() {
        return new ArrayList<>(slaves.keySet());
    }

    public void DEALER(ZmqRequestHandler handler) {
        if (!"slave".equals(options.getRole())) {
            throw new IllegalStateException("DEALER(handler) can only be called on slave");
        }
        this.dealerAutoHandler = handler;
        this.dealerAutoMultipartHandler = null;
    }

    public void DEALER_MULTIPART(ZmqMultipartRequestHandler handler) {
        if (!"slave".equals(options.getRole())) {
            throw new IllegalStateException("DEALER_MULTIPART(handler) can only be called on slave");
        }
        this.dealerAutoMultipartHandler = handler;
        this.dealerAutoHandler = null;
    }

    public CompletableFuture<List<byte[]>> DEALER(List<byte[]> payloadFrames, int timeoutMs) {
        if (!"slave".equals(options.getRole())) {
            throw new IllegalStateException("DEALER(request) can only be called on slave");
        }
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        ensurePendingCapacity();
        String requestId = UUID.randomUUID().toString();
        String key = pendingKey(requestId, null);

        PendingRequest req = new PendingRequest();
        req.future = future;
        req.requestId = requestId;
        req.timeoutMs = timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS;
        
        submitTask(() -> {
            try {
                if (dealerSocket == null) throw new IllegalStateException("DEALER socket not ready");
                pending.put(key, req);
                req.expireTimeMs = System.currentTimeMillis() + req.timeoutMs;
                
                List<byte[]> sealedPayloadFrames = sealPayloadFrames("request", requestId, payloadFrames, null);
                String proofOrAuthKey = secureEnabled ? createAuthProof("request", requestId, sealedPayloadFrames, null) : "";
                
                List<byte[]> frames = new ArrayList<>();
                frames.add(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_REQ.getBytes(StandardCharsets.UTF_8));
                frames.add(requestId.getBytes(StandardCharsets.UTF_8));
                if (proofOrAuthKey != null && !proofOrAuthKey.isEmpty()) {
                    frames.add(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                    frames.add(proofOrAuthKey.getBytes(StandardCharsets.UTF_8));
                }
                frames.addAll(sealedPayloadFrames);

                for (int i = 0; i < frames.size(); i++) {
                    if (i == frames.size() - 1) dealerSocket.send(frames.get(i));
                    else dealerSocket.sendMore(frames.get(i));
                }
            } catch (Exception e) {
                pending.remove(key);
                future.completeExceptionally(e);
            }
        }, () -> {
            pending.remove(key);
            future.completeExceptionally(new Exception("DEALER request discarded before send"));
        });
        return future;
    }

    public CompletableFuture<byte[]> DEALER(byte[] payload, int timeoutMs) {
        return DEALER(singlePayloadToFrames(payload), timeoutMs).thenApply(this::payloadFromFrames);
    }

    public void ROUTER(ZmqRequestHandler handler) {
        if (!"master".equals(options.getRole())) {
            throw new IllegalStateException("ROUTER(handler) can only be called on master");
        }
        this.routerAutoHandler = handler;
        this.routerAutoMultipartHandler = null;
    }

    public void ROUTER_MULTIPART(ZmqMultipartRequestHandler handler) {
        if (!"master".equals(options.getRole())) {
            throw new IllegalStateException("ROUTER_MULTIPART(handler) can only be called on master");
        }
        this.routerAutoMultipartHandler = handler;
        this.routerAutoHandler = null;
    }

    public CompletableFuture<List<byte[]>> ROUTER(String identity, List<byte[]> payloadFrames, int timeoutMs) {
        if (!"master".equals(options.getRole())) {
            throw new IllegalStateException("ROUTER(request) can only be called on master");
        }
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        ensurePendingCapacity();
        String requestId = UUID.randomUUID().toString();
        String key = pendingKey(requestId, identity);

        PendingRequest req = new PendingRequest();
        req.future = future;
        req.requestId = requestId;
        req.identityText = identity;
        req.timeoutMs = timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS;

        submitTask(() -> {
            try {
                if (routerSocket == null) throw new IllegalStateException("ROUTER socket not ready");
                
                SecurityUtils.Keys sKeys = null;
                if (secureEnabled) {
                    sKeys = resolveSlaveKeys(identity);
                    if (sKeys == null) {
                        future.completeExceptionally(new Exception("No authKey configured for slave: " + identity));
                        return;
                    }
                }
                
                pending.put(key, req);
                req.expireTimeMs = System.currentTimeMillis() + req.timeoutMs;
                
                List<byte[]> sealedPayloadFrames = sealPayloadFrames("request", requestId, payloadFrames, sKeys != null ? sKeys.encryptKey : null);
                String proofOrAuthKey = secureEnabled ? createAuthProof("request", requestId, sealedPayloadFrames, sKeys.signKey) : "";
                
                List<byte[]> frames = new ArrayList<>();
                frames.add(identity.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_REQ.getBytes(StandardCharsets.UTF_8));
                frames.add(requestId.getBytes(StandardCharsets.UTF_8));
                if (proofOrAuthKey != null && !proofOrAuthKey.isEmpty()) {
                    frames.add(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                    frames.add(proofOrAuthKey.getBytes(StandardCharsets.UTF_8));
                }
                frames.addAll(sealedPayloadFrames);

                for (int i = 0; i < frames.size(); i++) {
                    if (i == frames.size() - 1) routerSocket.send(frames.get(i));
                    else routerSocket.sendMore(frames.get(i));
                }
            } catch (Exception e) {
                pending.remove(key);
                future.completeExceptionally(e);
            }
        }, () -> {
            pending.remove(key);
            future.completeExceptionally(new Exception("ROUTER request discarded before send"));
        });
        return future;
    }

    public CompletableFuture<byte[]> ROUTER(String identity, byte[] payload, int timeoutMs) {
        return ROUTER(identity, singlePayloadToFrames(payload), timeoutMs).thenApply(this::payloadFromFrames);
    }

    public CompletableFuture<List<byte[]>> SERVICE(String identity, String service, List<byte[]> payloadFrames, int timeoutMs) {
        if (!"master".equals(options.getRole())) {
            throw new IllegalStateException("SERVICE(request) can only be called on master");
        }
        if (identity == null || identity.isEmpty()) {
            throw new IllegalArgumentException("identity cannot be empty");
        }
        if (service == null || service.isEmpty()) {
            throw new IllegalArgumentException("service cannot be empty");
        }

        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        ensurePendingCapacity();
        String requestId = UUID.randomUUID().toString();
        String securityRequestId = "svc:" + service + ":" + requestId;
        String key = pendingKey(securityRequestId, identity);

        PendingRequest req = new PendingRequest();
        req.future = future;
        req.requestId = securityRequestId;
        req.identityText = identity;
        req.timeoutMs = timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS;

        submitTask(() -> {
            try {
                if (routerSocket == null) throw new IllegalStateException("ROUTER socket not ready");

                SecurityUtils.Keys sKeys = null;
                if (secureEnabled) {
                    sKeys = resolveSlaveKeys(identity);
                    if (sKeys == null) {
                        future.completeExceptionally(new Exception("No authKey configured for slave: " + identity));
                        return;
                    }
                }

                pending.put(key, req);
                req.expireTimeMs = System.currentTimeMillis() + req.timeoutMs;

                List<byte[]> sealedPayloadFrames = sealPayloadFrames("request", securityRequestId, payloadFrames, sKeys != null ? sKeys.encryptKey : null);
                String proof = secureEnabled ? createAuthProof("request", securityRequestId, sealedPayloadFrames, sKeys.signKey) : "";

                List<byte[]> frames = new ArrayList<>();
                frames.add(identity.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_SVC_REQ.getBytes(StandardCharsets.UTF_8));
                frames.add(requestId.getBytes(StandardCharsets.UTF_8));
                frames.add(service.getBytes(StandardCharsets.UTF_8));
                if (proof != null && !proof.isEmpty()) {
                    frames.add(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                    frames.add(proof.getBytes(StandardCharsets.UTF_8));
                }
                frames.addAll(sealedPayloadFrames);

                for (int i = 0; i < frames.size(); i++) {
                    if (i == frames.size() - 1) routerSocket.send(frames.get(i));
                    else routerSocket.sendMore(frames.get(i));
                }
            } catch (Exception e) {
                pending.remove(key);
                future.completeExceptionally(e);
            }
        }, () -> {
            pending.remove(key);
            future.completeExceptionally(new Exception("SERVICE request discarded before send"));
        });
        return future;
    }

    public CompletableFuture<byte[]> SERVICE(String identity, String service, byte[] payload, int timeoutMs) {
        return SERVICE(identity, service, singlePayloadToFrames(payload), timeoutMs).thenApply(this::payloadFromFrames);
    }

    private void reply(String requestId, List<byte[]> payloadFrames) {
        submitTask(() -> {
            if (dealerSocket == null) return;
            try {
                List<byte[]> sealedPayloadFrames = sealPayloadFrames("response", requestId, payloadFrames, null);
                String proof = secureEnabled ? createAuthProof("response", requestId, sealedPayloadFrames, null) : "";
                
                List<byte[]> frames = new ArrayList<>();
                frames.add(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_RES.getBytes(StandardCharsets.UTF_8));
                frames.add(requestId.getBytes(StandardCharsets.UTF_8));
                if (proof != null && !proof.isEmpty()) {
                    frames.add(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                    frames.add(proof.getBytes(StandardCharsets.UTF_8));
                }
                frames.addAll(sealedPayloadFrames);
                
                for (int i = 0; i < frames.size(); i++) {
                    if (i == frames.size() - 1) dealerSocket.send(frames.get(i));
                    else dealerSocket.sendMore(frames.get(i));
                }
            } catch (Exception e) {
                logger.error("Failed to send reply", e);
            }
        });
    }

    private void reply(String requestId, byte[] payload) {
        reply(requestId, singlePayloadToFrames(payload));
    }

    private void replyTo(byte[] identity, String requestId, List<byte[]> payloadFrames) {
        submitTask(() -> {
            if (routerSocket == null) return;
            try {
                String identityText = new String(identity, StandardCharsets.UTF_8);
                SecurityUtils.Keys sKeys = null;
                if (secureEnabled) {
                    sKeys = resolveSlaveKeys(identityText);
                    if (sKeys == null) {
                        logger.error("Failed to send replyTo: No authKey configured for slave {}", identityText);
                        return;
                    }
                }
                
                List<byte[]> sealedPayloadFrames = sealPayloadFrames("response", requestId, payloadFrames, sKeys != null ? sKeys.encryptKey : null);
                String proof = secureEnabled ? createAuthProof("response", requestId, sealedPayloadFrames, sKeys.signKey) : "";
                
                List<byte[]> frames = new ArrayList<>();
                frames.add(identity);
                frames.add(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_RES.getBytes(StandardCharsets.UTF_8));
                frames.add(requestId.getBytes(StandardCharsets.UTF_8));
                if (proof != null && !proof.isEmpty()) {
                    frames.add(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                    frames.add(proof.getBytes(StandardCharsets.UTF_8));
                }
                frames.addAll(sealedPayloadFrames);
                
                for (int i = 0; i < frames.size(); i++) {
                    if (i == frames.size() - 1) routerSocket.send(frames.get(i));
                    else routerSocket.sendMore(frames.get(i));
                }
            } catch (Exception e) {
                logger.error("Failed to send replyTo", e);
            }
        });
    }

    private void replyTo(byte[] identity, String requestId, byte[] payload) {
        replyTo(identity, requestId, singlePayloadToFrames(payload));
    }

    private void replyService(String requestId, String service, List<byte[]> payloadFrames) {
        submitTask(() -> {
            if (dealerSocket == null) return;
            try {
                String securityRequestId = "svc:" + service + ":" + requestId;
                List<byte[]> sealedPayloadFrames = sealPayloadFrames("response", securityRequestId, payloadFrames, null);
                String proof = secureEnabled ? createAuthProof("response", securityRequestId, sealedPayloadFrames, null) : "";

                List<byte[]> frames = new ArrayList<>();
                frames.add(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_SVC_RES.getBytes(StandardCharsets.UTF_8));
                frames.add(requestId.getBytes(StandardCharsets.UTF_8));
                frames.add(service.getBytes(StandardCharsets.UTF_8));
                if (proof != null && !proof.isEmpty()) {
                    frames.add(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                    frames.add(proof.getBytes(StandardCharsets.UTF_8));
                }
                frames.addAll(sealedPayloadFrames);

                for (int i = 0; i < frames.size(); i++) {
                    if (i == frames.size() - 1) dealerSocket.send(frames.get(i));
                    else dealerSocket.sendMore(frames.get(i));
                }
            } catch (Exception e) {
                logger.error("Failed to send service reply", e);
            }
        });
    }

    private void replyServiceTo(byte[] identity, String requestId, String service, List<byte[]> payloadFrames) {
        submitTask(() -> {
            if (routerSocket == null) return;
            try {
                String identityText = new String(identity, StandardCharsets.UTF_8);
                SecurityUtils.Keys sKeys = null;
                if (secureEnabled) {
                    sKeys = resolveSlaveKeys(identityText);
                    if (sKeys == null) {
                        logger.error("Failed to send service replyTo: No authKey configured for slave {}", identityText);
                        return;
                    }
                }

                String securityRequestId = "svc:" + service + ":" + requestId;
                List<byte[]> sealedPayloadFrames = sealPayloadFrames("response", securityRequestId, payloadFrames, sKeys != null ? sKeys.encryptKey : null);
                String proof = secureEnabled ? createAuthProof("response", securityRequestId, sealedPayloadFrames, sKeys.signKey) : "";

                List<byte[]> frames = new ArrayList<>();
                frames.add(identity);
                frames.add(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_SVC_RES.getBytes(StandardCharsets.UTF_8));
                frames.add(requestId.getBytes(StandardCharsets.UTF_8));
                frames.add(service.getBytes(StandardCharsets.UTF_8));
                if (proof != null && !proof.isEmpty()) {
                    frames.add(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                    frames.add(proof.getBytes(StandardCharsets.UTF_8));
                }
                frames.addAll(sealedPayloadFrames);

                for (int i = 0; i < frames.size(); i++) {
                    if (i == frames.size() - 1) routerSocket.send(frames.get(i));
                    else routerSocket.sendMore(frames.get(i));
                }
            } catch (Exception e) {
                logger.error("Failed to send service replyTo", e);
            }
        });
    }

    private List<byte[]> buildServiceErrorPayload(Throwable error) {
        String msg = error != null && error.getMessage() != null ? error.getMessage() : String.valueOf(error);
        String escaped = msg.replace("\\", "\\\\").replace("\"", "\\\"");
        String json = "{\"ok\":false,\"error\":\"" + escaped + "\"}";
        return Collections.singletonList(json.getBytes(StandardCharsets.UTF_8));
    }

    // --- Event Emission ---

    private ZmqEvent emitMessage(ZmqEvent event) {
        for (ZmqEventListener listener : listeners) {
            try {
                if ("router".equals(event.getChannel())) listener.onRouter(event);
                if ("dealer".equals(event.getChannel())) listener.onDealer(event);
                listener.onMessage(event);
            } catch (Exception e) {
                emitError(e);
            }
        }
        return event;
    }

    private void emitRequest(ZmqEvent event) {
        for (ZmqEventListener listener : listeners) {
            try { listener.onRequest(event); } catch (Exception e) { emitError(e); }
        }
    }

    private void emitPublish(ZmqEvent event) {
        for (ZmqEventListener listener : listeners) {
            try { listener.onPublish(event); } catch (Exception e) { emitError(e); }
        }
    }

    private void emitPush(ZmqEvent event) {
        for (ZmqEventListener listener : listeners) {
            try { listener.onPush(event); } catch (Exception e) { emitError(e); }
        }
    }

    private void emitServiceRequest(ZmqEvent event) {
        for (ZmqEventListener listener : listeners) {
            try { listener.onServiceRequest(event); } catch (Exception e) { emitError(e); }
        }
    }

    private void emitServiceResponse(ZmqEvent event) {
        for (ZmqEventListener listener : listeners) {
            try { listener.onServiceResponse(event); } catch (Exception e) { emitError(e); }
        }
    }

    private void emitSlaveConnected(String slaveId) {
        for (ZmqEventListener listener : listeners) {
            try { listener.onSlaveConnected(slaveId); } catch (Exception e) { emitError(e); }
        }
    }

    private void emitSlaveDisconnected(String slaveId) {
        for (ZmqEventListener listener : listeners) {
            try { listener.onSlaveDisconnected(slaveId); } catch (Exception e) { emitError(e); }
        }
    }

    private void emitResponse(ZmqEvent event) {
        for (ZmqEventListener listener : listeners) {
            try { listener.onResponse(event); } catch (Exception e) { emitError(e); }
        }
    }

    private void emitAuthFailed(ZmqEvent event, String reason) {
        logger.warn("[ZNL-Auth] Authentication failed: {}", reason);
        ZmqEvent authEvent = new ZmqEvent(
                event.getChannel(),
                event.getFrames(),
                event.getKind(),
                event.getRequestId(),
                event.getAuthKey(),
                event.getPayloadFrames(),
                event.getPayload(),
                event.getIdentity(),
                event.getIdentityText(),
                event.getTopic(),
                reason,
                secureEnabled
        );
        for (ZmqEventListener listener : listeners) {
            try { listener.onAuthFailed(authEvent); } catch (Exception e) { emitError(e); }
        }
    }

    private void emitError(Throwable error) {
        for (ZmqEventListener listener : listeners) {
            try { listener.onError(error); } catch (Exception e) { e.printStackTrace(); }
        }
    }
}
