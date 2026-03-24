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
    private static final String CONTROL_AUTH = "__znl_v1_auth__";
    private static final String CONTROL_HEARTBEAT = "heartbeat";
    private static final String CONTROL_REGISTER = "register";
    private static final String CONTROL_UNREGISTER = "unregister";
    private static final String CONTROL_PUB = "pub";
    private static final String SECURITY_ENVELOPE_VERSION = "__znl_sec_v1__";

    private final ZmqClusterNodeOptions options;
    private final ZContext context;

    private ZMQ.Socket routerSocket;
    private ZMQ.Socket dealerSocket;
    private ZMQ.Socket wakePairRecv;
    private ZMQ.Socket wakePairSend;

    private Thread ioThread;
    private volatile boolean running = false;

    private final ConcurrentLinkedQueue<Runnable> taskQueue = new ConcurrentLinkedQueue<>();
    private final Map<String, PendingRequest> pending = new ConcurrentHashMap<>();
    private String authKey;
    private boolean requireAuth;

    private boolean secureEnabled;
    private SecurityUtils.Keys keys;
    private SecurityUtils.ReplayGuard replayGuard;
    private String masterNodeId = null;

    private ZmqRequestHandler routerAutoHandler = null;
    private ZmqRequestHandler dealerAutoHandler = null;

    private final List<ZmqEventListener> listeners = new CopyOnWriteArrayList<>();
    private final Map<String, Long> slaves = new ConcurrentHashMap<>();
    private final Map<String, java.util.function.Consumer<ZmqEvent>> subscriptions = new ConcurrentHashMap<>();
    
    private Timer heartbeatTimer;
    private Timer heartbeatCheckTimer;

    private static class PendingRequest {
        CompletableFuture<byte[]> future;
        long expireTimeMs;
        String requestId;
        String identityText;
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
        this.requireAuth = "master".equals(options.getRole()) && !this.authKey.isEmpty();
        
        this.secureEnabled = options.isEncrypted();
        if (this.secureEnabled) {
            if (this.authKey.isEmpty()) {
                throw new IllegalArgumentException("authKey cannot be empty when encrypted=true");
            }
            try {
                this.keys = SecurityUtils.deriveKeys(this.authKey);
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

    public synchronized CompletableFuture<Void> start() {
        if (running) return CompletableFuture.completedFuture(null);
        logger.info("[ZNL] Starting node. Role: {}, ID: {}", options.getRole(), options.getId());
        running = true;
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
        running = false;
        
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel();
            heartbeatTimer = null;
        }
        if (heartbeatCheckTimer != null) {
            heartbeatCheckTimer.cancel();
            heartbeatCheckTimer = null;
        }
        
        if ("slave".equals(options.getRole())) {
            sendUnregister();
            try { Thread.sleep(50); } catch (InterruptedException ignored) {}
        }
        
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
            dealerSocket = context.createSocket(SocketType.DEALER);
            dealerSocket.setIdentity(options.getId().getBytes(StandardCharsets.UTF_8));
            dealerSocket.connect(endpoint);
            logger.info("[ZNL-Slave] DEALER socket connected to {}", endpoint);
        }
    }

    private void closeAllSockets() {
        for (PendingRequest req : pending.values()) {
            req.future.completeExceptionally(new Exception("Node stopped, pending request cancelled."));
        }
        pending.clear();
        context.close();
    }

    private void runLoop() {
        Poller poller = context.createPoller(3);
        int wakeIdx = poller.register(wakePairRecv, Poller.POLLIN);
        int routerIdx = -1, dealerIdx = -1;

        if ("master".equals(options.getRole())) {
            routerIdx = poller.register(routerSocket, Poller.POLLIN);
        } else {
            dealerIdx = poller.register(dealerSocket, Poller.POLLIN);
        }

        long lastCheckTime = System.currentTimeMillis();

        while (running && !Thread.currentThread().isInterrupted()) {
            int rc = poller.poll(100);
            if (rc == -1) break;

            Runnable task;
            while ((task = taskQueue.poll()) != null) {
                try {
                    task.run();
                } catch (Exception e) {
                    emitError(e);
                }
            }

            if (poller.pollin(wakeIdx)) {
                wakePairRecv.recv(ZMQ.DONTWAIT);
            }

            if (routerIdx != -1 && poller.pollin(routerIdx)) {
                handleRouterFrames(recvFrames(routerSocket));
            }

            if (dealerIdx != -1 && poller.pollin(dealerIdx)) {
                handleDealerFrames(recvFrames(dealerSocket));
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
        taskQueue.offer(task);
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

    // --- Security Helpers ---

    private String createAuthProof(String kind, String requestId, List<byte[]> payloadFrames) {
        if (!secureEnabled || keys == null) return "";
        SecurityUtils.Envelope envelope = new SecurityUtils.Envelope();
        envelope.kind = kind;
        envelope.nodeId = options.getId();
        envelope.requestId = requestId != null ? requestId : "";
        envelope.timestamp = System.currentTimeMillis();
        envelope.nonce = SecurityUtils.generateNonce(16);
        try {
            envelope.payloadDigest = SecurityUtils.digestFrames(payloadFrames);
            return SecurityUtils.encodeAuthProofToken(keys.signKey, envelope);
        } catch (Exception e) {
            logger.error("Failed to create auth proof", e);
            return "";
        }
    }

    private List<byte[]> sealPayloadFrames(String kind, String requestId, byte[] payload) {
        List<byte[]> rawFrames = new ArrayList<>();
        if (payload != null) rawFrames.add(payload);
        
        if (!secureEnabled) return rawFrames;
        
        try {
            String aadStr = "znl-aad-v1|" + kind + "|" + options.getId() + "|" + (requestId != null ? requestId : "");
            byte[] aad = aadStr.getBytes(StandardCharsets.UTF_8);
            SecurityUtils.EncryptedResult res = SecurityUtils.encryptFrames(keys.encryptKey, rawFrames, aad);
            
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

    private List<byte[]> openPayloadFrames(String kind, String requestId, List<byte[]> payloadFrames, String senderNodeId) throws Exception {
        if (!secureEnabled) return payloadFrames;
        if (keys == null) throw new Exception("Encryption key not initialized");
        
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
        
        return SecurityUtils.decryptFrames(keys.encryptKey, iv, ciphertext, tag, aad);
    }

    private static class VerifyResult {
        boolean ok;
        String error;
        SecurityUtils.Envelope envelope;
    }

    private VerifyResult verifyIncomingProof(String kind, String proofToken, String requestId, List<byte[]> payloadFrames, String expectedNodeId) {
        VerifyResult res = new VerifyResult();
        if (!secureEnabled) {
            res.ok = true;
            return res;
        }
        if (keys == null) {
            res.ok = false; res.error = "Sign key not initialized"; return res;
        }
        if (proofToken == null || proofToken.isEmpty()) {
            res.ok = false; res.error = "Missing auth proof token"; return res;
        }
        
        try {
            SecurityUtils.Envelope env = SecurityUtils.decodeAuthProofToken(keys.signKey, proofToken, options.getMaxTimeSkewMs(), System.currentTimeMillis());
            
            if (!kind.equals(env.kind)) {
                res.ok = false; res.error = "Proof kind mismatch: expected " + kind + ", got " + env.kind; return res;
            }
            if (!Objects.equals(requestId != null ? requestId : "", env.requestId != null ? env.requestId : "")) {
                res.ok = false; res.error = "Proof requestId mismatch"; return res;
            }
            if (expectedNodeId != null && !expectedNodeId.isEmpty() && !expectedNodeId.equals(env.nodeId)) {
                res.ok = false; res.error = "Proof nodeId mismatch: expected " + expectedNodeId + ", got " + env.nodeId; return res;
            }
            
            String currentDigest = SecurityUtils.digestFrames(payloadFrames);
            if (!currentDigest.equals(env.payloadDigest)) {
                res.ok = false; res.error = "Payload digest mismatch"; return res;
            }
            
            String replayKey = env.kind + "|" + env.nodeId + "|" + env.nonce;
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
        heartbeatTimer = new Timer("ZmqClusterNode-Heartbeat-" + options.getId(), true);
        heartbeatTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                sendHeartbeat();
            }
        }, options.getHeartbeatInterval(), options.getHeartbeatInterval());
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
            if (dealerSocket == null) return;
            String proof = secureEnabled ? createAuthProof("heartbeat", "", Collections.emptyList()) : "";
            
            dealerSocket.sendMore(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
            if (secureEnabled) {
                dealerSocket.sendMore(CONTROL_HEARTBEAT.getBytes(StandardCharsets.UTF_8));
                dealerSocket.sendMore(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                dealerSocket.send(proof.getBytes(StandardCharsets.UTF_8));
            } else {
                dealerSocket.send(CONTROL_HEARTBEAT.getBytes(StandardCharsets.UTF_8));
            }
        });
    }

    private void sendRegister() {
        submitTask(() -> {
            if (dealerSocket == null) return;
            String token = secureEnabled ? createAuthProof("register", "", Collections.emptyList()) : authKey;
            
            dealerSocket.sendMore(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
            dealerSocket.sendMore(CONTROL_REGISTER.getBytes(StandardCharsets.UTF_8));
            if (token != null && !token.isEmpty()) {
                dealerSocket.sendMore(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                dealerSocket.send(token.getBytes(StandardCharsets.UTF_8));
            } else {
                dealerSocket.send(EMPTY_BUFFER);
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
        long timeout = options.getHeartbeatInterval() * 3L;
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

    private void handleRouterFrames(List<byte[]> frames) {
        if (frames.isEmpty()) return;
        byte[] identity = frames.get(0);
        String identityText = new String(identity, StandardCharsets.UTF_8);

        List<byte[]> bodyFrames = frames.size() > 1 ? frames.subList(1, frames.size()) : new ArrayList<>();
        ParsedFrames parsed = parseControlFrames(bodyFrames);
        
        if (CONTROL_HEARTBEAT.equals(parsed.kind)) {
            if (secureEnabled) {
                VerifyResult v = verifyIncomingProof("heartbeat", parsed.authProof, "", Collections.emptyList(), identityText);
                if (!v.ok) {
                    emitAuthFailed(new ZmqEvent("router", frames, "heartbeat", null, parsed.authProof, Collections.emptyList(), EMPTY_BUFFER, identity, identityText, null), v.error);
                    return;
                }
            }
            if (slaves.containsKey(identityText)) {
                slaves.put(identityText, System.currentTimeMillis());
            }
            return;
        }
        
        if (CONTROL_REGISTER.equals(parsed.kind)) {
            if (secureEnabled) {
                VerifyResult v = verifyIncomingProof("register", parsed.authKey, "", Collections.emptyList(), identityText);
                if (!v.ok) {
                    emitAuthFailed(new ZmqEvent("router", frames, "register", null, parsed.authKey, Collections.emptyList(), EMPTY_BUFFER, identity, identityText, null), v.error);
                    return;
                }
            } else if (requireAuth && !authKey.equals(parsed.authKey)) {
                emitAuthFailed(new ZmqEvent("router", frames, "register", null, parsed.authKey, Collections.emptyList(), EMPTY_BUFFER, identity, identityText, null), "auth mismatch");
                return;
            }
            
            if (!slaves.containsKey(identityText)) {
                logger.info("[ZNL-Master] Slave connected: {}", identityText);
                emitSlaveConnected(identityText);
            }
            slaves.put(identityText, System.currentTimeMillis());
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
        
        if ("request".equals(parsed.kind) || "response".equals(parsed.kind)) {
            if (secureEnabled) {
                String proof = "request".equals(parsed.kind) ? parsed.authKey : parsed.authProof;
                VerifyResult v = verifyIncomingProof(parsed.kind, proof, parsed.requestId, parsed.payloadFrames, identityText);
                if (!v.ok) {
                    emitAuthFailed(new ZmqEvent("router", frames, parsed.kind, parsed.requestId, proof, parsed.payloadFrames, EMPTY_BUFFER, identity, identityText, null), v.error);
                    return;
                }
                if (options.isEncrypted()) {
                    try {
                        finalFrames = openPayloadFrames(parsed.kind, parsed.requestId, parsed.payloadFrames, identityText);
                    } catch (Exception e) {
                        emitAuthFailed(new ZmqEvent("router", frames, parsed.kind, parsed.requestId, proof, parsed.payloadFrames, EMPTY_BUFFER, identity, identityText, null), "decryption failed: " + e.getMessage());
                        return;
                    }
                }
            } else if ("request".equals(parsed.kind) && requireAuth && !authKey.equals(parsed.authKey)) {
                emitAuthFailed(new ZmqEvent("router", frames, parsed.kind, parsed.requestId, parsed.authKey, parsed.payloadFrames, EMPTY_BUFFER, identity, identityText, null), "auth mismatch");
                return;
            }
        }
        
        payload = payloadFromFrames(finalFrames);
        ZmqEvent event = new ZmqEvent("router", frames, parsed.kind, parsed.requestId, parsed.authKey, finalFrames, payload, identity, identityText, null);
        event = emitMessage(event);

        if ("request".equals(parsed.kind)) {
            emitRequest(event);
            if (routerAutoHandler != null) {
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
                req.future.complete(payload);
            }
            emitResponse(event);
        }
    }

    private void handleDealerFrames(List<byte[]> frames) {
        if (frames.isEmpty()) return;
        ParsedFrames parsed = parseControlFrames(frames);
        
        List<byte[]> finalFrames = parsed.payloadFrames;
        byte[] payload = EMPTY_BUFFER;
        
        if (CONTROL_PUB.equals(parsed.kind) || "request".equals(parsed.kind) || "response".equals(parsed.kind)) {
            if (secureEnabled) {
                String proof = "request".equals(parsed.kind) ? parsed.authKey : parsed.authProof;
                String reqId = CONTROL_PUB.equals(parsed.kind) ? parsed.topic : parsed.requestId;
                VerifyResult v = verifyIncomingProof(CONTROL_PUB.equals(parsed.kind) ? "publish" : parsed.kind, proof, reqId, parsed.payloadFrames, null);
                if (!v.ok) {
                    emitAuthFailed(new ZmqEvent("dealer", frames, parsed.kind, parsed.requestId, proof, parsed.payloadFrames, EMPTY_BUFFER, null, null, parsed.topic), v.error);
                    return;
                }
                if (masterNodeId == null && v.envelope != null) {
                    masterNodeId = v.envelope.nodeId;
                }
                if (options.isEncrypted()) {
                    try {
                        String expectedNodeId = CONTROL_PUB.equals(parsed.kind) ? v.envelope.nodeId : masterNodeId;
                        finalFrames = openPayloadFrames(CONTROL_PUB.equals(parsed.kind) ? "publish" : parsed.kind, reqId, parsed.payloadFrames, expectedNodeId);
                    } catch (Exception e) {
                        emitAuthFailed(new ZmqEvent("dealer", frames, parsed.kind, parsed.requestId, proof, parsed.payloadFrames, EMPTY_BUFFER, null, null, parsed.topic), "decryption failed: " + e.getMessage());
                        return;
                    }
                }
            }
        }

        payload = payloadFromFrames(finalFrames);
        ZmqEvent event = new ZmqEvent("dealer", frames, parsed.kind, parsed.requestId, parsed.authKey, finalFrames, payload, null, null, parsed.topic);
        event = emitMessage(event);

        if ("request".equals(parsed.kind)) {
            emitRequest(event);
            if (dealerAutoHandler != null) {
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
                req.future.complete(payload);
            }
            emitResponse(event);
        } else if (CONTROL_PUB.equals(parsed.kind)) {
            emitPublish(event);
            java.util.function.Consumer<ZmqEvent> sub = subscriptions.get(parsed.topic);
            if (sub != null) {
                try { sub.accept(event); } catch (Exception e) { emitError(e); }
            }
        }
    }

    private static class ParsedFrames {
        String kind = "message";
        String requestId = null;
        String authKey = null;
        String authProof = null;
        String topic = null;
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
                    parsed.kind = action;
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

    // --- Public API ---

    public CompletableFuture<Void> publish(String topic, byte[] payload) {
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
                
                List<byte[]> payloadFrames = sealPayloadFrames("publish", finalTopic, payload);
                String proof = secureEnabled ? createAuthProof("publish", finalTopic, payloadFrames) : "";
                
                List<byte[]> frames = new ArrayList<>();
                frames.add(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_PUB.getBytes(StandardCharsets.UTF_8));
                frames.add(t);
                if (proof != null && !proof.isEmpty()) {
                    frames.add(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                    frames.add(proof.getBytes(StandardCharsets.UTF_8));
                }
                frames.addAll(payloadFrames);
                
                List<String> currentSlaves = new ArrayList<>(slaves.keySet());
                for (String slaveId : currentSlaves) {
                    byte[] identity = slaveId.getBytes(StandardCharsets.UTF_8);
                    routerSocket.sendMore(identity);
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

    public void subscribe(String topic, java.util.function.Consumer<ZmqEvent> handler) {
        subscriptions.put(topic, handler);
    }

    public void unsubscribe(String topic) {
        subscriptions.remove(topic);
    }

    public List<String> getSlaves() {
        return new ArrayList<>(slaves.keySet());
    }

    public void DEALER(ZmqRequestHandler handler) {
        this.dealerAutoHandler = handler;
    }

    public CompletableFuture<byte[]> DEALER(byte[] payload, int timeoutMs) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        ensurePendingCapacity();
        String requestId = UUID.randomUUID().toString();
        String key = pendingKey(requestId, null);

        PendingRequest req = new PendingRequest();
        req.future = future;
        req.requestId = requestId;
        req.expireTimeMs = System.currentTimeMillis() + (timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS);
        
        submitTask(() -> {
            try {
                if (dealerSocket == null) throw new IllegalStateException("DEALER socket not ready");
                pending.put(key, req);
                
                List<byte[]> payloadFrames = sealPayloadFrames("request", requestId, payload);
                String proofOrAuthKey = secureEnabled ? createAuthProof("request", requestId, payloadFrames) : authKey;
                
                List<byte[]> frames = new ArrayList<>();
                frames.add(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_REQ.getBytes(StandardCharsets.UTF_8));
                frames.add(requestId.getBytes(StandardCharsets.UTF_8));
                if (proofOrAuthKey != null && !proofOrAuthKey.isEmpty()) {
                    frames.add(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                    frames.add(proofOrAuthKey.getBytes(StandardCharsets.UTF_8));
                }
                frames.addAll(payloadFrames);

                for (int i = 0; i < frames.size(); i++) {
                    if (i == frames.size() - 1) dealerSocket.send(frames.get(i));
                    else dealerSocket.sendMore(frames.get(i));
                }
            } catch (Exception e) {
                pending.remove(key);
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    public void ROUTER(ZmqRequestHandler handler) {
        this.routerAutoHandler = handler;
    }

    public CompletableFuture<byte[]> ROUTER(String identity, byte[] payload, int timeoutMs) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        ensurePendingCapacity();
        String requestId = UUID.randomUUID().toString();
        String key = pendingKey(requestId, identity);

        PendingRequest req = new PendingRequest();
        req.future = future;
        req.requestId = requestId;
        req.identityText = identity;
        req.expireTimeMs = System.currentTimeMillis() + (timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS);

        submitTask(() -> {
            try {
                if (routerSocket == null) throw new IllegalStateException("ROUTER socket not ready");
                pending.put(key, req);
                
                List<byte[]> payloadFrames = sealPayloadFrames("request", requestId, payload);
                String proofOrAuthKey = secureEnabled ? createAuthProof("request", requestId, payloadFrames) : authKey;
                
                List<byte[]> frames = new ArrayList<>();
                frames.add(identity.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_REQ.getBytes(StandardCharsets.UTF_8));
                frames.add(requestId.getBytes(StandardCharsets.UTF_8));
                if (proofOrAuthKey != null && !proofOrAuthKey.isEmpty()) {
                    frames.add(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                    frames.add(proofOrAuthKey.getBytes(StandardCharsets.UTF_8));
                }
                frames.addAll(payloadFrames);

                for (int i = 0; i < frames.size(); i++) {
                    if (i == frames.size() - 1) routerSocket.send(frames.get(i));
                    else routerSocket.sendMore(frames.get(i));
                }
            } catch (Exception e) {
                pending.remove(key);
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    private void reply(String requestId, byte[] payload) {
        submitTask(() -> {
            if (dealerSocket == null) return;
            try {
                List<byte[]> payloadFrames = sealPayloadFrames("response", requestId, payload);
                String proof = secureEnabled ? createAuthProof("response", requestId, payloadFrames) : "";
                
                List<byte[]> frames = new ArrayList<>();
                frames.add(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_RES.getBytes(StandardCharsets.UTF_8));
                frames.add(requestId.getBytes(StandardCharsets.UTF_8));
                if (proof != null && !proof.isEmpty()) {
                    frames.add(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                    frames.add(proof.getBytes(StandardCharsets.UTF_8));
                }
                frames.addAll(payloadFrames);
                
                for (int i = 0; i < frames.size(); i++) {
                    if (i == frames.size() - 1) dealerSocket.send(frames.get(i));
                    else dealerSocket.sendMore(frames.get(i));
                }
            } catch (Exception e) {
                logger.error("Failed to send reply", e);
            }
        });
    }

    private void replyTo(byte[] identity, String requestId, byte[] payload) {
        submitTask(() -> {
            if (routerSocket == null) return;
            try {
                List<byte[]> payloadFrames = sealPayloadFrames("response", requestId, payload);
                String proof = secureEnabled ? createAuthProof("response", requestId, payloadFrames) : "";
                
                List<byte[]> frames = new ArrayList<>();
                frames.add(identity);
                frames.add(CONTROL_PREFIX.getBytes(StandardCharsets.UTF_8));
                frames.add(CONTROL_RES.getBytes(StandardCharsets.UTF_8));
                frames.add(requestId.getBytes(StandardCharsets.UTF_8));
                if (proof != null && !proof.isEmpty()) {
                    frames.add(CONTROL_AUTH.getBytes(StandardCharsets.UTF_8));
                    frames.add(proof.getBytes(StandardCharsets.UTF_8));
                }
                frames.addAll(payloadFrames);
                
                for (int i = 0; i < frames.size(); i++) {
                    if (i == frames.size() - 1) routerSocket.send(frames.get(i));
                    else routerSocket.sendMore(frames.get(i));
                }
            } catch (Exception e) {
                logger.error("Failed to send replyTo", e);
            }
        });
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
        for (ZmqEventListener listener : listeners) {
            try { listener.onAuthFailed(event); } catch (Exception e) { emitError(e); }
        }
    }

    private void emitError(Throwable error) {
        for (ZmqEventListener listener : listeners) {
            try { listener.onError(error); } catch (Exception e) { e.printStackTrace(); }
        }
    }
}
