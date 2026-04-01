package com.znl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZmqClusterNodeOptions {
    private String role;
    private String id;
    private Map<String, String> endpoints = new HashMap<>();
    private int maxPending = 1000;
    private String authKey = "";
    private Map<String, String> authKeyMap = null;
    private int heartbeatInterval = 3000;
    private int heartbeatTimeoutMs = 0;
    private boolean encrypted = false;
    private boolean enablePayloadDigest = false;
    private long maxTimeSkewMs = SecurityUtils.MAX_TIME_SKEW_MS;
    private long replayWindowMs = SecurityUtils.REPLAY_WINDOW_MS;

    public ZmqClusterNodeOptions() {
        endpoints.put("router", "tcp://127.0.0.1:6003");
    }

    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public Map<String, String> getEndpoints() { return endpoints; }
    public void setEndpoints(Map<String, String> endpoints) { this.endpoints = endpoints; }

    public int getMaxPending() { return maxPending; }
    public void setMaxPending(int maxPending) { this.maxPending = maxPending; }

    public String getAuthKey() { return authKey; }
    public void setAuthKey(String authKey) { this.authKey = authKey; }

    public Map<String, String> getAuthKeyMap() { return authKeyMap; }
    public void setAuthKeyMap(Map<String, String> authKeyMap) { this.authKeyMap = authKeyMap; }

    public int getHeartbeatInterval() { return heartbeatInterval; }
    public void setHeartbeatInterval(int heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public int getHeartbeatTimeoutMs() { return heartbeatTimeoutMs; }
    public void setHeartbeatTimeoutMs(int heartbeatTimeoutMs) { this.heartbeatTimeoutMs = heartbeatTimeoutMs; }

    public boolean isEncrypted() {
        return encrypted;
    }

    public void setEncrypted(boolean encrypted) {
        this.encrypted = encrypted;
    }

    public boolean isEnablePayloadDigest() { return enablePayloadDigest; }
    public void setEnablePayloadDigest(boolean enablePayloadDigest) { this.enablePayloadDigest = enablePayloadDigest; }

    public long getMaxTimeSkewMs() {
        return maxTimeSkewMs;
    }

    public void setMaxTimeSkewMs(long maxTimeSkewMs) {
        this.maxTimeSkewMs = maxTimeSkewMs;
    }

    public long getReplayWindowMs() {
        return replayWindowMs;
    }

    public void setReplayWindowMs(long replayWindowMs) {
        this.replayWindowMs = replayWindowMs;
    }
}
