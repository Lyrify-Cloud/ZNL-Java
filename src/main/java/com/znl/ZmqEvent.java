package com.znl;

import java.util.List;

public class ZmqEvent {
    private final String channel;
    private final List<byte[]> frames;
    private final String kind;
    private final String requestId;
    private final String authKey;
    private final List<byte[]> payloadFrames;
    private final byte[] payload;
    private final byte[] identity;
    private final String identityText;
    private final String topic;
    private final String authReason;
    private final boolean encrypted;

    public ZmqEvent(String channel, List<byte[]> frames, String kind, String requestId,
                    String authKey, List<byte[]> payloadFrames, byte[] payload, byte[] identity,
                    String identityText, String topic) {
        this(channel, frames, kind, requestId, authKey, payloadFrames, payload, identity, identityText, topic, null, false);
    }

    public ZmqEvent(String channel, List<byte[]> frames, String kind, String requestId,
                    String authKey, List<byte[]> payloadFrames, byte[] payload, byte[] identity,
                    String identityText, String topic, String authReason, boolean encrypted) {
        this.channel = channel;
        this.frames = frames;
        this.kind = kind;
        this.requestId = requestId;
        this.authKey = authKey;
        this.payloadFrames = payloadFrames;
        this.payload = payload;
        this.identity = identity;
        this.identityText = identityText;
        this.topic = topic;
        this.authReason = authReason;
        this.encrypted = encrypted;
    }

    public String getChannel() { return channel; }
    public List<byte[]> getFrames() { return frames; }
    public String getKind() { return kind; }
    public String getRequestId() { return requestId; }
    public String getAuthKey() { return authKey; }
    public List<byte[]> getPayloadFrames() { return payloadFrames; }
    public byte[] getPayload() { return payload; }
    public byte[] getIdentity() { return identity; }
    public String getIdentityText() { return identityText; }
    public String getTopic() { return topic; }
    public String getAuthReason() { return authReason; }
    public boolean isEncrypted() { return encrypted; }
}
