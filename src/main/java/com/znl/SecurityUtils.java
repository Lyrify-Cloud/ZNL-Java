package com.znl;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SecurityUtils {

    private static final String KDF_SALT = "znl-kdf-salt-v1";
    private static final String KDF_INFO_SIGN = "znl-kdf-sign-v1";
    private static final String KDF_INFO_ENCRYPT = "znl-kdf-encrypt-v1";
    private static final int KEY_BYTES = 32;
    private static final int ENCRYPT_IV_BYTES = 12;
    private static final int ENCRYPT_TAG_BYTES = 16;
    
    public static final long MAX_TIME_SKEW_MS = 30000;
    public static final long REPLAY_WINDOW_MS = 120000;

    private static final SecureRandom RANDOM = new SecureRandom();

    public static class Keys {
        public final byte[] signKey;
        public final byte[] encryptKey;

        public Keys(byte[] signKey, byte[] encryptKey) {
            this.signKey = signKey;
            this.encryptKey = encryptKey;
        }
    }

    public static Keys deriveKeys(String authKey) throws Exception {
        return deriveKeys(authKey, (byte[]) null);
    }

    public static Keys deriveKeys(String authKey, String salt) throws Exception {
        byte[] saltBytes = salt != null ? salt.getBytes(StandardCharsets.UTF_8) : null;
        return deriveKeys(authKey, saltBytes);
    }

    public static Keys deriveKeys(String authKey, byte[] saltInput) throws Exception {
        if (authKey == null || authKey.isEmpty()) {
            throw new IllegalArgumentException("authKey cannot be empty when security is enabled.");
        }
        byte[] ikm = authKey.getBytes(StandardCharsets.UTF_8);
        byte[] defaultSalt = KDF_SALT.getBytes(StandardCharsets.UTF_8);
        byte[] salt = (saltInput != null && saltInput.length > 0) ? Arrays.copyOf(saltInput, saltInput.length) : defaultSalt;
        
        byte[] signKey = hkdf(ikm, salt, KDF_INFO_SIGN.getBytes(StandardCharsets.UTF_8), KEY_BYTES);
        byte[] encryptKey = hkdf(ikm, salt, KDF_INFO_ENCRYPT.getBytes(StandardCharsets.UTF_8), KEY_BYTES);
        
        return new Keys(signKey, encryptKey);
    }

    private static byte[] hkdf(byte[] ikm, byte[] salt, byte[] info, int length) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        if (salt == null || salt.length == 0) {
            salt = new byte[32];
        }
        mac.init(new SecretKeySpec(salt, "HmacSHA256"));
        byte[] prk = mac.doFinal(ikm);

        mac.init(new SecretKeySpec(prk, "HmacSHA256"));
        byte[] result = new byte[length];
        byte[] t = new byte[0];
        int generated = 0;
        for (int i = 1; generated < length; i++) {
            mac.update(t);
            mac.update(info);
            mac.update((byte) i);
            t = mac.doFinal();
            int toCopy = Math.min(t.length, length - generated);
            System.arraycopy(t, 0, result, generated, toCopy);
            generated += toCopy;
        }
        return result;
    }

    public static String generateNonce(int bytes) {
        byte[] nonce = new byte[Math.max(8, bytes)];
        RANDOM.nextBytes(nonce);
        return bytesToHex(nonce);
    }

    public static boolean isTimestampFresh(long timestampMs, long maxSkewMs, long now) {
        if (timestampMs <= 0) return false;
        return Math.abs(now - timestampMs) <= Math.max(1, maxSkewMs);
    }

    public static String signText(byte[] signKey, String text) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(signKey, "HmacSHA256"));
        byte[] hash = mac.doFinal(text.getBytes(StandardCharsets.UTF_8));
        return bytesToHex(hash);
    }

    public static boolean verifyTextSignature(byte[] signKey, String text, String signatureHex) {
        try {
            byte[] expected = hexToBytes(signText(signKey, text));
            byte[] provided = hexToBytes(String.valueOf(signatureHex != null ? signatureHex : ""));
            byte[] normalized = new byte[expected.length];
            System.arraycopy(provided, 0, normalized, 0, Math.min(provided.length, expected.length));
            boolean sameContent = MessageDigest.isEqual(expected, normalized);
            boolean sameLength = provided.length == expected.length;
            return sameContent && sameLength;
        } catch (Exception e) {
            return false;
        }
    }

    public static String toBase64Url(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    public static String fromBase64Url(String text) {
        return new String(Base64.getUrlDecoder().decode(text), StandardCharsets.UTF_8);
    }

    public static class Envelope {
        public String kind;
        public String nodeId;
        public String requestId;
        public long timestamp;
        public String nonce;
        public String payloadDigest;

        public Envelope() {}
    }

    public static String encodeAuthProofToken(byte[] signKey, Envelope envelope) throws Exception {
        String header = "{\"alg\":\"HS256\",\"typ\":\"ZNL-AUTH-PROOF\",\"v\":1}";
        
        StringBuilder payloadBuilder = new StringBuilder();
        payloadBuilder.append("{")
            .append("\"kind\":\"").append(escapeJson(envelope.kind)).append("\",")
            .append("\"nodeId\":\"").append(escapeJson(envelope.nodeId)).append("\",")
            .append("\"requestId\":\"").append(escapeJson(envelope.requestId != null ? envelope.requestId : "")).append("\",")
            .append("\"timestamp\":").append(envelope.timestamp).append(",")
            .append("\"nonce\":\"").append(escapeJson(envelope.nonce)).append("\",")
            .append("\"payloadDigest\":\"").append(escapeJson(envelope.payloadDigest != null ? envelope.payloadDigest : "")).append("\"")
            .append("}");
        
        String h = toBase64Url(header);
        String p = toBase64Url(payloadBuilder.toString());
        String signingInput = h + "." + p;
        String signatureHex = signText(signKey, signingInput);
        return signingInput + "." + signatureHex;
    }

    public static Envelope decodeAuthProofToken(byte[] signKey, String token, long maxSkewMs, long now) throws Exception {
        if (token == null) throw new Exception("Token is null");
        String[] parts = token.split("\\.");
        if (parts.length != 3) {
            throw new Exception("Invalid token format");
        }
        String h = parts[0];
        String p = parts[1];
        String signatureHex = parts[2];
        String signingInput = h + "." + p;

        if (!verifyTextSignature(signKey, signingInput, signatureHex)) {
            throw new Exception("Signature verification failed");
        }

        String header = fromBase64Url(h);
        String payload = fromBase64Url(p);

        if (!header.contains("\"alg\":\"HS256\"") || !header.contains("\"typ\":\"ZNL-AUTH-PROOF\"")) {
            throw new Exception("Invalid token header");
        }

        Envelope env = new Envelope();
        env.kind = extractJsonString(payload, "kind");
        env.nodeId = extractJsonString(payload, "nodeId");
        env.requestId = extractJsonString(payload, "requestId");
        env.timestamp = extractJsonLong(payload, "timestamp");
        env.nonce = extractJsonString(payload, "nonce");
        env.payloadDigest = extractJsonString(payload, "payloadDigest");

        if (env.kind.isEmpty() || env.nodeId.isEmpty() || env.nonce.isEmpty()) {
            throw new Exception("Missing required fields in token payload");
        }

        if (!isTimestampFresh(env.timestamp, maxSkewMs, now)) {
            throw new Exception("Token expired or timestamp anomaly");
        }

        return env;
    }

    public static byte[] encodeFrames(List<byte[]> frames) {
        int totalLen = 4; // count
        for (byte[] frame : frames) {
            totalLen += 4 + frame.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(totalLen);
        buffer.putInt(frames.size());
        for (byte[] frame : frames) {
            buffer.putInt(frame.length);
            buffer.put(frame);
        }
        return buffer.array();
    }

    public static List<byte[]> decodeFrames(byte[] packed) throws Exception {
        if (packed.length < 4) throw new Exception("Invalid frames data: missing count");
        ByteBuffer buffer = ByteBuffer.wrap(packed);
        int count = buffer.getInt();
        List<byte[]> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            if (buffer.remaining() < 4) throw new Exception("Invalid frames data: missing length");
            int len = buffer.getInt();
            if (buffer.remaining() < len) throw new Exception("Invalid frames data: out of bounds");
            byte[] frame = new byte[len];
            buffer.get(frame);
            out.add(frame);
        }
        if (buffer.remaining() > 0) throw new Exception("Invalid frames data: unconsumed trailing bytes");
        return out;
    }

    public static String digestFrames(List<byte[]> frames) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        
        ByteBuffer head = ByteBuffer.allocate(4);
        head.putInt(frames == null ? 0 : frames.size());
        md.update(head.array());
        
        if (frames != null) {
            for (byte[] frame : frames) {
                ByteBuffer len = ByteBuffer.allocate(4);
                len.putInt(frame.length);
                md.update(len.array());
                if (frame.length > 0) {
                    md.update(frame);
                }
            }
        }
        
        byte[] hash = md.digest();
        return bytesToHex(hash);
    }

    public static class EncryptedResult {
        public byte[] iv;
        public byte[] ciphertext;
        public byte[] tag;
    }

    public static EncryptedResult encryptFrames(byte[] encryptKey, List<byte[]> frames, byte[] aad) throws Exception {
        byte[] packed = encodeFrames(frames);
        byte[] iv = new byte[ENCRYPT_IV_BYTES];
        RANDOM.nextBytes(iv);
        
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec spec = new GCMParameterSpec(ENCRYPT_TAG_BYTES * 8, iv);
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(encryptKey, "AES"), spec);
        if (aad != null && aad.length > 0) {
            cipher.updateAAD(aad);
        }
        
        byte[] ciphertextAndTag = cipher.doFinal(packed);
        int cipherLen = ciphertextAndTag.length - ENCRYPT_TAG_BYTES;
        byte[] ciphertext = new byte[cipherLen];
        byte[] tag = new byte[ENCRYPT_TAG_BYTES];
        System.arraycopy(ciphertextAndTag, 0, ciphertext, 0, cipherLen);
        System.arraycopy(ciphertextAndTag, cipherLen, tag, 0, ENCRYPT_TAG_BYTES);
        
        EncryptedResult result = new EncryptedResult();
        result.iv = iv;
        result.ciphertext = ciphertext;
        result.tag = tag;
        return result;
    }

    public static List<byte[]> decryptFrames(byte[] encryptKey, byte[] iv, byte[] ciphertext, byte[] tag, byte[] aad) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec spec = new GCMParameterSpec(ENCRYPT_TAG_BYTES * 8, iv);
        cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(encryptKey, "AES"), spec);
        if (aad != null && aad.length > 0) {
            cipher.updateAAD(aad);
        }
        
        byte[] ciphertextAndTag = new byte[ciphertext.length + tag.length];
        System.arraycopy(ciphertext, 0, ciphertextAndTag, 0, ciphertext.length);
        System.arraycopy(tag, 0, ciphertextAndTag, ciphertext.length, tag.length);
        
        byte[] packed = cipher.doFinal(ciphertextAndTag);
        return decodeFrames(packed);
    }

    public static class ReplayGuard {
        private final Map<String, Long> map = new ConcurrentHashMap<>();
        private final long windowMs;

        public ReplayGuard(long windowMs) {
            this.windowMs = Math.max(10000, windowMs);
        }

        public void sweep(long now) {
            map.entrySet().removeIf(entry -> entry.getValue() <= now);
        }

        public boolean seenOrAdd(String nonce, long now) {
            if (nonce == null || nonce.isEmpty()) return true;
            sweep(now);
            if (map.containsKey(nonce)) return true;
            map.put(nonce, now + windowMs);
            return false;
        }
    }

    // Helpers
    private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    private static byte[] hexToBytes(String hex) {
        if (hex == null) {
            return new byte[0];
        }
        String text = hex.trim();
        if ((text.length() & 1) != 0) {
            throw new IllegalArgumentException("Invalid hex length");
        }
        byte[] out = new byte[text.length() / 2];
        for (int i = 0; i < text.length(); i += 2) {
            int hi = Character.digit(text.charAt(i), 16);
            int lo = Character.digit(text.charAt(i + 1), 16);
            if (hi < 0 || lo < 0) {
                throw new IllegalArgumentException("Invalid hex character");
            }
            out[i / 2] = (byte) ((hi << 4) + lo);
        }
        return out;
    }

    private static String escapeJson(String str) {
        if (str == null) return "";
        return str.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String extractJsonString(String json, String key) {
        String search = "\"" + key + "\":\"";
        int start = json.indexOf(search);
        if (start == -1) return "";
        start += search.length();
        int end = json.indexOf("\"", start);
        if (end == -1) return "";
        return json.substring(start, end);
    }

    private static long extractJsonLong(String json, String key) {
        String search = "\"" + key + "\":";
        int start = json.indexOf(search);
        if (start == -1) return 0;
        start += search.length();
        int end = start;
        while (end < json.length() && Character.isDigit(json.charAt(end))) {
            end++;
        }
        if (start == end) return 0;
        try {
            return Long.parseLong(json.substring(start, end));
        } catch (Exception e) {
            return 0;
        }
    }
}
