package com.znl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.difflib.DiffUtils;
import com.github.difflib.UnifiedDiffUtils;
import com.github.difflib.patch.Patch;
import com.github.difflib.patch.PatchFailedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class FsService {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {};

    private static final String SERVICE = "fs";
    private static final int DEFAULT_TIMEOUT_MS = 5000;
    private static final int DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024;

    private static final String OPS_LIST = "file/list";
    private static final String OPS_GET = "file/get";
    private static final String OPS_PATCH = "file/patch";
    private static final String OPS_INIT = "file/init";
    private static final String OPS_RESUME = "file/resume";
    private static final String OPS_CHUNK = "file/chunk";
    private static final String OPS_ACK = "file/ack";
    private static final String OPS_COMPLETE = "file/complete";
    private static final String OPS_DOWNLOAD_INIT = "file/download_init";
    private static final String OPS_DOWNLOAD_CHUNK = "file/download_chunk";
    private static final String OPS_DOWNLOAD_COMPLETE = "file/download_complete";
    private static final String OPS_DELETE = "file/delete";
    private static final String OPS_RENAME = "file/rename";
    private static final String OPS_STAT = "file/stat";

    private static final long SESSION_TTL_MS = 30L * 60L * 1000L;

    private final ZmqClusterNode node;
    private volatile Path rootDir;
    private volatile FsPolicy policy = FsPolicy.defaults();
    private volatile boolean serviceRegistered = false;
    private volatile long rootGeneration = 0L;

    private final Map<String, Session> sessions = new ConcurrentHashMap<>();

    public FsService(ZmqClusterNode node) {
        this.node = Objects.requireNonNull(node);
    }

    // master API
    public Map<String, Object> list(String slaveId, String dirPath) throws Exception {
        return list(slaveId, dirPath, DEFAULT_TIMEOUT_MS);
    }

    public Map<String, Object> list(String slaveId, String dirPath, int timeoutMs) throws Exception {
        ParsedPayload parsed = request(slaveId, mapOf("service", SERVICE, "op", OPS_LIST, "path", str(dirPath)), Collections.emptyList(), timeoutMs);
        return assertOk(parsed.meta, OPS_LIST);
    }

    public Map<String, Object> stat(String slaveId, String targetPath) throws Exception {
        return stat(slaveId, targetPath, DEFAULT_TIMEOUT_MS);
    }

    public Map<String, Object> stat(String slaveId, String targetPath, int timeoutMs) throws Exception {
        ParsedPayload parsed = request(slaveId, mapOf("service", SERVICE, "op", OPS_STAT, "path", str(targetPath)), Collections.emptyList(), timeoutMs);
        return assertOk(parsed.meta, OPS_STAT);
    }

    public ParsedPayload get(String slaveId, String targetPath) throws Exception {
        return get(slaveId, targetPath, DEFAULT_TIMEOUT_MS);
    }

    public ParsedPayload get(String slaveId, String targetPath, int timeoutMs) throws Exception {
        ParsedPayload parsed = request(slaveId, mapOf("service", SERVICE, "op", OPS_GET, "path", str(targetPath)), Collections.emptyList(), timeoutMs);
        assertOk(parsed.meta, OPS_GET);
        return parsed;
    }

    public Map<String, Object> delete(String slaveId, String targetPath) throws Exception {
        return delete(slaveId, targetPath, DEFAULT_TIMEOUT_MS);
    }

    public Map<String, Object> delete(String slaveId, String targetPath, int timeoutMs) throws Exception {
        ParsedPayload parsed = request(slaveId, mapOf("service", SERVICE, "op", OPS_DELETE, "path", str(targetPath)), Collections.emptyList(), timeoutMs);
        return assertOk(parsed.meta, OPS_DELETE);
    }

    public Map<String, Object> rename(String slaveId, String fromPath, String toPath) throws Exception {
        return rename(slaveId, fromPath, toPath, DEFAULT_TIMEOUT_MS);
    }

    public Map<String, Object> rename(String slaveId, String fromPath, String toPath, int timeoutMs) throws Exception {
        ParsedPayload parsed = request(slaveId, mapOf("service", SERVICE, "op", OPS_RENAME, "from", str(fromPath), "to", str(toPath)), Collections.emptyList(), timeoutMs);
        return assertOk(parsed.meta, OPS_RENAME);
    }

    public Map<String, Object> patch(String slaveId, String targetPath, String unifiedDiff) throws Exception {
        return patch(slaveId, targetPath, unifiedDiff, DEFAULT_TIMEOUT_MS);
    }

    public Map<String, Object> patch(String slaveId, String targetPath, String unifiedDiff, int timeoutMs) throws Exception {
        ParsedPayload parsed = request(slaveId, mapOf("service", SERVICE, "op", OPS_PATCH, "path", str(targetPath), "patch", str(unifiedDiff)), Collections.emptyList(), timeoutMs);
        return assertOk(parsed.meta, OPS_PATCH);
    }

    public Map<String, Object> upload(String slaveId, String localPath, String remotePath) throws Exception {
        return upload(slaveId, localPath, remotePath, DEFAULT_TIMEOUT_MS, DEFAULT_CHUNK_SIZE, null);
    }

    public Map<String, Object> upload(String slaveId, String localPath, String remotePath, int timeoutMs, int chunkSizeInput, String sessionIdInput) throws Exception {
        ensureMaster();
        int chunkSize = normalizeChunkSize(chunkSizeInput);
        String sessionId = sessionIdInput != null && !sessionIdInput.isEmpty() ? sessionIdInput : createSessionId();
        Path absolutePath = Paths.get(str(localPath)).toAbsolutePath().normalize();
        if (!Files.exists(absolutePath) || !Files.isRegularFile(absolutePath)) {
            throw new IllegalArgumentException("upload requires local file path");
        }

        long fileSize = Files.size(absolutePath);
        ParsedPayload initResp = request(
                slaveId,
                mapOf("service", SERVICE, "op", OPS_INIT, "sessionId", sessionId, "path", str(remotePath), "fileName", absolutePath.getFileName().toString(), "fileSize", fileSize, "chunkSize", chunkSize),
                Collections.emptyList(),
                timeoutMs);
        assertOk(initResp.meta, OPS_INIT);

        long offset = toLong(initResp.meta.get("offset"), 0L);
        int totalChunks = (int) Math.ceil((double) fileSize / chunkSize);
        int chunkId = (int) (offset / chunkSize);

        while (offset < fileSize) {
            int readSize = (int) Math.min(chunkSize, fileSize - offset);
            byte[] chunk = Files.readAllBytes(absolutePath).length == readSize && offset == 0
                    ? Files.readAllBytes(absolutePath)
                    : readRange(absolutePath, offset, readSize);

            ParsedPayload ack = request(
                    slaveId,
                    mapOf("service", SERVICE, "op", OPS_CHUNK, "sessionId", sessionId, "path", str(remotePath), "chunkId", chunkId, "totalChunks", totalChunks, "offset", offset, "size", chunk.length),
                    Collections.singletonList(chunk),
                    timeoutMs);
            assertOk(ack.meta, OPS_CHUNK);
            String ackOp = str(ack.meta.get("op"));
            if (!ackOp.isEmpty() && !OPS_ACK.equals(ackOp)) {
                throw new IllegalStateException("upload ACK op mismatch: " + ackOp);
            }
            offset += chunk.length;
            chunkId += 1;
        }

        ParsedPayload complete = request(
                slaveId,
                mapOf("service", SERVICE, "op", OPS_COMPLETE, "sessionId", sessionId, "path", str(remotePath), "fileName", absolutePath.getFileName().toString(), "fileSize", fileSize, "totalChunks", totalChunks),
                Collections.emptyList(),
                timeoutMs);
        return assertOk(complete.meta, OPS_COMPLETE);
    }

    public Map<String, Object> download(String slaveId, String remotePath, String localPath) throws Exception {
        return download(slaveId, remotePath, localPath, DEFAULT_TIMEOUT_MS, DEFAULT_CHUNK_SIZE, null);
    }

    public Map<String, Object> download(String slaveId, String remotePath, String localPath, int timeoutMs, int chunkSizeInput, String sessionIdInput) throws Exception {
        ensureMaster();
        int chunkSize = normalizeChunkSize(chunkSizeInput);
        String sessionId = sessionIdInput != null && !sessionIdInput.isEmpty() ? sessionIdInput : createSessionId();

        Path absolutePath = Paths.get(str(localPath)).toAbsolutePath().normalize();
        if (Files.exists(absolutePath, LinkOption.NOFOLLOW_LINKS)) {
            BasicFileAttributes attrs = lstat(absolutePath);
            if (attrs.isDirectory() || attrs.isSymbolicLink() || isWindowsReparsePoint(absolutePath) || !attrs.isRegularFile()) {
                throw new IllegalArgumentException("download destination must be a regular file path");
            }
        }
        Files.createDirectories(absolutePath.getParent());
        Path tmpPath = Files.createTempFile(absolutePath.getParent(), absolutePath.getFileName().toString() + ".download-", ".tmp");
        if (lstat(tmpPath).isSymbolicLink() || isWindowsReparsePoint(tmpPath)) {
            throw new IllegalArgumentException("refuse download temp link");
        }

        long offset = Files.size(tmpPath);

        ParsedPayload initResp = request(
                slaveId,
                mapOf("service", SERVICE, "op", OPS_DOWNLOAD_INIT, "sessionId", sessionId, "path", str(remotePath), "chunkSize", chunkSize, "offset", offset),
                Collections.emptyList(),
                timeoutMs);
        assertOk(initResp.meta, OPS_DOWNLOAD_INIT);

        long fileSize = toLong(initResp.meta.get("fileSize"), toLong(initResp.meta.get("size"), 0L));
        offset = toLong(initResp.meta.get("offset"), offset);
        int totalChunks = (int) Math.ceil((double) fileSize / chunkSize);
        int chunkId = (int) (offset / chunkSize);

        while (offset < fileSize) {
            ParsedPayload resp = request(
                    slaveId,
                    mapOf("service", SERVICE, "op", OPS_DOWNLOAD_CHUNK, "sessionId", sessionId, "path", str(remotePath), "chunkId", chunkId, "offset", offset, "chunkSize", chunkSize, "totalChunks", totalChunks),
                    Collections.emptyList(),
                    timeoutMs);
            assertOk(resp.meta, OPS_DOWNLOAD_CHUNK);
            if (resp.body.isEmpty() || resp.body.get(0) == null) {
                throw new IllegalStateException("download chunk payload missing");
            }
            byte[] chunk = resp.body.get(0);
            try (var ch = java.nio.channels.FileChannel.open(tmpPath, StandardOpenOption.WRITE)) {
                ch.position(offset);
                ch.write(java.nio.ByteBuffer.wrap(chunk));
            }
            offset += chunk.length;
            chunkId += 1;
        }

        removeRegularFileIfExists(absolutePath);
        Files.move(tmpPath, absolutePath, StandardCopyOption.REPLACE_EXISTING);

        ParsedPayload complete = request(
                slaveId,
                mapOf("service", SERVICE, "op", OPS_DOWNLOAD_COMPLETE, "sessionId", sessionId, "path", str(remotePath), "fileSize", fileSize, "totalChunks", totalChunks),
                Collections.emptyList(),
                timeoutMs);
        return assertOk(complete.meta, OPS_DOWNLOAD_COMPLETE);
    }

    // slave API
    public synchronized FsService setRoot(String rootPath) {
        return setRoot(rootPath, FsPolicy.defaults());
    }

    public synchronized FsService setRoot(Path rootPath, FsPolicy nextPolicy) {
        return setRoot(rootPath == null ? null : rootPath.toString(), nextPolicy);
    }

    public synchronized FsService setRoot(String rootPath, FsPolicy nextPolicy) {
        ensureSlave();
        Path root = Paths.get(str(rootPath)).toAbsolutePath().normalize();
        if (!Files.exists(root) || !Files.isDirectory(root)) {
            throw new IllegalArgumentException("fs root must exist and be directory");
        }
        invalidateSessions();
        this.rootDir = root;
        this.policy = nextPolicy != null ? nextPolicy : FsPolicy.defaults();
        this.rootGeneration += 1;
        if (!serviceRegistered) {
            node.registerService(SERVICE, event -> CompletableFuture.completedFuture(handleServiceRequest(event.getPayloadFrames())));
            serviceRegistered = true;
        }
        return this;
    }

    public Path getRoot() {
        return rootDir;
    }

    public FsPolicy getPolicy() {
        return policy;
    }

    public boolean isEnabled() {
        return rootDir != null;
    }

    public String getServiceName() {
        return SERVICE;
    }

    public List<byte[]> handleServiceRequest(List<byte[]> payload) {
        try {
            cleanupExpiredSessions();
            ParsedPayload parsed = parseRpcPayload(payload);
            Map<String, Object> meta = parsed.meta;
            String op = str(meta.get("op"));

            switch (op) {
                case OPS_LIST:
                    return handleList(meta);
                case OPS_STAT:
                    return handleStat(meta);
                case OPS_GET:
                    return handleGet(meta);
                case OPS_DELETE:
                    return handleDelete(meta);
                case OPS_RENAME:
                    return handleRename(meta);
                case OPS_PATCH:
                    return handlePatch(meta);
                case OPS_INIT:
                    return handleInit(meta);
                case OPS_CHUNK:
                    return handleChunk(meta, parsed.body);
                case OPS_COMPLETE:
                    return handleComplete(meta);
                case OPS_DOWNLOAD_INIT:
                    return handleDownloadInit(meta);
                case OPS_DOWNLOAD_CHUNK:
                    return handleDownloadChunk(meta);
                case OPS_DOWNLOAD_COMPLETE:
                    return handleDownloadComplete(meta);
                default:
                    throw new IllegalArgumentException("unknown op: " + op);
            }
        } catch (Exception e) {
            String op = "error";
            try {
                op = str(parseRpcPayload(payload).meta.get("op"));
            } catch (Exception ignored) {
            }
            return buildRpcPayload(errMeta(op, e), Collections.emptyList());
        }
    }

    private List<byte[]> handleList(Map<String, Object> meta) throws Exception {
        ResolvedPath target = resolveReadPath(str(meta.get("path")), OPS_LIST);
        if (!Files.isDirectory(target.absolutePath)) {
            throw new IllegalArgumentException("target is not directory");
        }
        List<Map<String, Object>> entries = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(target.absolutePath)) {
            for (Path entry : stream) {
                BasicFileAttributes attrs = lstat(entry);
                entries.add(sanitizeListEntry(entry, attrs));
            }
        }
        Map<String, Object> ok = okMeta(OPS_LIST);
        ok.put("path", str(meta.get("path")));
        ok.put("entries", entries);
        return buildRpcPayload(ok, Collections.emptyList());
    }

    private List<byte[]> handleStat(Map<String, Object> meta) throws Exception {
        ResolvedPath target = resolveReadPath(str(meta.get("path")), OPS_STAT);
        BasicFileAttributes attrs = lstat(target.absolutePath);
        if (attrs.isSymbolicLink()) {
            throw new IllegalArgumentException("refuse symlink");
        }
        Map<String, Object> ok = okMeta(OPS_STAT);
        ok.put("path", str(meta.get("path")));
        ok.put("size", attrs.size());
        ok.put("mtime", attrs.lastModifiedTime().toMillis());
        ok.put("isFile", attrs.isRegularFile());
        ok.put("isDirectory", attrs.isDirectory());
        return buildRpcPayload(ok, Collections.emptyList());
    }

    private List<byte[]> handleGet(Map<String, Object> meta) throws Exception {
        ResolvedPath target = resolveReadPath(str(meta.get("path")), OPS_GET);
        BasicFileAttributes attrs = lstat(target.absolutePath);
        if (attrs.isSymbolicLink() || !attrs.isRegularFile()) {
            throw new IllegalArgumentException("target is not regular file");
        }
        byte[] bytes = Files.readAllBytes(target.absolutePath);
        Map<String, Object> ok = okMeta(OPS_GET);
        ok.put("path", str(meta.get("path")));
        ok.put("size", bytes.length);
        return buildRpcPayload(ok, Collections.singletonList(bytes));
    }

    private List<byte[]> handleDelete(Map<String, Object> meta) throws Exception {
        ResolvedPath target = resolveWritePath(str(meta.get("path")), OPS_DELETE, false);
        forbidRootMutation(target.relativePath, OPS_DELETE);
        removeIfExists(target.absolutePath);
        Map<String, Object> ok = okMeta(OPS_DELETE);
        ok.put("path", str(meta.get("path")));
        return buildRpcPayload(ok, Collections.emptyList());
    }

    private List<byte[]> handleRename(Map<String, Object> meta) throws Exception {
        ResolvedPath from = resolveWritePath(str(meta.get("from")), OPS_RENAME, false);
        ResolvedPath to = resolveWritePath(str(meta.get("to")), OPS_RENAME, true);
        forbidRootMutation(from.relativePath, OPS_RENAME);
        forbidRootMutation(to.relativePath, OPS_RENAME);
        BasicFileAttributes fromStat = lstat(from.absolutePath);
        if (fromStat.isSymbolicLink()) {
            throw new IllegalArgumentException("refuse rename symlink");
        }
        if (Files.exists(to.absolutePath, LinkOption.NOFOLLOW_LINKS)) {
            BasicFileAttributes toStat = lstat(to.absolutePath);
            if (toStat.isSymbolicLink()) {
                throw new IllegalArgumentException("refuse overwrite symlink");
            }
            if (fromStat.isDirectory() != toStat.isDirectory()) {
                throw new IllegalArgumentException("refuse rename across file/directory types");
            }
        }
        if (fromStat.isDirectory() && to.absolutePath.startsWith(from.absolutePath)) {
            throw new IllegalArgumentException("refuse renaming directory into its own descendant");
        }
        Files.createDirectories(to.absolutePath.getParent());
        Files.move(from.absolutePath, to.absolutePath, StandardCopyOption.REPLACE_EXISTING);
        Map<String, Object> ok = okMeta(OPS_RENAME);
        ok.put("from", str(meta.get("from")));
        ok.put("to", str(meta.get("to")));
        return buildRpcPayload(ok, Collections.emptyList());
    }

    private List<byte[]> handlePatch(Map<String, Object> meta) throws Exception {
        ResolvedPath target = resolveWritePath(str(meta.get("path")), OPS_PATCH, false);
        forbidRootMutation(target.relativePath, OPS_PATCH);
        BasicFileAttributes stat = lstat(target.absolutePath);
        if (stat.isSymbolicLink() || !stat.isRegularFile()) {
            throw new IllegalArgumentException("target is not regular file");
        }

        String content = Files.readString(target.absolutePath, StandardCharsets.UTF_8);
        String patchText = normalizePatchText(str(meta.get("patch")));
        String normalizedContent = content.replace("\r\n", "\n");

        List<String> candidates = Arrays.asList(
                patchText,
                patchText + "\n",
                patchText.startsWith("\uFEFF") ? patchText.substring(1) : patchText
        );

        String patched = null;
        for (String candidate : candidates) {
            if (candidate == null || candidate.isEmpty()) continue;
            try {
                patched = applyUnifiedPatch(normalizedContent, candidate);
                if (patched != null) {
                    break;
                }
            } catch (Exception ignored) {
            }
        }

        Map<String, Object> ok = okMeta(OPS_PATCH);
        ok.put("path", str(meta.get("path")));
        if (patched == null) {
            ok.put("applied", false);
            ok.put("message", "patch apply failed");
            return buildRpcPayload(ok, Collections.emptyList());
        }

        Path tmp = Files.createTempFile(target.absolutePath.getParent(), target.absolutePath.getFileName().toString() + ".patch-", ".tmp");
        if (isWindowsReparsePoint(tmp) || lstat(tmp).isSymbolicLink()) {
            throw new IllegalArgumentException("refuse patch temp link");
        }
        Files.writeString(tmp, patched, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
        if (Files.exists(target.absolutePath, LinkOption.NOFOLLOW_LINKS)) {
            BasicFileAttributes targetAttrs = lstat(target.absolutePath);
            if (targetAttrs.isSymbolicLink() || targetAttrs.isDirectory() || isWindowsReparsePoint(target.absolutePath) || !targetAttrs.isRegularFile()) {
                throw new IllegalArgumentException("refuse patch target replacement");
            }
        }
        Files.move(tmp, target.absolutePath, StandardCopyOption.REPLACE_EXISTING);
        ok.put("applied", true);
        return buildRpcPayload(ok, Collections.emptyList());
    }

    private List<byte[]> handleInit(Map<String, Object> meta) throws Exception {
        String sessionId = toSessionId(meta.get("sessionId"));
        if (sessions.containsKey(sessionId)) {
            throw new IllegalArgumentException("sessionId already exists");
        }
        int chunkSize = normalizeChunkSize((int) toLong(meta.get("chunkSize"), DEFAULT_CHUNK_SIZE));
        ResolvedPath target = resolveUploadTarget(meta);
        forbidRootMutation(target.relativePath, OPS_INIT);
        Path tmp = Files.createTempFile(target.absolutePath.getParent(), target.absolutePath.getFileName().toString() + ".upload-", ".tmp");
        if (isWindowsReparsePoint(tmp) || lstat(tmp).isSymbolicLink()) {
            throw new IllegalArgumentException("refuse symlink tmp file");
        }
        long offset = Files.size(tmp);

        Session s = new Session();
        s.mode = "upload";
        s.sessionId = sessionId;
        s.targetPath = target.absolutePath;
        s.relativePath = target.relativePath;
        s.tmpPath = tmp;
        s.chunkSize = chunkSize;
        s.fileSize = toLong(meta.get("fileSize"), 0L);
        s.requestPath = str(meta.get("path"));
        s.rootGeneration = rootGeneration;
        touchSession(s);
        sessions.put(sessionId, s);

        Map<String, Object> ok = okMeta(OPS_RESUME);
        ok.put("sessionId", sessionId);
        ok.put("path", str(meta.get("path")));
        ok.put("offset", offset);
        ok.put("chunkSize", chunkSize);
        return buildRpcPayload(ok, Collections.emptyList());
    }

    private ResolvedPath resolveUploadTarget(Map<String, Object> meta) throws Exception {
        String inputPath = str(meta.get("path"));
        String fileName = str(meta.get("fileName")).trim();
        ResolvedPath initial = resolveWritePath(inputPath, OPS_INIT, true);

        Path existing = initial.absolutePath;
        boolean looksLikeDirectory = inputPath.trim().isEmpty()
                || ".".equals(inputPath.trim())
                || "/".equals(toPosixPath(inputPath).trim())
                || toPosixPath(inputPath).endsWith("/");

        if (Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
            BasicFileAttributes attrs = lstat(existing);
            if (attrs.isSymbolicLink()) {
                throw new IllegalArgumentException("refuse upload target symlink");
            }
            if (attrs.isDirectory()) {
                looksLikeDirectory = true;
            }
        }

        if (!looksLikeDirectory) {
            return initial;
        }
        if (fileName.isEmpty()) {
            throw new IllegalArgumentException("upload target directory requires fileName");
        }

        String normalizedInput = toPosixPath(inputPath).trim();
        String childPath;
        if (normalizedInput.isEmpty() || ".".equals(normalizedInput) || "/".equals(normalizedInput)) {
            childPath = fileName;
        } else if (normalizedInput.endsWith("/")) {
            childPath = normalizedInput + fileName;
        } else {
            childPath = normalizedInput + "/" + fileName;
        }
        return resolveWritePath(childPath, OPS_INIT, true);
    }

    private List<byte[]> handleChunk(Map<String, Object> meta, List<byte[]> body) throws Exception {
        String sessionId = toSessionId(meta.get("sessionId"));
        Session s = sessions.get(sessionId);
        if (s == null || !"upload".equals(s.mode)) {
            throw new IllegalArgumentException("upload session not exists or expired");
        }
        validateActiveSession(s, OPS_CHUNK, str(meta.get("path")));

        enforcePathPolicy(policy, s.relativePath, OPS_CHUNK, true);
        touchSession(s);

        byte[] chunk = body.isEmpty() ? null : body.get(0);
        if (chunk == null) {
            throw new IllegalArgumentException("chunk payload missing");
        }

        long expectedOffset = toLong(meta.get("offset"), 0L);
        int expectedChunkId = (int) toLong(meta.get("chunkId"), 0L);
        long offset = Files.size(s.tmpPath);
        int currentChunkId = (int) (offset / s.chunkSize);

        if (offset != expectedOffset || currentChunkId != expectedChunkId) {
            throw new IllegalArgumentException("chunk offset mismatch: expected=" + expectedOffset + ", actual=" + offset);
        }

        if (Files.exists(s.tmpPath, LinkOption.NOFOLLOW_LINKS)) {
            BasicFileAttributes tmpAttrs = lstat(s.tmpPath);
            if (tmpAttrs.isSymbolicLink() || isWindowsReparsePoint(s.tmpPath) || tmpAttrs.isDirectory()) {
                throw new IllegalArgumentException("refuse unsafe upload temp path");
            }
        }

        try (var ch = java.nio.channels.FileChannel.open(s.tmpPath, StandardOpenOption.WRITE)) {
            ch.position(offset);
            ch.write(java.nio.ByteBuffer.wrap(chunk));
        }

        Map<String, Object> ok = okMeta(OPS_ACK);
        ok.put("sessionId", sessionId);
        ok.put("path", str(meta.get("path")));
        ok.put("chunkId", expectedChunkId);
        ok.put("offset", offset + chunk.length);
        ok.put("size", chunk.length);
        return buildRpcPayload(ok, Collections.emptyList());
    }

    private List<byte[]> handleComplete(Map<String, Object> meta) throws Exception {
        String sessionId = toSessionId(meta.get("sessionId"));
        Session s = sessions.get(sessionId);
        if (s == null || !"upload".equals(s.mode)) {
            throw new IllegalArgumentException("upload session not exists or expired");
        }
        validateActiveSession(s, OPS_COMPLETE, str(meta.get("path")));

        enforcePathPolicy(policy, s.relativePath, OPS_COMPLETE, true);
        if (Files.exists(s.tmpPath, LinkOption.NOFOLLOW_LINKS)) {
            BasicFileAttributes tmpAttrs = lstat(s.tmpPath);
            if (tmpAttrs.isSymbolicLink() || isWindowsReparsePoint(s.tmpPath) || tmpAttrs.isDirectory() || !tmpAttrs.isRegularFile()) {
                throw new IllegalArgumentException("refuse unsafe upload temp path");
            }
        }
        if (Files.exists(s.targetPath, LinkOption.NOFOLLOW_LINKS) && lstat(s.targetPath).isSymbolicLink()) {
            throw new IllegalArgumentException("refuse overwrite symlink");
        }

        if (Files.exists(s.targetPath, LinkOption.NOFOLLOW_LINKS)) {
            BasicFileAttributes targetAttrs = lstat(s.targetPath);
            if (targetAttrs.isDirectory()) {
                throw new IllegalArgumentException("refuse replacing directory with uploaded file");
            }
            if (isWindowsReparsePoint(s.targetPath)) {
                throw new IllegalArgumentException("refuse overwrite reparse point");
            }
        }

        Files.createDirectories(s.targetPath.getParent());
        removeRegularFileIfExists(s.targetPath);
        Files.move(s.tmpPath, s.targetPath, StandardCopyOption.REPLACE_EXISTING);
        sessions.remove(sessionId);

        Map<String, Object> ok = okMeta(OPS_COMPLETE);
        ok.put("sessionId", sessionId);
        ok.put("path", str(meta.get("path")));
        ok.put("ok", true);
        return buildRpcPayload(ok, Collections.emptyList());
    }

    private List<byte[]> handleDownloadInit(Map<String, Object> meta) throws Exception {
        String sessionId = toSessionId(meta.get("sessionId"));
        if (sessions.containsKey(sessionId)) {
            throw new IllegalArgumentException("sessionId already exists");
        }
        int chunkSize = normalizeChunkSize((int) toLong(meta.get("chunkSize"), DEFAULT_CHUNK_SIZE));
        ResolvedPath target = resolveReadPath(str(meta.get("path")), OPS_DOWNLOAD_INIT);

        BasicFileAttributes st = lstat(target.absolutePath);
        if (st.isSymbolicLink() || !st.isRegularFile()) {
            throw new IllegalArgumentException("target is not regular file");
        }

        long fileSize = st.size();
        long offsetInput = toLong(meta.get("offset"), 0L);
        long offset = Math.max(0L, offsetInput);
        if (offset > fileSize) {
            throw new IllegalArgumentException("download offset exceeds fileSize");
        }

        Session s = new Session();
        s.mode = "download";
        s.sessionId = sessionId;
        s.targetPath = target.absolutePath;
        s.relativePath = target.relativePath;
        s.chunkSize = chunkSize;
        s.fileSize = fileSize;
        s.requestPath = str(meta.get("path"));
        s.rootGeneration = rootGeneration;
        touchSession(s);
        sessions.put(sessionId, s);

        Map<String, Object> ok = okMeta(OPS_DOWNLOAD_INIT);
        ok.put("sessionId", sessionId);
        ok.put("path", str(meta.get("path")));
        ok.put("fileSize", fileSize);
        ok.put("offset", offset);
        ok.put("chunkSize", chunkSize);
        return buildRpcPayload(ok, Collections.emptyList());
    }

    private List<byte[]> handleDownloadChunk(Map<String, Object> meta) throws Exception {
        String sessionId = toSessionId(meta.get("sessionId"));
        Session s = sessions.get(sessionId);
        if (s == null || !"download".equals(s.mode)) {
            throw new IllegalArgumentException("download session not exists or expired");
        }
        validateActiveSession(s, OPS_DOWNLOAD_CHUNK, str(meta.get("path")));

        enforcePathPolicy(policy, s.relativePath, OPS_DOWNLOAD_CHUNK, false);
        touchSession(s);

        long offset = toLong(meta.get("offset"), 0L);
        if (offset < 0 || offset > s.fileSize) {
            throw new IllegalArgumentException("download offset invalid");
        }

        BasicFileAttributes fs = lstat(s.targetPath);
        if (fs.isSymbolicLink() || isWindowsReparsePoint(s.targetPath) || !fs.isRegularFile()) {
            throw new IllegalArgumentException("refuse reading unsafe download source");
        }

        int size = (int) Math.min(s.chunkSize, s.fileSize - offset);
        byte[] chunk = readRange(s.targetPath, offset, size);
        int chunkId = (int) toLong(meta.get("chunkId"), 0L);

        Map<String, Object> ok = okMeta(OPS_DOWNLOAD_CHUNK);
        ok.put("sessionId", sessionId);
        ok.put("path", str(meta.get("path")));
        ok.put("chunkId", chunkId);
        ok.put("offset", offset);
        ok.put("size", chunk.length);
        ok.put("eof", offset + chunk.length >= s.fileSize);
        return buildRpcPayload(ok, Collections.singletonList(chunk));
    }

    private List<byte[]> handleDownloadComplete(Map<String, Object> meta) {
        String sessionId = toSessionId(meta.get("sessionId"));
        Session s = sessions.get(sessionId);
        if (s == null || !"download".equals(s.mode)) {
            throw new IllegalArgumentException("download session not exists or expired");
        }
        validateActiveSession(s, OPS_DOWNLOAD_COMPLETE, str(meta.get("path")));
        sessions.remove(sessionId);

        Map<String, Object> ok = okMeta(OPS_DOWNLOAD_COMPLETE);
        ok.put("sessionId", sessionId);
        ok.put("path", str(meta.get("path")));
        ok.put("ok", true);
        return buildRpcPayload(ok, Collections.emptyList());
    }

    // shared helpers
    private ParsedPayload request(String slaveId, Map<String, Object> meta, List<byte[]> bodyFrames, int timeoutMs) throws Exception {
        ensureMaster();
        List<byte[]> payload = buildRpcPayload(meta, bodyFrames);
        List<byte[]> responseFrames = node.SERVICE(slaveId, SERVICE, payload, timeoutMs <= 0 ? DEFAULT_TIMEOUT_MS : timeoutMs).get();
        return parseRpcPayload(responseFrames);
    }

    private static ParsedPayload parseRpcPayload(List<byte[]> payload) throws IOException {
        List<byte[]> frames = payload != null ? payload : Collections.emptyList();
        if (frames.isEmpty()) {
            return new ParsedPayload(new HashMap<>(), Collections.emptyList());
        }
        byte[] metaFrame = frames.get(0);
        Map<String, Object> meta = metaFrame == null || metaFrame.length == 0 ? new HashMap<>() : MAPPER.readValue(metaFrame, MAP_TYPE);
        List<byte[]> body = frames.size() > 1 ? new ArrayList<>(frames.subList(1, frames.size())) : Collections.emptyList();
        return new ParsedPayload(meta, body);
    }

    private static List<byte[]> buildRpcPayload(Map<String, Object> meta, List<byte[]> bodyFrames) {
        try {
            List<byte[]> out = new ArrayList<>();
            out.add(MAPPER.writeValueAsBytes(meta));
            if (bodyFrames != null && !bodyFrames.isEmpty()) {
                out.addAll(bodyFrames);
            }
            return out;
        } catch (Exception e) {
            throw new RuntimeException("build rpc payload failed", e);
        }
    }

    private static Map<String, Object> assertOk(Map<String, Object> meta, String fallbackOp) {
        String op = str(meta.get("op"));
        if (op.isEmpty()) op = fallbackOp;
        Object ok = meta.get("ok");
        if (Boolean.FALSE.equals(ok)) {
            throw new RuntimeException(meta.get("error") != null ? str(meta.get("error")) : "remote fs op failed: " + op);
        }
        return meta;
    }

    private static String createSessionId() {
        return System.currentTimeMillis() + "-" + UUID.randomUUID();
    }

    private static int normalizeChunkSize(int size) {
        int v = size > 0 ? size : DEFAULT_CHUNK_SIZE;
        v = Math.max(64 * 1024, v);
        v = Math.min(32 * 1024 * 1024, v);
        return v;
    }

    private static Map<String, Object> okMeta(String op) {
        Map<String, Object> m = new HashMap<>();
        m.put("ok", true);
        m.put("op", op);
        return m;
    }

    private static Map<String, Object> errMeta(String op, Throwable e) {
        Map<String, Object> m = new HashMap<>();
        m.put("ok", false);
        m.put("op", op != null && !op.isEmpty() ? op : "error");
        m.put("error", e != null && e.getMessage() != null ? e.getMessage() : String.valueOf(e));
        return m;
    }

    private void ensureMaster() {
        if (!"master".equals(node.getRole())) {
            throw new IllegalStateException("fs master API only available on master node");
        }
    }

    private void ensureSlave() {
        if (!"slave".equals(node.getRole())) {
            throw new IllegalStateException("fs slave API only available on slave node");
        }
    }

    private Path ensureRootConfigured() {
        Path root = rootDir;
        if (root == null) {
            throw new IllegalStateException("fs root not configured, call fs().setRoot(...) first");
        }
        return root;
    }

    private ResolvedPath resolveReadPath(String inputPath, String op) throws Exception {
        Path root = ensureRootConfigured();
        Path target = root.resolve(normalizeClientPath(inputPath)).normalize();
        ensureNoLinkInPath(root, target, false);
        String relative = toPosixPath(root.relativize(target).toString());
        enforcePathPolicy(policy, relative, op, false);
        return new ResolvedPath(target, relative);
    }

    private ResolvedPath resolveWritePath(String inputPath, String op, boolean allowMissingLeaf) throws Exception {
        Path root = ensureRootConfigured();
        Path target = root.resolve(normalizeClientPath(inputPath)).normalize();
        ensureNoLinkInPath(root, target, allowMissingLeaf);
        String relative = toPosixPath(root.relativize(target).toString());
        enforcePathPolicy(policy, relative, op, true);
        return new ResolvedPath(target, relative);
    }

    private void ensureNoLinkInPath(Path root, Path target, boolean allowMissingLeaf) throws Exception {
        Path resolvedBase = root.toAbsolutePath().normalize();
        Path resolvedTarget = target.toAbsolutePath().normalize();
        if (!resolvedTarget.startsWith(resolvedBase)) {
            throw new IllegalArgumentException("path escapes root");
        }

        Path rootReal = resolvedBase.toRealPath();
        Path relative = resolvedBase.relativize(resolvedTarget);
        Path current = resolvedBase;
        for (int i = 0; i < relative.getNameCount(); i++) {
            current = current.resolve(relative.getName(i));
            if (!Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                if (allowMissingLeaf && i == relative.getNameCount() - 1) {
                    return;
                }
                throw new IllegalArgumentException("target not exists");
            }

            BasicFileAttributes attrs = lstat(current);
            if (attrs.isSymbolicLink() || isWindowsReparsePoint(current)) {
                throw new IllegalArgumentException("path contains symlink/junction");
            }

            Path currentReal = current.toRealPath();
            if (!currentReal.startsWith(rootReal)) {
                throw new IllegalArgumentException("path escapes root by link traversal");
            }
        }
    }

    private static boolean isWindowsReparsePoint(Path path) {
        try {
            Object v = Files.getAttribute(path, "dos:reparsePoint", LinkOption.NOFOLLOW_LINKS);
            return v instanceof Boolean && (Boolean) v;
        } catch (Exception ignored) {
            return false;
        }
    }

    private static String normalizeClientPath(String inputPath) {
        String raw = str(inputPath).trim();
        if (raw.isEmpty()) {
            throw new IllegalArgumentException("path cannot be empty");
        }
        Path p = Paths.get(raw);
        if (p.isAbsolute()) {
            if (p.getNameCount() == 0) {
                throw new IllegalArgumentException("absolute root path is forbidden");
            }
            Path root = p.getRoot();
            return root != null ? root.relativize(p).toString() : p.toString();
        }
        return raw;
    }

    private void forbidRootMutation(String relativePath, String op) {
        if (relativePath == null || relativePath.isEmpty()) {
            throw new IllegalArgumentException("operation on fs root is forbidden: " + op);
        }
    }

    private static String normalizePatchText(String rawPatch) {
        String[] lines = str(rawPatch).split("\\r?\\n");
        List<String> out = new ArrayList<>();
        for (String line : lines) {
            if (!line.startsWith("===")) {
                out.add(line);
            }
        }
        String joined = String.join("\n", out);
        return joined.replaceAll("\\s+$", "");
    }

    private static String applyUnifiedPatch(String original, String unifiedDiff) throws PatchFailedException {
        List<String> src = Arrays.asList(original.split("\n", -1));
        List<String> diffLines = Arrays.asList(unifiedDiff.split("\\r?\\n"));
        Patch<String> patch = UnifiedDiffUtils.parseUnifiedDiff(diffLines);
        List<String> result = DiffUtils.patch(src, patch);
        return String.join("\n", result);
    }

    private static void removeIfExists(Path target) throws IOException {
        if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) return;
        BasicFileAttributes attrs = lstat(target);
        if (attrs.isSymbolicLink()) {
            throw new IllegalArgumentException("refuse symlink operation");
        }
        if (attrs.isDirectory()) {
            Files.walk(target)
                    .sorted((a, b) -> Integer.compare(b.getNameCount(), a.getNameCount()))
                    .forEach(p -> {
                        try {
                            BasicFileAttributes childAttrs = lstat(p);
                            if (childAttrs.isSymbolicLink() || isWindowsReparsePoint(p)) {
                                throw new IllegalArgumentException("refuse deleting tree containing symlink/reparse point");
                            }
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } else {
            Files.deleteIfExists(target);
        }
    }

    private static void removeRegularFileIfExists(Path target) throws IOException {
        if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) return;
        BasicFileAttributes attrs = lstat(target);
        if (attrs.isSymbolicLink() || attrs.isDirectory() || isWindowsReparsePoint(target) || !attrs.isRegularFile()) {
            throw new IllegalArgumentException("refuse removing non-regular destination");
        }
        Files.deleteIfExists(target);
    }

    private static byte[] readRange(Path path, long offset, int size) throws IOException {
        byte[] out = new byte[size];
        try (var ch = java.nio.channels.FileChannel.open(path, StandardOpenOption.READ)) {
            ch.position(offset);
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(out);
            int total = 0;
            while (buf.hasRemaining()) {
                int n = ch.read(buf);
                if (n < 0) break;
                total += n;
            }
            if (total == out.length) {
                return out;
            }
            return Arrays.copyOf(out, Math.max(total, 0));
        }
    }

    private static BasicFileAttributes lstat(Path p) throws IOException {
        return Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    }

    private void cleanupExpiredSessions() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Session> entry : sessions.entrySet()) {
            Session s = entry.getValue();
            if (s.expiresAt > now) continue;
            sessions.remove(entry.getKey());
            if ("upload".equals(s.mode) && s.tmpPath != null) {
                try {
                    if (Files.exists(s.tmpPath, LinkOption.NOFOLLOW_LINKS)) {
                        BasicFileAttributes attrs = lstat(s.tmpPath);
                        if (!attrs.isSymbolicLink() && !attrs.isDirectory() && !isWindowsReparsePoint(s.tmpPath)) {
                            Files.deleteIfExists(s.tmpPath);
                        }
                    }
                } catch (Exception ignored) {
                }
            }
        }
    }

    private void invalidateSessions() {
        for (Session s : sessions.values()) {
            if ("upload".equals(s.mode) && s.tmpPath != null) {
                try {
                    if (Files.exists(s.tmpPath, LinkOption.NOFOLLOW_LINKS)) {
                        BasicFileAttributes attrs = lstat(s.tmpPath);
                        if (!attrs.isSymbolicLink() && !attrs.isDirectory() && !isWindowsReparsePoint(s.tmpPath)) {
                            Files.deleteIfExists(s.tmpPath);
                        }
                    }
                } catch (Exception ignored) {
                }
            }
        }
        sessions.clear();
    }

    private void validateActiveSession(Session session, String op, String requestPath) {
        if (session.rootGeneration != rootGeneration) {
            throw new IllegalArgumentException("session invalid after fs root change: " + op);
        }
        if (!str(session.requestPath).equals(str(requestPath))) {
            throw new IllegalArgumentException("session path mismatch: " + op);
        }
    }

    private void touchSession(Session s) {
        s.updatedAt = System.currentTimeMillis();
        s.expiresAt = s.updatedAt + SESSION_TTL_MS;
    }

    private static String toPosixPath(String value) {
        return str(value).replace('\\', '/');
    }

    private static boolean isPathAllowedByList(String relativePath, List<String> allowedPaths) {
        if (allowedPaths == null || allowedPaths.isEmpty()) return true;
        String target = toPosixPath(relativePath).replaceFirst("^/+", "");
        for (String entry : allowedPaths) {
            String normalized = toPosixPath(entry).replaceFirst("^/+", "").replaceAll("/+$", "");
            if (normalized.isEmpty()) return true;

            if (normalized.contains("*") || normalized.contains("?")) {
                if (matchesGlob(normalized, target)) return true;
                String prefix = normalized.replaceAll("/+(\\*\\*|\\*)$", "");
                if (!prefix.isEmpty() && (target.equals(prefix) || target.startsWith(prefix + "/"))) {
                    return true;
                }
            } else if (target.equals(normalized) || target.startsWith(normalized + "/")) {
                return true;
            }
        }
        return false;
    }

    private static boolean isPathDeniedByGlobs(String relativePath, List<String> denyGlobs) {
        if (denyGlobs == null || denyGlobs.isEmpty()) return false;
        String target = toPosixPath(relativePath).replaceFirst("^/+", "");
        for (String glob : denyGlobs) {
            if (matchesGlob(glob, target)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesGlob(String glob, String value) {
        Pattern p = globToRegExp(toPosixPath(glob));
        return p.matcher(toPosixPath(value)).matches();
    }

    private static Pattern globToRegExp(String glob) {
        StringBuilder pattern = new StringBuilder("^");
        for (int i = 0; i < glob.length(); i++) {
            char ch = glob.charAt(i);
            char next = i + 1 < glob.length() ? glob.charAt(i + 1) : '\0';
            if (ch == '*') {
                if (next == '*') {
                    pattern.append(".*");
                    i++;
                } else {
                    pattern.append("[^/]*");
                }
                continue;
            }
            if (ch == '?') {
                pattern.append("[^/]");
                continue;
            }
            if ("|\\{}()[].^$+?".indexOf(ch) >= 0) {
                pattern.append('\\');
            }
            pattern.append(ch);
        }
        pattern.append("$");
        return Pattern.compile(pattern.toString());
    }

    private static void enforcePathPolicy(FsPolicy policy, String relativePath, String op, boolean write) {
        String normalized = toPosixPath(relativePath).replaceFirst("^/+", "");

        if (!isPathAllowedByList(normalized, policy.getAllowedPaths())) {
            throw new IllegalArgumentException("access denied by allowedPaths (op=" + op + ")");
        }
        if (isPathDeniedByGlobs(normalized, policy.getDenyGlobs())) {
            throw new IllegalArgumentException("access denied by denyGlobs (op=" + op + ")");
        }
        if (write && policy.isReadOnly()) {
            throw new IllegalArgumentException("access denied: readOnly=true (op=" + op + ")");
        }
        if (write && OPS_DELETE.equals(op) && !policy.isAllowDelete()) {
            throw new IllegalArgumentException("access denied: allowDelete=false");
        }
        if (write && OPS_PATCH.equals(op) && !policy.isAllowPatch()) {
            throw new IllegalArgumentException("access denied: allowPatch=false");
        }
        if (write && (OPS_INIT.equals(op) || OPS_CHUNK.equals(op) || OPS_COMPLETE.equals(op)) && !policy.isAllowUpload()) {
            throw new IllegalArgumentException("access denied: allowUpload=false");
        }
    }

    private static String toSessionId(Object value) {
        String v = str(value);
        if (v.isEmpty()) {
            throw new IllegalArgumentException("sessionId cannot be empty");
        }
        return v;
    }

    private static long toLong(Object value, long fallback) {
        if (value == null) return fallback;
        if (value instanceof Number) return ((Number) value).longValue();
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private static String str(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static Map<String, Object> mapOf(Object... kv) {
        Map<String, Object> out = new HashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            out.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return out;
    }

    private static Map<String, Object> sanitizeListEntry(Path entry, BasicFileAttributes attrs) {
        Map<String, Object> m = new HashMap<>();
        m.put("name", entry.getFileName().toString());
        m.put("type", attrs.isSymbolicLink() ? "symlink" : (attrs.isDirectory() ? "dir" : "file"));
        m.put("size", attrs.size());
        m.put("mtime", attrs.lastModifiedTime().toMillis());
        m.put("isFile", attrs.isRegularFile());
        m.put("isDirectory", attrs.isDirectory());
        m.put("isSymbolicLink", attrs.isSymbolicLink());
        return m;
    }

    public static class ParsedPayload {
        private final Map<String, Object> meta;
        private final List<byte[]> body;

        public ParsedPayload(Map<String, Object> meta, List<byte[]> body) {
            this.meta = meta;
            this.body = body;
        }

        public Map<String, Object> getMeta() {
            return meta;
        }

        public List<byte[]> getBody() {
            return body;
        }
    }

    private static class ResolvedPath {
        private final Path absolutePath;
        private final String relativePath;

        private ResolvedPath(Path absolutePath, String relativePath) {
            this.absolutePath = absolutePath;
            this.relativePath = relativePath;
        }
    }

    private static class Session {
        private String mode;
        private String sessionId;
        private Path targetPath;
        private String relativePath;
        private String requestPath;
        private Path tmpPath;
        private int chunkSize;
        private long fileSize;
        private long rootGeneration;
        private long updatedAt;
        private long expiresAt;
    }
}
