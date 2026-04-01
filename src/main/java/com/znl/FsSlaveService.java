package com.znl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

public class FsSlaveService {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {};

    private static final String SERVICE = "fs";
    private static final String OPS_LIST = "file/list";
    private static final String OPS_STAT = "file/stat";
    private static final String OPS_GET = "file/get";
    private static final String OPS_DELETE = "file/delete";
    private static final String OPS_RENAME = "file/rename";

    private final ZmqClusterNode node;
    private volatile Path root;
    private volatile FsPolicy policy = FsPolicy.defaults();
    private volatile boolean registered = false;

    public FsSlaveService(ZmqClusterNode node) {
        this.node = Objects.requireNonNull(node);
    }

    public synchronized FsSlaveService setRoot(String rootPath, FsPolicy nextPolicy) {
        return setRoot(Paths.get(rootPath), nextPolicy);
    }

    public synchronized FsSlaveService setRoot(Path rootPath, FsPolicy nextPolicy) {
        if (rootPath == null) {
            throw new IllegalArgumentException("rootPath cannot be null");
        }
        try {
            Path normalized = rootPath.toAbsolutePath().normalize();
            if (!Files.exists(normalized) || !Files.isDirectory(normalized)) {
                throw new IllegalArgumentException("fs root must be an existing directory");
            }
            this.root = normalized;
            this.policy = nextPolicy != null ? nextPolicy : FsPolicy.defaults();
            if (!registered) {
                node.registerService(SERVICE, event -> CompletableFuture.completedFuture(handleServiceRequest(event.getPayloadFrames())));
                registered = true;
            }
            return this;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("failed to set fs root", e);
        }
    }

    public Path getRoot() {
        return root;
    }

    public FsPolicy getPolicy() {
        return policy;
    }

    public boolean isEnabled() {
        return root != null;
    }

    public String getServiceName() {
        return SERVICE;
    }

    public List<byte[]> handleServiceRequest(List<byte[]> payload) {
        String op = "error";
        try {
            ParsedPayload parsed = parsePayload(payload);
            Map<String, Object> meta = parsed.meta;
            op = str(meta.get("op"));
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
                default:
                    throw new IllegalArgumentException("unknown op: " + op);
            }
        } catch (Exception e) {
            return buildPayload(errMeta(op, e));
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
                BasicFileAttributes attrs = Files.readAttributes(entry, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                Map<String, Object> item = new HashMap<>();
                item.put("name", entry.getFileName().toString());
                item.put("type", attrs.isSymbolicLink() ? "symlink" : (attrs.isDirectory() ? "dir" : "file"));
                item.put("size", attrs.size());
                item.put("mtime", attrs.lastModifiedTime().toMillis());
                item.put("isFile", attrs.isRegularFile());
                item.put("isDirectory", attrs.isDirectory());
                item.put("isSymbolicLink", attrs.isSymbolicLink());
                entries.add(item);
            }
        }
        Map<String, Object> ok = okMeta(OPS_LIST);
        ok.put("path", str(meta.get("path")));
        ok.put("entries", entries);
        return buildPayload(ok);
    }

    private List<byte[]> handleStat(Map<String, Object> meta) throws Exception {
        ResolvedPath target = resolveReadPath(str(meta.get("path")), OPS_STAT);
        BasicFileAttributes attrs = Files.readAttributes(target.absolutePath, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (attrs.isSymbolicLink()) {
            throw new IllegalArgumentException("refuse symlink");
        }
        Map<String, Object> ok = okMeta(OPS_STAT);
        ok.put("path", str(meta.get("path")));
        ok.put("size", attrs.size());
        ok.put("mtime", attrs.lastModifiedTime().toMillis());
        ok.put("isFile", attrs.isRegularFile());
        ok.put("isDirectory", attrs.isDirectory());
        return buildPayload(ok);
    }

    private List<byte[]> handleGet(Map<String, Object> meta) throws Exception {
        ResolvedPath target = resolveReadPath(str(meta.get("path")), OPS_GET);
        BasicFileAttributes attrs = Files.readAttributes(target.absolutePath, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attrs.isRegularFile() || attrs.isSymbolicLink()) {
            throw new IllegalArgumentException("target is not regular file");
        }
        byte[] bytes = Files.readAllBytes(target.absolutePath);
        Map<String, Object> ok = okMeta(OPS_GET);
        ok.put("path", str(meta.get("path")));
        ok.put("size", bytes.length);
        return buildPayload(ok, Collections.singletonList(bytes));
    }

    private List<byte[]> handleDelete(Map<String, Object> meta) throws Exception {
        ResolvedPath target = resolveWritePath(str(meta.get("path")), OPS_DELETE, false);
        if (Files.exists(target.absolutePath, LinkOption.NOFOLLOW_LINKS)) {
            BasicFileAttributes attrs = Files.readAttributes(target.absolutePath, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (attrs.isSymbolicLink()) {
                throw new IllegalArgumentException("refuse delete symlink");
            }
            if (attrs.isDirectory()) {
                Files.walk(target.absolutePath)
                        .sorted((a, b) -> Integer.compare(b.getNameCount(), a.getNameCount()))
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            } else {
                Files.deleteIfExists(target.absolutePath);
            }
        }
        Map<String, Object> ok = okMeta(OPS_DELETE);
        ok.put("path", str(meta.get("path")));
        return buildPayload(ok);
    }

    private List<byte[]> handleRename(Map<String, Object> meta) throws Exception {
        ResolvedPath from = resolveWritePath(str(meta.get("from")), OPS_RENAME, false);
        ResolvedPath to = resolveWritePath(str(meta.get("to")), OPS_RENAME, true);
        if (Files.exists(to.absolutePath, LinkOption.NOFOLLOW_LINKS)) {
            BasicFileAttributes toAttrs = Files.readAttributes(to.absolutePath, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (toAttrs.isSymbolicLink()) {
                throw new IllegalArgumentException("refuse overwrite symlink");
            }
        }
        Files.createDirectories(to.absolutePath.getParent());
        Files.move(from.absolutePath, to.absolutePath, StandardCopyOption.REPLACE_EXISTING);
        Map<String, Object> ok = okMeta(OPS_RENAME);
        ok.put("from", str(meta.get("from")));
        ok.put("to", str(meta.get("to")));
        return buildPayload(ok);
    }

    private ResolvedPath resolveReadPath(String inputPath, String op) throws Exception {
        Path rootPath = ensureRootConfigured();
        String normalizedClient = normalizeClientPath(inputPath);
        Path target = rootPath.resolve(normalizedClient).normalize();
        ensureNoLinkTraversal(rootPath, target, false);
        if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("target not exists");
        }
        String rel = toPosixPath(rootPath.relativize(target).toString());
        enforcePolicy(rel, op, false);
        return new ResolvedPath(target, rel);
    }

    private ResolvedPath resolveWritePath(String inputPath, String op, boolean allowMissingLeaf) throws Exception {
        Path rootPath = ensureRootConfigured();
        String normalizedClient = normalizeClientPath(inputPath);
        Path target = rootPath.resolve(normalizedClient).normalize();
        ensureNoLinkTraversal(rootPath, target, allowMissingLeaf);
        String rel = toPosixPath(rootPath.relativize(target).toString());
        enforcePolicy(rel, op, true);
        return new ResolvedPath(target, rel);
    }

    private void ensureNoLinkTraversal(Path rootPath, Path target, boolean allowMissingLeaf) throws Exception {
        Path normalizedRoot = rootPath.toAbsolutePath().normalize();
        Path normalizedTarget = target.toAbsolutePath().normalize();
        if (!normalizedTarget.startsWith(normalizedRoot)) {
            throw new IllegalArgumentException("path escapes root");
        }
        Path rootReal = normalizedRoot.toRealPath();
        Path current = normalizedRoot;
        Path relative = normalizedRoot.relativize(normalizedTarget);
        int idx = 0;
        for (Path seg : relative) {
            idx++;
            current = current.resolve(seg);
            if (!Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                if (allowMissingLeaf && idx == relative.getNameCount()) {
                    return;
                }
                throw new IllegalArgumentException("target segment not exists");
            }
            if (Files.isSymbolicLink(current) || isWindowsReparsePoint(current)) {
                throw new IllegalArgumentException("path contains symlink or junction");
            }
            Path currentReal = current.toRealPath();
            if (!currentReal.startsWith(rootReal)) {
                throw new IllegalArgumentException("path escapes root by link traversal");
            }
        }
    }

    private boolean isWindowsReparsePoint(Path p) {
        try {
            Object v = Files.getAttribute(p, "dos:reparsePoint", LinkOption.NOFOLLOW_LINKS);
            return v instanceof Boolean && (Boolean) v;
        } catch (Exception ignored) {
            return false;
        }
    }

    private void enforcePolicy(String relativePath, String op, boolean write) {
        FsPolicy p = policy;
        String target = toPosixPath(relativePath).replaceFirst("^/+", "");

        if (!isPathAllowedByList(target, p.getAllowedPaths())) {
            throw new IllegalArgumentException("path is outside allowedPaths (op=" + op + ")");
        }
        if (isPathDeniedByGlobs(target, p.getDenyGlobs())) {
            throw new IllegalArgumentException("path matched denyGlobs (op=" + op + ")");
        }
        if (write && p.isReadOnly()) {
            throw new IllegalArgumentException("fs root is readOnly (op=" + op + ")");
        }
        if (write && OPS_DELETE.equals(op) && !p.isAllowDelete()) {
            throw new IllegalArgumentException("allowDelete=false");
        }
    }

    private static boolean isPathAllowedByList(String target, List<String> allowedPaths) {
        if (allowedPaths == null || allowedPaths.isEmpty()) {
            return true;
        }
        for (String entry : allowedPaths) {
            String normalized = toPosixPath(entry).replaceFirst("^/+", "").replaceAll("/+$", "");
            if (normalized.isEmpty()) {
                return true;
            }
            if (normalized.contains("*") || normalized.contains("?")) {
                if (matchesGlob(normalized, target)) {
                    return true;
                }
                String prefix = normalized.replaceFirst("/+(\\*\\*|\\*)$", "");
                if (!prefix.isEmpty() && (target.equals(prefix) || target.startsWith(prefix + "/"))) {
                    return true;
                }
            } else if (target.equals(normalized) || target.startsWith(normalized + "/")) {
                return true;
            }
        }
        return false;
    }

    private static boolean isPathDeniedByGlobs(String target, List<String> denyGlobs) {
        if (denyGlobs == null || denyGlobs.isEmpty()) {
            return false;
        }
        for (String g : denyGlobs) {
            if (matchesGlob(g, target)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesGlob(String glob, String value) {
        return globToRegex(toPosixPath(glob)).matcher(toPosixPath(value)).matches();
    }

    private static Pattern globToRegex(String glob) {
        StringBuilder sb = new StringBuilder("^");
        for (int i = 0; i < glob.length(); i++) {
            char ch = glob.charAt(i);
            char next = i + 1 < glob.length() ? glob.charAt(i + 1) : '\0';
            if (ch == '*') {
                if (next == '*') {
                    sb.append(".*");
                    i++;
                } else {
                    sb.append("[^/]*");
                }
            } else if (ch == '?') {
                sb.append("[^/]");
            } else {
                if (".|\\[]{}()+-^$".indexOf(ch) >= 0) {
                    sb.append('\\');
                }
                sb.append(ch);
            }
        }
        sb.append('$');
        return Pattern.compile(sb.toString());
    }

    private static String normalizeClientPath(String inputPath) {
        String raw = str(inputPath).trim();
        if (raw.isEmpty()) {
            throw new IllegalArgumentException("path cannot be empty");
        }
        Path p = Paths.get(raw);
        if (p.isAbsolute()) {
            Path root = p.getRoot();
            return toPosixPath(root.relativize(p).toString());
        }
        return toPosixPath(raw);
    }

    private Path ensureRootConfigured() {
        Path r = root;
        if (r == null) {
            throw new IllegalStateException("fs root not configured, call fs().setRoot() first");
        }
        return r;
    }

    private static String toPosixPath(String path) {
        return str(path).replace('\\', '/');
    }

    private static String str(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static ParsedPayload parsePayload(List<byte[]> payload) throws IOException {
        List<byte[]> safe = payload != null ? payload : Collections.emptyList();
        if (safe.isEmpty()) {
            return new ParsedPayload(new HashMap<>(), Collections.emptyList());
        }
        byte[] metaBytes = safe.get(0);
        Map<String, Object> meta = metaBytes == null || metaBytes.length == 0
                ? new HashMap<>()
                : MAPPER.readValue(metaBytes, MAP_TYPE);
        List<byte[]> body = safe.size() > 1 ? new ArrayList<>(safe.subList(1, safe.size())) : Collections.emptyList();
        return new ParsedPayload(meta, body);
    }

    private static List<byte[]> buildPayload(Map<String, Object> meta) {
        return buildPayload(meta, Collections.emptyList());
    }

    private static List<byte[]> buildPayload(Map<String, Object> meta, List<byte[]> body) {
        try {
            List<byte[]> out = new ArrayList<>();
            out.add(MAPPER.writeValueAsBytes(meta));
            if (body != null) {
                out.addAll(body);
            }
            return out;
        } catch (Exception e) {
            String fallback = "{\"ok\":false,\"op\":\"error\",\"error\":\"json encode failed\"}";
            return Collections.singletonList(fallback.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static Map<String, Object> okMeta(String op) {
        Map<String, Object> m = new HashMap<>();
        m.put("ok", true);
        m.put("op", op);
        return m;
    }

    private static Map<String, Object> errMeta(String op, Throwable error) {
        Map<String, Object> m = new HashMap<>();
        m.put("ok", false);
        m.put("op", op == null || op.isEmpty() ? "error" : op);
        m.put("error", error != null && error.getMessage() != null ? error.getMessage() : String.valueOf(error));
        return m;
    }

    private static final class ParsedPayload {
        private final Map<String, Object> meta;
        private final List<byte[]> body;

        private ParsedPayload(Map<String, Object> meta, List<byte[]> body) {
            this.meta = meta;
            this.body = body;
        }
    }

    private static final class ResolvedPath {
        private final Path absolutePath;
        private final String relativePath;

        private ResolvedPath(Path absolutePath, String relativePath) {
            this.absolutePath = absolutePath;
            this.relativePath = relativePath;
        }
    }
}
