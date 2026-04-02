package com.znl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

public class FsPolicy {
    private static final List<String> DEFAULT_GET_ALLOWED_EXTENSIONS = Collections.unmodifiableList(Arrays.asList(
            "txt", "md", "json", "js", "mjs", "cjs", "ts", "mts", "cts", "jsx", "tsx",
            "toml", "yaml", "yml", "ini", "conf", "env", "xml", "csv", "log", "html",
            "htm", "css", "scss", "less", "sql", "sh", "bat", "ps1"
    ));
    private static final double DEFAULT_MAX_GET_FILE_MB = 4d;

    private final boolean readOnly;
    private final boolean allowDelete;
    private final boolean allowPatch;
    private final boolean allowUpload;
    private final List<String> allowedPaths;
    private final List<String> denyGlobs;
    private final List<String> getAllowedExtensions;
    private final double maxGetFileMb;

    public FsPolicy(boolean readOnly,
                    boolean allowDelete,
                    boolean allowPatch,
                    boolean allowUpload,
                    List<String> allowedPaths,
                    List<String> denyGlobs) {
        this(readOnly, allowDelete, allowPatch, allowUpload, allowedPaths, denyGlobs, DEFAULT_GET_ALLOWED_EXTENSIONS, DEFAULT_MAX_GET_FILE_MB);
    }

    public FsPolicy(boolean readOnly,
                    boolean allowDelete,
                    boolean allowPatch,
                    boolean allowUpload,
                    List<String> allowedPaths,
                    List<String> denyGlobs,
                    List<String> getAllowedExtensions,
                    double maxGetFileMb) {
        this.readOnly = readOnly;
        this.allowDelete = allowDelete;
        this.allowPatch = allowPatch;
        this.allowUpload = allowUpload;
        this.allowedPaths = normalizePathList(allowedPaths);
        this.denyGlobs = normalizePathList(denyGlobs);
        this.getAllowedExtensions = normalizeExtensionList(getAllowedExtensions);
        this.maxGetFileMb = normalizeMaxGetFileMb(maxGetFileMb);
    }

    public static FsPolicy defaults() {
        return new FsPolicy(false, true, true, true, Collections.emptyList(), Collections.emptyList(), DEFAULT_GET_ALLOWED_EXTENSIONS, DEFAULT_MAX_GET_FILE_MB);
    }

    private static List<String> normalizePathList(List<String> input) {
        if (input == null || input.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> out = new ArrayList<>();
        for (String item : input) {
            if (item == null) continue;
            String v = item.trim();
            if (!v.isEmpty()) {
                out.add(v.replace('\\', '/'));
            }
        }
        return Collections.unmodifiableList(out);
    }

    private static List<String> normalizeExtensionList(List<String> input) {
        List<String> source = (input == null || input.isEmpty()) ? DEFAULT_GET_ALLOWED_EXTENSIONS : input;
        LinkedHashSet<String> normalized = new LinkedHashSet<>();
        for (String item : source) {
            if (item == null) continue;
            String v = item.trim().toLowerCase(Locale.ROOT);
            while (v.startsWith(".")) {
                v = v.substring(1);
            }
            if (!v.isEmpty()) {
                normalized.add(v);
            }
        }
        if (normalized.isEmpty()) {
            normalized.addAll(DEFAULT_GET_ALLOWED_EXTENSIONS);
        }
        return Collections.unmodifiableList(new ArrayList<>(normalized));
    }

    private static double normalizeMaxGetFileMb(double value) {
        return Double.isFinite(value) && value > 0 ? value : DEFAULT_MAX_GET_FILE_MB;
    }

    public boolean isReadOnly() { return readOnly; }
    public boolean isAllowDelete() { return allowDelete; }
    public boolean isAllowPatch() { return allowPatch; }
    public boolean isAllowUpload() { return allowUpload; }
    public List<String> getAllowedPaths() { return allowedPaths; }
    public List<String> getDenyGlobs() { return denyGlobs; }
    public List<String> getGetAllowedExtensions() { return getAllowedExtensions; }
    public double getMaxGetFileMb() { return maxGetFileMb; }
}
