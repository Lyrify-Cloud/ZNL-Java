package com.znl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FsPolicy {
    private final boolean readOnly;
    private final boolean allowDelete;
    private final boolean allowPatch;
    private final boolean allowUpload;
    private final List<String> allowedPaths;
    private final List<String> denyGlobs;

    public FsPolicy(boolean readOnly,
                    boolean allowDelete,
                    boolean allowPatch,
                    boolean allowUpload,
                    List<String> allowedPaths,
                    List<String> denyGlobs) {
        this.readOnly = readOnly;
        this.allowDelete = allowDelete;
        this.allowPatch = allowPatch;
        this.allowUpload = allowUpload;
        this.allowedPaths = normalizeList(allowedPaths);
        this.denyGlobs = normalizeList(denyGlobs);
    }

    public static FsPolicy defaults() {
        return new FsPolicy(false, true, true, true, Collections.emptyList(), Collections.emptyList());
    }

    private static List<String> normalizeList(List<String> input) {
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

    public boolean isReadOnly() { return readOnly; }
    public boolean isAllowDelete() { return allowDelete; }
    public boolean isAllowPatch() { return allowPatch; }
    public boolean isAllowUpload() { return allowUpload; }
    public List<String> getAllowedPaths() { return allowedPaths; }
    public List<String> getDenyGlobs() { return denyGlobs; }
}
