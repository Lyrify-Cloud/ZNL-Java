const path = require("path");
const fs = require("fs/promises");
const os = require("os");
const { pathToFileURL } = require("url");

(async () => {
    const moduleUrl = pathToFileURL(path.resolve(__dirname, "../../../../ZNL-latest/index.js")).href;
    const { ZNL } = await import(moduleUrl);

    const root = await fs.mkdtemp(path.join(os.tmpdir(), "znl-node-fs-get-guard-"));
    await fs.mkdir(path.join(root, "public"), { recursive: true });
    await fs.writeFile(path.join(root, "public", "allowed.txt"), "allowed-text", "utf8");
    await fs.writeFile(path.join(root, "public", "binary.bin"), Buffer.from([1, 2, 3, 4]));
    await fs.writeFile(path.join(root, "public", "large.txt"), "x".repeat(1400), "utf8");

    const slave = new ZNL({
        role: "slave",
        id: "node-slave-fs-get-guard",
        endpoints: {
            router: "tcp://127.0.0.1:6017"
        },
        authKey: "test-secret-key",
        encrypted: true
    });

    slave.fs.setRoot(root, {
        readOnly: false,
        allowDelete: true,
        allowPatch: true,
        allowUpload: true,
        allowedPaths: ["public/**"],
        denyGlobs: [],
        getAllowedExtensions: ["txt"],
        maxGetFileMb: 0.001
    });

    await slave.start();
    console.log("READY");

    setInterval(() => {}, 1000);
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
