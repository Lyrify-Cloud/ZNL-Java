const path = require("path");
const fs = require("fs/promises");
const os = require("os");
const { pathToFileURL } = require("url");

(async () => {
    const moduleUrl = pathToFileURL(path.resolve(__dirname, "../../../../ZNL-latest/index.js")).href;
    const { ZNL } = await import(moduleUrl);

    const root = await fs.mkdtemp(path.join(os.tmpdir(), "znl-node-fs-create-"));
    await fs.mkdir(path.join(root, "public"), { recursive: true });

    const slave = new ZNL({
        role: "slave",
        id: "node-slave-fs-create-mkdir",
        endpoints: {
            router: "tcp://127.0.0.1:6016"
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
        denyGlobs: []
    });

    await slave.start();
    console.log("READY");

    setInterval(() => {}, 1000);
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
