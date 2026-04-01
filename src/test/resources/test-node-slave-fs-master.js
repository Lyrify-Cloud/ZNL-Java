const path = require("path");
const fs = require("fs/promises");
const os = require("os");
const { pathToFileURL } = require("url");

(async () => {
    const moduleUrl = pathToFileURL(path.resolve(__dirname, "../../../../ZNL-latest/index.js")).href;
    const { ZNL } = await import(moduleUrl);

    const root = await fs.mkdtemp(path.join(os.tmpdir(), "znl-node-fs-"));
    await fs.mkdir(path.join(root, "public"), { recursive: true });
    await fs.writeFile(path.join(root, "public", "b.txt"), "hello-node", "utf8");

    const slave = new ZNL({
        role: "slave",
        id: "node-slave-fs-master",
        endpoints: {
            router: "tcp://127.0.0.1:6011"
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

    const deadline = Date.now() + 20000;
    while (Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 200));
    }

    await slave.stop();
    await fs.rm(root, { recursive: true, force: true });
    process.exit(0);
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
