const path = require("path");
const fs = require("fs/promises");
const os = require("os");
const { pathToFileURL } = require("url");

(async () => {
    const moduleUrl = pathToFileURL(path.resolve(__dirname, "../../../../ZNL-latest/index.js")).href;
    const { ZNL } = await import(moduleUrl);

    const master = new ZNL({
        role: "master",
        id: "node-master-fs-dir-upload",
        endpoints: {
            router: "tcp://127.0.0.1:6012"
        },
        authKey: "test-secret-key",
        encrypted: true
    });

    const uploadFile = path.join(os.tmpdir(), `znl-dir-upload-${Date.now()}.txt`);
    const uploadName = path.basename(uploadFile);
    await fs.writeFile(uploadFile, "dir-upload-content", "utf8");

    await master.start();
    console.log("READY");

    const deadline = Date.now() + 10000;
    while (Date.now() < deadline) {
        if (master.slaves.includes("java-slave-fs-dir-upload")) {
            break;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
    }

    if (!master.slaves.includes("java-slave-fs-dir-upload")) {
        throw new Error("Java fs slave dir upload did not connect");
    }

    await master.fs.upload("java-slave-fs-dir-upload", uploadFile, "public");
    const getRes = await master.fs.get("java-slave-fs-dir-upload", `public/${uploadName}`);
    if (getRes.body[0].toString("utf8") !== "dir-upload-content") {
        throw new Error("Directory upload content mismatch");
    }

    await fs.rm(uploadFile, { force: true });
    await master.stop();
    process.exit(0);
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
