const path = require("path");
const fs = require("fs/promises");
const os = require("os");
const { pathToFileURL } = require("url");

(async () => {
    const moduleUrl = pathToFileURL(path.resolve(__dirname, "../../../../ZNL-latest/index.js")).href;
    const { ZNL } = await import(moduleUrl);

    const master = new ZNL({
        role: "master",
        id: "node-master-fs-full",
        endpoints: {
            router: "tcp://127.0.0.1:6010"
        },
        authKey: "test-secret-key",
        encrypted: true
    });

    await master.start();
    console.log("READY");

    const deadline = Date.now() + 10000;
    while (Date.now() < deadline) {
        if (master.slaves.includes("java-slave-fs-full")) {
            break;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
    }

    if (!master.slaves.includes("java-slave-fs-full")) {
        throw new Error("Java fs full slave did not connect");
    }

    const slaveId = "java-slave-fs-full";

    const st = await master.fs.stat(slaveId, "public/a.txt");
    if (!st.isFile) throw new Error("stat failed");

    const g = await master.fs.get(slaveId, "public/a.txt");
    if (g.body[0].toString("utf8") !== "hello-a") throw new Error("get mismatch");

    await master.fs.rename(slaveId, "public/a.txt", "public/a-renamed.txt");

    const afterRename = await master.fs.get(slaveId, "public/a-renamed.txt");
    if (afterRename.body[0].toString("utf8") !== "hello-a") throw new Error("rename mismatch");

    const patchText = [
        "--- a/public/a-renamed.txt",
        "+++ b/public/a-renamed.txt",
        "@@ -1 +1 @@",
        "-hello-a",
        "+hello-patched"
    ].join("\n");
    const patchRes = await master.fs.patch(slaveId, "public/a-renamed.txt", patchText);
    if (patchRes.applied !== true) throw new Error("patch not applied");

    const afterPatch = await master.fs.get(slaveId, "public/a-renamed.txt");
    if (afterPatch.body[0].toString("utf8") !== "hello-patched") throw new Error("patch result mismatch");

    const uploadFile = path.join(os.tmpdir(), `znl-upload-${Date.now()}.txt`);
    await fs.writeFile(uploadFile, "upload-content-123", "utf8");
    await master.fs.upload(slaveId, uploadFile, "public/uploaded.txt");
    const uploaded = await master.fs.get(slaveId, "public/uploaded.txt");
    if (uploaded.body[0].toString("utf8") !== "upload-content-123") throw new Error("upload mismatch");

    const downloadFile = path.join(os.tmpdir(), `znl-download-${Date.now()}.txt`);
    await master.fs.download(slaveId, "public/uploaded.txt", downloadFile);
    const downloaded = await fs.readFile(downloadFile, "utf8");
    if (downloaded !== "upload-content-123") throw new Error("download mismatch");

    await master.fs.delete(slaveId, "public/uploaded.txt");
    let deleted = false;
    try {
        await master.fs.get(slaveId, "public/uploaded.txt");
    } catch {
        deleted = true;
    }
    if (!deleted) throw new Error("delete failed");

    await fs.rm(uploadFile, { force: true });
    await fs.rm(downloadFile, { force: true });
    await master.stop();
    process.exit(0);
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
