const path = require("path");
const { pathToFileURL } = require("url");

(async () => {
    const moduleUrl = pathToFileURL(path.resolve(__dirname, "../../../../ZNL-latest/index.js")).href;
    const { ZNL } = await import(moduleUrl);

    const master = new ZNL({
        role: "master",
        id: "node-master-fs-policy",
        endpoints: {
            router: "tcp://127.0.0.1:6009"
        },
        authKey: "test-secret-key",
        encrypted: true
    });

    await master.start();
    console.log("READY");

    const deadline = Date.now() + 10000;
    while (Date.now() < deadline) {
        if (master.slaves.includes("java-slave-fs")) {
            break;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
    }

    if (!master.slaves.includes("java-slave-fs")) {
        throw new Error("Java fs slave did not connect");
    }

    const slaveId = "java-slave-fs";

    const listOk = await master.fs.list(slaveId, "public");
    if (!Array.isArray(listOk.entries) || listOk.entries.length === 0) {
        throw new Error("Expected public list entries");
    }

    const getOk = await master.fs.get(slaveId, "public/a.txt");
    const text = Buffer.isBuffer(getOk.body?.[0]) ? getOk.body[0].toString("utf8") : "";
    if (text !== "hello-public") {
        throw new Error(`Unexpected file content: ${text}`);
    }

    let deniedByAllowed = false;
    try {
        await master.fs.get(slaveId, "secret/hidden.txt");
    } catch (e) {
        deniedByAllowed = true;
    }
    if (!deniedByAllowed) {
        throw new Error("Expected secret path denied by policy");
    }

    let deniedByExtension = false;
    try {
        await master.fs.get(slaveId, "public/data.bin");
    } catch (e) {
        deniedByExtension = /download/i.test(String(e?.message || ""));
    }
    if (!deniedByExtension) {
        throw new Error("Expected get denied by extension policy with download hint");
    }

    let deniedBySize = false;
    try {
        await master.fs.get(slaveId, "public/large.txt");
    } catch (e) {
        deniedBySize = /download/i.test(String(e?.message || ""));
    }
    if (!deniedBySize) {
        throw new Error("Expected get denied by size policy with download hint");
    }

    const downloadDir = path.resolve(__dirname, "../tmp");
    await require("fs/promises").mkdir(downloadDir, { recursive: true });
    const binaryOut = path.join(downloadDir, `policy-binary-${Date.now()}.bin`);
    const largeOut = path.join(downloadDir, `policy-large-${Date.now()}.txt`);

    await master.fs.download(slaveId, "public/data.bin", binaryOut);
    await master.fs.download(slaveId, "public/large.txt", largeOut);

    const [binaryDownloaded, largeDownloaded] = await Promise.all([
        require("fs/promises").readFile(binaryOut),
        require("fs/promises").readFile(largeOut, "utf8")
    ]);

    if (!Buffer.isBuffer(binaryDownloaded) || binaryDownloaded.length !== 4 || binaryDownloaded[0] !== 0x10) {
        throw new Error("download binary content mismatch");
    }
    if (largeDownloaded.length !== 6000) {
        throw new Error("download large content mismatch");
    }

    await Promise.all([
        require("fs/promises").rm(binaryOut, { force: true }),
        require("fs/promises").rm(largeOut, { force: true })
    ]);

    let deniedByReadOnly = false;
    try {
        await master.fs.delete(slaveId, "public/a.txt");
    } catch (e) {
        deniedByReadOnly = true;
    }
    if (!deniedByReadOnly) {
        throw new Error("Expected delete denied by readOnly");
    }

    await master.stop();
    process.exit(0);
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
