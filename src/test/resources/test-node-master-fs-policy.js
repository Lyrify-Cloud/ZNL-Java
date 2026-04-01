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

    const listOk = await master.fs.list("java-slave-fs", "public");
    if (!Array.isArray(listOk.entries) || listOk.entries.length === 0) {
        throw new Error("Expected public list entries");
    }

    const getOk = await master.fs.get("java-slave-fs", "public/a.txt");
    const text = Buffer.isBuffer(getOk.body?.[0]) ? getOk.body[0].toString("utf8") : "";
    if (text !== "hello-public") {
        throw new Error(`Unexpected file content: ${text}`);
    }

    let deniedByAllowed = false;
    try {
        await master.fs.get("java-slave-fs", "secret/hidden.txt");
    } catch (e) {
        deniedByAllowed = true;
    }
    if (!deniedByAllowed) {
        throw new Error("Expected secret path denied by policy");
    }

    let deniedByReadOnly = false;
    try {
        await master.fs.delete("java-slave-fs", "public/a.txt");
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
