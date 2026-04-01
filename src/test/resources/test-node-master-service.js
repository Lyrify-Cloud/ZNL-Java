const path = require("path");
const { pathToFileURL } = require("url");

(async () => {
    const moduleUrl = pathToFileURL(path.resolve(__dirname, "../../../../ZNL-latest/index.js")).href;
    const { ZNL } = await import(moduleUrl);

    const master = new ZNL({
        role: "master",
        id: "node-master-service",
        endpoints: {
            router: "tcp://127.0.0.1:6007"
        },
        authKey: "test-secret-key",
        encrypted: true
    });

    await master.start();
    console.log("READY");

    const deadline = Date.now() + 10000;
    while (Date.now() < deadline) {
        if (master.slaves.includes("java-slave-service")) {
            break;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
    }

    if (!master.slaves.includes("java-slave-service")) {
        throw new Error("Java slave did not connect");
    }

    const result = await master._serviceRequest(
        "echo",
        "java-slave-service",
        [Buffer.from("hello-service")],
        { timeoutMs: 3000 }
    );

    const text = Buffer.isBuffer(result) ? result.toString("utf8") : Buffer.from(result[0]).toString("utf8");
    if (text !== "java-echo:hello-service") {
        throw new Error(`Unexpected service response: ${text}`);
    }

    await master.stop();
    process.exit(0);
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
