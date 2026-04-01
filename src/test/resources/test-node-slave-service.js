const path = require("path");
const { pathToFileURL } = require("url");

(async () => {
    const moduleUrl = pathToFileURL(path.resolve(__dirname, "../../../../ZNL-latest/index.js")).href;
    const { ZNL } = await import(moduleUrl);

    const slave = new ZNL({
        role: "slave",
        id: "node-slave-service",
        endpoints: {
            router: "tcp://127.0.0.1:6008"
        },
        authKey: "test-secret-key",
        encrypted: true
    });

    slave.registerService("echo", async (payload) => {
        const text = Buffer.isBuffer(payload) ? payload.toString("utf8") : String(payload ?? "");
        return Buffer.from(`node-echo:${text}`);
    });

    await slave.start();
    console.log("READY");

    setInterval(() => {}, 1000);
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
