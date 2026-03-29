const path = require("path");
const { pathToFileURL } = require("url");

(async () => {
    const moduleUrl = pathToFileURL(path.resolve(__dirname, "../../../../ZNL-latest/index.js")).href;
    const { ZNL } = await import(moduleUrl);

    const slave = new ZNL({
        role: "slave",
        id: "node-slave-push",
        endpoints: {
            router: "tcp://127.0.0.1:6006"
        },
        authKey: "test-secret-key",
        encrypted: true
    });

    await slave.start();
    console.log("READY");

    const deadline = Date.now() + 10000;
    while (!slave.isMasterOnline() && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 100));
    }

    if (!slave.isMasterOnline()) {
        throw new Error("Master did not become online");
    }

    slave.PUSH("node-push-topic", Buffer.from("hello-from-node-push"));
    await new Promise((resolve) => setTimeout(resolve, 1000));
    await slave.stop();
    process.exit(0);
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
