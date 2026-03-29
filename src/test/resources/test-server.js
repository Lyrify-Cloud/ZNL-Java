const path = require("path");
const { pathToFileURL } = require("url");

(async () => {
    const moduleUrl = pathToFileURL(path.resolve(__dirname, "../../../../ZNL-latest/index.js")).href;
    const { ZNL } = await import(moduleUrl);
    let latestPush = null;

    const master = new ZNL({
        role: "master",
        id: "node-master-test",
        endpoints: {
            router: "tcp://127.0.0.1:6005"
        },
        authKey: "test-secret-key",
        encrypted: true
    });

    master.ROUTER(async (req) => {
        const payload = req.payload.toString("utf8");
        if (payload === "ping") {
            return Buffer.from("pong");
        }
        if (payload === "get-push") {
            return Buffer.from(JSON.stringify(latestPush));
        }
        return Buffer.from("unknown");
    });

    master.on("push", (event) => {
        latestPush = {
            identityText: event.identityText,
            topic: event.topic,
            payload: event.payload.toString("utf8")
        };
        console.log(`[NodeJS] Push received [${event.topic}] from ${event.identityText}: ${latestPush.payload}`);
    });

    master.on("slave_connected", (id) => {
        console.log(`[NodeJS] Slave connected: ${id}`);
        let count = 0;
        const interval = setInterval(() => {
            master.PUBLISH("test-topic", Buffer.from("hello-from-master"));
            console.log(`[NodeJS] Publish message sent ${++count}`);
            if (count >= 5) clearInterval(interval);
        }, 1000);
    });

    await master.start();
    console.log("[NodeJS] Master started on 6005");
    console.log("READY");
})().catch((err) => {
    console.error(err);
    process.exit(1);
});

setTimeout(() => {
    console.log("[NodeJS] Timeout, exiting");
    process.exit(1);
}, 30000);
