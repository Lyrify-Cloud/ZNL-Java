const path = require('path');
const ZmqClusterNode = require(path.resolve(__dirname, '../../../../ZNL_temp/src/ZNL')).ZNL;

const master = new ZmqClusterNode({
    role: 'master',
    id: 'node-master-test',
    endpoints: {
        router: 'tcp://127.0.0.1:6005'
    },
    authKey: 'test-secret-key',
    encrypted: true
});

master.ROUTER(async (req) => {
    const payload = req.payload.toString('utf8');
    if (payload === 'ping') {
        return Buffer.from('pong');
    }
    return Buffer.from('unknown');
});

master.on('slave_connected', (id) => {
    console.log(`[NodeJS] Slave connected: ${id}`);
    
    // We send multiple publishes just to be safe
    let count = 0;
    const interval = setInterval(() => {
        master.publish('test-topic', Buffer.from('hello-from-master'));
        console.log(`[NodeJS] Publish message sent ${++count}`);
        if (count >= 5) clearInterval(interval);
    }, 1000);
});

master.start().then(() => {
    console.log('[NodeJS] Master started on 6005');
    // Notify Java test runner that server is ready
    console.log('READY');
}).catch(err => {
    console.error(err);
    process.exit(1);
});

// Auto exit after 10s if test hangs
setTimeout(() => {
    console.log('[NodeJS] Timeout, exiting');
    process.exit(1);
}, 10000);
