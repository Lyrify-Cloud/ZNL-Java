package com.znl;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ZmqClusterNodeRecoveryTest {

    @Test
    public void testSlaveRestartDealerAfterMasterOffline() throws Exception {
        String endpoint = "tcp://127.0.0.1:6011";
        ZmqClusterNode master1 = null;
        ZmqClusterNode master2 = null;
        ZmqClusterNode slave = null;
        try {
            ZmqClusterNodeOptions masterOptions = new ZmqClusterNodeOptions();
            masterOptions.setRole("master");
            masterOptions.setId("master-a");
            masterOptions.setHeartbeatInterval(200);
            masterOptions.setHeartbeatTimeoutMs(400);
            masterOptions.getEndpoints().put("router", endpoint);
            master1 = new ZmqClusterNode(masterOptions);
            master1.ROUTER(req -> CompletableFuture.completedFuture("pong-1".getBytes(StandardCharsets.UTF_8)));
            master1.start().join();

            ZmqClusterNodeOptions slaveOptions = new ZmqClusterNodeOptions();
            slaveOptions.setRole("slave");
            slaveOptions.setId("slave-a");
            slaveOptions.setHeartbeatInterval(200);
            slaveOptions.setHeartbeatTimeoutMs(400);
            slaveOptions.getEndpoints().put("router", endpoint);
            slave = new ZmqClusterNode(slaveOptions);
            slave.start().join();

            long firstOnlineDeadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < firstOnlineDeadline && !slave.isMasterOnline()) {
                Thread.sleep(50);
            }
            assertTrue(slave.isMasterOnline());

            master1.stop().join();

            long offlineDeadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < offlineDeadline && slave.isMasterOnline()) {
                Thread.sleep(50);
            }
            assertFalse(slave.isMasterOnline());

            ZmqClusterNodeOptions master2Options = new ZmqClusterNodeOptions();
            master2Options.setRole("master");
            master2Options.setId("master-a");
            master2Options.setHeartbeatInterval(200);
            master2Options.setHeartbeatTimeoutMs(400);
            master2Options.getEndpoints().put("router", endpoint);
            master2 = new ZmqClusterNode(master2Options);

            AtomicInteger heartbeatCount = new AtomicInteger();
            CountDownLatch reconnectLatch = new CountDownLatch(1);
            master2.addListener(new ZmqEventListener() {
                @Override
                public void onSlaveConnected(String slaveId) {
                    if ("slave-a".equals(slaveId)) {
                        reconnectLatch.countDown();
                    }
                }

                @Override
                public void onRouter(ZmqEvent event) {
                    if ("heartbeat".equals(event.getKind())) {
                        heartbeatCount.incrementAndGet();
                    }
                }
            });
            master2.ROUTER(req -> CompletableFuture.completedFuture("pong-2".getBytes(StandardCharsets.UTF_8)));
            master2.start().join();

            assertTrue(reconnectLatch.await(5, TimeUnit.SECONDS));

            long secondOnlineDeadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < secondOnlineDeadline && !slave.isMasterOnline()) {
                Thread.sleep(50);
            }
            assertTrue(slave.isMasterOnline());

            Thread.sleep(700);
            assertTrue(heartbeatCount.get() <= 5, "heartbeat backlog flushed after reconnect: " + heartbeatCount.get());

            byte[] reply = slave.DEALER("ping".getBytes(StandardCharsets.UTF_8), 1500).get(3, TimeUnit.SECONDS);
            assertEquals("pong-2", new String(reply, StandardCharsets.UTF_8));
        } finally {
            if (slave != null) {
                slave.stop().join();
            }
            if (master1 != null) {
                master1.stop().join();
            }
            if (master2 != null) {
                master2.stop().join();
            }
        }
    }

    @Test
    public void testEncryptedSlaveRebindsToNewMasterIdAfterRestart() throws Exception {
        String endpoint = "tcp://127.0.0.1:6012";
        ZmqClusterNode master1 = null;
        ZmqClusterNode master2 = null;
        ZmqClusterNode slave = null;
        try {
            ZmqClusterNodeOptions masterOptions = new ZmqClusterNodeOptions();
            masterOptions.setRole("master");
            masterOptions.setId("master-a");
            masterOptions.setEncrypted(true);
            masterOptions.setAuthKey("rebind-secret");
            masterOptions.setHeartbeatInterval(200);
            masterOptions.setHeartbeatTimeoutMs(400);
            masterOptions.getEndpoints().put("router", endpoint);
            master1 = new ZmqClusterNode(masterOptions);
            master1.ROUTER(req -> CompletableFuture.completedFuture("pong-a".getBytes(StandardCharsets.UTF_8)));
            master1.start().join();

            ZmqClusterNodeOptions slaveOptions = new ZmqClusterNodeOptions();
            slaveOptions.setRole("slave");
            slaveOptions.setId("slave-b");
            slaveOptions.setEncrypted(true);
            slaveOptions.setAuthKey("rebind-secret");
            slaveOptions.setHeartbeatInterval(200);
            slaveOptions.setHeartbeatTimeoutMs(400);
            slaveOptions.getEndpoints().put("router", endpoint);
            slave = new ZmqClusterNode(slaveOptions);
            slave.start().join();

            long firstOnlineDeadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < firstOnlineDeadline && !slave.isMasterOnline()) {
                Thread.sleep(50);
            }
            assertTrue(slave.isMasterOnline());

            master1.stop().join();

            long offlineDeadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < offlineDeadline && slave.isMasterOnline()) {
                Thread.sleep(50);
            }
            assertFalse(slave.isMasterOnline());

            ZmqClusterNodeOptions master2Options = new ZmqClusterNodeOptions();
            master2Options.setRole("master");
            master2Options.setId("master-b");
            master2Options.setEncrypted(true);
            master2Options.setAuthKey("rebind-secret");
            master2Options.setHeartbeatInterval(200);
            master2Options.setHeartbeatTimeoutMs(400);
            master2Options.getEndpoints().put("router", endpoint);
            master2 = new ZmqClusterNode(master2Options);
            master2.ROUTER(req -> CompletableFuture.completedFuture("pong-b".getBytes(StandardCharsets.UTF_8)));
            master2.start().join();

            long secondOnlineDeadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < secondOnlineDeadline && !slave.isMasterOnline()) {
                Thread.sleep(50);
            }
            assertTrue(slave.isMasterOnline());

            byte[] reply = slave.DEALER("ping".getBytes(StandardCharsets.UTF_8), 1500).get(3, TimeUnit.SECONDS);
            assertEquals("pong-b", new String(reply, StandardCharsets.UTF_8));
        } finally {
            if (slave != null) {
                slave.stop().join();
            }
            if (master1 != null) {
                master1.stop().join();
            }
            if (master2 != null) {
                master2.stop().join();
            }
        }
    }
}
