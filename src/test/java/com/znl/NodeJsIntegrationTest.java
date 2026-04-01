package com.znl;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class NodeJsIntegrationTest {

    private static Process nodeProcess;
    
    @BeforeAll
    public static void startNodeServer() throws Exception {
        File scriptFile = new File("src/test/resources/test-server.js").getAbsoluteFile();
        assertTrue(scriptFile.exists(), "Node.js test script not found");
        nodeProcess = startNodeProcess(scriptFile);
    }
    
    @AfterAll
    public static void stopNodeServer() {
        if (nodeProcess != null) {
            nodeProcess.destroy();
        }
    }

    @Test
    public void testJavaSlaveToNodeMaster() throws Exception {
        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("slave");
        options.setId("java-slave-1");
        options.setAuthKey("test-secret-key");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6005");

        ZmqClusterNode slave = new ZmqClusterNode(options);
        
        CountDownLatch pubLatch = new CountDownLatch(1);
        String[] receivedPub = new String[1];
        
        slave.SUBSCRIBE("test-topic", event -> {
            receivedPub[0] = new String(event.getPayload(), StandardCharsets.UTF_8);
            pubLatch.countDown();
        });

        slave.start().join();

        long onlineDeadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < onlineDeadline && !slave.isMasterOnline()) {
            Thread.sleep(100);
        }
        assertTrue(slave.isMasterOnline(), "Java slave did not observe Node master online state");
        
        try {
            CompletableFuture<byte[]> replyFuture = slave.DEALER("ping".getBytes(StandardCharsets.UTF_8), 2000);
            byte[] reply = replyFuture.get(3, TimeUnit.SECONDS);
            assertEquals("pong", new String(reply, StandardCharsets.UTF_8), "Expected pong reply from Node.js");
        } catch (Exception e) {
            fail("Failed to get reply from Node.js master: " + e.getMessage());
        }
        
        boolean pubReceived = pubLatch.await(10, TimeUnit.SECONDS);
        assertTrue(pubReceived, "Did not receive publish message from Node.js master");
        assertEquals("hello-from-master", receivedPub[0], "Publish message content mismatch");
        
        slave.stop().join();
    }

    @Test
    public void testJavaSlavePushToNodeMaster() throws Exception {
        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("slave");
        options.setId("java-slave-push");
        options.setAuthKey("test-secret-key");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6005");

        ZmqClusterNode slave = new ZmqClusterNode(options);
        slave.start().join();

        long onlineDeadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < onlineDeadline && !slave.isMasterOnline()) {
            Thread.sleep(100);
        }
        assertTrue(slave.isMasterOnline(), "Java slave did not observe Node master online state before push");

        slave.PUSH("test-push", "hello-from-java-push".getBytes(StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS);

        String pushJson = "null";
        long pushDeadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < pushDeadline) {
            byte[] reply = slave.DEALER("get-push".getBytes(StandardCharsets.UTF_8), 2000).get(3, TimeUnit.SECONDS);
            pushJson = new String(reply, StandardCharsets.UTF_8);
            if (!"null".equals(pushJson)) {
                break;
            }
            Thread.sleep(100);
        }

        assertTrue(pushJson.contains("\"topic\":\"test-push\""), "Expected pushed topic in Node.js state");
        assertTrue(pushJson.contains("\"payload\":\"hello-from-java-push\""), "Expected pushed payload in Node.js state");
        assertTrue(pushJson.contains("\"identityText\":\"java-slave-push\""), "Expected pushed identity in Node.js state");

        slave.stop().join();
    }

    @Test
    public void testNodeSlavePushToJavaMaster() throws Exception {
        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("master");
        options.setId("java-master-test");
        options.setAuthKey("test-secret-key");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6006");

        ZmqClusterNode master = new ZmqClusterNode(options);
        CountDownLatch pushLatch = new CountDownLatch(1);
        ZmqEvent[] receivedPush = new ZmqEvent[1];
        master.addListener(new ZmqEventListener() {
            @Override
            public void onPush(ZmqEvent event) {
                receivedPush[0] = event;
                pushLatch.countDown();
            }
        });

        File scriptFile = new File("src/test/resources/test-node-slave-push.js").getAbsoluteFile();
        assertTrue(scriptFile.exists(), "Node.js slave push script not found");

        Process pushProcess = null;
        try {
            master.start().join();
            pushProcess = startNodeProcess(scriptFile);
            assertTrue(pushLatch.await(10, TimeUnit.SECONDS), "Java master did not receive Node.js push");
            assertNotNull(receivedPush[0], "Expected push event");
            assertEquals("node-slave-push", receivedPush[0].getIdentityText());
            assertEquals("node-push-topic", receivedPush[0].getTopic());
            assertEquals("hello-from-node-push", new String(receivedPush[0].getPayload(), StandardCharsets.UTF_8));
            assertEquals(0, pushProcess.waitFor(10, TimeUnit.SECONDS) ? pushProcess.exitValue() : -1, "Node.js push helper did not exit cleanly");
        } finally {
            if (pushProcess != null && pushProcess.isAlive()) {
                pushProcess.destroyForcibly();
            }
            master.stop().join();
        }
    }

    @Test
    public void testJavaMasterServiceToNodeSlave() throws Exception {
        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("master");
        options.setId("java-master-svc");
        options.setAuthKey("test-secret-key");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6008");

        ZmqClusterNode master = new ZmqClusterNode(options);
        File scriptFile = new File("src/test/resources/test-node-slave-service.js").getAbsoluteFile();
        assertTrue(scriptFile.exists(), "Node.js slave service script not found");

        Process svcProcess = null;
        try {
            master.start().join();
            svcProcess = startNodeProcess(scriptFile);

            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline && !master.getSlaves().contains("node-slave-service")) {
                Thread.sleep(100);
            }
            assertTrue(master.getSlaves().contains("node-slave-service"), "Node.js slave service did not connect to Java master");

            byte[] reply = master.SERVICE(
                    "node-slave-service",
                    "echo",
                    "hello-service".getBytes(StandardCharsets.UTF_8),
                    3000
            ).get(5, TimeUnit.SECONDS);
            assertEquals("node-echo:hello-service", new String(reply, StandardCharsets.UTF_8));
        } finally {
            if (svcProcess != null && svcProcess.isAlive()) {
                svcProcess.destroyForcibly();
            }
            master.stop().join();
        }
    }

    @Test
    public void testNodeMasterServiceToJavaSlave() throws Exception {
        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("slave");
        options.setId("java-slave-service");
        options.setAuthKey("test-secret-key");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6007");

        ZmqClusterNode slave = new ZmqClusterNode(options);
        slave.registerService("echo", event -> CompletableFuture.completedFuture(
                java.util.Collections.singletonList(
                        ("java-echo:" + new String(event.getPayload(), StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8)
                )
        ));

        File scriptFile = new File("src/test/resources/test-node-master-service.js").getAbsoluteFile();
        assertTrue(scriptFile.exists(), "Node.js master service script not found");

        Process svcProcess = null;
        try {
            slave.start().join();
            svcProcess = startNodeProcess(scriptFile);
            assertEquals(0, svcProcess.waitFor(12, TimeUnit.SECONDS) ? svcProcess.exitValue() : -1, "Node.js service helper did not exit cleanly");
        } finally {
            if (svcProcess != null && svcProcess.isAlive()) {
                svcProcess.destroyForcibly();
            }
            slave.stop().join();
        }
    }

    @Test
    public void testNodeMasterFsPolicyToJavaSlave() throws Exception {
        Path root = Files.createTempDirectory("znl-fs-");
        Files.createDirectories(root.resolve("public"));
        Files.createDirectories(root.resolve("secret"));
        Files.write(root.resolve("public").resolve("a.txt"), "hello-public".getBytes(StandardCharsets.UTF_8));
        Files.write(root.resolve("secret").resolve("hidden.txt"), "secret-data".getBytes(StandardCharsets.UTF_8));

        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("slave");
        options.setId("java-slave-fs");
        options.setAuthKey("test-secret-key");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6009");

        ZmqClusterNode slave = new ZmqClusterNode(options);
        slave.fs().setRoot(root, new FsPolicy(
                true,
                true,
                true,
                true,
                java.util.Collections.singletonList("public/**"),
                java.util.Collections.singletonList("secret/**")
        ));

        File scriptFile = new File("src/test/resources/test-node-master-fs-policy.js").getAbsoluteFile();
        assertTrue(scriptFile.exists(), "Node.js master fs policy script not found");

        Process fsProcess = null;
        try {
            slave.start().join();
            fsProcess = startNodeProcess(scriptFile);
            assertEquals(0, fsProcess.waitFor(12, TimeUnit.SECONDS) ? fsProcess.exitValue() : -1, "Node.js fs policy helper did not exit cleanly");
        } finally {
            if (fsProcess != null && fsProcess.isAlive()) {
                fsProcess.destroyForcibly();
            }
            slave.stop().join();
            Files.walk(root)
                    .sorted((a, b) -> Integer.compare(b.getNameCount(), a.getNameCount()))
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (Exception ignored) {
                        }
                    });
        }
    }

    private static Process startNodeProcess(File scriptFile) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("node", scriptFile.getAbsolutePath());
        pb.redirectErrorStream(true);
        Process process = pb.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        waitForReady(reader, process);
        Thread outputThread = new Thread(() -> {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[NodeJS] " + line);
                }
            } catch (Exception ignored) {
            }
        });
        outputThread.setDaemon(true);
        outputThread.start();
        return process;
    }

    private static void waitForReady(BufferedReader reader, Process process) throws Exception {
        boolean ready = false;
        long timeout = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < timeout) {
            if (reader.ready()) {
                String line = reader.readLine();
                System.out.println("[NodeJS] " + line);
                if ("READY".equals(line)) {
                    ready = true;
                    break;
                }
            } else {
                if (!process.isAlive()) {
                    break;
                }
                Thread.sleep(100);
            }
        }
        assertTrue(ready, "Node.js server failed to start within 5 seconds");
    }
}
