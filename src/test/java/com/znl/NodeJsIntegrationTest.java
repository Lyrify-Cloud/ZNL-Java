package com.znl;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class NodeJsIntegrationTest {

    private static Process nodeProcess;
    
    @BeforeAll
    public static void startNodeServer() throws Exception {
        // Find the absolute path to the nodejs test script
        File scriptFile = new File("src/test/resources/test-server.js").getAbsoluteFile();
        assertTrue(scriptFile.exists(), "Node.js test script not found");
        
        ProcessBuilder pb = new ProcessBuilder("node", scriptFile.getAbsolutePath());
        pb.redirectErrorStream(true);
        nodeProcess = pb.start();
        
        // Wait for READY signal from Node.js
        BufferedReader reader = new BufferedReader(new InputStreamReader(nodeProcess.getInputStream()));
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
                Thread.sleep(100);
            }
        }
        
        // Keep printing output in background
        new Thread(() -> {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[NodeJS] " + line);
                }
            } catch (Exception ignored) {}
        }).start();

        assertTrue(ready, "Node.js server failed to start within 5 seconds");
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
        
        slave.subscribe("test-topic", event -> {
            receivedPub[0] = new String(event.getPayload(), StandardCharsets.UTF_8);
            pubLatch.countDown();
        });

        slave.start().join();
        
        // 1. Test DEALER request to Node.js master (Encrypted)
        try {
            CompletableFuture<byte[]> replyFuture = slave.DEALER("ping".getBytes(StandardCharsets.UTF_8), 2000);
            byte[] reply = replyFuture.get(3, TimeUnit.SECONDS);
            assertEquals("pong", new String(reply, StandardCharsets.UTF_8), "Expected pong reply from Node.js");
        } catch (Exception e) {
            fail("Failed to get reply from Node.js master: " + e.getMessage());
        }
        
        // 2. Test receiving publish from Node.js master (Encrypted)
        boolean pubReceived = pubLatch.await(10, TimeUnit.SECONDS);
        assertTrue(pubReceived, "Did not receive publish message from Node.js master");
        assertEquals("hello-from-master", receivedPub[0], "Publish message content mismatch");
        
        slave.stop().join();
    }
}
