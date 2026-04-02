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
import java.util.Collections;
import java.util.Map;
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
        Files.write(root.resolve("public").resolve("data.bin"), new byte[] {0x10, 0x20, 0x30, 0x40});
        Files.writeString(root.resolve("public").resolve("large.txt"), "x".repeat(6000), StandardCharsets.UTF_8);
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
                java.util.Collections.singletonList("secret/**"),
                java.util.Collections.singletonList("txt"),
                0.001d
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

    @Test
    public void testNodeMasterFsFullToJavaSlave() throws Exception {
        Path root = Files.createTempDirectory("znl-fs-full-");
        Files.createDirectories(root.resolve("public"));
        Files.write(root.resolve("public").resolve("a.txt"), "hello-a".getBytes(StandardCharsets.UTF_8));

        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("slave");
        options.setId("java-slave-fs-full");
        options.setAuthKey("test-secret-key");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6010");

        ZmqClusterNode slave = new ZmqClusterNode(options);
        slave.fs().setRoot(root, new FsPolicy(
                false,
                true,
                true,
                true,
                java.util.Collections.singletonList("public/**"),
                java.util.Collections.emptyList()
        ));

        File scriptFile = new File("src/test/resources/test-node-master-fs-full.js").getAbsoluteFile();
        assertTrue(scriptFile.exists(), "Node.js master fs full script not found");

        Process fsProcess = null;
        try {
            slave.start().join();
            fsProcess = startNodeProcess(scriptFile);
            assertEquals(0, fsProcess.waitFor(20, TimeUnit.SECONDS) ? fsProcess.exitValue() : -1, "Node.js fs full helper did not exit cleanly");
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

    @Test
    public void testNodeMasterUploadIntoDirectoryToJavaSlave() throws Exception {
        Path root = Files.createTempDirectory("znl-fs-dir-upload-");
        Files.createDirectories(root.resolve("public"));

        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("slave");
        options.setId("java-slave-fs-dir-upload");
        options.setAuthKey("test-secret-key");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6012");

        ZmqClusterNode slave = new ZmqClusterNode(options);
        slave.fs().setRoot(root, new FsPolicy(
                false,
                true,
                true,
                true,
                java.util.Collections.singletonList("public/**"),
                java.util.Collections.emptyList()
        ));

        File scriptFile = new File("src/test/resources/test-node-master-fs-dir-upload.js").getAbsoluteFile();
        assertTrue(scriptFile.exists(), "Node.js master fs dir upload script not found");

        Process fsProcess = null;
        try {
            slave.start().join();
            fsProcess = startNodeProcess(scriptFile);
            assertEquals(0, fsProcess.waitFor(20, TimeUnit.SECONDS) ? fsProcess.exitValue() : -1, "Node.js fs dir upload helper did not exit cleanly");

            assertTrue(Files.isDirectory(root.resolve("public")), "public directory should remain a directory");
            try (var stream = Files.list(root.resolve("public"))) {
                Path uploaded = stream.filter(Files::isRegularFile).findFirst().orElseThrow();
                assertEquals("dir-upload-content", Files.readString(uploaded, StandardCharsets.UTF_8));
            }
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

    @Test
    public void testJavaSlaveUploadIntoExistingDirectoryKeepsDirectory() throws Exception {
        Path root = Files.createTempDirectory("znl-fs-dir-guard-");
        Files.createDirectories(root.resolve("public").resolve("existing-dir"));

        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("slave");
        options.setId("java-slave-dir-guard");
        options.setAuthKey("test-secret-key");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6013");

        ZmqClusterNode slave = new ZmqClusterNode(options);
        slave.fs().setRoot(root, new FsPolicy(false, true, true, true, Collections.singletonList("public/**"), Collections.emptyList()));

        Path upload = Files.createTempFile("znl-upload-guard-", ".txt");
        Files.writeString(upload, "guard-content", StandardCharsets.UTF_8);

        ZmqClusterNodeOptions masterOptions = new ZmqClusterNodeOptions();
        masterOptions.setRole("master");
        masterOptions.setId("java-master-dir-guard");
        masterOptions.setAuthKey("test-secret-key");
        masterOptions.setEncrypted(true);
        masterOptions.getEndpoints().put("router", "tcp://127.0.0.1:6013");

        ZmqClusterNode master = new ZmqClusterNode(masterOptions);
        try {
            slave.start().join();
            master.start().join();

            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline && !master.getSlaves().contains("java-slave-dir-guard")) {
                Thread.sleep(100);
            }

            master.fs().upload("java-slave-dir-guard", upload.toString(), "public/existing-dir", 3000, 0, null);
            assertTrue(Files.isDirectory(root.resolve("public").resolve("existing-dir")));
            assertEquals("guard-content", Files.readString(root.resolve("public").resolve("existing-dir").resolve(upload.getFileName()), StandardCharsets.UTF_8));
        } finally {
            master.stop().join();
            slave.stop().join();
            Files.deleteIfExists(upload);
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

    @Test
    public void testJavaSlaveDeleteCannotRemoveFsRoot() throws Exception {
        Path root = Files.createTempDirectory("znl-fs-root-guard-");
        Files.createDirectories(root.resolve("public"));

        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("slave");
        options.setId("java-slave-root-guard");
        options.setAuthKey("test-secret-key");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6014");

        ZmqClusterNode slave = new ZmqClusterNode(options);
        slave.fs().setRoot(root, new FsPolicy(false, true, true, true, Collections.emptyList(), Collections.emptyList()));

        ZmqClusterNodeOptions masterOptions = new ZmqClusterNodeOptions();
        masterOptions.setRole("master");
        masterOptions.setId("java-master-root-guard");
        masterOptions.setAuthKey("test-secret-key");
        masterOptions.setEncrypted(true);
        masterOptions.getEndpoints().put("router", "tcp://127.0.0.1:6014");

        ZmqClusterNode master = new ZmqClusterNode(masterOptions);
        try {
            slave.start().join();
            master.start().join();

            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline && !master.getSlaves().contains("java-slave-root-guard")) {
                Thread.sleep(100);
            }

            Exception ex = assertThrows(Exception.class, () -> master.fs().delete("java-slave-root-guard", "/", 3000));
            assertTrue(ex.getMessage().contains("root") || ex.getMessage().contains("forbidden"));
            assertTrue(Files.exists(root));
            assertTrue(Files.isDirectory(root));
        } finally {
            master.stop().join();
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

    @Test
    public void testJavaSlaveRenameDirectoryIntoDescendantFails() throws Exception {
        Path root = Files.createTempDirectory("znl-fs-rename-guard-");
        Files.createDirectories(root.resolve("public").resolve("dir").resolve("child"));

        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("slave");
        options.setId("java-slave-rename-guard");
        options.setAuthKey("test-secret-key");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6015");

        ZmqClusterNode slave = new ZmqClusterNode(options);
        slave.fs().setRoot(root, new FsPolicy(false, true, true, true, Collections.singletonList("public/**"), Collections.emptyList()));

        ZmqClusterNodeOptions masterOptions = new ZmqClusterNodeOptions();
        masterOptions.setRole("master");
        masterOptions.setId("java-master-rename-guard");
        masterOptions.setAuthKey("test-secret-key");
        masterOptions.setEncrypted(true);
        masterOptions.getEndpoints().put("router", "tcp://127.0.0.1:6015");

        ZmqClusterNode master = new ZmqClusterNode(masterOptions);
        try {
            slave.start().join();
            master.start().join();

            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline && !master.getSlaves().contains("java-slave-rename-guard")) {
                Thread.sleep(100);
            }

            Exception ex = assertThrows(Exception.class, () -> master.fs().rename("java-slave-rename-guard", "public/dir", "public/dir/child/sub", 3000));
            assertTrue(ex.getMessage().contains("descendant") || ex.getMessage().contains("refuse"));
            assertTrue(Files.isDirectory(root.resolve("public").resolve("dir")));
        } finally {
            master.stop().join();
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

    @Test
    public void testJavaMasterFsCreateAndMkdirToNodeSlave() throws Exception {
        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("master");
        options.setId("java-master-create-mkdir");
        options.setAuthKey("test-secret-key");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6016");

        ZmqClusterNode master = new ZmqClusterNode(options);
        File scriptFile = new File("src/test/resources/test-node-slave-fs-create-mkdir.js").getAbsoluteFile();
        assertTrue(scriptFile.exists(), "Node.js slave fs create/mkdir script not found");

        Process fsProcess = null;
        try {
            master.start().join();
            fsProcess = startNodeProcess(scriptFile);

            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline && !master.getSlaves().contains("node-slave-fs-create-mkdir")) {
                Thread.sleep(100);
            }
            assertTrue(master.getSlaves().contains("node-slave-fs-create-mkdir"), "Node fs create/mkdir slave did not connect");

            Map<String, Object> mkdir = master.fs().mkdir("node-slave-fs-create-mkdir", "public/nested/a", true, true, 3000);
            assertEquals(true, mkdir.get("created"));

            Map<String, Object> create = master.fs().create("node-slave-fs-create-mkdir", "public/nested/a/test.txt", true, false, 3000);
            assertEquals(true, create.get("created"));

            FsService.ParsedPayload get = master.fs().get("node-slave-fs-create-mkdir", "public/nested/a/test.txt");
            assertEquals(0, get.getBody().get(0).length);
        } finally {
            if (fsProcess != null && fsProcess.isAlive()) {
                fsProcess.destroyForcibly();
            }
            master.stop().join();
        }
    }

    @Test
    public void testJavaMasterFsToNodeSlave() throws Exception {
        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("master");
        options.setId("java-master-fs");
        options.setAuthKey("test-secret-key");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6011");

        ZmqClusterNode master = new ZmqClusterNode(options);
        File scriptFile = new File("src/test/resources/test-node-slave-fs-master.js").getAbsoluteFile();
        assertTrue(scriptFile.exists(), "Node.js slave fs master script not found");

        Process fsProcess = null;
        try {
            master.start().join();
            fsProcess = startNodeProcess(scriptFile);

            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline && !master.getSlaves().contains("node-slave-fs-master")) {
                Thread.sleep(100);
            }
            assertTrue(master.getSlaves().contains("node-slave-fs-master"), "Node fs slave did not connect");

            Map<String, Object> st = master.fs().stat("node-slave-fs-master", "public/b.txt");
            assertEquals(true, st.get("isFile"));

            FsService.ParsedPayload get = master.fs().get("node-slave-fs-master", "public/b.txt");
            assertEquals("hello-node", new String(get.getBody().get(0), StandardCharsets.UTF_8));

            master.fs().rename("node-slave-fs-master", "public/b.txt", "public/b2.txt");

            String patchText = String.join("\n",
                    "--- a/public/b2.txt",
                    "+++ b/public/b2.txt",
                    "@@ -1 +1 @@",
                    "-hello-node",
                    "+hello-node-patched"
            );
            Map<String, Object> patch = master.fs().patch("node-slave-fs-master", "public/b2.txt", patchText);
            assertEquals(true, patch.get("applied"));

            Path upload = Files.createTempFile("znl-java-upload-", ".txt");
            Path download = Files.createTempFile("znl-java-download-", ".txt");
            try {
                Files.write(upload, "upload-java-to-node".getBytes(StandardCharsets.UTF_8));
                master.fs().upload("node-slave-fs-master", upload.toString(), "public/up.txt");
                FsService.ParsedPayload uploaded = master.fs().get("node-slave-fs-master", "public/up.txt");
                assertEquals("upload-java-to-node", new String(uploaded.getBody().get(0), StandardCharsets.UTF_8));

                master.fs().download("node-slave-fs-master", "public/up.txt", download.toString());
                assertEquals("upload-java-to-node", Files.readString(download, StandardCharsets.UTF_8));

                master.fs().delete("node-slave-fs-master", "public/up.txt");
            } finally {
                Files.deleteIfExists(upload);
                Files.deleteIfExists(download);
            }
        } finally {
            if (fsProcess != null && fsProcess.isAlive()) {
                fsProcess.destroyForcibly();
            }
            master.stop().join();
        }
    }

    @Test
    public void testJavaMasterFsGetRestrictionsAndProgressToNodeSlave() throws Exception {
        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("master");
        options.setId("java-master-fs-get-guard");
        options.setAuthKey("test-secret-key");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6017");

        ZmqClusterNode master = new ZmqClusterNode(options);
        File scriptFile = new File("src/test/resources/test-node-slave-fs-get-guard.js").getAbsoluteFile();
        assertTrue(scriptFile.exists(), "Node.js slave fs get guard script not found");

        Process fsProcess = null;
        try {
            master.start().join();
            fsProcess = startNodeProcess(scriptFile);

            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline && !master.getSlaves().contains("node-slave-fs-get-guard")) {
                Thread.sleep(100);
            }
            assertTrue(master.getSlaves().contains("node-slave-fs-get-guard"), "Node fs get guard slave did not connect");

            FsService.ParsedPayload allowed = master.fs().get("node-slave-fs-get-guard", "public/allowed.txt");
            assertEquals("allowed-text", new String(allowed.getBody().get(0), StandardCharsets.UTF_8));

            Exception extEx = assertThrows(Exception.class, () -> master.fs().get("node-slave-fs-get-guard", "public/binary.bin"));
            assertTrue(extEx.getMessage().toLowerCase().contains("download"));

            Exception sizeEx = assertThrows(Exception.class, () -> master.fs().get("node-slave-fs-get-guard", "public/large.txt"));
            assertTrue(sizeEx.getMessage().toLowerCase().contains("download"));

            Path binaryDownload = Files.createTempFile("znl-java-progress-binary-", ".bin");
            Path largeDownload = Files.createTempFile("znl-java-progress-large-", ".txt");
            master.fs().download("node-slave-fs-get-guard", "public/binary.bin", binaryDownload.toString());
            master.fs().download("node-slave-fs-get-guard", "public/large.txt", largeDownload.toString());
            assertArrayEquals(new byte[] {1, 2, 3, 4}, Files.readAllBytes(binaryDownload));
            assertEquals("x".repeat(1400), Files.readString(largeDownload, StandardCharsets.UTF_8));

            Path download = Files.createTempFile("znl-java-progress-download-", ".txt");
            Path upload = Files.createTempFile("znl-java-progress-upload-", ".txt");
            try {
                java.util.List<Map<String, Object>> downloadEvents = new java.util.ArrayList<>();
                master.fs().download("node-slave-fs-get-guard", "public/allowed.txt", download.toString(), 3000, 64 * 1024, null, downloadEvents::add);
                assertFalse(downloadEvents.isEmpty());
                assertEquals("init", downloadEvents.get(0).get("phase"));
                assertEquals("download", downloadEvents.get(0).get("direction"));
                assertEquals("complete", downloadEvents.get(downloadEvents.size() - 1).get("phase"));
                assertTrue(downloadEvents.stream().anyMatch(e -> "chunk".equals(e.get("phase"))) || downloadEvents.size() >= 2);
                assertEquals("allowed-text", Files.readString(download, StandardCharsets.UTF_8));

                Files.writeString(upload, "java-progress-upload", StandardCharsets.UTF_8);
                java.util.List<Map<String, Object>> uploadEvents = new java.util.ArrayList<>();
                master.fs().upload("node-slave-fs-get-guard", upload.toString(), "public/uploaded.txt", 3000, 64 * 1024, null, uploadEvents::add);
                assertFalse(uploadEvents.isEmpty());
                assertEquals("init", uploadEvents.get(0).get("phase"));
                assertEquals("upload", uploadEvents.get(0).get("direction"));
                assertEquals("complete", uploadEvents.get(uploadEvents.size() - 1).get("phase"));
                assertTrue(uploadEvents.stream().anyMatch(e -> "chunk".equals(e.get("phase"))) || uploadEvents.size() >= 2);

                FsService.ParsedPayload uploaded = master.fs().get("node-slave-fs-get-guard", "public/uploaded.txt");
                assertEquals("java-progress-upload", new String(uploaded.getBody().get(0), StandardCharsets.UTF_8));

                Map<String, Object> sampleChunk = uploadEvents.stream()
                        .filter(e -> "chunk".equals(e.get("phase")))
                        .findFirst()
                        .orElse(uploadEvents.get(uploadEvents.size() - 1));
                assertTrue(sampleChunk.containsKey("transferred"));
                assertTrue(sampleChunk.containsKey("total"));
                assertTrue(sampleChunk.containsKey("percent"));
                assertTrue(sampleChunk.containsKey("speedBps"));
                assertTrue(sampleChunk.containsKey("etaSeconds"));
                assertTrue(sampleChunk.containsKey("chunkId"));
                assertTrue(sampleChunk.containsKey("totalChunks"));
                assertTrue(sampleChunk.containsKey("size"));
            } finally {
                Files.deleteIfExists(binaryDownload);
                Files.deleteIfExists(largeDownload);
                Files.deleteIfExists(download);
                Files.deleteIfExists(upload);
            }
        } finally {
            if (fsProcess != null && fsProcess.isAlive()) {
                fsProcess.destroyForcibly();
            }
            master.stop().join();
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
