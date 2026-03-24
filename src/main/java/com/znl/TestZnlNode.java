package com.znl;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class TestZnlNode {
    public static void main(String[] args) throws Exception {
        ZmqClusterNodeOptions options = new ZmqClusterNodeOptions();
        options.setRole("slave");
        options.setId("slave-java-001");
        options.setAuthKey("my-secret");
        options.setEncrypted(true);
        options.getEndpoints().put("router", "tcp://127.0.0.1:6003");

        ZmqClusterNode node = new ZmqClusterNode(options);
        
        node.addListener(new ZmqEventListener() {
            @Override
            public void onMessage(ZmqEvent event) {
                System.out.println("Message: " + event.getKind() + " " + event.getTopic());
            }
            @Override
            public void onAuthFailed(ZmqEvent event) {
                System.out.println("Auth failed!");
            }
            @Override
            public void onError(Throwable error) {
                error.printStackTrace();
            }
        });

        node.start().join();
        System.out.println("Started!");
        
        Thread.sleep(1000);
        
        try {
            byte[] reply = node.DEALER("hello from java".getBytes(StandardCharsets.UTF_8), 5000).join();
            System.out.println("Reply: " + new String(reply, StandardCharsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        node.stop().join();
    }
}
