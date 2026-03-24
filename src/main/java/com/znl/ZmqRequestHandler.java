package com.znl;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface ZmqRequestHandler {
    CompletableFuture<byte[]> handle(ZmqEvent event) throws Exception;
}