package com.znl;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface ZmqMultipartRequestHandler {
    CompletableFuture<List<byte[]>> handle(ZmqEvent event) throws Exception;
}
