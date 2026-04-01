package com.znl;

public interface ZmqEventListener {
    default void onRouter(ZmqEvent event) {}
    default void onDealer(ZmqEvent event) {}
    default void onRequest(ZmqEvent event) {}
    default void onResponse(ZmqEvent event) {}
    default void onMessage(ZmqEvent event) {}
    default void onPublish(ZmqEvent event) {}
    default void onPush(ZmqEvent event) {}
    default void onServiceRequest(ZmqEvent event) {}
    default void onServiceResponse(ZmqEvent event) {}
    default void onSlaveConnected(String slaveId) {}
    default void onSlaveDisconnected(String slaveId) {}
    default void onAuthFailed(ZmqEvent event) {}
    default void onError(Throwable error) {}
}
