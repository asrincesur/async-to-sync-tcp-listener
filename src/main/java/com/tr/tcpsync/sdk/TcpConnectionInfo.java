package com.tr.tcpsync.sdk;

/**
 * Connection parameters handed to the (simulated) external TCP SDK.
 *
 * <p>In a real integration this would be filled from your vendor's
 * documentation. Here it is populated by {@code TcpService} inside its
 * {@code @PostConstruct} step and passed to {@link TcpTransportEngine}.
 */
public record TcpConnectionInfo(
        String host,
        int port,
        String clientId,
        int connectTimeoutMs) {

    public TcpConnectionInfo {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        if (connectTimeoutMs <= 0) {
            connectTimeoutMs = 5000;
        }
    }
}
