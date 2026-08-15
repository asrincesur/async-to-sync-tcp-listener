package com.tr.tcpsync.dto;

/**
 * DTO built inside {@code TcpService.onDataReceived(byte[])} from the raw
 * bytes the TCP peer sent back, then broadcast over the WebSocket.
 *
 * @param correlationId  ties the event to the originating check-health call
 * @param status         peer-reported status, e.g. {@code "UP"}
 * @param peerEpochMillis timestamp reported by the peer
 * @param latencyMs      round-trip latency reported by the peer
 * @param rawFrame       the raw text frame received (for debugging/demo)
 * @param receivedAt     server-side receive time (epoch millis)
 */
public record HealthStatusDto(
        String correlationId,
        String status,
        long peerEpochMillis,
        long latencyMs,
        String rawFrame,
        long receivedAt) {
}
