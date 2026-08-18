package com.tr.tcpsync.tcp;

import com.tr.tcpsync.dto.HealthStatusDto;
import com.tr.tcpsync.sdk.TcpClient;
import com.tr.tcpsync.sdk.TcpConnectionInfo;
import com.tr.tcpsync.sdk.TcpTransportEngine;
import com.tr.tcpsync.sdk.TcpTransportException;
import com.tr.tcpsync.support.ResponseExchanger;
import com.tr.tcpsync.ws.HealthWebSocketHandler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Our implementation of the external SDK's {@link TcpClient} contract.
 *
 * <p>Lifecycle:
 * <ol>
 *     <li>{@code @PostConstruct} builds a {@link TcpConnectionInfo} from
 *         configuration and opens the transport ({@link #connect()}).</li>
 *     <li>{@link #sendMessage(byte[])} writes raw bytes to the peer.</li>
 *     <li>The SDK later calls {@link #onDataReceived(byte[])} on a transport
 *         thread; we turn those bytes into a {@link HealthStatusDto} and push
 *         it to WebSocket clients.</li>
 *     <li>{@code @PreDestroy} closes everything ({@link #disconnect()}).</li>
 * </ol>
 */
@Service
public class TcpService implements TcpClient {

    private static final Logger log = LoggerFactory.getLogger(TcpService.class);

    private final TcpTransportEngine transport = new TcpTransportEngine();
    private final HealthWebSocketHandler webSocketHandler;

    /**
     * Matches asynchronous TCP replies to the HTTP requests waiting for them,
     * by correlationId. All the CompletableFuture bookkeeping lives here.
     */
    private final ResponseExchanger<HealthStatusDto> exchanger = new ResponseExchanger<>();

    @Value("${tcp.host:127.0.0.1}")
    private String host;

    @Value("${tcp.port:9099}")
    private int port;

    @Value("${tcp.client-id:tcp-sync-client}")
    private String clientId;

    @Value("${tcp.connect-timeout-ms:5000}")
    private int connectTimeoutMs;

    public TcpService(HealthWebSocketHandler webSocketHandler) {
        this.webSocketHandler = webSocketHandler;
    }

    /**
     * Build the connection info and open the TCP connection as the bean
     * finishes initializing.
     *
     * <p>The connect is best-effort: if the external TCP server is not running
     * yet, we log a warning and let the web app boot anyway. The next
     * {@link #sendMessage(byte[])} will retry the connection.
     */
    @PostConstruct
    void init() {
        try {
            connect();
        } catch (TcpTransportException e) {
            log.warn("Initial TCP connect failed ({}). "
                    + "Start the server (TcpHealthServer) — it will reconnect on the next request.",
                    e.getMessage());
        }
    }

    @Override
    public void connect() {
        TcpConnectionInfo info = new TcpConnectionInfo(host, port, clientId, connectTimeoutMs);
        log.info("Connecting to TCP peer {}:{} as '{}'", info.host(), info.port(), info.clientId());
        // Register onDataReceived as the SDK's inbound callback.
        transport.connect(info, this::onDataReceived);
        log.info("TCP connection established (connected={})", isConnected());
    }

    @Override
    public void disconnect() {
        log.info("Disconnecting from TCP peer {}:{}", host, port);
        transport.disconnect();
        // Nobody will answer now — fail waiters instead of letting them time out.
        exchanger.failAll(new TcpTransportException("connection closed"));
    }

    @Override
    public void sendMessage(byte[] payload) {
        if (!isConnected()) {
            log.info("Not connected — attempting to (re)connect before sending");
            connect(); // throws TcpTransportException if the server is still down
        }
        log.debug("Writing {} bytes to TCP peer", payload.length);
        transport.write(payload);
    }

    /**
     * Send a frame and return a {@link CompletableFuture} that completes with
     * the peer's correlated reply. This bridges the asynchronous TCP
     * conversation to an asynchronous HTTP response without blocking a thread:
     * the controller returns this future straight to Spring MVC, which frees
     * the servlet thread and writes the response once the future settles.
     *
     * <p>The {@code correlationId} must be the same id embedded in
     * {@code payload}; the peer echoes it back so {@link #onDataReceived}
     * can match the reply to this waiter.
     *
     * <p>The returned future completes:
     * <ul>
     *     <li>normally with the reply when the peer answers;</li>
     *     <li>exceptionally with {@link java.util.concurrent.TimeoutException}
     *         if no reply arrives within {@code timeout} (via
     *         {@link CompletableFuture#orTimeout});</li>
     *     <li>exceptionally with {@link TcpTransportException} if the send fails.</li>
     * </ul>
     */
    public CompletableFuture<HealthStatusDto> exchange(String correlationId, byte[] payload, Duration timeout) {
        // Register BEFORE sending so we never miss a very fast reply.
        CompletableFuture<HealthStatusDto> future = exchanger.register(correlationId, timeout);
        try {
            sendMessage(payload);
        } catch (RuntimeException e) {
            exchanger.completeExceptionally(correlationId, e);
        }
        return future;
    }

    /**
     * SDK callback: raw bytes arrived from the peer. Parse them into a
     * {@link HealthStatusDto}, complete any HTTP request waiting on this
     * correlationId, and also broadcast over the WebSocket.
     *
     * <p>Runs on a transport thread, not a request thread.
     */
    @Override
    public void onDataReceived(byte[] data) {
        String frame = new String(data, StandardCharsets.UTF_8);
        log.debug("Received {} bytes from TCP peer: {}", data.length, frame);
        HealthStatusDto dto = parseFrame(frame);

        // Hand the reply to the HTTP request waiting on this correlationId, if any.
        boolean matched = exchanger.complete(dto.correlationId(), dto);

        // Also push to any WebSocket subscribers (kept for the async channel).
        webSocketHandler.broadcast(dto);
        log.info("Delivered health status '{}' (correlationId={}, awaited={}) to {} ws client(s)",
                dto.status(), dto.correlationId(), matched, webSocketHandler.connectedClients());
    }

    @Override
    public boolean isConnected() {
        return transport.isOpen();
    }

    @PreDestroy
    void shutdown() {
        transport.shutdown();
    }

    /**
     * Parse a peer frame of the form
     * {@code HEALTH_ACK|<correlationId>|<status>|<epochMillis>|<latencyMs>}.
     * Falls back to a DOWN/unknown DTO for anything that does not match.
     */
    private HealthStatusDto parseFrame(String frame) {
        long now = System.currentTimeMillis();
        String[] parts = frame.split("\\|");
        if (parts.length >= 5 && "HEALTH_ACK".equals(parts[0])) {
            return new HealthStatusDto(
                    parts[1],
                    parts[2],
                    parseLong(parts[3], now),
                    parseLong(parts[4], -1),
                    frame,
                    now);
        }
        return new HealthStatusDto("unknown", "DOWN", now, -1, frame, now);
    }

    private static long parseLong(String value, long fallback) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
