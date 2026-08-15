package com.tr.tcpsync.controller;

import com.tr.tcpsync.dto.CheckHealthRequest;
import com.tr.tcpsync.dto.HealthStatusDto;
import com.tr.tcpsync.sdk.TcpTransportException;
import com.tr.tcpsync.tcp.TcpService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

/**
 * Health-check endpoint.
 *
 * <p>The controller builds the raw {@code byte[]} frame itself
 * ({@code HEALTH|<correlationId>|<message>}), sends it over TCP, and returns
 * the peer's correlated reply as the HTTP response body.
 *
 * <p>It returns a {@link CompletableFuture} straight to Spring MVC, so the
 * request is handled asynchronously: the servlet thread is released while we
 * wait for the TCP reply, and Spring writes the response once the future
 * completes (in {@link TcpService#onDataReceived}, on the transport thread).
 */
@RestController
@RequestMapping("/api/check-health")
public class CheckHealthController {

    private final TcpService tcpService;

    @Value("${tcp.request-timeout-ms:5000}")
    private long requestTimeoutMs;

    public CheckHealthController(TcpService tcpService) {
        this.tcpService = tcpService;
    }

    /** Convenience GET so it can be triggered straight from a browser. */
    @GetMapping
    public CompletableFuture<ResponseEntity<HealthStatusDto>> checkHealthGet() {
        return checkHealth(new CheckHealthRequest("ping"));
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<HealthStatusDto>> checkHealth(
            @RequestBody(required = false) CheckHealthRequest request) {

        CheckHealthRequest safeRequest = (request != null) ? request : new CheckHealthRequest("ping");
        String correlationId = UUID.randomUUID().toString();

        // Build the raw byte frame ourselves: HEALTH|<correlationId>|<message>
        String frame = "HEALTH|" + correlationId + "|" + safeRequest.messageOrDefault();
        byte[] payload = frame.getBytes(StandardCharsets.UTF_8);

        // Send over TCP; when the correlated reply arrives, map it to a 200.
        return tcpService.exchange(correlationId, payload, Duration.ofMillis(requestTimeoutMs))
                .thenApply(ResponseEntity::ok)
                .exceptionally(this::toErrorResponse);
    }

    /** Translate future failures into meaningful HTTP statuses. */
    private ResponseEntity<HealthStatusDto> toErrorResponse(Throwable ex) {
        Throwable cause = (ex instanceof CompletionException && ex.getCause() != null)
                ? ex.getCause() : ex;

        if (cause instanceof TimeoutException) {
            return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                    .body(errorDto("TIMEOUT", "no TCP reply within " + requestTimeoutMs + "ms"));
        }
        if (cause instanceof TcpTransportException) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(errorDto("DOWN", cause.getMessage() + " — is TcpHealthServer running?"));
        }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(errorDto("ERROR", String.valueOf(cause.getMessage())));
    }

    private HealthStatusDto errorDto(String status, String detail) {
        long now = System.currentTimeMillis();
        return new HealthStatusDto("n/a", status, now, -1, detail, now);
    }
}
