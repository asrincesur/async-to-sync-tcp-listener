package com.tr.tcpsync.dto;

/**
 * Body of a POST /api/check-health call.
 *
 * @param message free-text payload the caller wants delivered to the peer;
 *                defaults to {@code "ping"} when blank.
 */
public record CheckHealthRequest(String message) {

    public String messageOrDefault() {
        return (message == null || message.isBlank()) ? "ping" : message;
    }
}
