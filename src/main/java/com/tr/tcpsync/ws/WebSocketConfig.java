package com.tr.tcpsync.ws;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * Exposes the health WebSocket endpoint at {@code /ws/health}.
 *
 * <p>Clients (a future front end) connect here and receive a
 * {@link com.tr.tcpsync.dto.HealthStatusDto} JSON frame each time a TCP
 * health reply arrives.
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final HealthWebSocketHandler healthWebSocketHandler;

    public WebSocketConfig(HealthWebSocketHandler healthWebSocketHandler) {
        this.healthWebSocketHandler = healthWebSocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(healthWebSocketHandler, "/ws/health")
                .setAllowedOriginPatterns("*");
    }
}
