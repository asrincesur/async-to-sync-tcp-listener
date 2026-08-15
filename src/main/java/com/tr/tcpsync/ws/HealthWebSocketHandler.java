package com.tr.tcpsync.ws;

import com.tr.tcpsync.dto.HealthStatusDto;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fan-out hub for health events.
 *
 * <p>Keeps every open session and pushes each {@link HealthStatusDto}
 * (serialized to JSON) to all of them. {@code TcpService} calls
 * {@link #broadcast(HealthStatusDto)} from inside its {@code onDataReceived}
 * callback once it has turned the inbound bytes into a DTO.
 */
@Component
public class HealthWebSocketHandler extends TextWebSocketHandler {

    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;

    public HealthWebSocketHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessions.put(session.getId(), session);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session.getId());
    }

    /** Serialize the DTO and push it to every connected client. */
    public void broadcast(HealthStatusDto dto) {
        // Jackson 3 throws unchecked JacksonException, so no checked catch here.
        TextMessage message = new TextMessage(objectMapper.writeValueAsBytes(dto));
        sessions.values().forEach(session -> sendQuietly(session, message));
    }

    public int connectedClients() {
        return sessions.size();
    }

    private void sendQuietly(WebSocketSession session, TextMessage message) {
        if (!session.isOpen()) {
            sessions.remove(session.getId());
            return;
        }
        try {
            synchronized (session) {
                session.sendMessage(message);
            }
        } catch (IOException e) {
            sessions.remove(session.getId());
        }
    }
}
