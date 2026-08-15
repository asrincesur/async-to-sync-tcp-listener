package com.tr.tcpsync.mockserver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone real TCP server — run this yourself, separately from the Spring
 * Boot app. It listens for the frames {@code TcpService} sends and answers
 * with real bytes over the socket.
 *
 * <p>Protocol (newline-delimited UTF-8):
 * <pre>
 *   in  : HEALTH|&lt;correlationId&gt;|&lt;message&gt;
 *   out : HEALTH_ACK|&lt;correlationId&gt;|UP|&lt;serverEpochMillis&gt;|&lt;processingMs&gt;
 * </pre>
 * Any other line is echoed back as {@code ECHO|<line>}.
 *
 * <h2>How to run</h2>
 * From IntelliJ: right-click this class &rarr; Run.
 * <br>From a terminal (after {@code ./mvnw compile}):
 * <pre>
 *   java -cp target/classes com.tr.tcpsync.mockserver.TcpHealthServer
 *   java -cp target/classes com.tr.tcpsync.mockserver.TcpHealthServer 9099
 * </pre>
 */
public class TcpHealthServer {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final AtomicLong REQUEST_COUNT = new AtomicLong();

    public static void main(String[] args) throws IOException {
        int port = resolvePort(args);
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            log("TCP health server listening on port " + port + " — waiting for connections...");
            while (true) {
                Socket client = serverSocket.accept();
                // One thread per connection so multiple clients can talk at once.
                Thread t = new Thread(() -> handleClient(client), "client-" + client.getPort());
                t.setDaemon(false);
                t.start();
            }
        }
    }

    private static void handleClient(Socket socket) {
        String peer = socket.getRemoteSocketAddress().toString();
        log("Client connected: " + peer);
        try (Socket s = socket;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter out = new BufferedWriter(
                     new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8))) {

            String line;
            while ((line = in.readLine()) != null) {
                long start = System.nanoTime();
                String response = buildResponse(line);
                long processingMs = (System.nanoTime() - start) / 1_000_000;
                // processingMs is basically 0; recompute the reply with the real value.
                response = fillProcessingMs(response, processingMs);

                out.write(response);
                out.write('\n');
                out.flush();

                log("#" + REQUEST_COUNT.incrementAndGet()
                        + "  <= " + line + "   =>  " + response);
            }
        } catch (IOException e) {
            log("Client I/O error (" + peer + "): " + e.getMessage());
        }
        log("Client disconnected: " + peer);
    }

    private static String buildResponse(String request) {
        String[] parts = request.split("\\|", 3);
        if (parts.length == 3 && "HEALTH".equals(parts[0])) {
            String correlationId = parts[1];
            // processingMs is a placeholder here; replaced in fillProcessingMs().
            return "HEALTH_ACK|" + correlationId + "|UP|" + System.currentTimeMillis() + "|0";
        }
        return "ECHO|" + request;
    }

    private static String fillProcessingMs(String response, long processingMs) {
        if (response.startsWith("HEALTH_ACK|")) {
            int lastPipe = response.lastIndexOf('|');
            return response.substring(0, lastPipe + 1) + processingMs;
        }
        return response;
    }

    private static int resolvePort(String[] args) {
        if (args.length > 0) {
            try {
                return Integer.parseInt(args[0].trim());
            } catch (NumberFormatException e) {
                log("Invalid port '" + args[0] + "', falling back to 9099");
            }
        }
        String env = System.getenv("TCP_SERVER_PORT");
        if (env != null && !env.isBlank()) {
            try {
                return Integer.parseInt(env.trim());
            } catch (NumberFormatException ignored) {
                // fall through to default
            }
        }
        return 9099;
    }

    private static void log(String message) {
        System.out.println("[" + LocalTime.now().format(TS) + "] " + message);
    }
}
