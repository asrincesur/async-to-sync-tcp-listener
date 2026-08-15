package com.tr.tcpsync.sdk;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Transport provided by the (simulated) external TCP library.
 *
 * <p>This now opens a <b>real</b> {@link Socket} to the peer instead of
 * faking it in memory. Frames are newline-delimited UTF-8 text:
 * <pre>
 *   request : HEALTH|&lt;correlationId&gt;|&lt;payload&gt;\n
 *   reply   : HEALTH_ACK|&lt;correlationId&gt;|UP|&lt;epochMillis&gt;|&lt;processingMs&gt;\n
 * </pre>
 *
 * <p>A background reader thread blocks on the socket, and for every inbound
 * line it invokes the consumer registered in
 * {@link #connect(TcpConnectionInfo, Consumer)} — exactly how a real SDK
 * surfaces asynchronous peer traffic to your callback.
 *
 * <p>Run {@code com.tr.tcpsync.mockserver.TcpHealthServer} to have something
 * on the other end.
 */
public class TcpTransportEngine {

    private volatile Socket socket;
    private volatile BufferedWriter writer;
    private volatile Consumer<byte[]> inboundConsumer;
    private volatile Thread readerThread;
    private volatile boolean open = false;

    private final Object writeLock = new Object();

    /**
     * Open a real TCP connection and register the callback that receives
     * peer bytes.
     *
     * @param info            connection parameters
     * @param inboundConsumer invoked (on the reader thread) for every inbound frame
     */
    public void connect(TcpConnectionInfo info, Consumer<byte[]> inboundConsumer) {
        if (open) {
            throw new TcpTransportException("transport already connected");
        }
        Socket s = new Socket();
        try {
            s.connect(new InetSocketAddress(info.host(), info.port()), info.connectTimeoutMs());
            this.socket = s;
            this.writer = new BufferedWriter(
                    new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8));
            this.inboundConsumer = inboundConsumer;
            this.open = true;
            startReader(s);
        } catch (IOException e) {
            closeQuietly(s);
            throw new TcpTransportException(
                    "failed to connect to " + info.host() + ":" + info.port()
                            + " (" + e.getMessage() + ")", e);
        }
    }

    private void startReader(Socket s) {
        Thread t = new Thread(() -> readLoop(s), "tcp-transport-reader");
        t.setDaemon(true);
        this.readerThread = t;
        t.start();
    }

    private void readLoop(Socket s) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while (open && (line = reader.readLine()) != null) {
                Consumer<byte[]> consumer = this.inboundConsumer;
                if (consumer == null) {
                    continue;
                }
                try {
                    consumer.accept(line.getBytes(StandardCharsets.UTF_8));
                } catch (RuntimeException ex) {
                    // A real SDK logs and keeps the reader loop alive.
                    System.err.println("[TcpTransportEngine] inbound consumer failed: " + ex.getMessage());
                }
            }
        } catch (IOException e) {
            if (open) {
                System.err.println("[TcpTransportEngine] reader stopped: " + e.getMessage());
            }
        } finally {
            open = false;
        }
    }

    /**
     * Write a frame to the peer. A trailing newline delimits the frame so the
     * server's {@code readLine()} can find its boundary.
     */
    public void write(byte[] payload) {
        if (!open) {
            throw new TcpTransportException("cannot write: transport not connected");
        }
        BufferedWriter w = this.writer;
        if (w == null) {
            throw new TcpTransportException("cannot write: no output stream");
        }
        try {
            synchronized (writeLock) {
                w.write(new String(payload, StandardCharsets.UTF_8));
                w.write('\n');
                w.flush();
            }
        } catch (IOException e) {
            open = false;
            throw new TcpTransportException("write failed: " + e.getMessage(), e);
        }
    }

    public boolean isOpen() {
        Socket s = this.socket;
        return open && s != null && s.isConnected() && !s.isClosed();
    }

    /** Close the connection. */
    public void disconnect() {
        open = false;
        this.inboundConsumer = null;
        closeQuietly(this.socket);
        this.socket = null;
        this.writer = null;
        Thread t = this.readerThread;
        if (t != null) {
            t.interrupt();
        }
    }

    /** Release everything. Call once, at shutdown. */
    public void shutdown() {
        disconnect();
    }

    private static void closeQuietly(Socket s) {
        if (s != null) {
            try {
                s.close();
            } catch (IOException ignored) {
                // nothing useful to do on close failure
            }
        }
    }
}
