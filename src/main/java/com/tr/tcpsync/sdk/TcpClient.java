package com.tr.tcpsync.sdk;

/**
 * Contract exposed by the (simulated) external TCP library.
 *
 * <p>The vendor gives you this interface and expects you to implement it.
 * {@code TcpService} is that implementation:
 * <ul>
 *     <li>{@link #connect()} / {@link #disconnect()} manage the socket lifecycle.</li>
 *     <li>{@link #sendMessage(byte[])} writes raw bytes to the peer.</li>
 *     <li>{@link #onDataReceived(byte[])} is the callback the SDK invokes
 *         (on its own thread) whenever the peer sends bytes back.</li>
 * </ul>
 */
public interface TcpClient {

    /** Open the connection using this client's own connection info. */
    void connect();

    /** Close the connection and release transport resources. */
    void disconnect();

    /** Write a raw byte payload to the connected peer. */
    void sendMessage(byte[] payload);

    /**
     * Invoked by the SDK when inbound bytes arrive from the peer.
     * Runs on a transport thread, not the request thread.
     */
    void onDataReceived(byte[] data);

    /** @return {@code true} while the transport is open. */
    boolean isConnected();
}
