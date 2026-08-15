package com.tr.tcpsync.sdk;

/**
 * Raised by the simulated SDK when the transport cannot be used
 * (not connected, connect failed, write failed, ...).
 */
public class TcpTransportException extends RuntimeException {

    public TcpTransportException(String message) {
        super(message);
    }

    public TcpTransportException(String message, Throwable cause) {
        super(message, cause);
    }
}
