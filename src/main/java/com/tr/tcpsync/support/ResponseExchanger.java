package com.tr.tcpsync.support;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Bridges an asynchronous request/response conversation to a
 * {@link CompletableFuture}, using a correlation id to match each reply to the
 * request that is waiting for it.
 *
 * <p>This is the reusable heart of the "async → sync" pattern, kept separate
 * from any transport (TCP, WebSocket, message queue, ...). Typical use:
 *
 * <pre>{@code
 * // sending side:
 * CompletableFuture<Reply> future = exchanger.register(correlationId, Duration.ofSeconds(5));
 * transport.send(...);   // AFTER register(), so a very fast reply is never missed
 * return future;
 *
 * // receiving side (some other thread):
 * exchanger.complete(reply.correlationId(), reply);
 * }</pre>
 *
 * <p>The map self-cleans: every future removes its own entry once it settles
 * (reply, timeout, or error), so there is no leak even for requests that never
 * get an answer.
 *
 * @param <R> the reply type handed back to waiters
 */
public class ResponseExchanger<R> {

    /** correlationId → waiter. Two threads touch this, so it is concurrent. */
    private final Map<String, CompletableFuture<R>> pending = new ConcurrentHashMap<>();

    /**
     * Register a waiter for {@code correlationId} and return its future.
     * Call this <b>before</b> sending the request.
     *
     * @param correlationId id echoed back by the peer inside its reply
     * @param timeout       how long to wait before failing with
     *                      {@link java.util.concurrent.TimeoutException}
     * @return a future that completes when {@link #complete} is called with the
     *         same id, or fails on timeout
     */
    public CompletableFuture<R> register(String correlationId, Duration timeout) {
        CompletableFuture<R> future = new CompletableFuture<>();
        pending.put(correlationId, future);
        // Drop the entry no matter how the future settles.
        future.whenComplete((reply, error) -> pending.remove(correlationId));
        // orTimeout returns the same future instance, so the cleanup above still applies.
        return future.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Hand a reply to the waiter registered under {@code correlationId}.
     *
     * @return {@code true} if a waiter was found and completed, {@code false}
     *         if nobody was waiting (late/duplicate/unknown reply)
     */
    public boolean complete(String correlationId, R reply) {
        CompletableFuture<R> future = pending.remove(correlationId);
        if (future != null) {
            future.complete(reply);
            return true;
        }
        return false;
    }

    /**
     * Fail the waiter registered under {@code correlationId} — e.g. when the
     * send itself failed after registering.
     *
     * @return {@code true} if a waiter was found and failed
     */
    public boolean completeExceptionally(String correlationId, Throwable error) {
        CompletableFuture<R> future = pending.remove(correlationId);
        if (future != null) {
            future.completeExceptionally(error);
            return true;
        }
        return false;
    }

    /**
     * Fail every outstanding waiter — use on disconnect so callers do not have
     * to wait out their individual timeouts.
     */
    public void failAll(Throwable error) {
        pending.forEach((id, future) -> future.completeExceptionally(error));
        pending.clear();
    }

    /** @return number of requests currently awaiting a reply. */
    public int pendingCount() {
        return pending.size();
    }
}
