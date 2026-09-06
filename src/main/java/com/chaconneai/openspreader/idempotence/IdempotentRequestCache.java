/*
 * Copyright 2026 ChaconneAI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chaconneai.openspreader.idempotence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * The leader-side reply cache: when the same request arrives again, the previous reply is
 * returned as it was, <b>without executing anything again</b>.
 *
 * <h2>The problem it solves: the request arrived, and the reply was lost on the way back</h2>
 * All the caller sees is a timeout, and "the request never arrived" looks <b>exactly like</b>
 * "it arrived and the reply was lost". So it sends the request again -- but the leader has
 * already executed it once.
 *
 * <p>What follows depends on whether the operation is idempotent:
 * <table border="1">
 *   <caption>What a resend causes without this cache</caption>
 *   <tr><th>Operation</th><th>Consequence</th><th>Any safety net</th></tr>
 *   <tr><td>{@code cache.set(k, v)}</td><td>Harmless; the result is the same</td><td>--</td></tr>
 *   <tr><td><b>{@code cache.incr(k)}</b></td><td><b>The count is one too high</b></td>
 *       <td><b>None</b>, and it is silent</td></tr>
 *   <tr><td>Acquiring a semaphore permit</td><td>One permit too many is held, and the caller
 *       returns only one</td><td>Released when the lease expires</td></tr>
 *   <tr><td>Arriving at a barrier</td><td>The leader counts a party that has already left</td>
 *       <td>The other parties wait out the timeout</td></tr>
 *   <tr><td>A remote method call</td><td>The business method runs twice</td>
 *       <td>Depends on whether the application is idempotent</td></tr>
 * </table>
 *
 * <p>The top row is where a resend is harmless; the second is the dangerous one -- <b>the
 * data is wrong and nothing reports it</b>.
 *
 * <h2>How this differs from de-duplication in the transport</h2>
 * spreader's transport already has a de-duplicator, keyed on {@code sender + seq}, but what
 * it guards against is <b>the same message being delivered twice</b> -- a problem of one-way
 * delivery.
 *
 * <p>The failure here has the <b>opposite</b> shape: the request was delivered once and
 * handled once, and it is the <b>reply</b> that failed to get back. The transport does not
 * know that one message is another's reply, so a failed request/reply round trip can only be
 * caught at this layer.
 *
 * <h2>Using it correctly means the client must reuse the requestId</h2>
 * Generating a new requestId on a resend means this cache <b>never hits once</b> -- the
 * leader takes it for an entirely new request. The two halves go together, and neither works
 * alone:
 * <ol>
 *   <li>The client generates the requestId once, <b>outside the retry loop</b></li>
 *   <li>The leader wraps the real handling in this cache</li>
 * </ol>
 *
 * <h2>Failures are not cached</h2>
 * When the handler throws, the entry is removed and the next resend <b>executes again</b>,
 * because failures are often transient -- "I am no longer the leader", "the state has not
 * caught up yet". Caching a failure would fix one chance failure as this request's final
 * answer.
 *
 * @param <T> the reply type
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class IdempotentRequestCache<T> {

    private static final Logger log = LoggerFactory.getLogger(IdempotentRequestCache.class);

    /** How long a reply is kept. Resends happen within seconds, so the window is wide
     *  enough. */
    private final long ttlMs;

    /** How many entries at most. At the limit, expired ones are swept, and failing to free
     *  space the request simply proceeds -- better to execute twice than to block new
     *  requests. */
    private final int maxEntries;

    private final Map<Key, Entry<T>> entries = new ConcurrentHashMap<>();

    /** When the last sweep ran. Sweeping is lazy; there is no thread of its own. */
    private volatile long lastSweepMs = System.currentTimeMillis();

    public IdempotentRequestCache(long ttlMs, int maxEntries) {
        this.ttlMs = ttlMs;
        this.maxEntries = maxEntries;
    }

    /**
     * Executes a request, or returns the previous reply.
     *
     * <h2>The same request arriving twice at once still executes once</h2>
     * A resend may arrive almost at the same time as the first request, with the first reply
     * still on its way. The second thread then <b>waits for the first one's result</b> rather
     * than executing as well -- otherwise this cache would fail at exactly the moment it is
     * needed most.
     *
     * @param senderId  the originating node's id. A requestId is unique only within one
     *                  originator, so it has to be part of the key
     * @param requestId the request id, <b>which must match the first attempt on a resend</b>
     * @param handler   the real handling, executed once only
     * @return the reply; on a resend, the previous one
     */
    public T execute(String senderId, long requestId, Supplier<T> handler) {
        sweepIfDue();

        Key key = new Key(senderId, requestId);
        Entry<T> entry = entries.get(key);
        if (entry != null) {
            log.debug("Request {}#{} is a resend; returning the previous reply",
                    senderId, requestId);
            return awaitResult(entry, handler);
        }

        if (entries.size() >= maxEntries) {
            evictExpired(System.currentTimeMillis());
            if (entries.size() >= maxEntries) {
                // No space freed: execute directly, with no idempotence protection. Better to
                // execute twice than to refuse service because a cache is full
                log.warn("The idempotence cache is full at {} entries, so this request gets no "
                        + "resend protection", maxEntries);
                return handler.get();
            }
        }

        Entry<T> created = new Entry<>();
        Entry<T> previous = entries.putIfAbsent(key, created);
        if (previous != null) {
            // Another thread created it first; wait for its result
            return awaitResult(previous, handler);
        }

        try {
            T result = handler.get();
            created.future.complete(result);
            return result;
        } catch (RuntimeException | Error e) {
            // Failures are not cached: the entry goes, so a resend executes again. See the
            // class javadoc for why
            entries.remove(key);
            created.future.completeExceptionally(e);
            throw e;
        }
    }

    /** Waits for another thread to finish executing this request. */
    private T awaitResult(Entry<T> entry, Supplier<T> handler) {
        try {
            return entry.future.join();
        } catch (RuntimeException e) {
            // That execution failed and the entry has been removed. Do it here instead
            return handler.get();
        }
    }

    /** How many replies are cached. For troubleshooting and tests. */
    public int size() {
        return entries.size();
    }

    /** Clears it. Call it on a change of leader or a register reset -- old replies mean
     *  nothing to a new term. */
    public void clear() {
        entries.clear();
    }

    private void sweepIfDue() {
        long now = System.currentTimeMillis();
        if (now - lastSweepMs < 1_000L || entries.isEmpty()) {
            return;
        }
        lastSweepMs = now;
        evictExpired(now);
    }

    private void evictExpired(long now) {
        for (Iterator<Map.Entry<Key, Entry<T>>> it = entries.entrySet().iterator(); it.hasNext(); ) {
            if (now - it.next().getValue().createdMs > ttlMs) {
                it.remove();
            }
        }
    }

    /** A requestId is unique only within one originator, so the key must include it. */
    private record Key(String senderId, long requestId) {
    }

    /**
     * One request's execution state.
     *
     * <p>A future rather than the result directly: on a concurrent resend, the later thread
     * must be able to <b>wait</b> for the first to finish, rather than seeing "no result yet"
     * and executing as well.
     */
    private static final class Entry<T> {

        final CompletableFuture<T> future = new CompletableFuture<>();
        final long createdMs = System.currentTimeMillis();
    }
}
