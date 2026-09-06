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
package com.chaconneai.openspreader.rpc;

import java.util.concurrent.TimeUnit;

/**
 * Global RPC defaults, from {@code spring.spreader.multiprocessing.rpc.*}.
 *
 * <h2>Configure globally, override locally</h2>
 * This is the only model this configuration follows: <b>set it once in configuration where
 * it can be set, and override on the annotation for the odd interface that differs</b>.
 *
 * <p>The benefit is concrete. Writing {@code timeout = 3} ten times for ten services is
 * meaningless repetition, and changing it means changing ten places -- miss one and there
 * is a timeout unlike all the others, an inconsistency usually discovered only when
 * something goes wrong in production.
 *
 * <p>An attribute left unset on the annotation reads as {@link RpcClient#USE_GLOBAL}, and
 * merging substitutes the value from here. Detecting "was it set" requires a sentinel and
 * cannot be done by comparing against the default -- that cannot tell "they did not
 * configure it" from "they configured a number that happens to equal the default".
 *
 * @param timeoutMs        timeout per attempt in milliseconds; 0 waits indefinitely
 * @param maxRetries       how many further attempts after a failure
 * @param retryIntervalMs  how long between two attempts, in milliseconds
 * @param maxConcurrent    the most requests in flight at once; 0 means unlimited
 * @param acquireTimeoutMs how long to wait for a slot once in-flight is full, in
 *                         milliseconds; 0 means do not wait
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public record RpcDefaults(long timeoutMs, int maxRetries, long retryIntervalMs,
                          int maxConcurrent, long acquireTimeoutMs) {

    /** The values used without a Spring container -- a unit test constructing a proxy directly, say. */
    public static final RpcDefaults FALLBACK = new RpcDefaults(60_000L, 3, 1_000L, 0, 0L);

    /**
     * Works out how long this attempt on this interface should wait.
     *
     * @return milliseconds; 0 waits indefinitely
     */
    public long resolveTimeoutMs(RpcClient config) {
        if (config.timeout() == RpcClient.USE_GLOBAL) {
            return timeoutMs;
        }
        return config.timeout() <= 0 ? 0L : config.timeUnit().toMillis(config.timeout());
    }

    /** Works out how many retries this interface gets. */
    public int resolveMaxRetries(RpcClient config) {
        return config.maxRetries() == RpcClient.USE_GLOBAL_INT
                ? maxRetries
                : Math.max(0, config.maxRetries());
    }

    /** Works out the most requests this interface may have in flight. 0 means unlimited. */
    public int resolveMaxConcurrent(RpcClient config) {
        return config.maxConcurrent() == RpcClient.USE_GLOBAL_INT
                ? maxConcurrent
                : Math.max(0, config.maxConcurrent());
    }

    /** Works out how long this interface waits for an in-flight slot, in milliseconds. */
    public long resolveAcquireTimeoutMs(RpcClient config) {
        if (config.acquireTimeout() == RpcClient.USE_GLOBAL) {
            return acquireTimeoutMs;
        }
        return config.acquireTimeout() <= 0
                ? 0L
                : config.timeUnit().toMillis(config.acquireTimeout());
    }

    /** Works out how long this interface waits between two attempts, in milliseconds. */
    public long resolveRetryIntervalMs(RpcClient config) {
        if (config.retryInterval() == RpcClient.USE_GLOBAL_INT) {
            return retryIntervalMs;
        }
        return config.retryInterval() <= 0
                ? 0L
                : config.timeUnit().toMillis(config.retryInterval());
    }
}
