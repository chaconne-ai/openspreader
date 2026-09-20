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
package com.chaconneai.openspreader.dag;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * How long to wait before trying a node again.
 *
 * <pre>{@code
 * .retry(Charge.class, 2)                                   // at once, as before
 * .retry(Charge.class, 2, Backoff.fixed(Duration.ofMillis(200)))
 * .retry(Charge.class, 3, Backoff.exponential(Duration.ofMillis(200)))
 * }</pre>
 *
 * <h2>Why it belongs on the graph rather than in a node</h2>
 * Retrying at once is not much of a favour to a service that is struggling rather than
 * flapping: three attempts inside a few milliseconds are three requests it did not need. A
 * node could sleep by itself, but then the waiting would be invisible to everything that
 * watches a run, and every node would carry its own copy of the same loop.
 *
 * <h2>Waiting costs no thread</h2>
 * The coordinator does not sleep and no timer is started. A node due to be retried is put
 * aside with the moment it becomes due, and the loop that is already waiting for completions
 * simply wakes up no later than that. So a graph whose every node is backing off occupies
 * exactly the threads it did before: none of its own.
 *
 * <p>The wait is <b>not</b> counted against a node any differently from the work: a run with
 * a ceiling on it, {@code invoke(state, timeout, unit)}, spends that ceiling on waiting too.
 * That is the honest reading of "this run may take at most a minute".
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/09/2026
 */
public final class Backoff {

    /** No waiting at all. The default, and what every graph did before this existed. */
    private static final Backoff NONE = new Backoff(0L, 1.0d, 0L, false);

    private final long initialMs;
    private final double multiplier;
    private final long capMs;
    private final boolean jitter;

    private Backoff(long initialMs, double multiplier, long capMs, boolean jitter) {
        this.initialMs = initialMs;
        this.multiplier = multiplier;
        this.capMs = capMs;
        this.jitter = jitter;
    }

    /** Try again at once. */
    public static Backoff none() {
        return NONE;
    }

    /** The same wait before every attempt. */
    public static Backoff fixed(Duration delay) {
        long ms = requirePositive(delay);
        return new Backoff(ms, 1.0d, ms, false);
    }

    /**
     * Doubling, from {@code initial}, up to thirty seconds.
     *
     * <p>Doubling is the usual shape because it backs away from a service that is not
     * recovering without punishing one that recovers at once: the first retry is quick.
     */
    public static Backoff exponential(Duration initial) {
        return exponential(initial, 2.0d, Duration.ofSeconds(30));
    }

    /**
     * Doubling by a multiplier of your own, never longer than {@code cap}.
     *
     * <p>The cap matters more than it looks: without one, a node retried five times with a
     * multiplier of three waits over two minutes on the last attempt, which is rarely what
     * anybody meant.
     */
    public static Backoff exponential(Duration initial, double multiplier, Duration cap) {
        if (multiplier < 1.0d) {
            throw new IllegalArgumentException("a backoff multiplier below 1 would make each "
                    + "attempt come sooner than the last: " + multiplier);
        }
        return new Backoff(requirePositive(initial), multiplier, requirePositive(cap), false);
    }

    /**
     * The same, with each wait randomised between half of it and all of it.
     *
     * <p>For the case backoff exists to handle at scale: fifty runs that failed together
     * because one service went down, all retrying at the same instant and knocking it over
     * again as it comes back. Spreading them out is the whole point.
     */
    public Backoff withJitter() {
        return new Backoff(initialMs, multiplier, capMs, true);
    }

    /**
     * How long to wait before attempt number {@code attempt}, counting the first as 1.
     *
     * @return milliseconds, 0 for no wait
     */
    public long delayMsBefore(int attempt) {
        if (initialMs <= 0L || attempt <= 1) {
            return 0L;
        }
        double delay = initialMs;
        for (int i = 2; i < attempt; i++) {
            delay *= multiplier;
            if (delay >= capMs) {
                delay = capMs;
                break;
            }
        }
        long ms = Math.min((long) delay, capMs);
        if (jitter && ms > 1L) {
            // Between half and all of it, which spreads a thundering herd without letting
            // any one attempt wait longer than the cap says it may
            ms = ThreadLocalRandom.current().nextLong(ms / 2, ms + 1);
        }
        return ms;
    }

    /** Whether this one waits at all. */
    public boolean isNone() {
        return initialMs <= 0L;
    }

    private static long requirePositive(Duration duration) {
        if (duration == null || duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException("a backoff needs a positive duration: " + duration);
        }
        return duration.toMillis();
    }

    /**
     * The form a definition stores: {@code none}, {@code fixed:200} or
     * {@code exponential:200:2.0:30000[:jitter]}.
     *
     * <p>A compact string rather than a nested object, for the same reason {@link Trigger}
     * uses one: it is read by people in a diff as often as by a parser.
     */
    @Override
    public String toString() {
        if (isNone()) {
            return "none";
        }
        if (multiplier == 1.0d && initialMs == capMs) {
            return "fixed:" + initialMs + (jitter ? ":jitter" : "");
        }
        return "exponential:" + initialMs + ':' + multiplier + ':' + capMs
                + (jitter ? ":jitter" : "");
    }

    /** Reads back what {@link #toString} wrote. */
    public static Backoff parse(String text) {
        if (text == null || text.isBlank() || "none".equals(text)) {
            return NONE;
        }
        String[] parts = text.split(":");
        boolean jitter = "jitter".equals(parts[parts.length - 1]);
        try {
            if (parts[0].equals("fixed")) {
                Backoff backoff = fixed(Duration.ofMillis(Long.parseLong(parts[1])));
                return jitter ? backoff.withJitter() : backoff;
            }
            if (parts[0].equals("exponential")) {
                Backoff backoff = exponential(Duration.ofMillis(Long.parseLong(parts[1])),
                        Double.parseDouble(parts[2]), Duration.ofMillis(Long.parseLong(parts[3])));
                return jitter ? backoff.withJitter() : backoff;
            }
        } catch (RuntimeException e) {
            throw new DagException("unrecognised backoff \"" + text + "\"", e);
        }
        throw new DagException("unrecognised backoff \"" + text + "\"");
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Backoff that)) {
            return false;
        }
        return initialMs == that.initialMs && capMs == that.capMs && jitter == that.jitter
                && Double.compare(multiplier, that.multiplier) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(initialMs, multiplier, capMs, jitter);
    }
}
