package com.chaconneai.openspreader.example.pooling;

import java.util.function.Supplier;

/**
 * The result of one task execution, and how long it took.
 *
 * <h2>Why an example reports the elapsed time</h2>
 * "Distributes the computation across the cluster" proves nothing on its own -- dispatch
 * has a cost, in serialisation, round trips and merging, and <b>whether it is faster is a
 * question of numbers</b>.
 *
 * <p>And that number has to be read together with the <b>replica count</b>. For one
 * computation:
 *
 * <ul>
 *   <li>1 replica: everything computed locally, no network cost -- the baseline</li>
 *   <li>3 replicas: three CPUs working together, at the cost of serialisation and round
 *       trips</li>
 * </ul>
 *
 * <p>For a small computation, dispatching is slower -- that is not a defect, it is a
 * <b>boundary worth knowing</b>. Claiming "supports distributed computation" without giving
 * the scale and the numbers says nothing at all.
 *
 * <h2>The time is measured from the caller's point of view</h2>
 * From entering {@code invoke} to the result coming back, <b>including</b> serialisation,
 * round trips and recomputation after a failure. That is what the application actually
 * waited; timing only how long the remote {@code compute()} ran gives a flattering and
 * useless number.
 *
 * @param value     the computed result
 * @param elapsedMs how many milliseconds the caller waited
 * @param peerCount how many <b>other</b> replicas were in the cluster at the start, this
 *                  node excluded
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
public record TimedResult<T>(T value, long elapsedMs, int peerCount) {

    /** How many replicas took part in total, this node included. */
    public int clusterSize() {
        return peerCount + 1;
    }

    /** Only one replica, meaning everything was computed locally. */
    public boolean ranAlone() {
        return peerCount == 0;
    }

    /**
     * Runs it once, timed.
     *
     * <p>{@code nanoTime} rather than {@code currentTimeMillis}: the latter moves with system
     * clock adjustments (NTP), and over a short interval can come out negative.
     */
    public static <T> TimedResult<T> measure(Supplier<T> action, int peerCount) {
        long start = System.nanoTime();
        T value = action.get();
        long ms = (System.nanoTime() - start) / 1_000_000L;
        return new TimedResult<>(value, ms, peerCount);
    }

    /** A one-line summary, for logs and for the README. */
    public String summary() {
        return ranAlone()
                ? String.format("computed locally on a single replica in %d ms", elapsedMs)
                : String.format("%d replicas took part, %d ms", clusterSize(), elapsedMs);
    }

    @Override
    public String toString() {
        return summary() + " -> " + value;
    }
}
