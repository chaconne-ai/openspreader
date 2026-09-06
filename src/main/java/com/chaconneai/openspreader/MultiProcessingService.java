package com.chaconneai.openspreader;

import com.chaconneai.spreader.event.GossipListener;
import java.util.Map;

/**
 * The contract every multiprocessing component implements -- cluster cache, mutex,
 * semaphore, latch, barrier, task pool, RPC and aggregation all sit behind it.
 *
 * <p>Extending {@link GossipListener} is what makes a component <i>reachable</i>:
 * it is the path by which cluster messages are delivered to it. Extending
 * {@link AutoCloseable} is what makes it <i>releasable</i>: Spring calls
 * {@code close()} on shutdown, so no component has to register a shutdown hook
 * of its own.
 *
 * <p>{@link #stats()} is deliberately part of the contract rather than an optional
 * extra. A component that cannot report on itself is a component nobody can
 * diagnose in production -- and by the time that matters, it is too late to add.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface MultiProcessingService extends GossipListener, AutoCloseable {

    void start();

    Map<String, Object> stats();

}
