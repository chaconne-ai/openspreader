package com.chaconneai.openspreader.pooling;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A recursive task that can be split across processes. Used exactly as
 * {@code java.util.concurrent.RecursiveTask} is, differing only in that subtasks may be sent
 * to <b>other processes</b> to compute.
 *
 * <pre>{@code
 * public class SumTask extends RecursiveTask<Long> {
 *     private final int from, to;
 *
 *     public SumTask(int from, int to) { this.from = from; this.to = to; }
 *
 *     protected Long compute() {
 *         if (to - from < 10) {                    // small enough; compute it directly
 *             long sum = 0;
 *             for (int i = from; i <= to; i++) sum += i;
 *             return sum;
 *         }
 *         int mid = (from + to) >>> 1;
 *         var left  = fork(new SumTask(from, mid));      // may land in another process
 *         var right = fork(new SumTask(mid + 1, to));
 *         return join(left) + join(right);
 *     }
 * }
 *
 * long result = pool.invoke(new SumTask(1, 100));
 * }</pre>
 *
 * <h2>The task object must be serialisable</h2>
 * The whole task object is serialised and sent to another process, so it and its fields must
 * be {@link Serializable}. Do not put Spring beans, database connections and their like in
 * the fields -- where a local resource is needed, fetch it inside {@link #compute()}.
 *
 * <h2>How deep before it stops going out</h2>
 * Each fork adds one to the depth, and past the configured limit every subtask stays local.
 * Without that limit, a recursion a few levels deep would inflate the cluster's message
 * volume exponentially while each subtask's computation grew ever smaller -- until it was all
 * network.
 *
 * <h2>What happens with only one replica</h2>
 * Everything runs in the local ForkJoinPool, behaving exactly as
 * {@code java.util.concurrent} would. Should a new replica join partway through,
 * <b>subsequent forks begin dispatching outward immediately</b> -- every fork consults the
 * member list afresh.
 *
 * <p>Subtasks only go to replicas of <b>the same application</b>: the peer needs the same
 * task class to deserialise and run it.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public abstract class RecursiveTask<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The execution context. transient because it must not be serialised along with the
     *  task; the peer injects its own on arrival. */
    private transient PoolService pool;

    /** The current depth, also injected by whoever executes it. */
    private transient int depth;

    /** Called by {@link PoolService} before execution. */
    final void bind(PoolService pool, int depth) {
        this.pool = pool;
        this.depth = depth;
    }

    /** The computation, implemented by the subclass. */
    protected abstract T compute();

    /**
     * Hands a subtask off, possibly to another process. Returns immediately, without
     * blocking.
     *
     * @return a handle to the result; read it with {@link #join}
     */
    protected final <R> CompletableFuture<R> fork(RecursiveTask<R> sub) {
        if (pool == null) {
            // No execution context bound means it was constructed and had compute() called
            // directly, so it degenerates to synchronous recursion
            sub.bind(null, depth + 1);
            return CompletableFuture.completedFuture(sub.compute());
        }
        return pool.submit(sub, depth + 1);
    }

    /** Forks a batch of subtasks at once. */
    protected final <R> List<CompletableFuture<R>> forkAll(List<RecursiveTask<R>> subs) {
        List<CompletableFuture<R>> futures = new ArrayList<>(subs.size());
        for (RecursiveTask<R> sub : subs) {
            futures.add(fork(sub));
        }
        return futures;
    }

    /**
     * Waits for one subtask's result.
     *
     * <p>When the subtask runs remotely, what is waited on is a network response, occupying
     * no computation thread.
     *
     * <p><b>When it runs locally this genuinely blocks.</b> Do not count on ForkJoinPool's
     * work stealing: {@code CompletableFuture.join()} on a worker thread goes through
     * {@code managedBlock}, and to maintain its parallelism the pool <b>creates a
     * compensating thread</b> rather than sending the current one off to help elsewhere. So
     * every level of recursion occupies one more thread.
     *
     * <p>{@code PoolService} bounds this with {@code maxDepth}: subtasks past that depth are
     * computed synchronously on the current thread and never enter the pool, which caps the
     * blocked thread count at 2^maxDepth. Keep it in mind when writing your own
     * {@code compute()} -- <b>do not fork vast numbers of subtasks deep in a recursion</b>.
     */
    protected final <R> R join(CompletableFuture<R> future) {
        return future.join();
    }

    /** The current recursion depth; 0 is the outermost. */
    protected final int depth() {
        return depth;
    }

    /**
     * Computes in this process, without dispatching.
     *
     * <p>For when a subtask is already small enough that a round trip plainly does not pay.
     */
    protected final <R> R computeLocally(RecursiveTask<R> sub) {
        sub.bind(pool, depth + 1);
        return sub.compute();
    }
}
