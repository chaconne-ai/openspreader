package com.chaconneai.openspreader.pooling;

/**
 * How the process pool is doing, shaped to match the familiar readings on
 * {@code java.util.concurrent.ThreadPoolExecutor}.
 *
 * <h2>One pool, two legs</h2>
 * There are in fact <b>two</b> thread pools beneath this component, with entirely different
 * roles:
 *
 * <ul>
 *   <li><b>local</b>, a {@code ForkJoinPool} -- runs the recursive tasks this process
 *       submitted. Fork/join is essential: a recursive task waits for its subtasks inside
 *       {@code compute()}, and an ordinary pool meeting "worker threads waiting on one
 *       another" exhausts itself, whereas work stealing sends a waiting thread off to help
 *       with something else</li>
 *   <li><b>inbound</b>, a {@code ThreadPoolExecutor} -- handles tasks sent by <b>other
 *       processes</b>. A full queue discards, and the submitter retries after its timeout;
 *       blocking it would block the entire dispatch chain</li>
 * </ul>
 *
 * <p>They are only meaningful read separately: a local queue means this process cannot keep
 * up computing, an inbound queue means others are pushing too hard at you.
 *
 * <h2>Why only the local side counts completions itself</h2>
 * {@code ThreadPoolExecutor} offers {@code getCompletedTaskCount()}, while
 * {@code ForkJoinPool} <b>has no equivalent</b> -- it exposes only instantaneous values, how
 * many threads are alive and how many are queued. So the local completion count is
 * accumulated by {@code runTask} itself, and a failure counts as a completion (otherwise the
 * number would quietly go short exactly when it most needs to be clear).
 *
 * @param localActive       threads in the local pool currently running a task
 * @param localRunning      threads in the local pool <b>not blocked in a join</b>. Noticeably
 *                          below {@code localActive} means deep recursion, with many threads
 *                          stuck waiting on subtasks
 * @param localQueued       tasks waiting in the local pool's work queues
 * @param localSubmitted    tasks submitted from <b>outside</b> the pool and not yet taken up
 * @param localCompleted    tasks the local pool finished, failures included
 * @param localFailed       tasks that ended by throwing
 * @param localPoolSize     the local pool's current thread count
 * @param localParallelism  the local pool's target parallelism
 * @param localSteals       the work-steal count, a reading unique to fork/join.
 *                          <b>Higher is better</b> -- it means idle threads really are
 *                          helping with others' work
 * @param inboundActive     threads currently running in the inbound pool
 * @param inboundQueued     tasks waiting in the inbound pool's queue
 * @param inboundCompleted  tasks the inbound pool finished
 * @param inboundPoolSize   the inbound pool's current thread count
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
public record PoolStats(
        int localActive,
        int localRunning,
        long localQueued,
        long localSubmitted,
        long localCompleted,
        long localFailed,
        int localPoolSize,
        int localParallelism,
        long localSteals,
        int inboundActive,
        int inboundQueued,
        long inboundCompleted,
        int inboundPoolSize) {

    /** Active count: threads running a task across both pools. */
    public int activeCount() {
        return localActive + inboundActive;
    }

    /** Completed count: tasks finished across both pools. */
    public long completedTaskCount() {
        return localCompleted + inboundCompleted;
    }

    /** Queued count: tasks not yet started across both pools. */
    public long queuedTaskCount() {
        return localQueued + localSubmitted + inboundQueued;
    }

    /**
     * Task failure rate, 0 to 1.
     *
     * <p>The local side only: an inbound failure travels back to the requester as a response
     * and that side decides what to do about it, so counting it here as well would be
     * double-counting.
     */
    public double failureRate() {
        return localCompleted == 0 ? 0d
                : Math.round(localFailed * 10000.0 / localCompleted) / 10000.0;
    }

    /**
     * The local pool's blocked ratio, 0 to 1: how many active threads are stuck in a join
     * waiting on subtasks.
     *
     * <p>Sitting near 1 means the recursion is split too deeply and the overwhelming majority
     * of threads are waiting rather than computing. Lowering {@code max-depth} makes the
     * deeper recursion run synchronously, which turns out faster.
     */
    public double localBlockedRate() {
        return localActive == 0 ? 0d
                : Math.round((localActive - localRunning) * 10000.0 / localActive) / 10000.0;
    }
}
