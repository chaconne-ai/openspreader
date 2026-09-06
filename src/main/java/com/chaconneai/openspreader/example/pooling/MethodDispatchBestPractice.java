package com.chaconneai.openspreader.example.pooling;

import com.chaconneai.openspreader.pooling.MultiProcessingCall;
import com.chaconneai.openspreader.pooling.ProcessingPoolException;
import com.chaconneai.openspreader.pooling.ForkJoinMultiProcessingPool;
import com.chaconneai.openspreader.pooling.ProcessingPool;
import com.chaconneai.openspreader.pooling.RecursiveTask;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * The cross-process task pool: it hands a method call to <b>one replica</b> of the same
 * application to execute.
 *
 * <h2>What it solves</h2>
 * The application scales to five replicas, but the work all lands on whichever one received
 * the request -- the other four sit with idle CPUs. This pool dispatches the call so that all
 * five compute together.
 *
 * <p>Note that it is <b>not</b> a load balancer, which is the gateway's job, and <b>not</b> a
 * message queue -- there is no persistence and no redelivery. It is a dispatcher for "anyone
 * may take this call".
 *
 * <h2>Three hard rules</h2>
 * <ol>
 *   <li>The target method must carry {@link MultiProcessingCall}, or the request is refused.
 *       Without an allow-list, any node in the cluster could reflectively call any method in
 *       this process</li>
 *   <li>Dispatch happens only <b>among replicas of the same application</b>. Another
 *       application has no such bean, so there is nothing to execute</li>
 *   <li>Arguments and return values must be serialisable. Primitives, String, collections and
 *       simple DTOs are all fine; a Spring bean, a database connection or a Stream plainly is
 *       not</li>
 * </ol>
 *
 * <h2>With a single replica it runs locally by itself</h2>
 * No network, no serialisation -- an ordinary reflective call. And the decision is made
 * <b>afresh on every call</b>: a new replica joins and the next call starts dispatching
 * outward;
 * every replica leaves and it returns to running locally. So local development, one process,
 * and production, a crowd of replicas, use the same code with no switch.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MethodDispatchBestPractice {

    private final ProcessingPool pool;

    /** Recursive division wants {@link ForkJoinMultiProcessingPool}, which adds an
     *  {@code invoke}. */
    private final ForkJoinMultiProcessingPool forkJoinPool;

    public MethodDispatchBestPractice(ProcessingPool pool, ForkJoinMultiProcessingPool forkJoinPool) {
        this.pool = pool;
        this.forkJoinPool = forkJoinPool;
    }

    // ==================================================================
    // 1. Two return shapes; choose the right one first
    // ==================================================================

    /**
     * <b>Use {@code submit} when the result is wanted</b>; it returns a future.
     *
     * <p>It does not block the calling thread. To wait synchronously, call {@code get} -- but
     * <b>always with a timeout</b>. When a remote node dies, a {@code get()} without one hangs:
     * the service has timeouts of its own, but a network partition does not guarantee they
     * fire.
     */
    public String renderReportSync(String month) throws Exception {
        return pool.<String>submit("reportService", "render", month)
                .get(30, TimeUnit.SECONDS);
    }

    /**
     * The asynchronous usage: dispatch it, carry on, and handle the result when it arrives.
     *
     * <p>This is the pool's normal usage -- waiting synchronously still occupies the caller's
     * thread, saving CPU but not threads.
     */
    public CompletableFuture<Void> renderReportAsync(String month) {
        return pool.<String>submit("reportService", "render", month)
                .thenAccept(this::onReportReady)
                .exceptionally(e -> {
                    // A method not exposed, a serialisation failure, an error at the far end,
                    // a timeout -- all arrive here, wrapped in a ProcessingException
                    log("Report generation failed", e);
                    return null;
                });
    }

    /**
     * <b>Use {@code execute} when the result is not wanted</b>: fire and forget.
     *
     * <p>It neither blocks nor waits, and a failure leaves a warning in the log and <b>throws
     * nothing</b> -- a caller that said it wanted no return value should not be interrupted by
     * an exception either.
     *
     * <p>It suits work that only needs sending out: warming a cache, sending a notification,
     * cleaning up temporary data. The semantics match {@code Executor.execute} against
     * {@code ExecutorService.submit}.
     *
     * <p>Conversely: <b>do not use it where the result matters</b>, because a failure will not
     * reach you.
     */
    public void refreshCacheEverywhere(String key) {
        pool.execute("cacheWarmer", "warm", key);
    }

    // ==================================================================
    // 2. Dispatching many things at once
    // ==================================================================

    /**
     * Batch dispatch: send N calls at once and wait for all of them.
     *
     * <p>This is where the gain is plainest -- what took N times as long in sequence is now
     * spread across several replicas in parallel.
     *
     * <p>Note that {@code allOf} waits for completion without collecting results, so they are
     * taken one by one afterwards. And <b>the batch needs a timeout of its own</b>, or one
     * stuck replica holds the whole batch up.
     */
    public List<String> renderAllMonths(List<String> months) throws Exception {
        List<CompletableFuture<String>> futures = months.stream()
                .map(m -> pool.<String>submit("reportService", "render", m))
                .toList();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(2, TimeUnit.MINUTES);

        return futures.stream().map(CompletableFuture::join).toList();
    }

    /**
     * A batch tolerating partial failure: the broken few fall back to a default rather than
     * bringing the batch down.
     *
     * <p>Reports, recommendations and statistics can usually accept a few missing entries,
     * which beats returning a 500 for the whole endpoint.
     */
    public List<String> renderAllMonthsTolerant(List<String> months) {
        return months.stream()
                .map(m -> pool.<String>submit("reportService", "render", m)
                        .exceptionally(e -> "(" + m + " failed to generate)"))
                .toList()
                .stream()
                .map(CompletableFuture::join)
                .toList();
    }

    // ==================================================================
    // 3. When it is worth using, and when it is not
    // ==================================================================

    /**
     * <b>Check first whether there are other replicas.</b> With {@code peerCount()} at 0,
     * every call ahead runs locally and dispatching achieves nothing.
     *
     * <p>The test itself is rarely worth writing: local execution is already automatic, and a
     * layer around it only adds noise. Its real use is <b>deciding whether to divide the
     * work</b>: dividing one large task into twenty and running them locally in sequence is
     * slower than not dividing at all, having added twenty reflective calls and twenty
     * schedulings. Asking about the replica count first is reasonable there.
     */
    public long sumRange(int from, int to) {
        if (pool.peerCount() == 0) {
            long sum = 0;                     // alone here, so dividing gains nothing
            for (int i = from; i <= to; i++) {
                sum += i;
            }
            return sum;
        }
        return forkJoinPool.invoke(new SumTask(from, to));
    }

    /**
     * The same work, bringing <b>the elapsed time</b> back with it.
     *
     * <h2>Why the examples measure time</h2>
     * "Dispatch it across the cluster" establishes nothing on its own -- dispatch has a cost in
     * serialisation, a network round trip and combining the results. <b>Whether it pays is a
     * question about numbers, and about the replica count alongside them.</b>
     *
     * <pre>{@code
     * TimedResult<Long> r = practice.sumRangeTimed(1, 20_000);
     * System.out.println(r.summary());
     * // computed locally on one replica, 3 ms
     * // or: three replicas took part, 11 ms   <- far too little work to be worth dispatching
     * }</pre>
     *
     * <p>That second line is exactly what deserves seeing: <b>a small task is slower once
     * dispatched</b>. Summing ten thousand integers is a matter of milliseconds, while a
     * network round trip costs hundreds of microseconds to a few milliseconds. What is worth
     * dispatching looks like {@link PiCalculationBestPractice} -- every term a hundred-digit
     * computation.
     */
    public TimedResult<Long> sumRangeTimed(int from, int to) {
        return TimedResult.measure(() -> sumRange(from, to), pool.peerCount());
    }

    /**
     * How long one remote method call takes.
     *
     * <p>What is measured is the time <b>from the caller's point of view</b>: choosing a
     * target, serialising the arguments, the network round trip, deserialising the result --
     * all of it. That is what the application actually waits for.
     *
     * <p>With a single replica the figure is very small, being one local reflective call; only
     * with several does it reflect the real cost of dispatch. Both numbers are needed to know
     * what dispatch cost.
     */
    public TimedResult<String> renderReportTimed(String month) {
        return TimedResult.measure(() -> {
            try {
                return renderReportSync(month);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }, pool.peerCount());
    }

    /**
     * Where it is <b>not worth using</b>; do not force it:
     *
     * <ul>
     *   <li>A light task, a few milliseconds -- one network round trip plus two serialisations
     *       already costs more than that, so dispatching is slower</li>
     *   <li>Large arguments or return values -- sending a 10MB list across to sum it puts the
     *       bottleneck in the network rather than the CPU</li>
     *   <li>A task depending on local state -- a local file, a local cache, a thread context --
     *       which is wrong on another machine</li>
     *   <li>Anything needing reliable delivery -- there is no persistence and no redelivery
     *       here, and a dead node loses the call. Reliability wants a message queue</li>
     * </ul>
     *
     * What is worth using is the opposite: <b>heavy computation, light transfer, no
     * state</b>.
     */
    public void whenNotToUse() {
    }

    // ==================================================================
    // 4. What the called side looks like
    // ==================================================================

    /**
     * How the target bean is written: add {@link MultiProcessingCall}, and no more.
     *
     * <pre>{@code
     * @Service
     * public class ReportService {
     *
     *     @MultiProcessingCall              // without this line, the request is refused
     *     public String render(String month) {
     *         return heavyRender(month);
     *     }
     * }
     * }</pre>
     *
     * <p>The annotation can publish a different name outwards:
     * {@code @MultiProcessingCall("renderReport")}, so that renaming the method does not affect
     * callers.
     *
     * <p><b>The method body must be able to run on any replica</b> -- the one real constraint.
     * Do not expose methods that depend on a local file, a local cache, or a ThreadLocal.
     */
    public void targetSideLooksLikeThis() {
    }

    // ==================================================================
    // 5. Recursive division, fork/join style
    // ==================================================================

    /**
     * A large task dividing itself: written exactly as
     * {@code java.util.concurrent.RecursiveTask} is, except that a forked subtask may land in
     * another process. See {@link SumTask} and {@link FibTask}.
     *
     * <p>Two things to watch:
     * <ol>
     *   <li><b>Set the threshold high enough.</b> Stop at a few dozen elements; dividing
     *       further costs more in serialisation and scheduling than the computation itself</li>
     *   <li><b>The division depth has a ceiling</b>, past which subtasks stay local and run
     *       synchronously. This is deliberate: without one, a deeply recursive task can burst
     *       the thread pool, since every waiting join occupies a thread</li>
     * </ol>
     *
     * <p>The task class and <b>every one of its fields</b> must be serialisable -- the whole
     * object is sent to the far side. Do not put a Spring bean or a database connection in a
     * task.
     *
     * <p>The entry points are {@link ForkJoinMultiProcessingPool#invoke}, which waits for the
     * result, and {@code submit}, which returns a future;
     * {@link RecursiveTask#compute()} is for subclasses to implement and is not called by the
     * caller directly.
     */
    public long fib(int n) {
        return forkJoinPool.invoke(new FibTask(n));
    }

    // ==================================================================
    // 6. How to read the exceptions
    // ==================================================================

    /**
     * Every failure is wrapped in {@link ProcessingPoolException} and emerges from the
     * future's exceptional path.
     *
     * <p>The common ones, in the order worth investigating:
     * <ul>
     *   <li>"the method is not exposed" -- the target method lacks
     *       {@link MultiProcessingCall}, or has it while <b>that replica is still running the
     *       old code</b>, which is common partway through a rolling deployment</li>
     *   <li>"no such bean or method" -- a misspelt name, or an argument count that does not
     *       match</li>
     *   <li>A serialisation failure -- some argument or return value is not serialisable; look
     *       at the innermost cause</li>
     *   <li>A timeout -- the far end is still computing, or the far end is gone</li>
     * </ul>
     *
     * <p>An exception the remote business method threw is carried back <b>as it was</b>, inside
     * one wrapper, so {@code getCause()} holds the real one.
     */
    public String renderWithFallback(String month) {
        try {
            return pool.<String>submit("reportService", "render", month)
                    .get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            log("Remote generation failed; falling back to local", e);
            return localRender(month);        // the fallback: compute it here
        }
    }

    // ==================================================================

    private void onReportReady(String report) {
    }

    private String localRender(String month) {
        return "local-" + month;
    }

    private void log(String message, Throwable e) {
    }
}
