package com.chaconneai.openspreader.pooling;

import com.chaconneai.openspreader.MultiProcessingService;
import com.chaconneai.spreader.GossipCluster;
import com.chaconneai.spreader.Node;
import com.chaconneai.openspreader.concurrent.ExecutorServiceHolder;
import com.chaconneai.openspreader.cluster.SelfRegisteringListener;
import com.chaconneai.openspreader.idempotence.IdempotentRequestCache;
import com.chaconneai.openspreader.serialization.ObjectCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Task dispatch's communication core: method calls and recursive tasks are sent to other
 * processes to run, and the replies return the same way.
 *
 * <h2>Dispatch happens only among replicas of the same application</h2>
 * It does not cross applications. Dispatch works on the premise that the peer has the same
 * bean and the same method, or the same task class, and only another replica of the same
 * application satisfies that. Other applications running in the cluster are unaffected and are
 * never sent tasks by mistake.
 *
 * <h2>The target is looked up on every call, not fixed at startup</h2>
 * This matters: {@link #pickPeer} reads the member list afresh every time. So a change in the
 * replica count while running is followed <b>immediately</b> -- an application that started as
 * one process begins dispatching outward on the next call once a second replica joins, and
 * conversely returns to local execution when every other replica leaves. Nowhere caches "how
 * many nodes there are".
 *
 * <h2>With only this process, it runs locally</h2>
 * No network, no serialisation -- an ordinary reflective call. So adding this package to a
 * single-machine deployment costs nothing extra.
 *
 * <h2>Two kinds of payload</h2>
 * <ul>
 *   <li>{@link PoolMessageType#INVOKE} -- calls a method of some bean, the target of which
 *       must carry {@link MultiProcessingCall} or it is refused</li>
 *   <li>{@link PoolMessageType#TASK} -- runs a recursive task, the whole task object being
 *       serialised across</li>
 * </ul>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class PoolService
        implements MultiProcessingService, SelfRegisteringListener {

    private static final Logger log = LoggerFactory.getLogger(PoolService.class);

    /** Task dispatch's own channel, separate from locks, semaphores and the application's own
     *  messages. */
    public static final String CHANNEL = "spreader.pool";

    private final GossipCluster cluster;
    private final MethodRegistry registry;
    private final ObjectCodec codec;
    private final long requestTimeoutMs;
    private final int maxDepth;

    /** Requests awaiting a reply. */
    private final Map<Long, CompletableFuture<PoolMessage>> pending = new ConcurrentHashMap<>();

    private final AtomicLong requestIdGen = new AtomicLong();

    // ------------------------------------------------------------------
    // Statistics: local execution and remote dispatch differ in throughput by a factor of 25,
    // measured at 89K against 3.6K QPS, so "how many tasks really went out" decides overall
    // throughput outright and has to be visible
    // ------------------------------------------------------------------

    /** Tasks run directly in this process. */
    private final AtomicLong localRuns = new AtomicLong();

    /** Tasks successfully sent to another process. */
    private final AtomicLong remoteDispatches = new AtomicLong();

    /** Tasks sent out where the peer timed out or errored, and which were recomputed
     *  locally. */
    private final AtomicLong remoteFallbacks = new AtomicLong();

    /** Times the recursion hit its depth limit and ran synchronously on the current thread. */
    private final AtomicLong depthCapped = new AtomicLong();

    /**
     * Tasks this process actually finished, successes and failures alike.
     *
     * <p>It has to be counted here, because {@link ForkJoinPool} <b>has no</b>
     * {@code getCompletedTaskCount()} -- that belongs to {@code ThreadPoolExecutor}. ForkJoin
     * offers only instantaneous values such as how many threads are alive and how much is
     * queued.
     */
    private final AtomicLong tasksCompleted = new AtomicLong();

    /** Tasks that ended by throwing. Together with tasksCompleted, this is the success
     *  rate. */
    private final AtomicLong tasksFailed = new AtomicLong();

    /** Handles inbound requests. It must not occupy spreader's event dispatch thread. */
    private final ExecutorService inbound;

    /**
     * The reply cache, which stops "a resend caused by a lost reply".
     *
     * <p>On the dispatch side what it guards against is a business method or a recursive task
     * running twice -- which is not merely wasteful: with a method that has side effects, the
     * result is wrong.
     */
    private final IdempotentRequestCache<PoolMessage> requestIdempotence =
            new IdempotentRequestCache<>(60_000L, 10_000);

    /**
     * For running recursive tasks locally.
     *
     * <p>A {@link ForkJoinPool} deliberately: a recursive task waits for its subtasks inside
     * {@code compute()}, and an ordinary thread pool meeting worker threads that wait on each
     * other exhausts itself, whereas ForkJoinPool's work-stealing helps with other tasks while
     * joining and never seizes up.
     */
    private final ForkJoinPool localPool;

    private volatile boolean closed;

    /**
     * @param executors thread pools all come from {@link ExecutorServiceHolder}. The
     *                  parallelism is set there too, so {@code parallelism} is not a parameter
     *                  here
     */
    public PoolService(GossipCluster cluster, MethodRegistry registry, ObjectCodec codec,
                       long requestTimeoutMs, int maxDepth, ExecutorServiceHolder executors) {
        this.cluster = cluster;
        this.registry = registry;
        this.codec = codec;
        this.requestTimeoutMs = requestTimeoutMs;
        this.maxDepth = maxDepth;
        this.inbound = executors.forPoolInbound();
        this.localPool = executors.forRecursiveTasks();
    }

    public void start() {
        cluster.addListener(CHANNEL, this);
        log.info("Task dispatch service started: request timeout={}ms, maximum dispatch "
                + "depth={}, parallelism={}",
                requestTimeoutMs, maxDepth, localPool.getParallelism());
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        cluster.removeListener(this);
        pending.values().forEach(f -> f.completeExceptionally(
                new IllegalStateException("the task dispatch service is closed")));
        pending.clear();
        // Neither pool is closed: they belong to ExecutorServiceHolder
    }

    // ------------------------------------------------------------------
    // The public surface: method calls
    // ------------------------------------------------------------------

    /**
     * Calls a method asynchronously; it may land in another process.
     *
     * <p>It does not block the calling thread: the whole call -- choosing a target, serialising,
     * waiting for the reply -- goes to the local thread pool. To wait synchronously,
     * {@code join()} the future returned.
     */
    public <T> CompletableFuture<T> submitCall(String className, String beanName,
                                               String methodName, Object[] args) {
        checkOpen();
        CompletableFuture<T> result = new CompletableFuture<>();
        localPool.execute(() -> {
            try {
                @SuppressWarnings("unchecked")
                T value = (T) execute(className, beanName, methodName, args);
                result.complete(value);
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        return result;
    }

    /**
     * Fire and forget: it neither waits for a result nor cares whether it succeeded.
     *
     * <p>A failure leaves a line in the log. That is deliberate -- a caller that chose to want
     * no return value should not be interrupted by an exception; where the result matters, use
     * {@link #submitCall}.
     */
    public void fireAndForget(String className, String beanName, String methodName, Object[] args) {
        checkOpen();
        localPool.execute(() -> {
            try {
                execute(className, beanName, methodName, args);
            } catch (RuntimeException e) {
                log.warn("The fire-and-forget call {}#{} failed: {}", beanName, methodName,
                        e.toString());
            }
        });
    }

    /**
     * Calls a method and waits for the result; it may land in another process.
     *
     * <p>With only this process in the cluster it is a local reflective call; with others, one
     * is chosen and it is sent there. This is the internal entry point; the two public forms
     * are {@link #submitCall} and {@link #fireAndForget}.
     */
    public Object execute(String className, String beanName, String methodName, Object[] args) {
        checkOpen();
        Object[] safeArgs = args == null ? new Object[0] : args;

        Node peer = pickPeer();
        if (peer == null) {
            // This process is the application's only replica, so there is no reason to touch
            // the network
            log.debug("This process is the application's only replica, so {}#{} runs locally",
                    beanName, methodName);
            return invokeLocally(beanName, className, methodName, safeArgs);
        }

        long requestId = requestIdGen.incrementAndGet();
        byte[] payload;
        try {
            payload = codec.encode(safeArgs);
        } catch (Exception e) {
            throw new ProcessingPoolException("the arguments failed to serialise; confirm that "
                    + "they all implement Serializable", e);
        }

        PoolMessage request = PoolMessage.invoke(requestId, beanName, className, methodName, payload);
        PoolMessage response = exchange(peer, requestId, request);
        return decodeResult(response, peer);
    }

    /**
     * A reflective call in this process. Remote execution ends here too.
     *
     * <p>What is called is normally <b>the proxy</b>, so that aspects such as transactions and
     * caching are not bypassed. The cost is that it enters
     * {@link MultiProcessingInterceptor} again -- hence the "this thread is executing locally"
     * mark raised beforehand, without which it is an infinite recursion: dispatch, execute,
     * dispatch again. The mark must be cleared in a finally.
     *
     * <p>A method carrying {@code @Async} calls the real object instead; see
     * {@link MethodRegistry.Target#targetFor(Method)} for why -- that annotation switches
     * threads, and a ThreadLocal mark does not cross a thread boundary.
     */
    private Object invokeLocally(String beanName, String className, String methodName, Object[] args) {
        MethodRegistry.Target target = registry.find(beanName, className, methodName, args.length);
        if (target == null) {
            throw new ProcessingPoolException("the method is not exposed for remote calls: "
                    + beanName + "#" + methodName + "(" + args.length + " argument(s)). Add "
                    + "@MultiProcessingCall to the target method");
        }
        Method method = target.method();
        ReflectionUtils.makeAccessible(method);
        MultiProcessingInterceptor.enterLocalExecution();
        try {
            return method.invoke(target.targetFor(method), args);
        } catch (InvocationTargetException e) {
            // An exception the business method threw itself is rethrown as it is, and not
            // wrapped in anything
            Throwable cause = e.getTargetException();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw new ProcessingPoolException("the method failed: " + methodName, cause);
        } catch (IllegalAccessException e) {
            throw new ProcessingPoolException("the method is inaccessible: " + methodName, e);
        } finally {
            MultiProcessingInterceptor.exitLocalExecution();
        }
    }

    // ------------------------------------------------------------------
    // The public surface: recursive tasks
    // ------------------------------------------------------------------

    /**
     * Submits a recursive task.
     *
     * @param depth the current recursion depth; past the limit nothing is dispatched outward
     *              and it is computed locally
     */
    public <T> CompletableFuture<T> submit(RecursiveTask<T> task, int depth) {
        checkOpen();

        if (depth >= maxDepth) {
            // Divided deeply enough: computed on the current thread, sent neither outward nor
            // into the local pool.
            //
            // The second half of that is essential: a parent forks a subtask and joins it, and
            // CompletableFuture.join() on a ForkJoinPool worker does not trigger work-stealing
            // -- it goes through managedBlock, and the pool creates a compensating thread to
            // maintain its parallelism. So each level of recursion blocks one more thread and
            // creates one more compensating thread, and SumTask(1, 1000000) was measured
            // exhausting them outright: unable to create native thread.
            //
            // The top few levels still go through the pool, which is where local multi-core
            // parallelism comes from; deeper down it becomes synchronous recursion, and the
            // thread count is bounded by 2^maxDepth.
            depthCapped.incrementAndGet();
            return CompletableFuture.completedFuture(runTask(task, depth));
        }

        Node peer = pickPeer();
        if (peer == null) {
            // This process is the application's only replica. It does not send to itself: the
            // transport refuses that anyway -- excluding self is the first thing unicastOn does
            // -- and sending to itself would go through serialisation and the inbound thread
            // pool, which discards when full, turning a local call that cannot fail into a
            // network request that may be silently dropped.
            localRuns.incrementAndGet();
            return localFuture(task, depth);
        }

        long requestId = requestIdGen.incrementAndGet();
        byte[] payload;
        try {
            payload = codec.encode(task);
        } catch (Exception e) {
            throw new ProcessingPoolException("the task failed to serialise; confirm that the "
                    + "task class and all its fields implement Serializable", e);
        }

        PoolMessage request = PoolMessage.task(requestId, depth, payload);
        CompletableFuture<PoolMessage> future = register(requestId);
        try {
            if (!cluster.unicastOn(CHANNEL, peer, request.encode())) {
                pending.remove(requestId);
                // Failing to send falls back to local computation; the whole computation must
                // not fail
                log.debug("Failed to send the task to {}; running it locally instead",
                        peer.label());
                remoteFallbacks.incrementAndGet();
                return CompletableFuture.supplyAsync(() -> runTask(task, depth), localPool);
            }
        } catch (RuntimeException e) {
            pending.remove(requestId);
            remoteFallbacks.incrementAndGet();
            return CompletableFuture.supplyAsync(() -> runTask(task, depth), localPool);
        }

        // * This must be handleAsync with localPool, and not handle. That is not a matter of
        // style; it is a deadlock.
        //
        // orTimeout()'s timeout is delivered by the JDK's internal CompletableFuture$Delayer, a
        // single-threaded ScheduledThreadPoolExecutor shared by the whole JVM, whose thread is
        // named CompletableFutureDelayScheduler. With handle, the callback runs on that thread
        // when the timeout fires -- and runTask inside the callback forks subtasks and joins
        // them, so that thread blocks.
        //
        // Once it blocks, no orTimeout anywhere in the JVM fires again, including the sibling
        // subtasks' own timeouts. Nobody's timeout arrives, nobody wakes, and it hangs for good.
        //
        // Measured: a lost packet under NETTY/UDP fired a timeout, and the main thread sat on
        // ForkJoinMultiProcessingPool.invoke's join() for 1740 seconds with the CPU at zero
        // throughout. The last line in the log was this callback's "running it locally
        // instead", and nothing followed.
        //
        // With handleAsync(..., localPool), the callback runs on the ForkJoinPool: it neither
        // occupies that global single thread, nor troubles work-stealing with the nested waits
        // of a join. The catch branch above was always right, using supplyAsync with localPool;
        // this timeout path was the one that had been missed.
        remoteDispatches.incrementAndGet();
        return future
                .orTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS)
                .handleAsync((response, error) -> {
                    pending.remove(requestId);
                    if (error != null) {
                        remoteFallbacks.incrementAndGet();
                        // The far end timed out or errored, so it is recomputed locally.
                        // Better slower than a lost result
                        log.warn("The task failed on {} ({}); recomputing it locally",
                                peer.label(), error.toString());
                        return runTask(task, depth);
                    }
                    @SuppressWarnings("unchecked")
                    T value = (T) decodeResult(response, peer);
                    return value;
                }, localPool);
    }

    /**
     * Runs a recursive task locally.
     *
     * <h2>What matters is who is calling</h2>
     * <ul>
     *   <li><b>An external thread</b> -- an application thread calling invoke -- submits to
     *       {@code localPool}, where the computation runs and local multi-core parallelism
     *       comes from</li>
     *   <li><b>{@code localPool}'s own worker</b> -- <b>computes on the current thread</b>, and
     *       must never submit to the pool again</li>
     * </ul>
     *
     * <h2>Why a worker thread must run synchronously</h2>
     * A parent forks a subtask and {@code join()}s it. Were the subtask submitted to the same
     * pool, it would become "waiting inside a pool worker for a task in the pool" -- every
     * worker occupied and blocked by parents, and the subtasks queued behind a worker that
     * never frees.
     *
     * <p>{@code CompletableFuture.join()} does not help here: it goes through
     * {@code managedBlock}, and the pool maintains its parallelism by creating <b>compensating
     * threads</b> rather than, as {@code ForkJoinTask.join()} does, having the current thread
     * help with what is queued. Each level of recursion blocks one more thread and adds one
     * more compensating thread -- the thread count grows exponentially, and was measured
     * exhausting the JVM's threads.
     *
     * <p>This path once used {@code supplyAsync(..., localPool)}, and hung a full NETTY/UDP
     * regression for three hours: all three workers stopped on the {@code join()} here, with
     * the CPU at zero throughout. The class's other two local-execution paths --
     * {@code execute()} and {@code depth >= maxDepth} -- had always run on the current thread;
     * <b>only this one went through the pool</b>.
     */
    private <T> CompletableFuture<T> localFuture(RecursiveTask<T> task, int depth) {
        if (inLocalPool()) {
            return CompletableFuture.completedFuture(runTask(task, depth));
        }
        return CompletableFuture.supplyAsync(() -> runTask(task, depth), localPool);
    }

    /** Whether the current thread is one of {@code localPool}'s own workers. */
    private boolean inLocalPool() {
        return Thread.currentThread() instanceof ForkJoinWorkerThread w
                && w.getPool() == localPool;
    }

    /** Runs a recursive task in this process, injecting the context beforehand so it can fork
     *  subtasks of its own. */
    @SuppressWarnings("unchecked")
    <T> T runTask(RecursiveTask<T> task, int depth) {
        task.bind(this, depth);
        try {
            T result = (T) task.compute();
            tasksCompleted.incrementAndGet();
            return result;
        } catch (RuntimeException | Error e) {
            // A failure still counts as one completion -- otherwise the completion count would
            // quietly fall short exactly when it most needs to be clear
            tasksCompleted.incrementAndGet();
            tasksFailed.incrementAndGet();
            throw e;
        }
    }

    // ------------------------------------------------------------------
    // Choosing a target
    // ------------------------------------------------------------------

    /**
     * Chooses another process to do the work.
     *
     * <p><b>Dispatch happens only among instances of the same application</b> and does not
     * cross applications. The reason is direct: dispatch relies on the peer having the same
     * bean and method, or the same task class, and only another replica of the same application
     * satisfies that. Sending an order service's task to a reporting service's process reaches
     * something that has no such class and can only fail.
     *
     * <p><b>The member list is read afresh on every call</b>, and no judgement about "how many
     * nodes there are" is cached. So a change in the process count is followed on the next
     * call: an application that started as one process begins dispatching outward as soon as a
     * second replica joins, and returns to local execution once every replica leaves.
     *
     * <p>Which one is chosen is decided by the cluster's configured load-balancing strategy
     * ({@code spring.spreader.load-balancer}), round-robin by default.
     *
     * @return the node chosen, or null when this process is the application's only replica,
     *         meaning it should run locally
     */
    private Node pickPeer() {
        String selfId = cluster.self().id();
        List<Node> candidates = new ArrayList<>();
        for (Node n : cluster.membersOf(cluster.self().name())) {
            if (!n.id().equals(selfId)) {
                candidates.add(n);
            }
        }
        if (candidates.isEmpty()) {
            return null;
        }
        // The cluster's configured load-balancing strategy rather than one fixed here, so that
        // changing spring.spreader.load-balancer to round-robin or hashing changes task dispatch
        // along with it. Round-robin is the default
        return cluster.config().loadBalancer().choose(candidates, null);
    }

    // ------------------------------------------------------------------
    // Sending and receiving
    // ------------------------------------------------------------------

    private CompletableFuture<PoolMessage> register(long requestId) {
        CompletableFuture<PoolMessage> future = new CompletableFuture<>();
        pending.put(requestId, future);
        return future;
    }

    private PoolMessage exchange(Node peer, long requestId, PoolMessage request) {
        CompletableFuture<PoolMessage> future = register(requestId);
        try {
            if (!cluster.unicastOn(CHANNEL, peer, request.encode())) {
                throw new ProcessingPoolException("the request failed to send to: "
                        + peer.label());
            }
            return future.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessingPoolException("interrupted while waiting for the remote result",
                    e);
        } catch (ProcessingPoolException e) {
            throw e;
        } catch (Exception e) {
            throw new ProcessingPoolException("the remote node " + peer.label()
                    + " failed or timed out", e);
        } finally {
            pending.remove(requestId);
        }
    }

    private Object decodeResult(PoolMessage response, Node peer) {
        if (response == null) {
            throw new ProcessingPoolException("no reply was received from " + peer.label());
        }
        if (!response.success()) {
            throw new ProcessingPoolException("the remote node " + peer.label() + " failed: "
                    + response.message());
        }
        try {
            return codec.decode(response.payload());
        } catch (Exception e) {
            throw new ProcessingPoolException("the return value failed to deserialise", e);
        }
    }

    @Override
    public void onPayload(Node sender, byte[] content) {
        PoolMessage msg = PoolMessage.decode(content);
        if (msg == null) {
            log.debug("An undecodable task message arrived from {}", sender.label());
            return;
        }

        if (msg.type() == PoolMessageType.RESPONSE) {
            CompletableFuture<PoolMessage> future = pending.get(msg.requestId());
            if (future != null) {
                future.complete(msg);
            }
            return;
        }

        // Both the work and the reply take time, and neither may sit on spreader's event
        // dispatch thread
        inbound.execute(() -> {
            // A resend of the same requestId returns the previous reply directly, without
            // running anything again
            PoolMessage response = requestIdempotence.execute(sender.id(), msg.requestId(),
                    () -> handleRequest(msg, sender));
            try {
                cluster.unicastOn(CHANNEL, sender, response.encode());
            } catch (Exception e) {
                log.warn("Failed to send the result back to {}: {}", sender.label(),
                        e.toString());
            }
        });
    }

    private PoolMessage handleRequest(PoolMessage msg, Node sender) {
        try {
            Object result = switch (msg.type()) {
                case INVOKE -> {
                    Object[] args = (Object[]) codec.decode(msg.payload());
                    yield invokeLocally(msg.beanName(), msg.className(), msg.methodName(),
                            args == null ? new Object[0] : args);
                }
                case TASK -> {
                    RecursiveTask<?> task = (RecursiveTask<?>) codec.decode(msg.payload());
                    // The depth travels with it, so the far end knows which level it is on
                    // when it dispatches further
                    yield runTask(task, msg.depth());
                }
                case RESPONSE -> throw new ProcessingPoolException(
                        "not a legitimate request type");
            };
            return PoolMessage.ok(msg.requestId(), codec.encode(result));
        } catch (Throwable t) {
            // A remote exception can only be carried back as text: the exception class may not
            // exist on the calling side
            log.debug("A request from {} failed while executing: {}", sender.label(),
                    t.toString());
            String reason = t.getClass().getSimpleName()
                    + (t.getMessage() == null ? "" : ": " + t.getMessage());
            return PoolMessage.fail(msg.requestId(), reason);
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("the task dispatch service is closed");
        }
    }

    /**
     * How many processes of the same application there are besides this one.
     *
     * <p>Computed afresh each time, with no cache. 0 means every call ahead runs locally.
     */
    public int peerCount() {
        String selfId = cluster.self().id();
        int n = 0;
        for (Node node : cluster.membersOf(cluster.self().name())) {
            if (!node.id().equals(selfId)) {
                n++;
            }
        }
        return n;
    }

    public int maxDepth() {
        return maxDepth;
    }

    /**
     * How long one remote request waits.
     *
     * <p>Note that this is a <b>single-hop</b> timeout and not the whole recursive task's. A
     * forked subtask that times out is recomputed locally, and that recomputation may fork new
     * remote subtasks which time out again -- so one {@code invoke} can take several times this
     * value in total. For an end-to-end limit, use
     * {@link ForkJoinMultiProcessingPool#invoke(RecursiveTask, long,
     * java.util.concurrent.TimeUnit)}.
     */
    public long requestTimeoutMs() {
        return requestTimeoutMs;
    }

    public MethodRegistry registry() {
        return registry;
    }

    /**
     * Readings from the thread pool's point of view, shaped to match
     * {@code ThreadPoolExecutor}.
     *
     * <p>{@code inbound} is declared as the {@link ExecutorService} interface, but the
     * implementation underneath has always been a {@link ThreadPoolExecutor}; see
     * {@code ExecutorUtils}. Failing to obtain one falls back to 0 and throws nothing --
     * <b>metrics must not break the application</b>.
     */
    public PoolStats poolStats() {
        int inActive = 0, inQueued = 0, inSize = 0;
        long inCompleted = 0L;
        if (inbound instanceof ThreadPoolExecutor tpe) {
            inActive = tpe.getActiveCount();
            inQueued = tpe.getQueue().size();
            inCompleted = tpe.getCompletedTaskCount();
            inSize = tpe.getPoolSize();
        }
        return new PoolStats(
                localPool.getActiveThreadCount(),
                localPool.getRunningThreadCount(),
                localPool.getQueuedTaskCount(),
                localPool.getQueuedSubmissionCount(),
                tasksCompleted.get(),
                tasksFailed.get(),
                localPool.getPoolSize(),
                localPool.getParallelism(),
                localPool.getStealCount(),
                inActive, inQueued, inCompleted, inSize);
    }

    /**
     * Task dispatch's figures.
     *
     * <h2>remoteRatio is the one to watch</h2>
     * Local execution and remote dispatch differ in throughput <b>by a factor of 25</b> --
     * measured at 89,526 QPS locally against 3,632 remotely -- and the difference is all
     * network round trips and serialisation.
     *
     * <p>So this ratio decides overall throughput outright:
     * <ul>
     *   <li>Near 0 -- almost nothing is being dispatched. Either the cluster holds one replica,
     *       or {@code applicationName} has narrowed the scope to nothing. This component is then
     *       a local thread pool and is doing nothing distributed</li>
     *   <li>Near 1 -- everything is going out. Throughput is far lower, but that is <b>the
     *       expected cost</b>, bought by spreading CPU-bound work across several machines</li>
     * </ul>
     *
     * <p>{@code remoteFallbacks} is <b>an anomaly signal</b>: a task went out and no result came
     * back, so it was recomputed locally -- a wasted network request. Persistently non-zero, it
     * means the peer has a problem (a timeout, a crash, or packet loss), and the application
     * layer sees none of it and merely feels that things are slow.
     */
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        long local = localRuns.get();
        long remote = remoteDispatches.get();
        m.put("localRuns", local);
        m.put("remoteDispatches", remote);
        m.put("remoteFallbacks", remoteFallbacks.get());
        m.put("depthCapped", depthCapped.get());
        m.put("remoteRatio", local + remote == 0 ? 0d
                : Math.round(remote * 10000.0 / (local + remote)) / 10000.0);
        m.put("pendingRequests", pending.size());
        m.put("maxDepth", maxDepth);
        PoolStats ps = poolStats();
        m.put("activeCount", ps.activeCount());
        m.put("completedTaskCount", ps.completedTaskCount());
        m.put("queuedTaskCount", ps.queuedTaskCount());
        m.put("failureRate", ps.failureRate());
        m.put("localBlockedRate", ps.localBlockedRate());
        m.put("localSteals", ps.localSteals());
        m.put("localParallelism", ps.localParallelism());
        m.put("inboundQueued", ps.inboundQueued());
        return m;
    }

}
