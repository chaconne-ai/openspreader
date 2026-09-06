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

import com.chaconneai.openspreader.MultiProcessingService;
import com.chaconneai.openspreader.cluster.SelfRegisteringListener;
import com.chaconneai.openspreader.idempotence.IdempotentRequestCache;
import com.chaconneai.openspreader.pooling.MethodRegistry;
import com.chaconneai.openspreader.concurrent.ExecutorServiceHolder;
import com.chaconneai.openspreader.serialization.ObjectCodec;
import com.chaconneai.spreader.GossipCluster;
import com.chaconneai.spreader.Node;
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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RPC's communication core: it sends a method call to an instance of another application, and
 * the reply comes back the same way.
 *
 * <h2>It differs from task dispatch in one thing: who it sends to</h2>
 * {@link com.chaconneai.openspreader.pooling.PoolService} dispatches only among replicas of
 * <b>the same application</b>, because its premise is that anyone can do the work. Here it is
 * the opposite -- the capability being called <b>exists only on the other side</b>, so the
 * target is chosen by {@code serviceId}, the peer's application name.
 *
 * <p>Everything else is the same: the same allow-list ({@code @MultiProcessingCall}), the same
 * serialisation ({@link ObjectCodec}), the same unicast channel. That is not for convenience;
 * these things should only ever exist once.
 *
 * <h2>The target is chosen afresh every time</h2>
 * Each call reads the member list again and caches nothing about how many instances the peer
 * has. So the next call follows the peer's scaling immediately, and a retry naturally moves to
 * another instance.
 *
 * <h2>No available instance fails outright</h2>
 * Unlike task dispatch, it does not fall back to local execution -- this process has no such
 * bean at all, and falling back would only produce a more baffling error.
 *
 * <h2>The inbound queue is bounded</h2>
 * When it cannot keep up, requests wait in the queue. <b>An unbounded queue is a trap
 * here</b>: it hides the overload and shows it as "every caller times out" -- and a caller
 * that times out usually retries, so the queue grows faster still, until it reaches OOM with
 * nothing anywhere having reported an error.
 *
 * <p>Bounded, a full queue answers "overloaded" directly. The caller knows <b>at once</b> that
 * the peer cannot take more and can degrade or limit its rate, rather than waiting out the
 * timeout. This is the premise on which back-pressure works: a refusal must be quick, and a
 * slow refusal is much the same as none.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class RpcService
        implements MultiProcessingService, SelfRegisteringListener {

    private static final Logger log = LoggerFactory.getLogger(RpcService.class);

    /** RPC's own channel, separate from locks, task dispatch and the cache. */
    public static final String CHANNEL = "spreader.rpc";

    /** The inbound queue's default capacity. See the class javadoc on why an unbounded queue
     *  is a trap. */
    public static final int DEFAULT_QUEUE_CAPACITY = 1_000;

    private final GossipCluster cluster;
    private final MethodRegistry registry;
    private final ObjectCodec codec;

    /** Requests awaiting a reply. */
    private final Map<Long, CompletableFuture<RpcMessage>> pending = new ConcurrentHashMap<>();

    private final AtomicLong requestIdGen = new AtomicLong();

    /** Handles inbound requests. It must not occupy spreader's event dispatch thread. */
    private final ExecutorService inbound;

    // Calling-side statistics
    private final AtomicLong calls = new AtomicLong();
    private final AtomicLong answered = new AtomicLong();
    private final AtomicLong callFailures = new AtomicLong();
    private final AtomicLong callTimeouts = new AtomicLong();
    private final AtomicLong remoteErrors = new AtomicLong();
    private final AtomicLong latencyNanos = new AtomicLong();

    // Serving-side statistics
    private final AtomicLong served = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();

    /**
     * The reply cache on the serving side.
     *
     * <p>At the RPC layer what it guards against is <b>the business method running twice</b>:
     * the request arrived and the method completed, and only the reply was lost on the way back
     * -- whereupon the caller's retry ({@code maxRetries}) would run it again.
     *
     * <p>The documentation has always required the called method to be idempotent, because
     * that was all it could require. With this cache, a resend of <b>the same call</b> does not
     * execute again; different calls of course still each run, so the requirement for
     * application-level idempotence has not gone away -- it simply no longer pays for the
     * transport's jitter.
     */
    private final IdempotentRequestCache<RpcMessage> requestIdempotence =
            new IdempotentRequestCache<>(60_000L, 10_000);

    private volatile boolean closed;

    /**
     * @param executors thread pools all come from {@link ExecutorServiceHolder}. The thread
     *                  count and the queue capacity are set there too, so neither is a
     *                  parameter here
     */
    public RpcService(GossipCluster cluster, MethodRegistry registry, ObjectCodec codec,
                      ExecutorServiceHolder executors) {
        this.cluster = cluster;
        this.registry = registry;
        this.codec = codec;
        // This pool rejects rather than discards: a full queue must be able to answer
        // "overloaded" so the caller degrades at once, instead of waiting out the timeout while
        // sending new requests all the while. Catching RejectedExecutionException below is what
        // does that.
        //
        // CallerRunsPolicy will not do either: it would have spreader's event dispatch thread
        // execute the business method, and one slow method could block message dispatch
        // entirely -- gossip heartbeats included
        this.inbound = executors.forRpcInbound();
    }

    public void start() {
        cluster.addListener(CHANNEL, this);
        log.info("RPC service started with serialisation={}", codec.type());
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        cluster.removeListener(this);
        pending.values().forEach(
                f -> f.completeExceptionally(new RpcException("the RPC service is closed")));
        pending.clear();
        // inbound is not closed: it belongs to ExecutorServiceHolder
    }

    // ------------------------------------------------------------------
    // The calling side
    // ------------------------------------------------------------------

    /**
     * Calls a remote method and blocks for the result.
     *
     * @param serviceId the target application name; blank searches the whole cluster
     * @param beanName  the target bean name, which may be blank
     * @param className the target class name, used when {@code beanName} is blank
     * @param method    the method being called; the argument and return types are read from it
     * @param args      the arguments
     * @param timeoutMs this attempt's timeout; 0 or negative waits indefinitely
     * @throws RpcException on a communication failure: no available instance, unable to send,
     *                      a timeout, or an undecodable reply
     * @throws Throwable    the exception the peer's business method threw, restored as it
     *                      was
     */
    public Object invoke(String serviceId, String beanName, String className, Method method,
                         Object[] args, long timeoutMs) throws Throwable {
        checkOpen();

        Node target = pick(serviceId);
        if (target == null) {
            throw new RpcException(describeTarget(serviceId) + " has no available instance. "
                    + "Confirm that the peer is up, that it is in the same cluster as this node "
                    + "(the same spring.spreader.name), and that its spring.application.name is "
                    + "exactly this name");
        }

        Object[] safeArgs = args == null ? new Object[0] : args;
        List<byte[]> payloads = new ArrayList<>(safeArgs.length);
        for (Object arg : safeArgs) {
            payloads.add(codec.encode(arg));
        }

        long requestId = requestIdGen.incrementAndGet();
        RpcMessage request = RpcMessage.request(requestId, beanName, className,
                method.getName(), payloads);

        RpcMessage response = exchange(target, requestId, request, timeoutMs);
        if (!response.success()) {
            throw restore(response, target);
        }
        return codec.decode(response.returnValue(), method.getReturnType());
    }

    /** Sends the request and waits for the reply. */
    private RpcMessage exchange(Node target, long requestId, RpcMessage request, long timeoutMs) {
        CompletableFuture<RpcMessage> future = new CompletableFuture<>();
        pending.put(requestId, future);
        calls.incrementAndGet();
        long startedAt = System.nanoTime();
        try {
            if (!cluster.unicastOn(CHANNEL, target, request.encode())) {
                throw new RpcException("the request could not be sent to " + target.label()
                        + "; it may have just departed");
            }
            RpcMessage response = timeoutMs <= 0
                    ? future.get()
                    : future.get(timeoutMs, TimeUnit.MILLISECONDS);
            // Only answered round trips are counted. Failed ones are not comparable -- a
            // timed-out call always equals the timeout, and mixing those in would make the mean
            // a function of the configuration
            latencyNanos.addAndGet(System.nanoTime() - startedAt);
            answered.incrementAndGet();
            if (!response.success()) {
                remoteErrors.incrementAndGet();
            }
            return response;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            callFailures.incrementAndGet();
            throw new RpcException("interrupted while waiting for " + target.label()
                    + "'s reply", e);
        } catch (RpcException e) {
            callFailures.incrementAndGet();
            throw e;
        } catch (TimeoutException e) {
            // Counted separately: a timeout and "the peer answered with an explicit error" are
            // entirely different failures -- the first points at the network and the peer's
            // load, the second at the application code. One combined number cannot triage
            callTimeouts.incrementAndGet();
            callFailures.incrementAndGet();
            throw new RpcException(target.label() + " did not answer within " + timeoutMs
                    + "ms");
        } catch (Exception e) {
            callFailures.incrementAndGet();
            throw new RpcException("the call to " + target.label() + " failed", e);
        } finally {
            pending.remove(requestId);
        }
    }

    /**
     * Chooses a target instance.
     *
     * <p>It uses the cluster's configured load-balancing strategy
     * ({@code spring.spreader.load-balancer}, round-robin by default). <b>The member list is
     * read afresh every time</b>, so the peer's scaling takes effect at once and a retry
     * naturally moves to another instance.
     *
     * @return null when there is none
     */
    private Node pick(String serviceId) {
        List<Node> candidates = available(serviceId == null || serviceId.isBlank()
                ? cluster.members()
                : cluster.membersOf(serviceId));
        if (candidates.isEmpty()) {
            return null;
        }
        return cluster.config().loadBalancer().choose(candidates, null);
    }

    /**
     * Filters out instances that are <b>resting</b> ({@code cluster.takeBreak(true)}).
     *
     * <h2>Why the filter is here rather than in the transport</h2>
     * All the transport sees is "a message on the spreader.rpc channel", and it cannot tell a
     * <b>new request</b> from a <b>response</b>. Filtering at that layer indiscriminately would
     * stop a resting node from ever returning an RPC result -- and the caller would only wait
     * out its timeout, which is far worse than being given no work.
     *
     * <p>This layer can tell them apart: what {@code pick} chooses is certainly the target of a
     * new request, so filtering resting nodes is exactly right, while a response goes through
     * {@code unicastOn(CHANNEL, sender, ...)}, does not come through here, and is sent as
     * usual.
     *
     * <p>A resting instance is still a full member of the cluster and merely takes no new
     * calls. With every instance resting this returns an empty list and the caller receives "no
     * available instance" -- which is more honest than forcing a request onto an instance that
     * has plainly said it is not taking work.
     */
    private static List<Node> available(List<Node> candidates) {
        if (candidates.isEmpty()) {
            return candidates;
        }
        List<Node> usable = new ArrayList<>(candidates.size());
        for (Node n : candidates) {
            if (!n.onBreak()) {
                usable.add(n);
            }
        }
        return usable;
    }

    /**
     * How many available <b>peer</b> instances the target application has. For troubleshooting
     * and monitoring, computed afresh each time.
     *
     * <p><b>It excludes this node</b>: the member list holds only others to begin with. So
     * calling it for this node's own application returns "how many besides me" rather than "how
     * many in total".
     */
    public int instanceCount(String serviceId) {
        // The same basis pick uses: a resting instance does not count as available, or this
        // number would disagree with whether a call actually gets through
        return available(serviceId == null || serviceId.isBlank()
                ? cluster.members()
                : cluster.membersOf(serviceId)).size();
    }

    private static String describeTarget(String serviceId) {
        return serviceId == null || serviceId.isBlank()
                ? "the cluster"
                : "application " + serviceId;
    }

    /**
     * Restores the peer's exception.
     *
     * <p>It is restored to its original type where possible, so that a caller catching a
     * specific exception works as it should. Where the class is absent from this process --
     * the two sides having different dependencies -- it degrades to {@link RpcException} with
     * the original class name in the message, so nothing is lost.
     */
    private Throwable restore(RpcMessage response, Node target) {
        String type = response.errorType();
        String message = response.errorMessage();
        if (type.isEmpty()) {
            return new RpcException(target.label() + " failed to execute it: " + message);
        }
        try {
            Class<?> clazz = Class.forName(type, false, getClass().getClassLoader());
            if (Throwable.class.isAssignableFrom(clazz)) {
                return (Throwable) clazz.getConstructor(String.class).newInstance(message);
            }
        } catch (Throwable ignore) {
            // The class is absent, or has no (String) constructor. A degradation, not an
            // error
        }
        return new RpcException(target.label() + " threw " + type + ": " + message);
    }

    // ------------------------------------------------------------------
    // The serving side
    // ------------------------------------------------------------------

    @Override
    public void onPayload(Node sender, byte[] content) {
        RpcMessage msg = RpcMessage.decode(content);
        if (msg == null) {
            log.debug("An undecodable RPC message arrived from {}", sender.label());
            return;
        }

        if (msg.type() == RpcMessageType.RESPONSE) {
            CompletableFuture<RpcMessage> future = pending.get(msg.requestId());
            if (future != null) {
                future.complete(msg);
            }
            return;
        }

        // Both the work and the reply take time, and neither may sit on spreader's event
        // dispatch thread
        try {
            inbound.execute(() -> {
                // A resend of the same requestId returns the previous reply directly, without
                // running the business method again
                RpcMessage response = requestIdempotence.execute(sender.id(), msg.requestId(),
                        () -> handle(msg, sender));
                served.incrementAndGet();
                reply(sender, response);
            });
        } catch (RejectedExecutionException e) {
            // The queue is full. Answer "overloaded" at once so the caller degrades, rather
            // than waiting out its timeout while sending new requests all the while
            rejected.incrementAndGet();
            log.warn("The RPC inbound queue is full; refusing request {} from {}",
                    describeMethod(msg), sender.label());
            reply(sender, RpcMessage.fail(msg.requestId(),
                    RpcOverloadException.class.getName(),
                    "the peer's RPC inbound queue is full; retry shortly or degrade"));
        }
    }

    private void reply(Node sender, RpcMessage response) {
        try {
            cluster.unicastOn(CHANNEL, sender, response.encode());
        } catch (Exception e) {
            log.warn("Failed to send the RPC result back to {}: {}", sender.label(),
                    e.toString());
        }
    }

    /** How many requests wait in the inbound queue. For monitoring -- a rise here precedes
     *  overload. */
    public int queuedRequests() {
        return inbound instanceof ThreadPoolExecutor pool ? pool.getQueue().size() : 0;
    }

    /**
     * Runtime figures from both the calling and the serving side.
     *
     * <h2>How to read them</h2>
     * <ul>
     *   <li>{@code callTimeouts} and {@code remoteErrors} <b>must be read apart</b>: the first
     *       is "the peer said nothing", pointing at the network and the peer's load, and the
     *       second is "the peer answered with an explicit exception", pointing at the
     *       application code. Combined into one failure rate, neither can be triaged</li>
     *   <li>{@code avgLatencyMillis} counts <b>answered</b> round trips only. A timed-out call
     *       always equals the configured timeout, and mixing those in would make the mean a
     *       function of the configuration rather than a reflection of the peer's real
     *       speed</li>
     *   <li>{@code queued} and {@code rejected} are this node's overload signals <b>as the
     *       serving side</b>. A steadily rising queued precedes trouble, and a non-zero
     *       rejected means requests are already being dropped</li>
     *   <li>{@code calls - answered - callFailures} is the in-flight count. It equals
     *       {@code pending}, and a disagreement means some request never reached its finally
     *       cleanup</li>
     * </ul>
     */
    @Override
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        long total = calls.get();
        long ok = answered.get();
        m.put("calls", total);
        m.put("answered", ok);
        m.put("callFailures", callFailures.get());
        m.put("callTimeouts", callTimeouts.get());
        m.put("remoteErrors", remoteErrors.get());
        m.put("avgLatencyMillis", ok == 0 ? 0d
                : Math.round(latencyNanos.get() / (double) ok / 1e4) / 100.0);
        m.put("failureRate", total == 0 ? 0d
                : Math.round(callFailures.get() * 10000.0 / total) / 10000.0);
        m.put("inflight", pending.size());
        m.put("served", served.get());
        m.put("rejected", rejected.get());
        m.put("queued", queuedRequests());
        return m;
    }

    private RpcMessage handle(RpcMessage msg, Node sender) {
        MethodRegistry.Target target = registry.find(msg.beanName(), msg.className(),
                msg.methodName(), msg.args().size());
        if (target == null) {
            return RpcMessage.fail(msg.requestId(), RpcException.class.getName(),
                    "the method is not exposed for remote calls: " + describeMethod(msg)
                            + ". Add @MultiProcessingCall to the target method");
        }

        Method method = target.method();
        Object[] args;
        try {
            // Argument types are read from the method signature rather than the message --
            // one fewer place where the two sides can disagree
            Class<?>[] types = method.getParameterTypes();
            args = new Object[types.length];
            for (int i = 0; i < types.length; i++) {
                args[i] = codec.decode(msg.args().get(i), types[i]);
            }
        } catch (RuntimeException e) {
            return RpcMessage.fail(msg.requestId(), e.getClass().getName(),
                    "the arguments failed to deserialise: " + e.getMessage());
        }

        ReflectionUtils.makeAccessible(method);
        try {
            Object result = method.invoke(target.bean(), args);
            return RpcMessage.ok(msg.requestId(), codec.encode(result));
        } catch (InvocationTargetException e) {
            // Thrown by the business method itself: the original type is carried along so the
            // caller has a chance of restoring the same exception
            Throwable cause = e.getTargetException();
            log.debug("An RPC request from {} failed while executing: {}", sender.label(),
                    cause.toString());
            return RpcMessage.fail(msg.requestId(), cause.getClass().getName(),
                    cause.getMessage() == null ? cause.getClass().getSimpleName()
                            : cause.getMessage());
        } catch (Throwable t) {
            log.debug("An RPC request from {} errored while executing: {}", sender.label(),
                    t.toString());
            return RpcMessage.fail(msg.requestId(), t.getClass().getName(),
                    String.valueOf(t.getMessage()));
        }
    }

    private static String describeMethod(RpcMessage msg) {
        String owner = msg.beanName().isEmpty() ? msg.className() : msg.beanName();
        return owner + "#" + msg.methodName() + "(" + msg.args().size() + " argument(s))";
    }

    private void checkOpen() {
        if (closed) {
            throw new RpcException("the RPC service is closed");
        }
    }
}
