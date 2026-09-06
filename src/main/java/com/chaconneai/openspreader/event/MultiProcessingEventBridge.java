package com.chaconneai.openspreader.event;

import com.chaconneai.openspreader.MultiProcessingService;
import com.chaconneai.openspreader.serialization.ObjectCodec;
import com.chaconneai.spreader.GossipCluster;
import com.chaconneai.spreader.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Carries Spring application events between processes. <b>Not one line of application code
 * changes.</b>
 *
 * <pre>{@code
 * // Publishing: still the standard shape
 * publisher.publishEvent(new OrderPaidEvent(orderId));
 *
 * // Receiving: still the standard shape, only now other processes receive it too
 * @EventListener
 * public void on(OrderPaidEvent e) { ... }
 * }</pre>
 *
 * <p>The switch is {@code spring.spreader.multiprocessing.event.enabled}, off by default.
 * Switched off, this bean does not exist at all and events keep Spring's original in-JVM
 * semantics.
 *
 * <h2>How the API stays unchanged</h2>
 * An {@link ApplicationListener} for <b>every</b> event is registered: a locally published
 * event is dispatched within this JVM as usual -- Spring's doing -- and then caught here,
 * serialised and broadcast. Another node, receiving it, publishes it once in <b>its own</b>
 * context, so listeners there fire as though the event were local.
 *
 * <h2>Three problems that must be handled</h2>
 *
 * <b>One: the loop.</b> A remote event published locally is caught here again and broadcast
 * once more -- two nodes can play ping-pong with it, and one event becomes an unbounded
 * number. A {@link ThreadLocal} marks "this thread is replaying a remote event", and nothing
 * is broadcast while it is set. A wrapper event type is not used because it would change the
 * type listeners see, which is exactly what "the API stays unchanged" forbids.
 *
 * <b>Two: Spring's own events must not be broadcast.</b> {@code ContextRefreshedEvent},
 * {@code ServletRequestHandledEvent} and the rest describe <b>this process's lifecycle</b>.
 * Broadcasting them means nothing and would have other nodes believe their own context had
 * refreshed. Only event classes outside Spring's namespace pass.
 *
 * <b>Three: events must be serialisable.</b> An event that cannot be sent <b>must not fail
 * the application</b> -- local dispatch has already succeeded, and a failure in the
 * cross-process half is only logged. Otherwise turning on one configuration switch would
 * start throwing exceptions in code that was working.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
public class MultiProcessingEventBridge implements ApplicationListener<ApplicationEvent>,
        ApplicationEventPublisherAware, MultiProcessingService {

    public static final String CHANNEL = "spreader.event";

    private static final Logger log = LoggerFactory.getLogger(MultiProcessingEventBridge.class);

    /**
     * Whether the current thread is replaying an event that <b>came from elsewhere</b>.
     *
     * <p>Publishing during a replay fires this listener again, and unguarded the event would be
     * broadcast straight back, the peer would broadcast it back again -- two nodes playing
     * ping-pong, one event becoming an unbounded number.
     *
     * <p>A ThreadLocal rather than a wrapper event type: wrapping would change the type
     * listeners see, so {@code @EventListener(OrderPaidEvent.class)} would no longer match --
     * and "the API stays unchanged" is the whole point of this feature.
     */
    private static final ThreadLocal<Boolean> REPLAYING = ThreadLocal.withInitial(() -> false);

    private final GossipCluster cluster;
    private final ObjectCodec codec;
    private final String applicationName;
    private final boolean clusterWide;

    private ApplicationEventPublisher publisher;

    private final AtomicLong published = new AtomicLong();
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong skipped = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();

    /**
     * @param applicationName sends only to applications of the same name; ignored when
     *                         {@code clusterWide} is true
     * @param clusterWide     true sends to every application in the cluster, false only to
     *                        applications of the same name
     */
    public MultiProcessingEventBridge(GossipCluster cluster, ObjectCodec codec,
                                      String applicationName, boolean clusterWide) {
        this.cluster = cluster;
        this.codec = codec;
        this.applicationName = applicationName;
        this.clusterWide = clusterWide;
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    public void start() {
        cluster.addListener(CHANNEL, this);
        log.info("Cluster events enabled: channel={}, scope={}", CHANNEL,
                clusterWide ? "the whole cluster" : "applications named " + applicationName);
    }

    @Override
    public void close() {
        cluster.removeListener(this);
    }

    // ------------------------------------------------------------------
    // Outbound: locally published events are broadcast
    // ------------------------------------------------------------------

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (REPLAYING.get()) {
            // This one came from another node and is being replayed locally. Broadcasting it
            // again would be ping-pong
            return;
        }
        if (!shouldBroadcast(event)) {
            skipped.incrementAndGet();
            return;
        }
        try {
            byte[] payload = codec.encode(event);
            int sent = clusterWide
                    ? cluster.multicastOn(CHANNEL, null, payload)
                    : cluster.multicastOn(CHANNEL, applicationName, payload);
            published.incrementAndGet();
            log.debug("Cluster event broadcast: {} to {} node(s)",
                    event.getClass().getSimpleName(), sent);
        } catch (RuntimeException e) {
            // A failed broadcast must not fail the application: local dispatch has already
            // succeeded, and the cross-process half failing only means one notification fewer.
            // Letting it out would make turning on a configuration switch start breaking code
            // that was working
            failures.incrementAndGet();
            log.warn("Cluster event broadcast failed; local dispatch is unaffected: {} - {}",
                    event.getClass().getName(), e.toString());
        }
    }

    /**
     * Whether this event should be sent.
     *
     * <h2>Spring's own events are never sent</h2>
     * {@code ContextRefreshedEvent}, {@code ApplicationReadyEvent},
     * {@code ServletRequestHandledEvent} and the like describe <b>this process's
     * lifecycle</b>. Broadcast, they would have other nodes believe their own context had
     * refreshed -- an entirely wrong signal.
     *
     * <p>The test is the package name: only classes outside {@code org.springframework} are
     * sent. That is more reliable than excluding types one by one -- Spring's event types grow
     * with each version, and a list of exclusions will sooner or later miss the newest one.
     */
    private boolean shouldBroadcast(ApplicationEvent event) {
        if (event instanceof ApplicationContextEvent) {
            return false;
        }
        String pkg = event.getClass().getName();
        return !pkg.startsWith("org.springframework.");
    }

    // ------------------------------------------------------------------
    // Inbound: events from other nodes are replayed locally
    // ------------------------------------------------------------------

    @Override
    public void onPayload(Node sender, byte[] content) {
        if (publisher == null) {
            return;
        }
        try {
            Object decoded = codec.decode(content, Object.class);
            if (!(decoded instanceof ApplicationEvent event)) {
                log.warn("What arrived was not an ApplicationEvent; ignored: {}",
                        decoded == null ? "null" : decoded.getClass().getName());
                return;
            }
            received.incrementAndGet();
            REPLAYING.set(true);
            try {
                // Published once in this node's context. Listeners see the original event
                // type, no different from a locally published one -- which is precisely where
                // "the API stays unchanged" lands
                publisher.publishEvent(event);
            } finally {
                REPLAYING.set(false);
            }
        } catch (Exception e) {
            // Undecodable means discarded. The usual cause is that the peer has the event
            // class and this side does not -- during a rolling upgrade the new version
            // publishes a new event type the old nodes do not recognise. That must not bring an
            // old node down; skipping it is enough
            failures.incrementAndGet();
            log.warn("Cluster event failed to decode and was skipped; it came from {}: {}",
                    sender.label(), e.toString());
        }
    }

    // ------------------------------------------------------------------

    /** Runtime statistics, for wiring into metrics. */
    @Override
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("published", published.get());
        m.put("received", received.get());
        m.put("skipped", skipped.get());
        m.put("failures", failures.get());
        m.put("clusterWide", clusterWide);
        return m;
    }
}
