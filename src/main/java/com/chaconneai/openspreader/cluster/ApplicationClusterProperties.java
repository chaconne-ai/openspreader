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
package com.chaconneai.openspreader.cluster;

import com.chaconneai.spreader.transport.TransportProvider;
import com.chaconneai.spreader.transport.TransportType;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The cluster's own configuration, under the prefix {@code spring.spreader}.
 *
 * <p>The tools built on top of the cluster -- locks, the cache, the process pool and the rest
 * -- are configured under {@code spring.spreader.multiprocessing.*}; see
 * {@link com.chaconneai.openspreader.MultiProcessingProperties}.
 *
 * <h2>It runs with no configuration at all</h2>
 * Without a word of configuration, two instances form a cluster -- discovery defaults to
 * {@code 127.0.0.1}, so several instances on one machine work out of the box. <b>Production
 * needs exactly one line changed</b>: {@link #ipAddresses}, to the real list of machines.
 *
 * <pre>
 * # Local development: nothing at all
 *
 * # Production: this one line
 * spring.spreader.ip-addresses=192.168.0.111,192.168.0.63,192.168.0.77
 * </pre>
 *
 * <h2>Empty means "use the component's default"</h2>
 * A wrapper-typed field ({@code Integer}, {@code Long}, {@code Boolean}) left empty <b>does
 * not</b> override the spreader component's own default. Only the following carry defaults
 * this package <b>deliberately redefined</b>, calibrated by measurement; see the note on each:
 * {@link #ipAddresses}, {@link Advanced#suspectTimeoutMs},
 * {@link Advanced#leaderQuietPeriodMs}.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@Data
@ConfigurationProperties(prefix = "spring.spreader")
public class ApplicationClusterProperties {

    /**
     * Whether the cluster is enabled.
     *
     * <p>Switched off, no port is bound and no {@code GossipCluster} is created -- and with it,
     * nothing that depends on it: locks, the cache, and the rest. Useful for running the
     * application's own code locally without ports or networking getting in the way.
     */
    private boolean enabled = true;

    /**
     * The cluster name. <b>Only nodes sharing a name form a cluster</b>, and this is the only
     * way to keep environments apart -- dev, staging and prod on one network are separated by
     * it alone.
     */
    private String name = "spreader-cluster";

    /**
     * This node's application name. Left empty it takes {@code spring.application.name}, and
     * failing that the component's default. <b>The great majority of projects never set
     * it.</b>
     *
     * <p>It <b>does not affect joining</b>, which goes by {@link #name}. Its purpose is to
     * distinguish applications within one cluster, so that an application can "send only to the
     * order service". <b>Several instances may share a name</b>: an application scaled to five
     * replicas has the same application name on all five.
     */
    private String applicationName;

    /** The interface to listen on. All interfaces by default. */
    private String bindHost = "0.0.0.0";

    /**
     * The IP advertised outward. Left empty, this machine's private address is detected
     * automatically.
     *
     * <p>Worth setting explicitly on a multi-homed machine or inside a container -- automatic
     * detection may choose an interface nobody else can reach, docker0 for instance.
     */
    private String advertiseHost;

    /**
     * The cluster port, which <b>must agree across the whole cluster</b>.
     *
     * <p>This one port does two things: whoever takes it is the leader -- port exclusion on one
     * machine being the operating system's guarantee -- and it is also the door every new node
     * knocks on. So in the whole configuration, <b>this is the only number every party must
     * agree on beforehand</b>.
     */
    private int port = 22000;

    /**
     * Which hosts to look for the cluster on. <b>Hosts only, without ports</b> -- the port is
     * always {@link #port}. Host names work, so a Docker container name, a compose service
     * name or a Kubernetes Service name can be written directly.
     *
     * <p><b>The default is {@code 127.0.0.1}</b>: a few instances on one machine form a cluster
     * directly, without a line of configuration. That is the entire reason this default exists
     * -- to have someone running within five minutes.
     *
     * <p>Production <b>must change it to the real list of machines</b>, or each machine forms a
     * cluster of its own. One answering host is enough: the joining node receives the full
     * member list from it and the rest converges through gossip, so discovery stops at the
     * first hit rather than scanning the whole list.
     */
    private List<String> ipAddresses = new ArrayList<>(List.of("127.0.0.1"));

    /**
     * IP ranges to scan; this and {@link #ipAddresses} are two ways of writing the same thing.
     *
     * <p>It accepts {@code 192.168.0.1-192.168.5.254}, {@code 192.168.1.0/24}, and a shorthand
     * giving only the last octet. Its advantage is that <b>a new machine needs no configuration
     * change at all</b>.
     */
    private List<String> ipAddressRange = new ArrayList<>();

    /** Advanced configuration; leaving it empty is fine. See {@link Advanced}. */
    private Advanced advanced = new Advanced();

    /** The strategy for choosing a target member for a unicast. */
    private Balance loadBalancer = Balance.ROUND_ROBIN;

    /** The load-balancing strategies selectable from a configuration file. */
    public enum Balance {
        /** Round-robin, the default. The most even distribution. */
        ROUND_ROBIN,
        /** Random. */
        RANDOM,
        /** By the key's hash: one key always lands on one node, which suits stickiness. */
        HASH
    }

    /**
     * Application metadata carried along with gossip, readable by other nodes in the member
     * information.
     *
     * <p>In a web application the framework adds this node's web address to it once the web
     * server has bound -- {@code server.port}, {@code management.port} and, where they are
     * not at the root, the context path, the servlet path and the actuator's base path. See
     * {@code WebAddressMetadataListener}. Only the actual bound port is worth publishing, so
     * those keys cannot be written here by hand; anything else can.
     */
    private Map<String, String> metadata = new LinkedHashMap<>();

    /**
     * Whether startup blocks until discovery finishes.
     *
     * <p>On by default, so that the member list is <b>usable the moment the application is
     * up</b> -- otherwise {@code cluster.members()} may hold only this node for the first few
     * seconds, and "it behaves wrongly just after starting and is fine a few seconds later" is
     * the hardest kind of problem to track down.
     */
    private boolean awaitJoinOnStartup = true;

    /** The upper bound on that wait, in seconds. Discovery usually finishes within a second;
     *  this is only a backstop. */
    private long awaitJoinTimeoutSeconds = 30;

    /**
     * Advanced configuration: {@code spring.spreader.advanced.*}.
     *
     * <h2>Why it is a level of its own</h2>
     * What is above are the things <b>a deployment really has to fill in</b>: the cluster name,
     * the port, where to look for peers. What is here is <b>for tuning and adapting</b> --
     * empty means defaults calibrated by measurement, and the great majority of projects never
     * touch one from launch to retirement.
     *
     * <p>Separated, the first thing a configuration file shows is the handful of decisions that
     * must be made, rather than thirty knobs of uncertain purpose.
     *
     * <h2>When this section is worth reading</h2>
     * <ul>
     *   <li>A firewall requires fixed ports -- {@code bind-port} and {@code work-port-*}</li>
     *   <li>An unsteady network is having nodes judged departed wrongly -- raise
     *       {@code suspect-timeout-ms}</li>
     *   <li>Very many members, and the member list will not fit in UDP --
     *       {@code transport-type=TCP}, which is the default anyway</li>
     *   <li>A load test finds the bottleneck in the message layer -- {@code payload-*} and
     *       {@code pool-*}</li>
     * </ul>
     */
    @Data
    public static class Advanced {

        /**
         * This node's work port. <b>Leave it empty</b>; by default one is chosen at random
         * between 50000 and 60000.
         *
         * <p>It is only this node's own communication address, propagated with the member list,
         * and nobody needs to know it beforehand. Setting it explicitly is usually necessary
         * only where a firewall requires a fixed port.
         */
        private Integer bindPort;

        /** The lower bound for a random work port. */
        private Integer workPortMin;

        /** The upper bound for a random work port. */
        private Integer workPortMax;

        /** With {@link #bindPort} set explicitly, how many times to step forward when the
         *  port is taken. */
        private Integer portAutoIncrementRetry;

        /**
         * The transport protocol: TCP, the default, or UDP.
         *
         * <p>UDP has no connection setup, which saves a great deal in a large cluster; a
         * message larger than one datagram is fragmented and reassembled automatically, so
         * many members still fit. The cost is that a lost packet is only made good by gossip's
         * periodic repetition.
         */
        private TransportType transportType;

        /**
         * Who moves the bytes: NIO -- the default, built in, with no dependency -- NETTY,
         * MINA or GRIZZLY.
         *
         * <p><b>The default is the best choice</b>, and there is no reason to change it unless
         * you are comparing them under load. The built-in implementation is protocol-identical
         * to the other three -- the same frame format, the same port contention, the same
         * request/response semantics -- and nodes on different implementations can even share
         * one cluster. So changing implementation changes no behaviour, only performance
         * characteristics.
         *
         * <p>Another choice means <b>adding the corresponding jar yourself</b>, since they are
         * optional in spreader and are not brought in transitively: NETTY wants
         * {@code io.netty:netty-all}, MINA wants {@code org.apache.mina:mina-core}, and
         * GRIZZLY wants {@code org.glassfish.grizzly:grizzly-framework}. Configured without the
         * jar, it falls back to NIO with a warning rather than failing to start.
         */
        private TransportProvider transportProvider;

        /** The timeout for scanning one address, in milliseconds. */
        private Integer scanTimeoutMs;

        /** The scanning concurrency. */
        private Integer scanConcurrency;

        /** The gossip interval, in milliseconds. */
        private Long gossipIntervalMs;

        /**
         * How long a suspected node stays suspect before it is judged departed, in
         * milliseconds.
         *
         * <p><b>This package lowers it from the component's default of 5000 to 3000</b>: three
         * seconds comfortably outlasts an ordinary GC pause while finding "the process holding
         * the lock has crashed" sooner -- locks, latches and barriers all release their
         * resources on that event. An unsteady network wants it raised again, or nodes are
         * judged departed wrongly and often.
         */
        private Long suspectTimeoutMs = 3_000L;

        /** How long one request waits for its response, in milliseconds. A business message
         *  waiting for an ACK uses it too. */
        private Integer probeTimeoutMs;

        /** The connect timeout, in milliseconds. */
        private Integer connectTimeoutMs;

        /**
         * The quiet period before a leader is announced, in milliseconds.
         *
         * <p><b>This package lowers it from the component's default of 3000 to 2000.</b> It
         * affects the total failover time: with these parameters, takeover was measured at 3.3
         * to 4.4 seconds, and the default 8000 of
         * {@code spring.spreader.multiprocessing.cache.request-timeout-ms} was chosen to leave
         * headroom above that. <b>Change this one, and review that one alongside it.</b>
         */
        private Long leaderQuietPeriodMs = 2_000L;

        /**
         * The stagger unit for contending for the cluster port, in milliseconds: the nth node
         * in the takeover order waits {@code n} times this value before trying.
         *
         * <p>With the stagger, the first in the order normally takes it outright and the rest
         * never even try.
         */
        private Long takeoverDelayMs;

        /** How often a node already in a cluster probes the cluster port again, in
         *  milliseconds, so that split sub-clusters can be merged. */
        private Long rediscoverIntervalMs;

        /** How often an isolated node starts full discovery again, in milliseconds. */
        private Long aloneRediscoverIntervalMs;

        /**
         * The takeover priority; <b>a lower number takes precedence</b> in becoming leader.
         *
         * <p>Lower it on the better-provisioned machines to have them lead first. Everything
         * defaults to 0, in which case start time and other factors decide the order.
         */
        private int priority = 0;

        /**
         * Whether observability data is collected; <b>on</b> by default.
         *
         * <p>Collection is lock-free, and what one send or receive costs extra is a few atomic
         * additions -- negligible. The switch exists for cases that pursue latency to the
         * extreme: switched off, the empty methods are eliminated entirely by the JIT.
         *
         * <p>Note that this switches off <b>collection itself</b>. To switch off only the
         * Prometheus export and the {@code /actuator/spreader} endpoint, use
         * {@code spring.spreader.metrics.enabled=false}, which keeps the data and merely stops
         * exposing it.
         */
        private Boolean metricsEnabled;

        /**
         * Whether business messages require an acknowledgement.
         *
         * <p>true, the default, waits for the peer's ACK and resends without one; false is fire
         * and forget, far quicker but with no delivery guarantee. The receiving side
         * de-duplicates on "sender plus sequence", so a resend never delivers the same message
         * twice to the application.
         *
         * <p>The measured difference is large: single-threaded unicast reaches about 2600 a
         * second with ACKs and about 20,000 without. But the cache's replication stream is sent
         * <b>in batches</b>, hundreds per frame, and reaches 150,000 a second with ACKs on --
         * so <b>do not switch it off for performance</b> before confirming the bottleneck is
         * really here.
         */
        private Boolean payloadAck;

        /** How many times to resend without an acknowledgement before discarding. 0 means no
         *  resend. */
        private Integer payloadRetries;

        /** The interval between resends, in milliseconds. */
        private Long payloadRetryDelayMs;

        /** How long the receiving side keeps de-duplication records, in milliseconds. It
         *  <b>must cover the whole resend cycle</b>, and startup validates this. */
        private Long payloadDedupTtlMs;

        /** The maximum number of de-duplication records. */
        private Integer payloadDedupMaxEntries;

        /** The sending concurrency for a multicast. */
        private Integer payloadConcurrency;

        /**
         * How many threads dispatch business messages.
         *
         * <p>1 by default, that is serial dispatch. Raised, messages on one channel are handled
         * concurrently and <b>order is no longer guaranteed</b> -- the cache's replication
         * stream depends on order and has an ordering mechanism of its own, but your own
         * {@code onPayload} must tolerate reordering before this is raised.
         */
        private Integer payloadDispatchThreads;

        /**
         * Whether direct buffers are used for sending and receiving. Off by default.
         *
         * <p>A pure optimisation, with no difference in behaviour. It is off by default because
         * on <b>small messages</b> it is usually slower: this transport's public API sends and
         * receives {@code byte[]}, and the copy direct buffers save is paid back at the "off
         * heap to heap" step.
         *
         * <p>It is worth switching on for <b>large messages at high frequency</b>: the larger
         * the datagram, the more that one copy costs in absolute terms, and the fixed cost of
         * off-heap allocation is spread thinner. Measure before switching it on, and leave
         * {@code -XX:MaxDirectMemorySize} enough headroom -- off-heap memory is not bounded by
         * the heap limit, a leak triggers no GC, and the process is simply killed by the
         * system.
         */
        private Boolean directBuffers;

        /**
         * Whether outbound TCP connections are reused. On by default.
         *
         * <p>Switched off, it returns to one connection per message -- measured to hold
         * sustained throughput at a few hundred a second, and leaving a great many TIME_WAITs
         * behind. <b>There is no reason to switch it off.</b>
         */
        private Boolean connectionPoolEnabled;

        /** How many idle connections are cached per target address. */
        private Integer poolMaxIdlePerHost;

        /**
         * How long a pooled connection may sit idle, in milliseconds. It <b>must be shorter
         * than the server's 60000ms idle reclaim time</b>, or the pool fills with connections
         * the peer has already closed.
         */
        private Long poolIdleTimeoutMs;
    }
}
