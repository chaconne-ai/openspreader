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
package com.chaconneai.openspreader;

import com.chaconneai.openspreader.cluster.MultiProcessingListenerRegistrar;
import com.chaconneai.openspreader.cluster.ApplicationClusterProperties;
import com.chaconneai.openspreader.cluster.WebAddressMetadataListener;
import com.chaconneai.spreader.GossipCluster;
import com.chaconneai.spreader.GossipConfig;
import com.chaconneai.spreader.Node;
import com.chaconneai.spreader.event.GossipListener;
import com.chaconneai.spreader.loadbalance.LoadBalancer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.server.context.WebServerInitializedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.concurrent.TimeUnit;

/**
 * Auto-configuration for the cluster itself: translates the Spring configuration into the
 * component's, then creates and starts a {@code GossipCluster}.
 *
 * <h2>The application writes nothing</h2>
 * Adding the dependency gives you a cluster with not one line of configuration -- discovery
 * defaults to {@code 127.0.0.1}, so several instances on one machine form a cluster
 * directly. Going to production means replacing
 * {@code spring.spreader.ip-addresses} with the real machine list.
 *
 * <p>To receive cluster events, an application writes a
 * {@code @Component implements GossipListener} and
 * {@link MultiProcessingListenerRegistrar} attaches it automatically.
 *
 * <h2>Why it comes before the toolkit</h2>
 * Services such as the lock and the cache all depend on {@code GossipCluster}, which has to
 * exist first. {@code @AutoConfiguration(before = ...)} guarantees that order, rather than
 * leaving it to the luck of bean definition sequence.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@AutoConfiguration(before = MultiProcessingAutoConfiguration.class)
@EnableConfigurationProperties(ApplicationClusterProperties.class)
@ConditionalOnProperty(prefix = "spring.spreader", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class ApplicationClusterAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ApplicationClusterAutoConfiguration.class);

    /** spreader's internal logging all hangs off this logger, so {@code logging.level}
     *  controls it. */
    private static final Logger GOSSIP_LOG = LoggerFactory.getLogger("com.chaconneai.spreader");

    /**
     * Creates and starts the cluster node.
     *
     * <p>{@code destroyMethod = "stop"} makes container shutdown a graceful departure: a
     * LEAVE is broadcast to every known member, so the others know at once rather than waiting
     * out the failure detection timeout. Without this step, every rolling deployment would
     * leave the cluster carrying members that are in fact gone for several seconds.
     *
     * <p>{@code @ConditionalOnMissingBean} leaves a way out: where the application defines a
     * {@code GossipCluster} bean of its own -- assembled by hand in a test, for instance --
     * this stays out of the way.
     */
    @Bean(destroyMethod = "stop")
    @ConditionalOnMissingBean
    public GossipCluster gossipCluster(ApplicationClusterProperties p, Environment env) throws IOException {
        GossipConfig config = buildConfig(p, env.getProperty("spring.application.name"));

        GossipCluster cluster = GossipCluster.create(config);
        cluster.start();
        log.info("Cluster node started: cluster={}, this node={}, clusterPort={}",
                cluster.clusterName(), cluster.self().label(), config.clusterPort());

        if (p.isAwaitJoinOnStartup()) {
            awaitJoin(cluster, p);
        }
        return cluster;
    }

    /**
     * Waits for discovery to finish.
     *
     * <p>So the member list is usable the moment the application is up. Without the wait,
     * {@code members()} may hold only this node for the first few seconds after startup --
     * and "it behaves wrongly just after starting and comes right a few seconds later" is
     * the hardest kind of problem to track down, well worth a second or two at startup.
     */
    private static void awaitJoin(GossipCluster cluster, ApplicationClusterProperties p) {
        try {
            if (!cluster.awaitJoin(p.getAwaitJoinTimeoutSeconds(), TimeUnit.SECONDS)) {
                log.warn("Timed out after {}s waiting to join the cluster; discovery continues in "
                                + "the background",
                        p.getAwaitJoinTimeoutSeconds());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Attaches the application's listeners to the default channel automatically.
     *
     * <p>Listeners are not resolved inside {@code gossipCluster()} because that would create
     * a circular dependency; see {@link MultiProcessingListenerRegistrar}.
     */
    @Bean
    @ConditionalOnMissingBean
    public MultiProcessingListenerRegistrar clusterListenerRegistrar(
            GossipCluster cluster, ObjectProvider<GossipListener> listeners) {
        return new MultiProcessingListenerRegistrar(cluster, listeners);
    }

    /**
     * Publishing this process's web address into the node metadata.
     *
     * <h2>Why it is a configuration of its own</h2>
     * The listener's signature names {@code WebServerInitializedEvent}, a class that lives in
     * {@code spring-boot-web-server} -- a module a non-web application does not have. Nested
     * inside a {@code @Configuration} carrying {@code @ConditionalOnClass}, the class is never
     * loaded where it is absent, and a batch job or a command-line tool starts exactly as
     * before.
     *
     * <p>{@code @ConditionalOnWebApplication} on top of that: the classes may well be present
     * -- a web starter on the classpath of an application deliberately started with
     * {@code WebApplicationType.NONE} -- and then no web server binds, no event fires, and a
     * listener would merely sit there.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(WebServerInitializedEvent.class)
    @ConditionalOnWebApplication
    static class WebAddressConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public WebAddressMetadataListener clusterWebAddressMetadataListener(
                GossipCluster cluster, Environment env) {
            return new WebAddressMetadataListener(cluster, env);
        }
    }

    // ------------------------------------------------------------------

    /**
     * Translates the Spring configuration into the component's.
     *
     * <p><b>Only explicitly configured values override the component's defaults</b> -- a
     * wrapper type left null is not touched at all, so adjusting a default inside the
     * component requires no change here.
     */
    private static GossipConfig buildConfig(ApplicationClusterProperties p, String springApplicationName) {
        GossipConfig.Builder b = GossipConfig.builder()
                .clusterName(p.getName())
                .nodeName(resolveApplicationName(p, springApplicationName))
                .bindHost(p.getBindHost())
                .clusterPort(p.getPort())
                .leaderEligible(p.isLeaderEligible())
                .priority(p.getAdvanced().getPriority())
                .loadBalancer(switch (p.getLoadBalancer()) {
                    case RANDOM -> LoadBalancer.random();
                    case HASH -> LoadBalancer.hash();
                    case ROUND_ROBIN -> LoadBalancer.roundRobin();
                })
                .metadata(p.getMetadata())
                .log(GOSSIP_LOG);

        if (p.getAdvertiseHost() != null && !p.getAdvertiseHost().isBlank()) {
            b.advertiseHost(p.getAdvertiseHost());
        }
        b.ipAddresses(p.getIpAddresses());
        b.ipAddressRange(p.getIpAddressRange());

        ApplicationClusterProperties.Advanced a = p.getAdvanced();
        applyIfPresent(a.getBindPort(), b::bindPort);
        applyIfPresent(a.getPortAutoIncrementRetry(), b::portAutoIncrementRetry);
        if (a.getWorkPortMin() != null && a.getWorkPortMax() != null) {
            b.workPortRange(a.getWorkPortMin(), a.getWorkPortMax());
        }
        applyIfPresent(a.getTransportType(), b::transportType);
        applyIfPresent(a.getTransportProvider(), b::transportProvider);
        applyIfPresent(a.getScanTimeoutMs(), b::scanTimeoutMs);
        applyIfPresent(a.getScanConcurrency(), b::scanConcurrency);
        applyIfPresent(a.getGossipIntervalMs(), b::gossipIntervalMs);
        applyIfPresent(a.getSuspectTimeoutMs(), b::suspectTimeoutMs);
        applyIfPresent(a.getLeaderQuietPeriodMs(), b::leaderQuietPeriodMs);
        applyIfPresent(a.getTakeoverDelayMs(), b::takeoverDelayMs);
        applyIfPresent(a.getRediscoverIntervalMs(), b::rediscoverIntervalMs);
        applyIfPresent(a.getAloneRediscoverIntervalMs(), b::aloneRediscoverIntervalMs);
        applyIfPresent(a.getProbeTimeoutMs(), b::probeTimeoutMs);
        applyIfPresent(a.getConnectTimeoutMs(), b::connectTimeoutMs);
        applyIfPresent(a.getMetricsEnabled(), b::metricsEnabled);
        applyIfPresent(a.getPayloadAck(), b::payloadAck);
        applyIfPresent(a.getPayloadRetries(), b::payloadRetries);
        applyIfPresent(a.getPayloadRetryDelayMs(), b::payloadRetryDelayMs);
        applyIfPresent(a.getPayloadDedupTtlMs(), b::payloadDedupTtlMs);
        applyIfPresent(a.getPayloadDedupMaxEntries(), b::payloadDedupMaxEntries);
        applyIfPresent(a.getPayloadConcurrency(), b::payloadConcurrency);
        applyIfPresent(a.getPayloadDispatchThreads(), b::payloadDispatchThreads);
        applyIfPresent(a.getDirectBuffers(), b::directBuffers);
        applyIfPresent(a.getConnectionPoolEnabled(), b::connectionPoolEnabled);
        applyIfPresent(a.getPoolMaxIdlePerHost(), b::poolMaxIdlePerHost);
        applyIfPresent(a.getPoolIdleTimeoutMs(), b::poolIdleTimeoutMs);

        try {
            return b.build();
        } catch (IllegalStateException e) {
            // Add some context, so the reader is not left with the component's internal error
            // and no idea which setting to change
            throw new IllegalStateException("something in spring.spreader.* is wrong: "
                    + e.getMessage(), e);
        }
    }

    /**
     * Where the application name comes from, in order:
     * {@code spring.spreader.application-name}, then {@code spring.application.name}, then
     * the component's default.
     *
     * <p>The overwhelming majority of projects configure nothing and use
     * {@code spring.application.name} directly.
     */
    private static String resolveApplicationName(ApplicationClusterProperties p, String springApplicationName) {
        if (p.getApplicationName() != null && !p.getApplicationName().isBlank()) {
            return p.getApplicationName().trim();
        }
        if (springApplicationName != null && !springApplicationName.isBlank()) {
            return springApplicationName.trim();
        }
        return Node.DEFAULT_NAME;
    }

    private static <T> void applyIfPresent(T value, Consumer<T> setter) {
        if (value != null) {
            setter.accept(value);
        }
    }
}
