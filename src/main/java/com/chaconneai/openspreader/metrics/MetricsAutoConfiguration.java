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
package com.chaconneai.openspreader.metrics;

import com.chaconneai.openspreader.concurrent.ExecutorServiceHolder;
import com.chaconneai.spreader.GossipCluster;
import com.chaconneai.openspreader.cache.CacheService;
import com.chaconneai.openspreader.pooling.PoolService;
import com.chaconneai.openspreader.scheduling.MultiProcessingTaskStats;
import com.chaconneai.openspreader.rpc.RpcService;
import com.chaconneai.openspreader.sync.BarrierService;
import com.chaconneai.openspreader.sync.ExchangerService;
import com.chaconneai.openspreader.sync.LatchService;
import com.chaconneai.openspreader.sync.MutexService;
import com.chaconneai.openspreader.sync.SemaphoreService;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Configuration for observability data: one collection, three outlets.
 *
 * <ol>
 *   <li>{@link MetricsService} -- <b>always configured</b>. Inject it and every metric is
 *       there, ready to render in a system of your own</li>
 *   <li>{@link SpreaderMeterBinder} -- configured only with Micrometer on the classpath, for
 *       Prometheus and Grafana</li>
 *   <li>{@code SpreaderMetricsEndpoint} -- configured only with actuator present, giving JSON
 *       at {@code /actuator/spreader}</li>
 * </ol>
 *
 * <p>All three read <b>the same</b> underlying collection; there are never two sets of
 * numbers.
 *
 * <p>The overall switch is {@code spring.spreader.metrics.enabled}, on by default. Switching
 * off the collection itself is a matter for the spreader layer's
 * {@code GossipConfig.metricsEnabled(false)} -- after which there is nothing here to
 * report.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
/*
 * This must come after Micrometer's auto-configuration.
 *
 * MicrometerBinding below uses @ConditionalOnBean(MeterRegistry.class), and @ConditionalOnBean
 * looks only at the bean definitions registered at the moment it is evaluated. Without an
 * order declared, this class may run before MetricsExportAutoConfiguration, when MeterRegistry
 * does not yet exist and the condition is false -- so every Micrometer metric goes
 * unregistered, silently, /actuator/prometheus is empty, and nothing in the log says anything.
 * That is exactly the symptom that was observed.
 */
@AutoConfiguration(afterName = {
        // afterName (strings) rather than after (classes): Micrometer is an optional
        // dependency, and a class reference would stop a project without it from compiling.
        // The string form is quietly ignored when the class is absent, which is exactly what
        // an optional dependency wants.
        //
        // Mind the package names -- Spring Boot 4.x moved these classes from
        // org.springframework.boot.actuate.autoconfigure.metrics to
        // org.springframework.boot.micrometer.metrics.autoconfigure. A wrong package name
        // reports nothing and merely voids the ordering, with the same symptom as omitting
        // it
        "org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.export.simple.SimpleMetricsExportAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.export.prometheus.PrometheusMetricsExportAutoConfiguration"
})
@EnableConfigurationProperties(MetricsProperties.class)
@ConditionalOnBean(GossipCluster.class)
@ConditionalOnProperty(prefix = "spring.spreader.metrics", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class MetricsAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MetricsAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    public MetricsService spreaderMetricsService(
            GossipCluster cluster,
            ObjectProvider<ExecutorServiceHolder> executors) {
        log.info("Cluster observability enabled: inject MetricsService for the data, or visit "
                + "/actuator/spreader");
        // ExecutorServiceHolder may be absent, with every multi-processing component switched
        // off, leaving only spreader's ring-buffer levels -- which works just as well
        return new MetricsService(cluster, executors.getIfAvailable());
    }

    /**
     * The {@code /actuator/spreader} endpoint.
     *
     * <p>For an external system to pull JSON from, and for one curl to show the whole picture
     * without standing up Prometheus first.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(Endpoint.class)
    static class EndpointConfiguration {

        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnAvailableEndpoint(endpoint = SpreaderMetricsEndpoint.class)
        public SpreaderMetricsEndpoint spreaderMetricsEndpoint(MetricsService metrics,
                ObjectProvider<CacheService> cache,
                ObjectProvider<MutexService> mutex,
                ObjectProvider<PoolService> pool,
                ObjectProvider<MultiProcessingTaskStats> scheduled) {
            return new SpreaderMetricsEndpoint(metrics, cache.getIfAvailable(),
                    mutex.getIfAvailable(), pool.getIfAvailable(), scheduled.getIfAvailable());
        }
        /**
         * A plain-text report for people to read: {@code /actuator/spreader-report}.
         *
         * <p>Its division of labour with {@code /actuator/prometheus} is clear -- that one is
         * 575 lines and 36KB, for Grafana to scrape; this one is for a glance while
         * troubleshooting, opening with whether anything is wrong rather than laying out 177
         * metrics.
         */
        @Bean
        @ConditionalOnMissingBean
        public SpreaderReportEndpoint spreaderReportEndpoint(MetricsService metrics,
                ObjectProvider<CacheService> cache,
                ObjectProvider<MutexService> mutex,
                ObjectProvider<PoolService> pool,
                ObjectProvider<MultiProcessingTaskStats> scheduled,
                ObjectProvider<LatchService> latch,
                ObjectProvider<BarrierService> barrier,
                ObjectProvider<SemaphoreService> semaphore,
                ObjectProvider<ExchangerService> exchanger,
                ObjectProvider<RpcService> rpc) {
            return new SpreaderReportEndpoint(metrics, cache.getIfAvailable(),
                    mutex.getIfAvailable(), pool.getIfAvailable(), scheduled.getIfAvailable(),
                    latch.getIfAvailable(), barrier.getIfAvailable(),
                    semaphore.getIfAvailable(), exchanger.getIfAvailable(),
                    rpc.getIfAvailable());
        }

        /**
         * {@code /actuator/spreader-break}: puts a node on a break, or calls it back to work.
         *
         * <p>This is the project's <b>only write operation</b> endpoint, and it needs no extra
         * switch: actuator exposes only {@code health} to the web by default, so unless it is
         * named in {@code management.endpoints.web.exposure.include} it does not reach the
         * network.
         *
         * <p>{@code @ConditionalOnAvailableEndpoint} is <b>not</b> used here, matching the
         * report endpoint next door -- the condition saves one object while tying "does the
         * bean exist" to "is the endpoint exposed", which makes it easy to reach a wrong
         * conclusion while troubleshooting. Whether it reaches the network is actuator's own
         * decision.
         */
        @Bean
        @ConditionalOnMissingBean
        public ApplicationClusterBreakEndpoint spreaderClusterBreakEndpoint(
                GossipCluster cluster) {
            log.info("Cluster break endpoint registered: POST/DELETE "
                    + "/actuator/spreader-break, not exposed by default. It is a write "
                    + "operation, so make sure the management port is authenticated before "
                    + "exposing it, or anyone can take the nodes out one by one while the "
                    + "cluster looks perfectly healthy");
            return new ApplicationClusterBreakEndpoint(cluster);
        }

        /**
         * The cluster's section of {@code /actuator/health}.
         *
         * <p>Cluster state is <b>the framework's</b> information, and having every application
         * rewrite a {@code /cluster/status} of its own makes no sense. Putting it in health
         * also connects it to the whole ecosystem for free: Kubernetes probes, load-balancer
         * health checks and alerting systems all understand this endpoint.
         */
        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnClass(HealthIndicator.class)
        public ApplicationClusterHealthIndicator spreaderClusterHealthIndicator(
                GossipCluster cluster, MetricsService metrics, MetricsProperties props) {
            log.info("Cluster health check registered at /actuator/health; a split brain "
                    + "unhealed for longer than {} reports DOWN",
                    props.getSplitBrainDownAfter());
            return new ApplicationClusterHealthIndicator(cluster, metrics, props);
        }
    }

    /**
     * The Micrometer binding, for Prometheus.
     *
     * <p>Configured only with Micrometer on the classpath -- it is an optional dependency, and
     * a project not using Prometheus should not be made to pull an observability library
     * in.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean(MeterRegistry.class)
    static class MicrometerBinding {

        @Bean
        @ConditionalOnMissingBean
        public SpreaderMeterBinder spreaderMeterBinder(MetricsService metrics) {
            log.info("Cluster observability bound to Micrometer: metrics are prefixed "
                    + "spreader.channel.* and grouped by the channel tag");
            return new SpreaderMeterBinder(metrics);
        }

        /**
         * Registers newly appeared channels periodically.
         *
         * <p>Channels appear at runtime -- a user may start a new one at any moment, and at
         * startup there are usually none at all. Without this, a new channel's metrics would
         * never appear in Prometheus, and nothing would report it.
         *
         * <p>Every ten seconds is enough: missing a collection interval or two does not affect
         * a trend, and the scan itself only walks a small map.
         */
        @Bean
        public ChannelRegistrationRefresher spreaderChannelRefresher(SpreaderMeterBinder binder) {
            return new ChannelRegistrationRefresher(binder);
        }

        /**
         * Component-level metrics: cache, locks, task dispatch, scheduled tasks, latches,
         * barriers, semaphores, exchange points and RPC.
         *
         * <p>The nine components have <b>switches of their own</b>, so they are injected
         * through {@code ObjectProvider} rather than directly: switching the cache on without
         * locks is a perfectly ordinary usage, and direct injection would stop the whole
         * application from starting over a missing bean. An absent one is passed as
         * {@code null} and its metrics are simply not registered -- rather than registering a
         * set of permanent zeroes, which is worse than nothing and reads as "measured, and
         * fine".
         */
        @Bean
        @ConditionalOnMissingBean
        public ComponentMeterBinder spreaderComponentMeterBinder(
                ObjectProvider<CacheService> cache,
                ObjectProvider<MutexService> mutex,
                ObjectProvider<PoolService> pool,
                ObjectProvider<MultiProcessingTaskStats> scheduled,
                ObjectProvider<LatchService> latch,
                ObjectProvider<BarrierService> barrier,
                ObjectProvider<SemaphoreService> semaphore,
                ObjectProvider<ExchangerService> exchanger,
                ObjectProvider<RpcService> rpc) {
            ComponentMeterBinder binder = new ComponentMeterBinder(
                    cache.getIfAvailable(), mutex.getIfAvailable(),
                    pool.getIfAvailable(), scheduled.getIfAvailable(),
                    latch.getIfAvailable(), barrier.getIfAvailable(),
                    semaphore.getIfAvailable(), exchanger.getIfAvailable(),
                    rpc.getIfAvailable());
            log.info("Component observability bound to Micrometer: spreader.cache.* / "
                    + "spreader.mutex.* / spreader.pool.* / spreader.scheduled.* / "
                    + "spreader.latch.* / spreader.barrier.* / spreader.semaphore.* / "
                    + "spreader.exchanger.* / spreader.rpc.*");
            return binder;
        }

        /**
         * Registers newly appeared scheduled tasks periodically.
         *
         * <p>For the same reason as channels: a task name is only registered on its first
         * firing and is unavailable at startup. Without this, a new task's metrics would never
         * appear, and nothing would report it.
         */
        @Bean
        public TaskRegistrationRefresher spreaderTaskRefresher(
                ComponentMeterBinder binder, MeterRegistry registry) {
            return new TaskRegistrationRefresher(binder, registry);
        }
    }

    /** Scans periodically for newly appeared scheduled tasks. */
    public static class TaskRegistrationRefresher {

        private final ComponentMeterBinder binder;
        private final MeterRegistry registry;

        TaskRegistrationRefresher(ComponentMeterBinder binder, MeterRegistry registry) {
            this.binder = binder;
            this.registry = registry;
        }

        @Scheduled(fixedDelay = 10_000L, initialDelay = 5_000L)
        public void refresh() {
            binder.bindKnownTasks(registry);
        }
    }

    /** Scans periodically for new channels. It is a class of its own so that
     *  {@code @Scheduled} can be proxied. */
    public static class ChannelRegistrationRefresher {

        private final SpreaderMeterBinder binder;

        ChannelRegistrationRefresher(SpreaderMeterBinder binder) {
            this.binder = binder;
        }

        @Scheduled(fixedDelay = 10_000L, initialDelay = 5_000L)
        public void refresh() {
            binder.bindKnownChannels();
        }
    }
}
