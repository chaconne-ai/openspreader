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

import com.chaconneai.openspreader.cache.CacheOptions;
import com.chaconneai.openspreader.cache.CachePersistence;
import com.chaconneai.openspreader.event.MultiProcessingEventBridge;
import com.chaconneai.openspreader.cache.CacheService;
import java.nio.file.Path;
import com.chaconneai.openspreader.cache.ProcessingCache;
import com.chaconneai.openspreader.cache.MultiProcessingCache;
import com.chaconneai.openspreader.sync.BarrierService;
import com.chaconneai.openspreader.sync.LatchService;
import com.chaconneai.openspreader.sync.MutexService;
import com.chaconneai.openspreader.sync.MultiProcessingSyncService;
import com.chaconneai.openspreader.sync.ProcessingSyncService;
import com.chaconneai.openspreader.pooling.MultiProcessingPool;
import com.chaconneai.openspreader.pooling.ForkJoinMultiProcessingPool;
import com.chaconneai.openspreader.pooling.MethodRegistry;
import com.chaconneai.openspreader.pooling.MultiProcessingAdvisor;
import com.chaconneai.openspreader.pooling.MultiProcessingInterceptor;
import com.chaconneai.openspreader.pooling.PoolService;
import com.chaconneai.openspreader.serialization.ObjectCodec;
import com.chaconneai.openspreader.serialization.ObjectCodecs;
import com.chaconneai.openspreader.scheduling.MultiProcessingSchedulingConfigurer;
import com.chaconneai.openspreader.scheduling.MultiProcessingTaskStats;
import com.chaconneai.openspreader.sync.SemaphoreService;
import com.chaconneai.openspreader.aggregation.MapReduceJob;
import com.chaconneai.openspreader.aggregation.MapReduceService;
import com.chaconneai.openspreader.aggregation.MultiProcessingMapReduce;
import com.chaconneai.openspreader.aggregation.ProcessingMapReduce;
import com.chaconneai.spreader.GossipCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.Map;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import com.chaconneai.openspreader.concurrent.ExecutorServiceHolder;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.scheduling.TaskScheduler;

/**
 * Auto-configuration for the multi-processing toolkit: locks, semaphores, latches, barriers,
 * scheduled-task exclusion, the process pool, and the cluster cache.
 *
 * <p>All of it is built on {@link GossipCluster}, so it comes after
 * {@link ApplicationClusterAutoConfiguration}, which brings the cluster up. Without a cluster
 * ({@code spring.spreader.enabled=false}) not one bean here is created.
 *
 * <p>Each tool can be switched off on its own; see {@link MultiProcessingProperties}.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@AutoConfiguration(after = ApplicationClusterAutoConfiguration.class)
@EnableConfigurationProperties(MultiProcessingProperties.class)
@ConditionalOnBean(GossipCluster.class)
public class MultiProcessingAutoConfiguration {

    private static final Logger log =
            LoggerFactory.getLogger(MultiProcessingAutoConfiguration.class);

    /**
     * The single place every thread pool is maintained.
     *
     * <p>This was once three beans -- two schedulers and a holder -- with each component
     * additionally creating an executor of its own. It is now one: the container creates it,
     * the container closes it, and each component takes what it needs.
     *
     * <p>The pools are <b>created on demand</b>, so a component that is switched off does not
     * occupy a resident thread pool for nothing; see {@link ExecutorServiceHolder}. The bean
     * itself is therefore very light, and when it is created makes no difference.
     */
    @Bean
    @ConditionalOnMissingBean
    public ExecutorServiceHolder executorServiceHolder(MultiProcessingProperties props) {
        return new ExecutorServiceHolder(
                props.getPooling().getParallelism(),
                props.getCache().getInboundThreads(),
                props.getRpc().getInboundThreads(),
                props.getRpc().getInboundQueueCapacity());
    }

    /**
     * The lock service.
     *
     * <p>{@code destroyMethod = "close"} returns every held lock as the container shuts down,
     * so nobody has to idle out a whole lease.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "spring.spreader.multiprocessing.mutex", name = "enabled",
            havingValue = "true", matchIfMissing = false)
    public MutexService mutexService(GossipCluster cluster, MultiProcessingProperties props,
            ExecutorServiceHolder executors) {
        MultiProcessingProperties.Mutex m = props.getMutex();
        MutexService service = new MutexService(
                cluster, m.getLeaseMs(), m.getRequestTimeoutMs(), m.getRetryIntervalMs(),
                executors);
        service.start();
        return service;
    }

    /**
     * The semaphore service. Entirely independent of locks: its own channel, its own register,
     * and switching one off does not affect the other.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "spring.spreader.multiprocessing.semaphore", name = "enabled",
            havingValue = "true", matchIfMissing = false)
    public SemaphoreService semaphoreService(GossipCluster cluster, MultiProcessingProperties props,
            ExecutorServiceHolder executors) {
        MultiProcessingProperties.Semaphore s = props.getSemaphore();
        SemaphoreService service = new SemaphoreService(
                cluster, s.getLeaseMs(), s.getRequestTimeoutMs(), s.getRetryIntervalMs(),
                executors);
        service.start();
        return service;
    }

    // ------------------------------------------------------------------
    // Cross-process latches and barriers
    // ------------------------------------------------------------------

    /**
     * The latch service.
     *
     * <p>Its channel and register are its own, separate from locks and semaphores, and
     * switching any one of them off does not affect the others.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "spring.spreader.multiprocessing.latch", name = "enabled",
            havingValue = "true", matchIfMissing = false)
    public LatchService latchService(GossipCluster cluster, MultiProcessingProperties props,
            ExecutorServiceHolder executors) {
        MultiProcessingProperties.Latch l = props.getLatch();
        LatchService service = new LatchService(cluster, l.getRequestTimeoutMs(), l.getIdleTimeoutMs(), executors);
        service.start();
        return service;
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "spring.spreader.multiprocessing.barrier", name = "enabled",
            havingValue = "true", matchIfMissing = false)
    public BarrierService barrierService(GossipCluster cluster, MultiProcessingProperties props,
            ExecutorServiceHolder executors) {
        MultiProcessingProperties.Barrier b = props.getBarrier();
        BarrierService service = new BarrierService(cluster, b.getRequestTimeoutMs(), b.getIdleTimeoutMs(), executors);
        service.start();
        return service;
    }

    /**
     * The single entry point for locks, latches and barriers.
     *
     * <p>The four services have <b>switches of their own</b>, so they are injected through
     * {@code ObjectProvider} rather than directly: switching locks on without latches is a
     * perfectly ordinary usage, and direct injection would fail the whole configuration for a
     * missing bean. An absent one is passed as {@code null}, and
     * {@link ProcessingSyncService} reports "enable this property" when it is actually
     * used.
     */
    @Bean
    @ConditionalOnMissingBean
    public ProcessingSyncService syncs(
            ObjectProvider<MutexService> mutexService,
            ObjectProvider<LatchService> latchService,
            ObjectProvider<BarrierService> barrierService,
            ObjectProvider<SemaphoreService> semaphoreService,
            GossipCluster cluster) {
        return new MultiProcessingSyncService(mutexService.getIfAvailable(), latchService.getIfAvailable(),
                barrierService.getIfAvailable(), semaphoreService.getIfAvailable(), cluster);
    }

    /**
     * Carries Spring application events between processes. Off by default.
     *
     * <p>Switched off, this bean does not exist at all and events keep Spring's original in-JVM
     * semantics -- which is to say that without this switch, adding the starter changes nothing
     * about how events behave.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "spring.spreader.multiprocessing.event", name = "enabled",
            havingValue = "true", matchIfMissing = false)
    public MultiProcessingEventBridge multiProcessingEventBridge(GossipCluster cluster,
                                                         MultiProcessingProperties props, ObjectCodec codec) {
        MultiProcessingProperties.Event e = props.getEvent();
        MultiProcessingEventBridge bridge = new MultiProcessingEventBridge(
                cluster, codec, cluster.self().name(), e.isClusterWide());
        bridge.start();
        return bridge;
    }

    /**
     * Distributed aggregation (MapReduce). Off by default.
     *
     * <p>Switched off, neither bean exists, and even
     * {@link ExecutorServiceHolder#forMapReduceInbound()}'s thread pool is never created --
     * those pools are <b>built lazily on demand</b> and do not exist until something asks for
     * one. So without this switch, it costs the process nothing in threads, memory, or
     * dashboard clutter.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "spring.spreader.multiprocessing.aggregation",
            name = "enabled", havingValue = "true", matchIfMissing = false)
    public MapReduceService mapReduceService(GossipCluster cluster, ApplicationContext context,
            ObjectCodec codec, MultiProcessingProperties props, ExecutorServiceHolder executors) {
        // Every job collected by type, the bean name being the name submit() uses.
        // Having none still configures normally -- only some nodes in a cluster may define
        // jobs, and those that do not must still be able to receive a MAP_TASK and answer
        // clearly that this node has no such job
        Map<String, MapReduceJob<?, ?, ?, ?>> jobs = collectJobs(context);
        MapReduceService service = new MapReduceService(cluster, jobs, codec,
                props.getAggregation().getRequestTimeoutMs(), executors);
        service.start();
        return service;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Map<String, MapReduceJob<?, ?, ?, ?>> collectJobs(ApplicationContext context) {
        return (Map) context.getBeansOfType(MapReduceJob.class);
    }

    /**
     * The facade injected into application code.
     *
     * <p>It is separate from the service so that the application-facing interface has <b>three
     * methods</b>: the service also carries message handling and the state machines for three
     * roles, and none of that belongs in an application developer's autocomplete.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(MapReduceService.class)
    public ProcessingMapReduce processingMapReduce(MapReduceService service) {
        return new MultiProcessingMapReduce(service);
    }

    @Bean
    @ConditionalOnMissingBean
    public MultiProcessingTaskStats clusterTaskStats() {
        return new MultiProcessingTaskStats();
    }

    /**
     * Cluster-wide exclusion for scheduled tasks.
     *
     * <p>It registers one {@code SchedulingConfigurer} and <b>no TaskScheduler bean</b>, so the
     * application's own scheduler is untouched and anything injecting one elsewhere still gets
     * the original.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(MutexService.class)
    @ConditionalOnProperty(prefix = "spring.spreader.multiprocessing.scheduling", name = "enabled",
            havingValue = "true", matchIfMissing = false)
    public MultiProcessingSchedulingConfigurer multiProcessingSchedulingConfigurer(
            ProcessingSyncService syncs,
            MultiProcessingTaskStats stats,
            ObjectProvider<TaskScheduler> schedulerProvider,
            MultiProcessingProperties props) {
        MultiProcessingProperties.Scheduling s = props.getScheduling();
        return new MultiProcessingSchedulingConfigurer(syncs, stats, schedulerProvider,
                s.isApplyToAll(), s.getDefaultScope());
    }

    // ------------------------------------------------------------------
    // Cross-process task dispatch
    // ------------------------------------------------------------------

    /**
     * The allow-list of remotely callable methods.
     *
     * <p><b>It does not follow pooling's switch</b>: task dispatch and RPC share this one
     * allow-list, with {@code @MultiProcessingCall} and {@code @RpcClient} registering in the
     * same place. It was once tied to {@code pooling.enabled}, so switching off task dispatch
     * <b>silently killed RPC along with it</b> -- {@code RpcService} is
     * {@code @ConditionalOnBean(MethodRegistry.class)} and is not created without its
     * dependency, so what got reported was "no RpcService bean" while the user's configuration
     * had not switched RPC off at all.
     *
     * <p>It is only an empty registry in itself, starting no threads and holding no resources,
     * so creating it unconditionally is the right thing.
     *
     * <p>The {@code static} is required: a BeanPostProcessor must be in place before other
     * beans initialise, and a non-static @Bean method would instantiate the whole configuration
     * class early, dragging what it depends on into existence with it.
     */
    @Bean
    @ConditionalOnMissingBean
    public static MethodRegistry multiProcessingMethodRegistry() {
        return new MethodRegistry();
    }

    /**
     * Object serialisation, shared by task dispatch, RPC and the cache.
     *
     * <p>JDK by default, which adds no dependency; switching to KRYO needs only a change to
     * {@code spring.spreader.multiprocessing.serialization} and the corresponding library on
     * the classpath. Both are byte streams carrying their own type information, so the choice
     * is transparent to everything above. To implement one entirely -- Protobuf, Avro, and so
     * on -- define a bean of the same type and it replaces this one.
     */
    @Bean
    @ConditionalOnMissingBean
    public ObjectCodec objectCodec(MultiProcessingProperties props) {
        return ObjectCodecs.create(props.getSerialization());
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnBean(MethodRegistry.class)
    @ConditionalOnProperty(prefix = "spring.spreader.multiprocessing.pooling", name = "enabled",
            havingValue = "true", matchIfMissing = false)
    public PoolService poolService(GossipCluster cluster, MethodRegistry registry,
                                   ObjectCodec codec, MultiProcessingProperties props,
                                   ExecutorServiceHolder executors) {
        MultiProcessingProperties.Pooling p = props.getPooling();
        PoolService service = new PoolService(cluster, registry, codec,
                p.getRequestTimeoutMs(), p.getMaxDepth(), executors);
        service.start();
        return service;
    }

    /**
     * What actually makes the {@code @MultiProcessingCall} annotation take effect.
     *
     * <p>Without it the annotation is only the called side's allow-list; with it, a local call
     * to an annotated method goes through the process pool as well -- which is what makes "this
     * method may run in another process" true.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(PoolService.class)
    public MultiProcessingInterceptor multiProcessingInterceptor(
            PoolService service, MethodRegistry registry) {
        return new MultiProcessingInterceptor(service, registry);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(MultiProcessingInterceptor.class)
    public MultiProcessingAdvisor multiProcessingAdvisor(MultiProcessingInterceptor interceptor) {
        return new MultiProcessingAdvisor(interceptor);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(PoolService.class)
    public MultiProcessingPool defaultProcessingPool(PoolService service) {
        return new MultiProcessingPool(service);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(PoolService.class)
    public ForkJoinMultiProcessingPool forkJoinProcessingPool(PoolService service) {
        return new ForkJoinMultiProcessingPool(service);
    }

    // ------------------------------------------------------------------
    // The cluster cache
    // ------------------------------------------------------------------

    /**
     * The cache's replication service.
     *
     * <p>Its channel is its own, separate from locks and semaphores and affecting neither;
     * switching any one of them off leaves the others working as usual.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "spring.spreader.multiprocessing.cache", name = "enabled",
            havingValue = "true", matchIfMissing = false)
    public CacheService cacheService(GossipCluster cluster, MultiProcessingProperties props,
            ExecutorServiceHolder executors) {
        MultiProcessingProperties.Cache c = props.getCache();
        CacheOptions options = new CacheOptions(
                c.getApplicationName(), c.getRequestTimeoutMs(), c.getRetryIntervalMs(),
                c.getGapTimeoutMs(), c.getSweepIntervalMs(), c.getSnapshotTimeoutMs(),
                c.getMaintenanceIntervalMs(), c.getSnapshotChunkBytes(), c.getMaxBatchSize(),
                c.getMaxBatchBytes(), c.getOutboxCapacity(), c.getInboundThreads(),
                c.getMaxKeys(), c.getMaxBytes(), c.getEvictionPolicy(), c.getEvictionSamples(),
                c.getEvictionBatch(), c.getAccessReportIntervalMs(),
                c.getAccessReportSampleRate(), c.getAccessReportMaxKeys());
        CachePersistence persistence = null;
        if (c.isPersistent()) {
            // Without a configured path it lands in ~/.spreader/cache. user.home rather than
            // the working directory: started as a service -- systemd, a container entrypoint --
            // the working directory is often not what you assume, and the result is "it saved,
            // and next time it cannot be found"
            Path file = (c.getPersistentFile() == null || c.getPersistentFile().isBlank())
                    ? Path.of(System.getProperty("user.home"), ".spreader", "cache")
                    : Path.of(c.getPersistentFile());
            persistence = new CachePersistence(file,
                    LoggerFactory.getLogger(CachePersistence.class));
            log.info("Disk persistence enabled for the cluster cache at {}: loaded once at "
                    + "startup, written once at shutdown, and never written while running", file);
        }

        CacheService service = new CacheService(cluster, options, executors, persistence);
        service.start();
        return service;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(CacheService.class)
    public ProcessingCache clusterCache(CacheService service) {
        return new MultiProcessingCache(service);
    }
}
