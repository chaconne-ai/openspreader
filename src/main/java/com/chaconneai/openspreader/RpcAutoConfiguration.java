package com.chaconneai.openspreader;

import com.chaconneai.openspreader.pooling.MethodRegistry;
import com.chaconneai.openspreader.rpc.RpcClientRegistrar;
import com.chaconneai.openspreader.rpc.RpcDefaults;
import com.chaconneai.openspreader.concurrent.ExecutorServiceHolder;
import com.chaconneai.openspreader.rpc.RpcService;
import com.chaconneai.openspreader.serialization.ObjectCodec;
import com.chaconneai.openspreader.serialization.ObjectCodecs;
import com.chaconneai.openspreader.serialization.SerializationType;
import com.chaconneai.spreader.GossipCluster;
import com.chaconneai.spreader.transport.TransportProvider;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.retry.support.RetryTemplate;

import java.util.List;

/**
 * Auto-configuration for cross-application remote calls ({@code @RpcClient}).
 *
 * <h2>Why it is a configuration class of its own</h2>
 * RPC needs spring-retry, which is {@code optional}. Placed inside
 * {@link MultiProcessingAutoConfiguration}, a project without spring-retry would find
 * <b>the entire multiprocessing toolkit failing to configure</b> -- taking locks, the cache
 * and task dispatch down with it. Split out, a missing library affects RPC alone.
 *
 * <p>{@link ConditionalOnClass} is where that isolation lands: without the
 * {@code RetryTemplate} class, this configuration is never even loaded.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@AutoConfiguration(after = MultiProcessingAutoConfiguration.class)
@ConditionalOnClass(RetryTemplate.class)
@ConditionalOnBean(GossipCluster.class)
@EnableConfigurationProperties(MultiProcessingProperties.class)
@ConditionalOnProperty(prefix = "spring.spreader.multiprocessing.rpc", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class RpcAutoConfiguration {

    /**
     * The communication core.
     *
     * <p>It shares {@link MethodRegistry} with task dispatch -- the two have the same
     * allow-list to begin with ({@code @MultiProcessingCall}) -- and shares
     * {@link ObjectCodec} as well.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnBean(MethodRegistry.class)
    public RpcService rpcService(GossipCluster cluster, MethodRegistry registry,
                                 ObjectCodec codec, MultiProcessingProperties props,
                                 ExecutorServiceHolder executors) {
        warnIfTransportMismatch(cluster, props);
        RpcService service = new RpcService(cluster, registry, rpcCodec(codec, props), executors);
        service.start();
        return service;
    }

    /**
     * Uses RPC's own serialisation when it is configured separately, and follows the global
     * setting otherwise.
     *
     * <p>When separate configuration makes sense: RPC carries heavy traffic and its DTOs all
     * have no-argument constructors, so RPC takes KRYO for throughput while everything else
     * stays on JDK for simplicity.
     *
     * <p>Configured the same as the global one, the same instance is reused rather than built
     * twice -- codecs are stateless, and the Kryo one carries an object pool, so a second
     * copy is pure waste.
     */
    private ObjectCodec rpcCodec(ObjectCodec global, MultiProcessingProperties props) {
        SerializationType own = props.getRpc().getSerialization();
        return own == null || own == global.type() ? global : ObjectCodecs.create(own);
    }

    /**
     * The global RPC defaults: timeout, retry count, retry interval.
     *
     * <p>An interface's {@code @RpcClient} overrides only the attributes it wants to change
     * and takes the rest from here -- set once globally, adjusted per interface.
     */
    @Bean
    @ConditionalOnMissingBean
    public RpcDefaults rpcDefaults(MultiProcessingProperties props) {
        MultiProcessingProperties.Rpc rpc = props.getRpc();
        long timeoutMs = rpc.getTimeout() <= 0
                ? 0L
                : rpc.getTimeUnit().toMillis(rpc.getTimeout());
        long retryIntervalMs = rpc.getRetryInterval() <= 0
                ? 0L
                : rpc.getTimeUnit().toMillis(rpc.getRetryInterval());
        long acquireTimeoutMs = rpc.getAcquireTimeout() <= 0
                ? 0L
                : rpc.getTimeUnit().toMillis(rpc.getAcquireTimeout());
        return new RpcDefaults(timeoutMs, Math.max(0, rpc.getMaxRetries()), retryIntervalMs,
                Math.max(0, rpc.getMaxConcurrent()), acquireTimeoutMs);
    }

    /**
     * Says something when RPC's configured transport does not match the one the cluster
     * actually uses.
     *
     * <p>RPC reuses the connections the cluster has already established and cannot change the
     * implementation -- but the setting sits there, and it is easy to assume changing it
     * takes effect. Left unsaid, that misunderstanding surfaces only when benchmark figures
     * fail to match expectations.
     */
    private void warnIfTransportMismatch(GossipCluster cluster,
                                         MultiProcessingProperties props) {
        TransportProvider expected = props.getRpc().getTransportProvider();
        if (expected == null) {
            return;
        }
        TransportProvider actual = cluster.config().transportProvider();
        if (expected != actual) {
            LoggerFactory.getLogger(RpcAutoConfiguration.class).warn(
                    "spring.spreader.multiprocessing.rpc.transport-provider is set to {}, but the "
                            + "cluster actually uses {}. RPC reuses the cluster's connections, so the "
                            + "latter is what takes effect. To change the implementation, set "
                            + "spring.spreader.advanced.transport-provider",
                    expected, actual);
        }
    }

    /**
     * Scans for {@code @RpcClient} interfaces and registers the proxies.
     *
     * <p>{@code static} is required: a {@code BeanDefinitionRegistryPostProcessor} has to run
     * before other bean definitions are in place, and a non-static {@code @Bean} method would
     * instantiate the whole configuration class -- along with everything it depends on --
     * prematurely, while the container is not yet ready.
     *
     * <p>Being static is also why nothing can be injected here, and the scan packages have to
     * be fetched from the {@code beanFactory} on the spot. Hence a {@link BeanFactory}
     * parameter rather than the specific things it needs.
     */
    @Bean
    @ConditionalOnMissingBean
    public static RpcClientRegistrar rpcClientRegistrar(BeanFactory beanFactory) {
        // The same scan scope as @SpringBootApplication: the main class's package and its
        // subpackages
        List<String> packages = AutoConfigurationPackages.has(beanFactory)
                ? AutoConfigurationPackages.get(beanFactory)
                : List.of();
        return new RpcClientRegistrar(packages);
    }
}
