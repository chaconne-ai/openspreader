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
import com.chaconneai.spreader.Node;
import com.chaconneai.spreader.metrics.BufferMetrics;
import com.chaconneai.spreader.metrics.ChannelMetrics;
import com.chaconneai.spreader.metrics.SplitBrainStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The entry point for monitoring data: <b>inject it and you have every metric</b>.
 *
 * <h2>Three ways to use it, one set of data</h2>
 * <ol>
 *   <li><b>Rendered by your own system</b> -- inject this bean, call {@link #snapshot()} for
 *       a self-contained {@link NodeMetrics}, and return it to the front end as JSON</li>
 *   <li><b>Prometheus and Grafana</b> -- {@code SpreaderMeterBinder} registers the same data
 *       as Micrometer gauges, served at {@code /actuator/prometheus}</li>
 *   <li><b>Diagnostics</b> -- read the JSON directly at {@code /actuator/spreader}</li>
 * </ol>
 *
 * <p>All three read the single collection inside spreader; <b>there are not two sets of
 * numbers</b>. That is deliberate: instrumenting twice inevitably produces two sets that
 * disagree, and nobody can say which to believe.
 *
 * <h2>It does not aggregate</h2>
 * It reports <b>this node's</b> data alone. Aggregating across nodes belongs to whatever
 * collects it -- Prometheus by job, or your own system by nodeId. Having every node pull the
 * whole cluster's data would be N-squared calls for N nodes, and every node would compute a
 * different answer anyway.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
public class MetricsService {

    private final GossipCluster cluster;
    private final ExecutorServiceHolder executors;
    private final long startedAt = System.currentTimeMillis();

    public MetricsService(GossipCluster cluster) {
        this(cluster, null);
    }

    public MetricsService(GossipCluster cluster, ExecutorServiceHolder executors) {
        this.cluster = cluster;
        this.executors = executors;
    }

    /**
     * A complete snapshot of this node.
     *
     * <p>Taken afresh on every call. The metrics themselves accumulate lock-free, so one call
     * costs a walk over the channels to summarise them -- a matter of microseconds, safe to
     * call directly inside an HTTP request.
     */
    public NodeMetrics snapshot() {
        Node self = cluster.self();
        Node leader = cluster.leader();
        return new NodeMetrics(
                cluster.clusterName(),
                self.id(),
                self.name(),
                self.address(),
                cluster.isLeader(),
                leader == null ? null : leader.address(),
                cluster.isOnBreak(),
                cluster.members().size(),
                System.currentTimeMillis() - startedAt,
                System.currentTimeMillis(),
                cluster.metrics(),
                buffers());
    }

    /**
     * The fill level of every inbound buffer, <b>from two sources combined</b>:
     *
     * <ul>
     *   <li>spreader's RingBuffers -- the event dispatch queue, and the components that use
     *       {@code BufferedGossipListener}: locks, semaphores, latches, barriers</li>
     *   <li>{@link ExecutorServiceHolder}'s bounded pool queues -- the cache, task dispatch
     *       and RPC take this path, since they do their work in pools of their own and cannot
     *       use those RingBuffers</li>
     * </ul>
     *
     * <p>The implementations differ, but to a user they are the same thing: <b>a full queue
     * means messages are gone</b>. Reporting them separately would only have people miss half
     * of it.
     */
    public List<BufferMetrics> buffers() {
        List<BufferMetrics> out = new ArrayList<>(cluster.bufferMetrics());
        if (executors != null) {
            out.addAll(executors.bufferMetrics());
        }
        return out;
    }

    /** Only the buffers that <b>have dropped something</b>. Alerting on this list being
     *  non-empty is enough. */
    public List<BufferMetrics> droppingBuffers() {
        return buffers().stream().filter(BufferMetrics::hasDropped).toList();
    }

    /**
     * The split-brain self-check result, <b>cluster level</b>.
     *
     * <h2>Why it needed an outlet here</h2>
     * Before this it appeared only in {@code /actuator/health}, and <b>conditionally</b> at
     * that: with {@code everSplit()} false, those fields did not appear at all. So
     * {@link com.chaconneai.spreader.metrics.SplitBrainStatus#everSplit()}
     * had a javadoc saying "alert on this" while <b>no outlet whatsoever</b> handed that
     * number to a monitoring system reliably.
     *
     * <p>It is the last thing that should have been missing: a split is usually over in a
     * flash, and once it heals {@code occurrences} is the only trace left.
     */
    public SplitBrainStatus splitBrainStatus() {
        return cluster.splitBrainStatus();
    }

    /**
     * Whether the underlying collection is on ({@code GossipConfig.metricsEnabled}).
     *
     * <h2>Why this boolean has to be reported</h2>
     * With collection off, {@link #channels()} returns an <b>empty map</b> -- indistinguishable
     * from a quiet cluster through which not one message has passed. A reader would conclude
     * that all is well and there is simply no traffic, when the truth is that <b>nothing is
     * being measured</b>.
     *
     * <p>The components have long observed this rule -- a component that is not enabled does
     * not appear, rather than reporting a row of zeros -- while the collection switch itself
     * went unreported.
     */
    public boolean isMetricsEnabled() {
        return cluster.config().metricsEnabled();
    }

    /** Whether the cluster is still running. Once stopped, every metric freezes at its last
     *  value, and not reporting this would leave a reader thinking it was still alive. */
    public boolean isRunning() {
        return cluster.isRunning();
    }

    /** Per channel, keyed by channel name. */
    public Map<String, ChannelMetrics> channels() {
        return cluster.metrics();
    }

    /** One channel. A channel with no traffic yields an all-zero snapshot rather than null. */
    public ChannelMetrics channel(String name) {
        return cluster.metrics(name);
    }

    /** The user's own business channels only. */
    public List<ChannelMetrics> businessChannels() {
        return snapshot().businessChannels();
    }

    /** The framework's channels only: cache, locks, RPC, task dispatch. */
    public List<ChannelMetrics> systemChannels() {
        return snapshot().systemChannels();
    }

    /**
     * Resets the statistics to zero.
     *
     * <p><b>Do not call it with Prometheus attached</b>: its model is a monotonically
     * increasing counter whose rate the query side computes, and a reset partway through makes
     * {@code rate()} spike negative. It is only useful where data is read as "since the last
     * collection".
     */
    public void reset() {
        cluster.resetMetrics();
    }
}
