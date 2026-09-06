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

import com.chaconneai.openspreader.cache.CacheService;
import com.chaconneai.openspreader.pooling.PoolService;
import com.chaconneai.openspreader.pooling.PoolStats;
import com.chaconneai.openspreader.scheduling.MultiProcessingTaskStats;
import com.chaconneai.openspreader.sync.MutexService;
import com.chaconneai.spreader.metrics.BufferMetrics;
import com.chaconneai.spreader.metrics.ChannelMetrics;
import com.chaconneai.spreader.metrics.LatencySnapshot;
import com.chaconneai.spreader.metrics.SplitBrainStatus;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code /actuator/spreader}: the observability endpoint that returns JSON directly.
 *
 * <h2>Who it is for</h2>
 * <ul>
 *   <li><b>Your own monitoring system</b> -- one pull gives every figure for this node,
 *       self-contained with its identity and a cluster overview, needing nothing else to piece
 *       together</li>
 *   <li><b>Troubleshooting</b> -- one curl shows it, with no Prometheus to stand up first</li>
 * </ul>
 *
 * <p>The Prometheus route is {@code /actuator/prometheus}: the same data in a different
 * format.
 *
 * <h2>The interface</h2>
 * <pre>
 * GET /actuator/spreader              a snapshot of the whole node
 * GET /actuator/spreader/{channel}    one channel; the default channel is "default"
 * </pre>
 *
 * <h2>The output has four layers, answering four different questions</h2>
 * <table border="1">
 *   <caption>Structure</caption>
 *   <tr><th>Section</th><th>Layer</th><th>Question answered</th></tr>
 *   <tr><td>{@code node}</td><td>Node identity</td>
 *       <td>Who am I, where am I, am I the leader</td></tr>
 *   <tr><td>{@code cluster}</td><td><b>Cluster level</b></td>
 *       <td>What is happening to the cluster as a whole -- split brain, still running,
 *           collection on or off</td></tr>
 *   <tr><td>{@code summary} / {@code channels} / {@code buffers}</td><td>Transport</td>
 *       <td>Message throughput, latency and buffer levels</td></tr>
 *   <tr><td>{@code components}</td><td><b>Component level</b></td>
 *       <td>How the cache, the locks, task dispatch and scheduled tasks are each doing</td></tr>
 * </table>
 *
 * <p>Keeping {@code cluster} and {@code node} apart is deliberate: troubleshooting asks two
 * different questions -- "what is wrong with <b>this machine</b>" and "what is wrong with
 * <b>the cluster</b>". Mixed together, a cluster-level fact such as a split brain gets read as
 * a property of this node.
 *
 * <h2>Why the maps are assembled by hand rather than returning records</h2>
 * A record serialised directly gives field names like {@code p99Nanos}, and <b>computed</b>
 * values such as {@code errorRate()} never appear in the JSON at all -- they are not fields.
 * Listing them explicitly means what the front end receives is ready to chart, with nothing to
 * recompute, and so nothing to get wrong.
 *
 * <p>Durations are converted to <b>milliseconds</b> along the way: nobody can read nanoseconds
 * on a dashboard.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
@Endpoint(id = "spreader")
public class SpreaderMetricsEndpoint {

    private final MetricsService metrics;

    /**
     * The sources of component-level statistics; all four are <b>optional</b>, each with a
     * switch of its own in the configuration.
     *
     * <p>Without them this endpoint reports the transport layer alone -- which is exactly how
     * it was: {@code /actuator/spreader} had channels and buffers only, showing nothing of how
     * much the cache had evicted or how heavily the locks were contended, so troubleshooting
     * meant going to {@code /actuator/prometheus} as well. Two outlets reporting different
     * things is the shape most likely to lead someone to a wrong conclusion.
     */
    private final CacheService cache;
    private final MutexService mutex;
    private final PoolService pool;
    private final MultiProcessingTaskStats scheduled;

    public SpreaderMetricsEndpoint(MetricsService metrics) {
        this(metrics, null, null, null, null);
    }

    public SpreaderMetricsEndpoint(MetricsService metrics, CacheService cache,
                                   MutexService mutex, PoolService pool,
                                   MultiProcessingTaskStats scheduled) {
        this.metrics = metrics;
        this.cache = cache;
        this.mutex = mutex;
        this.pool = pool;
        this.scheduled = scheduled;
    }

    /** A snapshot of the whole node. */
    @ReadOperation
    public Map<String, Object> snapshot() {
        NodeMetrics n = metrics.snapshot();

        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("clusterName", n.clusterName());
        node.put("nodeId", n.nodeId());
        node.put("nodeName", n.nodeName());
        node.put("address", n.address());
        node.put("leader", n.leader());
        node.put("leaderAddress", n.leaderAddress());
        // No business traffic while resting is normal rather than a fault, and a dashboard
        // has to tell the two apart
        node.put("onBreak", n.onBreak());
        node.put("memberCount", n.memberCount());
        node.put("uptimeMillis", n.uptimeMillis());
        out.put("node", node);

        // Cluster level: belonging to no single node, this says what state the cluster as a
        // whole is in. It sits apart from node above because troubleshooting asks two different
        // questions -- "what is wrong with this machine" and "what is wrong with the cluster"
        out.put("cluster", cluster());

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("totalTps", round(n.totalTps()));
        summary.put("errorRate", round(n.errorRate()));
        summary.put("channelCount", n.channels().size());
        // Business and framework channels are counted separately: framework channels, prefixed
        // spreader., are what the cache, the locks and RPC use for themselves, and their volume
        // is a different thing from business volume -- one combined number says nothing
        summary.put("businessChannelCount", n.businessChannels().size());
        summary.put("systemChannelCount", n.systemChannels().size());
        // Dropped messages happen silently, so a dashboard needs one conspicuous red light
        summary.put("hasDroppedMessages", n.hasDroppedMessages());
        // The fullest buffer is the next one to fill up. It is in the buffers list too, of
        // course, but that means comparing them one by one -- and with overload approaching,
        // this line should come for free
        BufferMetrics hottest = n.hottestBuffer();
        if (hottest != null) {
            Map<String, Object> h = new LinkedHashMap<>();
            h.put("name", hottest.name());
            h.put("usage", round(hottest.usage()));
            summary.put("hottestBuffer", h);
        }
        out.put("summary", summary);

        Map<String, Object> channels = new LinkedHashMap<>();
        n.channels().forEach((name, m) -> channels.put(displayName(name), toMap(m)));
        out.put("channels", channels);

        List<Map<String, Object>> buffers = new ArrayList<>();
        for (BufferMetrics b : n.buffers()) {
            buffers.add(toMap(b));
        }
        out.put("buffers", buffers);

        out.put("timestamp", n.timestamp());

        // Component level and transport level share one outlet. Reported separately,
        // troubleshooting would mean going back and forth between two endpoints whose sampling
        // instants differ besides
        Map<String, Object> components = components();
        if (!components.isEmpty()) {
            out.put("components", components);
        }
        return out;
    }

    /**
     * Cluster-level state: split brain, whether it is running, and whether collection is on.
     *
     * <h2>Why split brain has to be here and not in health alone</h2>
     * It used to appear only in {@code /actuator/health}, and <b>conditionally</b> at that --
     * where nothing had ever split, the fields did not exist. So {@code everSplit()}'s javadoc
     * said "alert on this" while no outlet handed the number over <b>dependably</b>: not
     * Prometheus, and not this endpoint.
     *
     * <p>A split brain is usually momentary, and once it heals {@code occurrences} is the only
     * trace left. A field that appears only when something is wrong cannot be alerted on -- a
     * monitoring system needs a number that is always there and normally 0.
     */
    private Map<String, Object> cluster() {
        SplitBrainStatus split = metrics.splitBrainStatus();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("running", metrics.isRunning());
        // With collection off, channels is empty, which looks exactly like "quiet, no
        // traffic". Without this boolean, a reader takes "nothing was measured" for "all is
        // well"
        out.put("metricsEnabled", metrics.isMetricsEnabled());

        Map<String, Object> sb = new LinkedHashMap<>();
        sb.put("healthy", split.healthy());
        sb.put("splitting", split.splitting());
        // Monotonic and normally 0 -- this is the number to alert on
        sb.put("occurrences", split.occurrences());
        sb.put("everSplit", split.everSplit());
        sb.put("leader", split.leader());
        // More than one means a split brain in progress. Note that this is what this node
        // sees: during a partition each side sees one leader and neither can tell -- as CAP
        // dictates
        sb.put("holders", split.holders());
        sb.put("lastDetectedAt", split.lastDetectedAt());
        // "How long it has been split" is worth more than "is it split": a few hundred
        // milliseconds is an ordinary race, and only minutes without healing mean the
        // stand-down mechanism has genuinely failed
        sb.put("splittingDurationMillis", split.splittingDurationMillis());
        out.put("splitBrain", sb);
        return out;
    }

    /** Buffer levels: queued, capacity, dropped. */
    private static Map<String, Object> toMap(BufferMetrics b) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("name", b.name());
        out.put("size", b.pending());
        out.put("capacity", b.capacity());
        out.put("remaining", b.remaining());
        out.put("usage", round(b.usage()));
        out.put("handled", b.handled());
        out.put("dropped", b.dropped());
        out.put("dropRate", round(b.dropRate()));
        // Whether anything was dropped is a yes-or-no question, and the caller should not have
        // to compare with 0. The top-level hasDroppedMessages is global; this one says which
        // buffer is dropping
        out.put("hasDropped", b.hasDropped());
        return out;
    }

    /**
     * Component-level statistics, <b>the same data</b> Micrometer reports.
     *
     * <p>A component that is not enabled simply does not appear, rather than reporting a set of
     * zeroes -- zero and "switched off" are different things, and mixing them reads as "working
     * fine, just no traffic".
     */
    private Map<String, Object> components() {
        Map<String, Object> out = new LinkedHashMap<>();
        if (cache != null) {
            out.put("cache", cache.stats());
        }
        if (mutex != null) {
            out.put("mutex", mutex.stats());
        }
        if (pool != null) {
            Map<String, Object> p = new LinkedHashMap<>(pool.stats());
            p.put("threadPool", poolThreads());
            out.put("pool", p);
        }
        if (scheduled != null) {
            out.put("scheduled", scheduled.snapshot());
        }
        return out;
    }

    /** Readings from the thread pool's point of view, shaped to match
     *  {@code ThreadPoolExecutor}. */
    private Map<String, Object> poolThreads() {
        PoolStats ps = pool.poolStats();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("activeCount", ps.activeCount());
        m.put("completedTaskCount", ps.completedTaskCount());
        m.put("queuedTaskCount", ps.queuedTaskCount());
        m.put("failureRate", ps.failureRate());
        Map<String, Object> local = new LinkedHashMap<>();
        local.put("active", ps.localActive());
        local.put("running", ps.localRunning());
        local.put("queued", ps.localQueued());
        local.put("submitted", ps.localSubmitted());
        local.put("completed", ps.localCompleted());
        local.put("failed", ps.localFailed());
        local.put("poolSize", ps.localPoolSize());
        local.put("parallelism", ps.localParallelism());
        local.put("steals", ps.localSteals());
        local.put("blockedRate", ps.localBlockedRate());
        m.put("local", local);
        Map<String, Object> inbound = new LinkedHashMap<>();
        inbound.put("active", ps.inboundActive());
        inbound.put("queued", ps.inboundQueued());
        inbound.put("completed", ps.inboundCompleted());
        inbound.put("poolSize", ps.inboundPoolSize());
        m.put("inbound", inbound);
        return m;
    }

    /** One channel. {@code default} means the user's default channel. */
    @ReadOperation
    public Map<String, Object> channel(@Selector String channel) {
        String key = "default".equals(channel) ? "" : channel;
        return toMap(metrics.channel(key));
    }

    private static Map<String, Object> toMap(ChannelMetrics m) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("channel", displayName(m.channel()));
        out.put("systemChannel", m.isSystemChannel());

        Map<String, Object> throughput = new LinkedHashMap<>();
        throughput.put("tps", round(m.tps()));
        throughput.put("sentTps", round(m.sentTps()));
        throughput.put("receivedTps", round(m.receivedTps()));
        throughput.put("peakTps", round(m.peakTps()));
        // Outbound and inbound peaks are reported separately. With only a total, "the outbound
        // side is saturated" and "the inbound side is saturated" are the same line on the chart
        // -- and the two overloads are investigated in entirely different places
        throughput.put("peakSentTps", round(m.peakSentTps()));
        throughput.put("peakReceivedTps", round(m.peakReceivedTps()));
        out.put("throughput", throughput);

        Map<String, Object> counters = new LinkedHashMap<>();
        counters.put("sent", m.sent());
        counters.put("sendFailures", m.sendFailures());
        counters.put("retries", m.retries());
        counters.put("received", m.received());
        counters.put("receiveFailures", m.receiveFailures());
        counters.put("duplicates", m.duplicates());
        out.put("counters", counters);

        Map<String, Object> concurrency = new LinkedHashMap<>();
        concurrency.put("current", m.inflight());
        concurrency.put("peak", m.peakInflight());
        out.put("concurrency", concurrency);

        Map<String, Object> rates = new LinkedHashMap<>();
        rates.put("errorRate", round(m.errorRate()));
        rates.put("sendErrorRate", round(m.sendErrorRate()));
        rates.put("receiveErrorRate", round(m.receiveErrorRate()));
        rates.put("retryRate", round(m.retryRate()));
        out.put("rates", rates);

        Map<String, Object> latency = new LinkedHashMap<>();
        latency.put("outbound", toMap(m.outboundLatency()));
        latency.put("inbound", toMap(m.inboundProcessing()));
        out.put("latencyMillis", latency);

        return out;
    }

    /** Durations are converted to milliseconds throughout -- nobody can read nanoseconds on a
     *  dashboard. */
    private static Map<String, Object> toMap(LatencySnapshot s) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("count", s.count());
        out.put("min", round(s.minMillis()));
        out.put("avg", round(s.avgMillis()));
        out.put("max", round(s.maxMillis()));
        out.put("p50", round(s.p50Millis()));
        out.put("p95", round(s.p95Millis()));
        out.put("p99", round(s.p99Millis()));
        return out;
    }

    private static String displayName(String channel) {
        return channel == null || channel.isEmpty() ? "default" : channel;
    }

    /** Three decimal places. A long floating-point tail in JSON is only a distraction. */
    private static double round(double v) {
        return Math.round(v * 1000d) / 1000d;
    }
}
