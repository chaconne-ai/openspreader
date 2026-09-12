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
import com.chaconneai.openspreader.rpc.RpcService;
import com.chaconneai.openspreader.sync.BarrierService;
import com.chaconneai.openspreader.sync.ExchangerService;
import com.chaconneai.openspreader.sync.LatchService;
import com.chaconneai.openspreader.sync.SemaphoreService;
import com.chaconneai.openspreader.pooling.PoolStats;
import com.chaconneai.openspreader.scheduling.MultiProcessingTaskStats;
import com.chaconneai.openspreader.sync.MutexService;
import com.chaconneai.spreader.metrics.BufferMetrics;
import com.chaconneai.spreader.metrics.ChannelMetrics;
import com.chaconneai.spreader.metrics.SplitBrainStatus;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;

import java.util.List;
import java.util.Map;

/**
 * One page for <b>a person</b> to read: {@code /actuator/spreader-report}.
 *
 * <h2>Why not simply read the two endpoints that already exist</h2>
 * <ul>
 *   <li>{@code /actuator/prometheus} -- 575 lines and 36KB, for Grafana to scrape; finding one
 *       number by eye takes an age</li>
 *   <li>{@code /actuator/spreader} -- 4KB of JSON on <b>a single line</b>, which a browser can
 *       fold and a terminal curl cannot</li>
 * </ul>
 *
 * <p>This produces plain text, readable in a browser and under {@code curl} alike.
 *
 * <h2>It is not "list every metric"</h2>
 * Listing them all amounts to listing none -- what is actually useful is <b>saying first
 * whether anything is wrong</b>. So it opens with a health check that picks out the figures
 * where anything non-zero is an anomaly: split-brain occurrences, broadcast overflows, lock
 * acquisition timeouts, task fallbacks, buffer drops. Every one of these <b>makes no sound</b>
 * in the log, and is never found unless somebody looks.
 *
 * <p>The grouped details come afterwards, in the order troubleshooting reads them.
 *
 * <h2>It carries the same content as {@code /actuator/spreader}</h2>
 * The two endpoints <b>must report the same things</b> and differ only in rendering -- JSON
 * for machines, this for people. If one has what the other lacks, troubleshooting goes back
 * and forth between two outlets, and that is the shape most likely to lead to a wrong
 * conclusion.
 *
 * <p>The layering matches too: {@code Node} (who am I), {@code Cluster} (<b>cluster level</b>:
 * split brain, running state, the collection switch), {@code Transport}, and the
 * <b>component-level</b> sections that follow.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
@Endpoint(id = "spreader-report")
public class SpreaderReportEndpoint {

    private final MetricsService metrics;
    private final CacheService cache;
    private final MutexService mutex;
    private final PoolService pool;
    private final MultiProcessingTaskStats scheduled;
    private final LatchService latch;
    private final BarrierService barrier;
    private final SemaphoreService semaphore;
    private final ExchangerService exchanger;
    private final RpcService rpc;

    public SpreaderReportEndpoint(MetricsService metrics, CacheService cache,
                                  MutexService mutex, PoolService pool,
                                  MultiProcessingTaskStats scheduled) {
        this(metrics, cache, mutex, pool, scheduled, null, null, null, null, null);
    }

    public SpreaderReportEndpoint(MetricsService metrics, CacheService cache,
                                  MutexService mutex, PoolService pool,
                                  MultiProcessingTaskStats scheduled, LatchService latch,
                                  BarrierService barrier, SemaphoreService semaphore,
                                  ExchangerService exchanger, RpcService rpc) {
        this.metrics = metrics;
        this.cache = cache;
        this.mutex = mutex;
        this.pool = pool;
        this.scheduled = scheduled;
        this.latch = latch;
        this.barrier = barrier;
        this.semaphore = semaphore;
        this.exchanger = exchanger;
        this.rpc = rpc;
    }

    @ReadOperation
    public String report() {
        StringBuilder sb = new StringBuilder(4096);
        NodeMetrics n = metrics.snapshot();

        title(sb, "Node");
        kv(sb, "Cluster", n.clusterName());
        kv(sb, "This node", n.nodeName() + " @ " + n.address());
        kv(sb, "Role", n.leader() ? "leader" : "follower (leader: " + n.leaderAddress() + ")");
        kv(sb, "Members", n.memberCount());
        kv(sb, "Uptime", humanMillis(n.uptimeMillis()));
        kv(sb, "On break", n.onBreak() ? "yes" : "no");

        // ---------- Cluster level: not this node's properties, but the whole cluster's state
        SplitBrainStatus split = metrics.splitBrainStatus();
        title(sb, "Cluster");
        kv(sb, "Running", metrics.isRunning() ? "yes" : "no");
        kv(sb, "Collection", metrics.isMetricsEnabled()
                ? "enabled"
                : "disabled <- every number below is 0, which does not mean all is well");
        if (split.splitting()) {
            kv(sb, "Split brain", "in progress! the cluster port is held by "
                    + split.holders().size() + " nodes at once " + split.holders()
                    + ", for " + split.splittingDurationMillis() + "ms so far");
        } else if (split.everSplit()) {
            // Once it heals this is the only trace left. Without a word here, nobody would know
            // that the data from that stretch is suspect
            kv(sb, "Split brain", "healed, but it has happened " + split.occurrences()
                    + " time(s) since startup");
        } else {
            kv(sb, "Split brain", "never happened");
        }
        kv(sb, "Leader", split.leader() == null ? "none (takeover in progress)" : split.leader());

        // ---------- The health check: only the figures where non-zero is an anomaly
        title(sb, "Health check");
        int warns = 0;
        // First of all: with collection off, every check below "passes" and the report ends
        // with "nothing wrong" -- the shape in which this report most readily deceives. Better
        // to pour the cold water first
        if (!metrics.isMetricsEnabled()) {
            sb.append("  ! Metric collection is disabled -- every check below is void, and "
                    + "\"nothing wrong\" means only that there is no data\n");
            warns++;
        }
        // Split brain belongs in the health check because it is monotonic and normally 0, which
        // is exactly "non-zero is an anomaly". The class javadoc had always listed it as the
        // first check while the implementation lacked it -- a report has to mean what it says
        warns += check(sb, "Split brains", split.occurrences(),
                "the cluster port was held by several nodes at once, and for that stretch two "
                        + "leaders were granting locks and writing to the cache");
        warns += check(sb, "Buffer drops",
                n.buffers().stream().mapToLong(BufferMetrics::dropped).sum(),
                "messages were discarded outright, and neither end knows");
        if (cache != null) {
            Map<String, Object> c = cache.stats();
            warns += check(sb, "Outbox overflows", num(c.get("outboxOverflow")),
                    "updates were discarded and replicas can only catch up by pulling a full "
                            + "snapshot -- the beginning of an avalanche");
            warns += check(sb, "Buffered updates", num(c.get("bufferedUpdates")),
                    "messages are missing in between, and it is waiting for them");
        }
        if (mutex != null) {
            warns += check(sb, "Lock timeouts", num(mutex.stats().get("acquireTimeouts")),
                    "requests timed out without the lock");
            warns += check(sb, "Lock leak",
                    num(mutex.stats().get("acquired")) - num(mutex.stats().get("released")),
                    "acquired minus released; growing without falling means something took a "
                            + "lock and never returned it");
        }
        if (pool != null) {
            warns += check(sb, "Task fallbacks", num(pool.stats().get("remoteFallbacks")),
                    "dispatched without a result, so the network round trip was wasted");
            warns += check(sb, "Task failures", pool.poolStats().localFailed(),
                    "tasks ended by throwing");
        }
        if (semaphore != null) {
            Map<String, Object> sm = semaphore.stats();
            warns += check(sb, "Permit leak", num(sm.get("heldNow")),
                    "acquired minus released. Exhausted permits show up as everyone stalling, "
                            + "not as an error anywhere");
            warns += check(sb, "Permit timeouts", num(sm.get("acquireTimeouts")),
                    "acquisitions timed out without a permit, so there are not enough of them");
            warns += check(sb, "Stale releases", num(sm.get("staleReleases")),
                    "permits released that were never held, usually a duplicate release");
        }
        if (latch != null) {
            warns += check(sb, "Latch timeouts", num(latch.stats().get("awaitTimeouts")),
                    "waits timed out with the latch above zero, usually a party that never "
                            + "called countDown");
            warns += check(sb, "Latch invalidations", num(latch.stats().get("invalidations")),
                    "the leader is unsteady, and waiters were forced to start again");
        }
        if (barrier != null) {
            warns += check(sb, "Barriers broken", num(barrier.stats().get("broken")),
                    "another party timed out, was interrupted, departed, or the leader changed");
        }
        if (exchanger != null) {
            Map<String, Object> xm = exchanger.stats();
            warns += check(sb, "Exchange timeouts", num(xm.get("timeouts")),
                    "no partner turned up, usually one side of the pairing never written");
            warns += check(sb, "Exchange invalidations", num(xm.get("invalidations")),
                    "the leader changed while parties were waiting; any exchange caught "
                            + "mid-pairing lost its item");
        }
        if (rpc != null) {
            Map<String, Object> r = rpc.stats();
            warns += check(sb, "RPC timeouts", num(r.get("callTimeouts")),
                    "the peer said nothing; look at the network and the peer's load");
            warns += check(sb, "RPC rejections", num(r.get("rejected")),
                    "the inbound queue is full and this node is dropping requests");
        }
        if (warns == 0) {
            sb.append("  Nothing wrong found\n");
        }

        // ---------- Transport ----------
        title(sb, "Transport");
        kv(sb, "Total TPS", round(n.totalTps()));
        kv(sb, "Error rate", pct(n.errorRate()));
        // Business and framework channels are counted separately: framework channels, prefixed
        // spreader., are what the cache, the locks and RPC use for themselves, and their volume
        // is a different thing from business volume
        kv(sb, "Channels", n.channels().size()
                + " (business " + n.businessChannels().size()
                + " / framework " + n.systemChannels().size() + ")");
        for (Map.Entry<String, ChannelMetrics> e : n.channels().entrySet()) {
            ChannelMetrics m = e.getValue();
            String name = e.getKey().isEmpty() ? "default" : e.getKey();
            // Peak sends and receives are listed apart: with only a total, "the outbound side
            // is saturated" and "the inbound side is saturated" are the same number, and the
            // two overloads are investigated in entirely different places
            sb.append(String.format("  %-22s sent %-8d recv %-8d failed %-6d "
                            + "peak TPS %.0f/%.0f  P99 %.2fms%n",
                    name, m.sent(), m.received(), m.sendFailures(),
                    m.peakSentTps(), m.peakReceivedTps(),
                    m.outboundLatency().p99Nanos() / 1_000_000.0));
        }

        // ---------- Buffers: only those holding something or that have dropped ----------
        List<BufferMetrics> busy = n.buffers().stream()
                .filter(b -> b.pending() > 0 || b.dropped() > 0)
                .toList();
        if (!busy.isEmpty()) {
            title(sb, "Buffers (non-empty only)");
            for (BufferMetrics b : busy) {
                sb.append(String.format("  %-22s queued %-6d/%-6d usage %-6s dropped %d%n",
                        b.name(), b.pending(), b.capacity(), pct(b.usage()), b.dropped()));
            }
            // The fullest buffer is the next one to fill up. The list above is in its original
            // order, and with overload approaching nobody should have to compare them by hand
            BufferMetrics hottest = n.hottestBuffer();
            if (hottest != null && hottest.usage() > 0) {
                kv(sb, "Fullest", hottest.name() + " (" + pct(hottest.usage()) + ")");
            }
        }

        if (cache != null) {
            Map<String, Object> c = cache.stats();
            title(sb, "Cache");
            kv(sb, "Role", Boolean.TRUE.equals(c.get("leader")) ? "leader" : "follower");
            kv(sb, "Keys / bytes",
                    c.get("keyCount") + " / " + humanBytes(num(c.get("approxBytes"))));
            kv(sb, "Epoch / version", c.get("epoch") + " / " + c.get("appliedVersion"));
            kv(sb, "Ops applied", c.get("opsApplied"));
            kv(sb, "Broadcast ops/frames", c.get("opsSent") + " / " + c.get("framesSent")
                    + " (" + c.get("opsPerFrame") + " per frame)");
            kv(sb, "Evicted", c.get("evicted"));
            kv(sb, "Full syncs", c.get("resyncCount"));
        }

        if (mutex != null) {
            Map<String, Object> m = mutex.stats();
            title(sb, "Locks");
            kv(sb, "Acquired / contended", m.get("acquired") + " / " + m.get("contended"));
            kv(sb, "Contention rate", pct(num(m.get("contentionRate"))));
            kv(sb, "Mean wait", m.get("avgWaitMillis") + " ms");
            kv(sb, "Held now", m.get("heldNow"));
        }

        if (semaphore != null) {
            Map<String, Object> m = semaphore.stats();
            title(sb, "Semaphores");
            kv(sb, "Acquired / local / denied", m.get("acquired") + " / "
                    + m.get("localBlocked") + " / " + m.get("remoteDenied"));
            kv(sb, "Contention rate", pct(num(m.get("contentionRate"))));
            kv(sb, "Mean wait", m.get("avgWaitMillis") + " ms");
            kv(sb, "Held now", m.get("heldNow"));
            kv(sb, "Semaphores", m.get("gates"));
        }

        if (latch != null) {
            Map<String, Object> m = latch.stats();
            title(sb, "Latches");
            kv(sb, "Declared / counted", m.get("declares") + " / " + m.get("countDowns"));
            kv(sb, "Waits ok/timeout/void", m.get("satisfied") + " / "
                    + m.get("awaitTimeouts") + " / " + m.get("invalidations"));
            kv(sb, "Mean wait", m.get("avgWaitMillis") + " ms");
            kv(sb, "Waiting now", m.get("waitingNow"));
        }

        if (barrier != null) {
            Map<String, Object> m = barrier.stats();
            title(sb, "Barriers");
            kv(sb, "Tripped / broken / timeout", m.get("tripped") + " / "
                    + m.get("broken") + " / " + m.get("awaitTimeouts"));
            kv(sb, "Resets", m.get("resets"));
            kv(sb, "Mean wait",
                    m.get("avgWaitMillis") + " ms (set by the slowest party)");
            kv(sb, "Waiting now", m.get("waitingNow"));
        }

        if (exchanger != null) {
            Map<String, Object> m = exchanger.stats();
            title(sb, "Exchange points");
            kv(sb, "Calls / exchanged / timeout", m.get("arrivals") + " / "
                    + m.get("exchanges") + " / " + m.get("timeouts"));
            kv(sb, "Pairings (leader only)", m.get("pairings"));
            kv(sb, "Late / void", m.get("lateExchanges") + " / " + m.get("invalidations"));
            kv(sb, "Mean wait",
                    m.get("avgWaitMillis") + " ms (how long the second party took to arrive)");
            kv(sb, "Waiting now", m.get("waitingNow"));
        }

        if (rpc != null) {
            Map<String, Object> m = rpc.stats();
            title(sb, "RPC");
            kv(sb, "Calls made/ok/failed", m.get("calls") + " / "
                    + m.get("answered") + " / " + m.get("callFailures"));
            kv(sb, "Failures timeout/remote",
                    m.get("callTimeouts") + " / " + m.get("remoteErrors"));
            kv(sb, "Mean round trip", m.get("avgLatencyMillis") + " ms");
            kv(sb, "In flight", m.get("inflight"));
            kv(sb, "Served/rejected/queued", m.get("served") + " / "
                    + m.get("rejected") + " / " + m.get("queued"));
        }

        if (pool != null) {
            PoolStats ps = pool.poolStats();
            Map<String, Object> p = pool.stats();
            title(sb, "Task dispatch");
            kv(sb, "Active/done/queued", ps.activeCount() + " / "
                    + ps.completedTaskCount() + " / " + ps.queuedTaskCount());
            kv(sb, "Local / remote", p.get("localRuns") + " / " + p.get("remoteDispatches")
                    + " (remote share " + pct(num(p.get("remoteRatio"))) + ")");
            kv(sb, "Replicas available", p.get("peers") == null ? pool.peerCount() : p.get("peers"));
            sb.append(String.format("  %-14s active %d (running %d, blocked %s) queued %d "
                            + "steals %d parallelism %d%n",
                    "Local pool", ps.localActive(), ps.localRunning(), pct(ps.localBlockedRate()),
                    ps.localQueued(), ps.localSteals(), ps.localParallelism()));
            sb.append(String.format("  %-14s active %d queued %d done %d threads %d%n",
                    "Inbound pool", ps.inboundActive(), ps.inboundQueued(),
                    ps.inboundCompleted(), ps.inboundPoolSize()));
        }

        if (scheduled != null) {
            Map<String, Map<String, Object>> tasks = scheduled.snapshot();
            if (!tasks.isEmpty()) {
                title(sb, "Scheduled tasks");
                tasks.forEach((name, m) -> sb.append(String.format(
                        "  %-46s ran %-6s skipped %-6s failed %-4s mean %sms%n",
                        shorten(name), m.get("executed"), m.get("skipped"),
                        m.get("failed"), m.get("avgElapsedMs"))));
            }
        }

        return sb.toString();
    }

    // ------------------------------------------------------------------

    /** Reports one warning line when non-zero and returns 1; otherwise returns 0. */
    private int check(StringBuilder sb, String what, double value, String why) {
        if (value <= 0) {
            return 0;
        }
        sb.append(String.format("  ! %-24s %-10s  %s%n", what, fmt(value), why));
        return 1;
    }

    private static void title(StringBuilder sb, String t) {
        sb.append('\n').append("── ").append(t).append(' ')
                .append("─".repeat(Math.max(0, 60 - t.length()))).append('\n');
    }

    private static void kv(StringBuilder sb, String k, Object v) {
        sb.append(String.format("  %-14s %s%n", k, v));
    }

    private static double num(Object v) {
        return v instanceof Number n ? n.doubleValue() : 0d;
    }

    private static String fmt(double v) {
        return v == Math.floor(v) ? String.valueOf((long) v) : String.valueOf(round(v));
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    private static String pct(double v) {
        return Math.round(v * 10000.0) / 100.0 + "%";
    }

    private static String humanBytes(double b) {
        if (b < 1024) {
            return (long) b + " B";
        }
        if (b < 1024 * 1024) {
            return round(b / 1024) + " KB";
        }
        return round(b / 1024 / 1024) + " MB";
    }

    private static String humanMillis(long ms) {
        long s = ms / 1000;
        if (s < 60) {
            return s + "s";
        }
        if (s < 3600) {
            return s / 60 + "m" + s % 60 + "s";
        }
        return s / 3600 + "h" + (s % 3600) / 60 + "m";
    }

    /** A task name carries the cluster and application prefixes and is too long, so the
     *  leading part is cut away. */
    private static String shorten(String name) {
        int i = name.lastIndexOf('/');
        return i < 0 ? name : name.substring(i + 1);
    }
}
