package com.chaconneai.openspreader.metrics;

import com.chaconneai.spreader.metrics.BufferMetrics;
import com.chaconneai.spreader.metrics.ChannelMetrics;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * A complete monitoring snapshot of one node, <b>ready to serialise straight to JSON</b>.
 *
 * <h2>Why this layer exists rather than handing out spreader's Map directly</h2>
 * An external system rendering a dashboard needs more than the per-channel message counts
 * -- it needs to know <b>whose data this is</b>: which node, in what role, how large the
 * cluster is, whether it is resting. Without those, data gathered from several nodes cannot
 * be told apart, and "why does this node have no traffic" has no answer (it may simply be
 * resting rather than dead).
 *
 * <p>So node identity, a cluster summary and the channel metrics are packed here into
 * <b>one self-contained bundle</b>: fetch it once and it can be rendered, with nothing to
 * piece together from other endpoints.
 *
 * <p>Records and primitives throughout, so any JSON library serialises it directly, with no
 * dependence on Jackson annotations.
 *
 * @param clusterName  the cluster name
 * @param nodeId       the node id
 * @param nodeName     the application name
 * @param address      {@code host:port}
 * @param leader       whether this node is the leader
 * @param leaderAddress the current leader's address; null when there is none
 * @param onBreak      whether this node is resting. No business traffic while resting is
 *                     normal, not a fault
 * @param memberCount  how many members the cluster has right now
 * @param uptimeMillis how long this node has been running
 * @param timestamp    when this snapshot was taken, as a millisecond timestamp
 * @param channels     the per-channel metrics, keyed by channel name -- the empty string
 *                     being the user's default channel
 * @param buffers      the fill level of each inbound buffer. A non-zero {@code dropped}
 *                     means messages really were lost, and the dropping is <b>silent</b> --
 *                     without looking here there is no sound at all
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
public record NodeMetrics(
        String clusterName,
        String nodeId,
        String nodeName,
        String address,
        boolean leader,
        String leaderAddress,
        boolean onBreak,
        int memberCount,
        long uptimeMillis,
        long timestamp,
        Map<String, ChannelMetrics> channels,
        List<BufferMetrics> buffers) {

    /** Business channels only -- the user's own messages, excluding the framework channels
     *  prefixed {@code spreader.}. */
    public List<ChannelMetrics> businessChannels() {
        return channels.values().stream().filter(c -> !c.isSystemChannel()).toList();
    }

    /** Framework channels only: cache, locks, RPC, task dispatch and the rest. */
    public List<ChannelMetrics> systemChannels() {
        return channels.values().stream().filter(ChannelMetrics::isSystemChannel).toList();
    }

    /** Whether any buffer has ever dropped a message. The red light that most deserves the
     *  top of a dashboard. */
    public boolean hasDroppedMessages() {
        return buffers.stream().anyMatch(BufferMetrics::hasDropped);
    }

    /** The fullest buffer -- the one that will fill first. */
    public BufferMetrics hottestBuffer() {
        return buffers.stream().max(Comparator.comparingDouble(BufferMetrics::usage)).orElse(null);
    }

    /** Total TPS across every channel, for an overview chart. */
    public double totalTps() {
        return channels.values().stream().mapToDouble(ChannelMetrics::tps).sum();
    }

    /**
     * The node's overall error rate, 0 to 1.
     *
     * <p>Weighted by <b>message count</b> rather than being the arithmetic mean of the
     * per-channel rates -- the latter would let a quiet channel that sent two messages and
     * failed one weigh as heavily on the overall rate as a main channel that carried a
     * million.
     */
    public double errorRate() {
        long errors = 0;
        long total = 0;
        for (ChannelMetrics c : channels.values()) {
            errors += c.sendFailures() + c.receiveFailures();
            total += c.sent() + c.received() + c.sendFailures() + c.receiveFailures();
        }
        return total == 0 ? 0d : (double) errors / total;
    }
}
