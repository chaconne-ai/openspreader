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

import com.chaconneai.spreader.GossipCluster;
import com.chaconneai.spreader.Node;
import com.chaconneai.spreader.metrics.SplitBrainStatus;
// Spring Boot 4 split health out of actuator into a module of its own, and the package moved
// from boot.actuate.health to boot.health.contributor
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The cluster's section of {@code /actuator/health}.
 *
 * <h2>What counts as unhealthy</h2>
 * health is read by <b>load balancers and orchestration systems</b> -- reporting DOWN takes
 * traffic away and may have the container restarted. So the judgement must be restrained, and
 * above all must distinguish <b>"the cluster has a problem" from "this node has a
 * problem"</b>:
 *
 * <table border="1">
 *   <caption>How the status is decided</caption>
 *   <tr><th>Situation</th><th>Status</th><th>Why</th></tr>
 *   <tr><td>The cluster is not running</td><td><b>DOWN</b></td>
 *       <td>It genuinely cannot work</td></tr>
 *   <tr><td>Resting after {@code takeBreak}</td><td><b>OUT_OF_SERVICE</b></td>
 *       <td>Alive but deliberately not taking work: take the traffic away, but <b>do not
 *           restart it</b> -- a restart is exactly what the break was avoiding</td></tr>
 *   <tr><td>Split brain within the grace period</td><td>UP with a warning</td>
 *       <td>An ordinary race; the stand-down mechanism converges within a second</td></tr>
 *   <tr><td><b>Split brain beyond the grace period</b></td><td><b>DOWN</b></td>
 *       <td>Failing to heal means the stand-down mechanism has genuinely failed, and the
 *           orchestrator must step in</td></tr>
 *   <tr><td>A buffer has dropped messages</td><td>UP with a warning, configurable</td>
 *       <td>Overloaded but still working; taking it away only presses harder on the
 *           others</td></tr>
 *   <tr><td>Currently leaderless, mid-takeover</td><td>UP</td><td>An expected gap</td></tr>
 * </table>
 *
 * <h2>Why a split brain gets a grace period before DOWN</h2>
 * DOWN at the first sign will not do: during a split brain <b>every node sees it</b>, and all
 * of them reporting DOWN would have the orchestrator restart the entire cluster -- which would
 * otherwise have healed itself.
 *
 * <p>A warning for ever will not do either: while it is split, two leaders are granting locks
 * and coordinating cache writes, and consistency is not guaranteed.
 *
 * <p>So it is given time to heal ({@code spring.spreader.metrics.split-brain-down-after}, five
 * minutes by default), and only beyond that is it a real fault. Ordinary healing takes under a
 * second, three orders of magnitude away, so nothing fires wrongly and inconsistency does not
 * last long.
 *
 * <h2>Why this lives here rather than in a controller of the application's own</h2>
 * Cluster state is <b>the framework's</b> information, and having every application rewrite
 * the same {@code /cluster/status} makes no sense. Putting it in health also connects it to
 * the whole ecosystem for free: Kubernetes probes, load-balancer health checks and alerting
 * systems all understand this endpoint.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
public class ApplicationClusterHealthIndicator implements HealthIndicator {

    private final GossipCluster cluster;
    private final MetricsService metrics;
    private final MetricsProperties props;

    /**
     * When this node <b>stopped seeing a leader</b>; 0 means there is one now.
     *
     * <h2>Why this state has to be watched on its own</h2>
     * 200 milliseconds without a leader is an ordinary handover; five minutes without one is a
     * cluster at a standstill -- and until this existed, the two <b>looked exactly alike</b> in
     * the health check: {@code leaderAddress} said "no leader, mid-takeover" in both cases, and
     * nobody counted how long it had lasted.
     *
     * <p>The danger is that each component looks "normal" on its own: the write path retries
     * and throws on timeout, a lock returns "not acquired", a scheduled task skips its round.
     * All three are <b>degradations by design</b> and none is wrong in itself. Together they
     * mean the whole cluster is doing nothing while health reports UP, Kubernetes does not
     * restart it, the load balancer does not take it out, and no alert makes a sound.
     *
     * <p>volatile rather than a lock: the health check is called infrequently, and concurrent
     * probes at worst overwrite the start with a value close to it, which does not materially
     * affect "how long it has lasted".
     */
    private volatile long leaderlessSince;

    public ApplicationClusterHealthIndicator(GossipCluster cluster, MetricsService metrics,
                                             MetricsProperties props) {
        this.cluster = cluster;
        this.metrics = metrics;
        this.props = props;
    }

    @Override
    public Health health() {
        if (!cluster.isRunning()) {
            return Health.down()
                    .withDetail("reason", "the cluster is not running")
                    .withDetail("clusterName", cluster.clusterName())
                    .build();
        }

        Node self = cluster.self();
        Node leader = cluster.leader();
        // Taken once: members() builds a fresh list on every call, and memberCount and
        // otherMembers below must come from the same instant, or the output contradicts itself
        // with "count=3" beside a list holding one other node
        List<Node> members = cluster.members();
        SplitBrainStatus split = cluster.splitBrainStatus();

        // A split brain unhealed beyond the grace period: the stand-down mechanism has
        // genuinely failed, and the orchestrator must step in
        long splitMillis = split.splittingDurationMillis();
        long downAfter = props.getSplitBrainDownAfter() == null
                ? 0L : props.getSplitBrainDownAfter().toMillis();
        boolean splitTooLong = split.splitting() && downAfter > 0 && splitMillis > downAfter;

        boolean dropping = !metrics.droppingBuffers().isEmpty();

        // How long it has been leaderless. A leader resets it -- the brief gap during a
        // handover should not accumulate
        long leaderlessMillis = 0L;
        if (leader == null) {
            long since = leaderlessSince;
            if (since == 0L) {
                leaderlessSince = System.currentTimeMillis();
            } else {
                leaderlessMillis = System.currentTimeMillis() - since;
            }
        } else {
            leaderlessSince = 0L;
        }
        long leaderlessDownAfter = props.getLeaderlessDownAfter() == null
                ? 0L : props.getLeaderlessDownAfter().toMillis();
        boolean leaderlessTooLong =
                leaderlessDownAfter > 0 && leaderlessMillis > leaderlessDownAfter;

        Health.Builder builder;
        if (leaderlessTooLong) {
            // Ahead of split brain: leaderless means nobody is working, while a split brain
            // means two are racing to. The first is a full standstill and the second is at
            // least still serving, so the more serious one is reported first
            builder = Health.down().withDetail("reason",
                    "no leader has been visible for " + leaderlessMillis + "ms, beyond "
                            + leaderlessDownAfter + "ms -- writes, locks and scheduled tasks "
                            + "are all spinning");
        } else if (splitTooLong) {
            builder = Health.down().withDetail("reason",
                    "the split brain has lasted " + splitMillis + "ms and has not healed "
                            + "within " + downAfter + "ms; the stand-down mechanism may have "
                            + "failed");
        } else if (dropping && props.isDownOnDroppedMessages()) {
            builder = Health.down().withDetail("reason", "a buffer has dropped messages");
        } else if (cluster.isOnBreak()) {
            // Resting: alive, but deliberately not taking work. The load balancer should take
            // it out, and the orchestrator should not restart it
            builder = Health.outOfService()
                    .withDetail("reason", "the node is on a break (takeBreak)");
        } else {
            builder = Health.up();
        }

        builder.withDetail("clusterName", cluster.clusterName())
                .withDetail("nodeName", self.name())
                .withDetail("nodeId", self.shortId())
                .withDetail("address", self.address())
                .withDetail("clusterPort", cluster.config().clusterPort())
                .withDetail("leader", cluster.isLeader())
                // null during the gap after the old leader departs and before the new one takes
                // the port -- expected behaviour, not a fault
                .withDetail("leaderAddress",
                        leader == null ? "no leader (takeover in progress)" : leader.address())
                .withDetail("memberCount", members.size())
                .withDetail("otherMembers", otherMembers(members, self, leader))
                .withDetail("leaderlessMillis", leaderlessMillis)
                .withDetail("transport", cluster.config().transportType()
                        + "/" + cluster.config().transportProvider())
                // The web ports and paths are in here (see WebAddressMetadataListener), which
                // is what makes one node's health enough to reach any other node's: gossip
                // carries host:22000, and only the metadata says where HTTP answers
                .withDetail("metadata", self.metadata());

        // ---- What follows wants a person's attention but does not stop this node working,
        // so none of it changes the status ----

        if (split.everSplit()) {
            // While it is split, two leaders are granting locks and coordinating cache writes,
            // and consistency over that stretch is not guaranteed. Once it heals this is the
            // only trace left, so it has to stay in health
            builder.withDetail("splitBrain", split.splitting()
                            ? "in progress! the cluster port is held by " + split.holders()
                              + " at once, for " + splitMillis + "ms so far"
                            : "healed")
                    .withDetail("splitBrainOccurrences", split.occurrences());
            if (split.splitting() && downAfter > 0) {
                builder.withDetail("splitBrainDownAfterMillis", downAfter);
            }
        }

        if (dropping) {
            // A full buffer discards messages outright, and neither sender nor receiver knows.
            // Without a word here, overload makes no sound at all
            builder.withDetail("droppedMessages", metrics.droppingBuffers().stream()
                    .map(b -> b.name() + "=" + b.dropped())
                    .toList());
        }

        return builder.build();
    }

    /**
     * The membership list, excluding this node.
     *
     * <h2>Why {@code memberCount} alone is not enough</h2>
     * A number can say "one is missing" but not <b>which one</b>. And troubleshooting cares
     * about precisely that: a process that never started on one machine, an availability zone
     * that cannot be reached, a node still stuck in its tombstone period -- all of which look
     * identical in a count.
     *
     * <p>Worse is <b>an inconsistent view</b>: A sees B while B does not see A, and both sides'
     * {@code memberCount} may read 2. Only laying the nodes' lists side by side brings a
     * half-open network problem like that into the open.
     *
     * <h2>Why this node is excluded</h2>
     * This node's own information is listed above under {@code nodeName}, {@code nodeId} and
     * {@code address}. Mixed in, reading this field would mean picking out "which one is me"
     * every time, and the self-description would become noise when diffing two nodes' output.
     *
     * <h2>Why a single node returns an empty list rather than omitting the field</h2>
     * Monitoring scripts and dashboards go by whether a field is there. A field that comes and
     * goes makes an expression like {@code jq '.details.otherMembers | length'} fail on a
     * single node -- and a single node is exactly the abnormal state that most needs seeing.
     *
     * @param members the membership snapshot already taken, this node included
     * @param self    this node
     * @param leader  the current leader, possibly null during a gap
     */
    private static List<Map<String, Object>> otherMembers(List<Node> members, Node self,
                                                          Node leader) {
        // Excluded by id: addresses repeat with several processes on one machine, and names
        // are allowed to repeat by design
        String selfId = self.id();
        String leaderId = leader == null ? null : leader.id();
        List<Map<String, Object>> result = new ArrayList<>(Math.max(0, members.size() - 1));
        for (Node node : members) {
            if (node.id().equals(selfId)) {
                continue;
            }
            // LinkedHashMap rather than Map.of: the field order is stable once serialised to
            // JSON, so two nodes' output can be diffed directly
            Map<String, Object> one = new LinkedHashMap<>();
            one.put("nodeName", node.name());
            one.put("nodeId", node.shortId());
            one.put("address", node.address());
            one.put("state", node.state().name());
            one.put("leader", node.id().equals(leaderId));
            // A resting node is still a full member but sends and receives no business
            // messages. Without this line, "the cluster is fine, so why is nobody working?"
            // cannot be answered
            one.put("onBreak", node.onBreak());
            // The point of the whole section: with the web address in here, a node found to
            // be misbehaving can be visited straight away -- one curl to any member's health
            // gives every other member's actuator URL
            one.put("metadata", node.metadata());
            result.add(one);
        }
        return result;
    }
}
