package com.chaconneai.openspreader.metrics;

import com.chaconneai.spreader.GossipCluster;
import com.chaconneai.spreader.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.endpoint.annotation.DeleteOperation;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code /actuator/spreader-break}: puts a node <b>on a break</b>, or calls it back to work.
 *
 * <h2>Why an outward-facing way in is needed</h2>
 * {@code GossipCluster.takeBreak(boolean)} has always been there, but it is only a <b>Java
 * method</b> -- using it means writing a controller of your own in the application first. So
 * every project using this library rewrites the same endpoint, and the path operations end up
 * holding differs from one to the next.
 *
 * <p>The more practical problem is that the moment a node needs a break is usually the moment
 * <b>the code cannot be changed</b>: an instance in production is wobbling, and the wish is to
 * take business traffic off it while leaving it up to investigate at leisure.
 *
 * <h2>The interface</h2>
 * <pre>
 * GET    /actuator/spreader-break    whether it is resting, and for how long
 * POST   /actuator/spreader-break    take a break
 * DELETE /actuator/spreader-break    end the break and take work again
 * </pre>
 *
 * <p>None of the three <b>takes a parameter</b>: the verb already says what is meant, and one
 * optional boolean fewer is one fewer way to take a node out by passing the wrong value. Both
 * write operations are <b>idempotent</b>; calling one twice merely confirms the state and
 * broadcasts nothing again.
 *
 * <h2>A break is not a departure</h2>
 * A resting node is still a full member of the cluster: it gossips as usual, answers probes as
 * usual, takes its usual place in the leader takeover order, and a node that was the leader
 * <b>stays the leader</b>. All that changes is business messaging, in both directions --
 * others do not send to it, and it sends nothing outward.
 *
 * <p>So this endpoint is not a graceful shutdown. For that, use {@code stop()}.
 *
 * <h2>It is a write operation; think before exposing it</h2>
 * This is the project's <b>only endpoint that can change the cluster's behaviour</b>; every
 * other one is read-only. Actuator exposes only {@code health} to the web by default, so it is
 * naturally <b>opt-in</b>: unless it is named in
 * {@code management.endpoints.web.exposure.include}, it does not reach the network.
 *
 * <p>(The bean itself is registered unconditionally, regardless of exposure -- saving one
 * object is not worth entangling "does the bean exist" with "is the endpoint on the network".)
 *
 * <p>Once it is named, do put authentication on the management port -- otherwise anyone can
 * take your nodes out one by one while the cluster <b>looks perfectly healthy</b>: the member
 * count is unchanged and nothing alerts, only nobody is doing any work. That is worth
 * remembering more than the endpoint itself.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 05/09/2026
 */
@Endpoint(id = "spreader-break")
public class ApplicationClusterBreakEndpoint {

    private static final Logger log =
            LoggerFactory.getLogger(ApplicationClusterBreakEndpoint.class);

    private final GossipCluster cluster;

    /**
     * When this break began; 0 means it is not resting at present.
     *
     * <h2>Why it is recorded</h2>
     * The commonest accident is not taking the wrong node out, but <b>forgetting to put one
     * back</b> -- a node three days into a break looks perfectly normal from
     * {@code /actuator/health}, with the member count intact and its state still ALIVE, only it
     * never does any work. This duration is what gives a monitoring script a chance of finding
     * it.
     *
     * <h2>Why it can only say "at least"</h2>
     * {@code takeBreak} may also be called from application code, and that path does not come
     * through here. So timing starts when the node is first <b>observed</b> resting, and what
     * is reported is a lower bound. Under-reporting beats over-reporting: too large a figure
     * would suggest a long-standing problem and send someone off investigating for nothing.
     *
     * <p>volatile rather than a lock: this is an infrequent operational action, and concurrent
     * calls at worst overwrite the start with a value close to it.
     */
    private volatile long breakSince;

    public ApplicationClusterBreakEndpoint(GossipCluster cluster) {
        this.cluster = cluster;
    }

    /** Whether it is resting. */
    @ReadOperation
    public Map<String, Object> status() {
        return snapshot(null);
    }

    /** Takes a break: steps out of business messaging without departing. Idempotent. */
    @WriteOperation
    public Map<String, Object> takeBreak() {
        boolean was = cluster.isOnBreak();
        cluster.takeBreak(true);
        if (!was) {
            // Taking traffic off a node must leave a trace in the log: looking back at "when
            // did it stop working?" afterwards, this line is what answers it
            log.warn("Node {} has gone on a break: it no longer sends or receives business "
                    + "messages, but remains a full cluster member -- gossiping as usual, and "
                    + "taking its usual place in the takeover order",
                    cluster.self().address());
        }
        return snapshot(was
                ? "it was already resting; nothing changed"
                : "on a break; business messages are no longer sent or received");
    }

    /** Ends the break and takes work again. Idempotent. */
    @DeleteOperation
    public Map<String, Object> endBreak() {
        boolean was = cluster.isOnBreak();
        cluster.takeBreak(false);
        if (was) {
            log.info("Node {} has ended its break and sends and receives business messages "
                    + "again", cluster.self().address());
        }
        return snapshot(was
                ? "the break has ended and it is taking work again"
                : "it was not resting; nothing changed");
    }

    /**
     * The common response body.
     *
     * <p>All three operations return <b>the same shape</b>, and the write operations return the
     * state <b>after</b> the operation -- so a caller need not GET again to confirm, and the
     * "I POSTed, so I assume it took effect" case cannot arise.
     *
     * @param message what this call did; null for the read operation, where it is left out
     */
    private Map<String, Object> snapshot(String message) {
        boolean onBreak = cluster.isOnBreak();
        long since = breakSince;
        if (onBreak) {
            if (since == 0L) {
                breakSince = System.currentTimeMillis();
                since = 0L;
            }
        } else {
            breakSince = 0L;
            since = 0L;
        }

        Node self = cluster.self();
        Map<String, Object> out = new LinkedHashMap<>();
        if (message != null) {
            out.put("message", message);
        }
        out.put("onBreak", onBreak);
        // "Resting for at least this long" -- the start may be later than the real one; see
        // the note on breakSince
        out.put("onBreakAtLeastMillis",
                since == 0L ? 0L : System.currentTimeMillis() - since);
        out.put("clusterName", cluster.clusterName());
        out.put("nodeName", self.name());
        out.put("nodeId", self.shortId());
        out.put("address", self.address());
        // The leader may take a break too, and stays the leader while resting. Without this
        // laid out, it is easy to assume that taking the leader out triggers a change of leader
        out.put("leader", cluster.isLeader());
        out.put("memberCount", cluster.members().size());
        return out;
    }
}
