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

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration for monitoring and health checks. Prefix: {@code spring.spreader.metrics}.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
@Data
@ConfigurationProperties(prefix = "spring.spreader.metrics")
public class MetricsProperties {

    /**
     * Whether to expose the monitoring data: the Prometheus export, the
     * {@code /actuator/spreader} endpoint, and the health check.
     *
     * <p>This switches off <b>exposure</b>, not collection. To switch off the collection as
     * well, use {@code spring.spreader.advanced.metrics-enabled=false}.
     */
    private boolean enabled = true;

    /**
     * A split brain lasting longer than this makes the health check report <b>DOWN</b>; before
     * that it only warns. Five minutes by default.
     *
     * <h2>Why there is a grace period rather than DOWN on the first split</h2>
     * A split <b>happening</b> is usually not a problem: two nodes starting at almost the same
     * moment collide, and the yielding mechanism converges within a gossip round -- a few
     * hundred milliseconds.
     *
     * <p>And health is read by <b>load balancers and orchestrators</b>, where DOWN means
     * traffic is withdrawn and the container may be restarted. During a split <b>every node
     * sees the split</b>, so going DOWN immediately would have the orchestrator restart
     * <b>the entire cluster</b> -- one that was about to heal itself.
     *
     * <h2>Why warning forever will not do either</h2>
     * During a split both leaders are doing leader work -- granting locks, coordinating cache
     * writes -- and data consistency has no guarantee for that window. Failing to heal means
     * the yielding mechanism genuinely is not working: one-way network failure, a wedged node,
     * a misconfiguration. That is when the orchestrator has to step in.
     *
     * <p>So: give it the chance to heal, and call it a fault only once that chance has
     * expired. Five minutes is a conservative default -- normal healing is sub-second, three
     * orders of magnitude away, so it neither fires by mistake nor lets an inconsistency
     * persist for long.
     *
     * <p>0 or a negative value means <b>never report DOWN because of a split</b>, only
     * warn.
     */
    private Duration splitBrainDownAfter = Duration.ofMinutes(5);

    /**
     * Whether dropped messages should make the health check report DOWN. <b>false</b> by
     * default, leaving only a warning in the health output.
     *
     * <p>The reasoning matches the split brain: dropping means overload, but the node itself is
     * still working, and withdrawing it only puts more pressure on those remaining -- turning
     * an overload into a cascade.
     */
    private boolean downOnDroppedMessages = false;

    /**
     * How long <b>without a visible leader</b> counts as a fault. Thirty seconds by default.
     *
     * <h2>Why this state needs watching on its own</h2>
     * Leaderless for 200 milliseconds is an ordinary handover; leaderless for five minutes is
     * a cluster at a standstill -- and before this setting existed the two <b>looked exactly
     * alike</b> in the health check, both showing nothing but "no leader (taking over)", with
     * nobody counting how long it had lasted.
     *
     * <p>The danger is that every component looks normal in isolation: the write path retries
     * to its timeout and throws, the lock returns "not acquired", the scheduled task skips this
     * round. All three are <b>degradations by design</b> and none of them is wrong. Together
     * they are a cluster doing no work at all, while health reports UP, Kubernetes does not
     * restart anything, the load balancer withdraws nothing, and no alert makes a sound.
     *
     * <p>Where thirty seconds comes from: a normal handover is sub-second, and takeover delay
     * plus failure detection is a few seconds at most. Thirty is already an order of magnitude
     * beyond that -- reaching it means something is genuinely stuck rather than still
     * converging.
     *
     * <p>0 or a negative value means <b>never report DOWN for leaderlessness</b>, only publish
     * {@code leaderlessMillis} in the details for monitoring to judge for itself.
     */
    private Duration leaderlessDownAfter = Duration.ofSeconds(30);
}
