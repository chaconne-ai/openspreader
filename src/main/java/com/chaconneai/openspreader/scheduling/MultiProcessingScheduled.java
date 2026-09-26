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
package com.chaconneai.openspreader.scheduling;

import com.chaconneai.openspreader.Scope;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Placed on a {@code @Scheduled} method, this makes the task run on <b>one instance in the
 * cluster at a time</b>.
 *
 * <pre>{@code
 * @Scheduled(fixedDelay = 60_000)
 * @MultiProcessingScheduled                 // this one line is enough
 * public void syncOrders() {
 *     // Scale the application to five replicas, and one replica still runs each round
 * }
 * }</pre>
 *
 * <h2>How it works</h2>
 * Before each firing it takes a
 * {@link com.chaconneai.openspreader.sync.ProcessingMutex}: taking it runs the task, and
 * failing to take it <b>skips the round</b> rather than queueing, which would only pile tasks
 * up.
 *
 * <p>The lock <b>is not released when the task finishes</b>; it is held to the end of the
 * round, see {@link #lockAtLeastMs()}. Without that, a task finishing in milliseconds releases
 * the lock, another instance takes it at once, and the same round runs several times, which is
 * no exclusion at all.
 *
 * <p>Every instance's scheduler still fires on time. The one holding the lock runs the body and
 * the others return after one network round trip for the lock. What is saved is the repeated
 * execution of the body, not the scheduling.
 *
 * <h2>Which instance it lands on</h2>
 * Not evenly distributed, and not meant to be. The leader takes the lock by reading a register
 * in its own memory while everyone else needs a round trip, about 1.5 milliseconds' difference,
 * so the leader wins nearly every race. In practice cluster-scoped tasks are effectively
 * pinned to the leader, and application-scoped tasks rotate among that application's instances
 * only when none of them is the leader, loosely rather than round-robin.
 *
 * <p>Tasks move to the new leader on a change of leadership, without interruption. This
 * <b>does not guarantee</b> "always the same machine"; stickiness needs another approach.
 *
 * <h2>Do not use "am I the leader" instead</h2>
 * {@code cluster.isLeader()} is <b>cluster-level</b> and does not distinguish applications.
 * Where an order service and a reporting service share a cluster, the leader may be a reporting
 * service instance, and an order service task guarded by {@code isLeader()} would <b>never run
 * at all</b>. A scheduled task wants "one of the instances of this application", which means
 * taking a lock, and that is what this annotation does inside.
 *
 * <h2>Running longer than the lease</h2>
 * Nothing goes wrong: the lock service renews the lease while it is held, so a task may run as
 * long as it likes. Only a crashed process, or one stuck badly enough to stop renewing, loses
 * the lock to someone else.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MultiProcessingScheduled {

    /**
     * The lock name. Left blank, it becomes "class name # method name", which is usually
     * enough.
     *
     * <p>Where two different methods should exclude each other -- two entry points to the same
     * data, say -- give them the same name.
     */
    String value() default "";

    /**
     * The granularity, {@link Scope#APPLICATION} by default -- exclusive only against
     * <b>other instances of the same application</b>.
     *
     * <p>This is what a scheduled task wants: one of the order service's five replicas runs it,
     * without preventing the reporting service from running a task of its own by the same name.
     *
     * <p>{@link Scope#CLUSTER} instead means one across the whole cluster, without regard to
     * application.
     */
    Scope scope() default Scope.APPLICATION;

    /**
     * How long to wait for the lock at most, in milliseconds; 0 by default, which does not
     * wait and skips the round.
     *
     * <p>A scheduled task usually should not wait: succeeding only crowds two executions
     * together. It is worth setting only for a short task that genuinely requires "someone must
     * complete every round".
     */
    long waitMs() default 0L;

    /**
     * How long the lock is held at minimum, in milliseconds -- that is, how long it stays held
     * down after the task finishes.
     *
     * <p><b>-1 by default, meaning this task's scheduling interval</b> -- the value of
     * {@code fixedDelay} or {@code fixedRate} -- which gives "once per period", and is what a
     * scheduled task nearly always wants.
     *
     * <p>Why it is needed: a task body often finishes in a few milliseconds, and releasing the
     * lock then lets another instance take it at once, so the same round runs several times.
     * Only holding it down for a period is genuine de-duplication.
     *
     * <p>A cron task's interval cannot be worked out, so it defaults to 0 -- released as soon
     * as it finishes. Where such a task's firings are far apart the window for a repeat is
     * short and usually harmless; for more certainty, give a value here slightly below the
     * firing interval.
     *
     * <p>0 means released as soon as it finishes, and whoever takes it next has it.
     */
    long lockAtLeastMs() default -1L;
}
