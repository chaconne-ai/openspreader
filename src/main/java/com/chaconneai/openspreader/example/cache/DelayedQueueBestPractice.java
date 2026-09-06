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
package com.chaconneai.openspreader.example.cache;

import com.chaconneai.openspreader.cache.ProcessingCache;
import com.chaconneai.openspreader.cache.ScoredMember;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A delayed queue: "cancel the order if it is still unpaid in 30 minutes".
 *
 * <h2>How to use it</h2>
 * <pre>{@code
 * DelayedQueueBestPractice queue = new DelayedQueueBestPractice(cache, "order:timeout");
 *
 * // Submitted when the order is placed
 * queue.schedule("order-123", 30, TimeUnit.MINUTES);
 *
 * // Every instance runs this scheduled task, but one order is handled by one instance
 * @Scheduled(fixedDelay = 1000)
 * public void poll() {
 *     for (String orderId : queue.pollDue(100)) {
 *         cancelIfUnpaid(orderId);
 *     }
 * }
 * }</pre>
 *
 * <h2>Why not {@code @Scheduled} over a database table scan</h2>
 * The table-scan shape ({@code select ... where due_at < now() and status = 'pending'}) has
 * two troubles: several instances <b>scan the same batch at the same time</b>, so a lock or
 * an optimistic update has to be added on top; and the larger the order table grows the
 * slower the scan gets, going back to the table again and again even with an index.
 *
 * <p>Here the due time is the score in a sorted set, so taking due tasks is
 * {@code O(log n + m)}, and {@code zpopmin} resolves serially on the leader, so <b>one task
 * reaches exactly one instance</b> -- measured with five nodes racing for 20 tasks, all 20
 * succeeded with no duplicates.
 *
 * <h2>It is not a replacement for a message queue</h2>
 * The cache <b>is not persistent</b>: restart the whole cluster and tasks not yet due are
 * gone. So:
 *
 * <ul>
 *   <li>where the odd loss is acceptable -- cache warming, transient reminders -- use it
 *       directly</li>
 *   <li>where nothing may be lost -- order timeouts, reconciliation -- <b>the database must
 *       hold the authoritative record</b> and this is only an accelerating layer; after a
 *       restart, refill it from the database once</li>
 * </ul>
 *
 * <p>There is no negotiating this one: something that does not persist cannot be treated as
 * reliable delivery, however it is used.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
public class DelayedQueueBestPractice {

    private final ProcessingCache cache;
    private final String key;

    public DelayedQueueBestPractice(ProcessingCache cache, String key) {
        this.cache = cache;
        this.key = key;
    }

    /**
     * Submits a delayed task.
     *
     * <p>Submitting the same taskId again <b>overwrites</b> the due time rather than producing
     * a second entry -- which is exactly what renewal needs: the user has paid for another
     * period, so the due time moves back.
     *
     * @param taskId the task identifier, usually a business key such as an order number or a
     *               user id
     * @param delay  how long until it is due
     */
    public void schedule(String taskId, long delay, TimeUnit unit) {
        long dueAt = System.currentTimeMillis() + unit.toMillis(delay);
        cache.zadd(key, taskId.getBytes(StandardCharsets.UTF_8), dueAt);
    }

    /** Makes it due at an absolute point in time. */
    public void scheduleAt(String taskId, long epochMillis) {
        cache.zadd(key, taskId.getBytes(StandardCharsets.UTF_8), epochMillis);
    }

    /**
     * Takes the tasks that are due, <b>removing them as it takes them</b>.
     *
     * <h2>Why pop one at a time rather than query then delete</h2>
     * "{@code zrangeByScore} first, then {@code zrem} one by one" is wrong across instances:
     * two instances may find the same batch and then <b>each handle it</b>. Orders get
     * cancelled twice and messages get sent twice.
     *
     * <p>{@code zpopmin} takes and deletes atomically, resolving serially on the leader, so
     * one task reaches exactly one instance.
     *
     * <h2>Why the score is checked</h2>
     * {@code zpopmin} takes the <b>lowest-scoring</b> member, that is the one due soonest --
     * but it <b>pays no attention to whether it is due</b>. So the time is checked once it is
     * out: not yet due means there are no due tasks left in the queue, so it goes back and the
     * round ends.
     *
     * @param limit how many to take this round at most, so one round cannot occupy the thread
     *              for too long
     */
    public List<String> pollDue(int limit) {
        long now = System.currentTimeMillis();
        List<String> due = new ArrayList<>();

        for (int i = 0; i < limit; i++) {
            ScoredMember head = cache.zpopmin(key);
            if (head == null) {
                break;                      // The queue is empty
            }
            if (head.score() > now) {
                // Even the earliest is not due yet, so nothing is due. Put it back
                cache.zadd(key, head.member(), head.score());
                break;
            }
            due.add(new String(head.member(), StandardCharsets.UTF_8));
        }
        return due;
    }

    /**
     * Looks at what is due <b>without taking it</b>.
     *
     * <p>For monitoring and troubleshooting: "how much unhandled work is backed up?" This
     * method <b>must not be used to consume</b> -- it deletes nothing, so every instance would
     * find the same entries. Consuming always goes through {@link #pollDue}.
     */
    public List<String> peekDue() {
        List<ScoredMember> members = cache.zrangeByScore(key, 0, System.currentTimeMillis());
        List<String> out = new ArrayList<>(members.size());
        for (ScoredMember m : members) {
            out.add(new String(m.member(), StandardCharsets.UTF_8));
        }
        return out;
    }

    /**
     * Cancels a task that is not yet due.
     *
     * <p>The typical case: the user has paid, so the "cancel on timeout" task should no longer
     * run.
     *
     * @return true when the task really existed and was cancelled; false when it has already
     *         been consumed or never existed
     */
    public boolean cancel(String taskId) {
        return cache.zrem(key, taskId.getBytes(StandardCharsets.UTF_8));
    }

    /** How many tasks the queue holds, those not yet due included. */
    public int size() {
        return cache.zcard(key);
    }

    /**
     * The backlog: how many are due but not yet handled.
     *
     * <p><b>This number deserves an alert.</b> Climbing steadily, it means consumption is not
     * keeping up with submission, or that every instance's scheduled task is stuck.
     */
    public int backlog() {
        return cache.zcount(key, 0, System.currentTimeMillis());
    }
}
