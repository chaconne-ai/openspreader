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
package com.chaconneai.openspreader.pooling;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Fork/join at the process level: a task that divides recursively is spread across several
 * processes in the cluster.
 *
 * <pre>{@code
 * long sum = forkJoinPool.invoke(new SumTask(1, 100));
 * }</pre>
 *
 * <p>There is exactly one difference from {@code java.util.concurrent.ForkJoinPool}: a
 * subtask may be serialised and sent to another process to run. Dividing and combining are
 * written just as they always were -- see {@link RecursiveTask}.
 *
 * <h2>Beyond a certain depth, nothing is sent outward</h2>
 * Each fork adds one to the depth, and past the limit every subtask stays local. Without that
 * limit, a recursion a few levels deep would inflate the cluster's message volume
 * exponentially while the subtasks grew ever smaller, until all the time went on the network.
 *
 * <p>Summing 1 to 100, for instance: by the third level there are eight sub-ranges of a dozen
 * or so numbers each, and sending those outward is pure waste.
 *
 * <h2>A failure at the far end falls back to local computation</h2>
 * When a subtask times out or the peer reports an error, it is recomputed locally rather than
 * failing the whole computation. The cost is that the subtask ran twice -- so <b>a task's
 * compute() must be pure computation</b>, with no database writes, no messages sent, nothing
 * that goes wrong when done twice.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class ForkJoinMultiProcessingPool implements ProcessingPool {

    private final PoolService service;

    public ForkJoinMultiProcessingPool(PoolService service) {
        this.service = service;
    }

    /**
     * Runs a recursive task and waits for it to finish.
     *
     * <p>It uses the default timeout, which is <b>the single-hop timeout times (maximum depth
     * + 2)</b>. The factor is not arbitrary; see {@link #defaultTimeoutMs()}.
     *
     * @throws ProcessingPoolException when the task fails, or the default timeout passes
     */
    public <T> T invoke(RecursiveTask<T> task) {
        return invoke(task, defaultTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    /**
     * Runs a recursive task, waiting no longer than this.
     *
     * <h2>Why the single-hop timeout is not enough, and this layer exists</h2>
     * {@code PoolService} puts a timeout on every remote request and <b>falls back to local
     * computation</b> when it fires. Any one hop is therefore bounded -- but the local
     * recomputation forks new remote subtasks of its own, which time out, and are recomputed
     * again. In the worst case <b>the timeouts of every level add up</b>, plus whatever the
     * user's {@code compute()} takes.
     *
     * <p>Put another way: <b>bounded at each level is not bounded end to end</b>. This
     * parameter gives the end-to-end limit.
     *
     * <p>(There was once something worse: the timeout callback ran on the JDK's single-threaded
     * scheduler shared by the whole JVM, and joining a subtask inside the callback blocked it,
     * so <b>no orTimeout anywhere in the JVM fired again</b> -- measured hanging for 1740
     * seconds. That deadlock was fixed on 20 August; see the comment marked with a star in
     * {@code PoolService}. Even so, a caller should still have a limit of its own.)
     *
     * <h2>A timeout only stops waiting; it does not stop the work</h2>
     * Computations already running at the far end and locally <b>are not cancelled</b>; they
     * run to completion, and their results are discarded on arrival when nobody is waiting.
     * Interrupting a distributed recursion halfway through is far harder than letting it
     * finish, and the gain is a few seconds of CPU.
     *
     * <p>So retrying the same task immediately after a timeout runs it <b>alongside</b> what is
     * left of the previous attempt. This is another reason {@code compute()} must be pure
     * computation.
     *
     * @throws ProcessingPoolException when the task fails, times out, or the wait is
     *                                 interrupted
     */
    public <T> T invoke(RecursiveTask<T> task, long timeout, TimeUnit unit) {
        CompletableFuture<T> future = submit(task);
        try {
            return future.get(timeout, unit);
        } catch (TimeoutException e) {
            throw new ProcessingPoolException("the recursive task did not finish within "
                    + timeout + " " + unit.name().toLowerCase()
                    + ". Note that the computations at the far end and locally have not "
                    + "stopped; there is simply nobody waiting for the result. Is the task "
                    + "divided too coarsely, or is a node in the cluster never answering?", e);
        } catch (InterruptedException e) {
            // The interrupt flag goes back: swallowing it breaks cancellation further up
            Thread.currentThread().interrupt();
            throw new ProcessingPoolException("interrupted while waiting for a recursive task", e);
        } catch (ExecutionException e) {
            throw unwrap(e.getCause() == null ? e : e.getCause());
        } catch (CompletionException e) {
            throw unwrap(e.getCause() == null ? e : e.getCause());
        }
    }

    /**
     * The default end-to-end timeout: <b>the single-hop timeout times (maximum depth + 2)</b>.
     *
     * <p>The factor comes from this: in the worst case every level's outbound subtask times out
     * once ({@code maxDepth} levels), the level that falls back computes once more, and one
     * share remains as headroom for combining and scheduling.
     *
     * <p>It <b>does not cover</b> a user {@code compute()} that is slow in itself -- that needs
     * a timeout passed in.
     */
    public long defaultTimeoutMs() {
        return service.requestTimeoutMs() * (service.maxDepth() + 2L);
    }

    private static ProcessingPoolException unwrap(Throwable cause) {
        if (cause instanceof ProcessingPoolException pe) {
            return pe;
        }
        return new ProcessingPoolException("the task failed: " + cause, cause);
    }

    /**
     * Submits a recursive task and returns at once.
     *
     * <p>It starts at depth 0. Where a task lands is decided by {@link PoolService}, which
     * reads the member list afresh each time.
     */
    public <T> CompletableFuture<T> submit(RecursiveTask<T> task) {
        return service.submit(task, 0);
    }

    @Override
    public <T> CompletableFuture<T> submit(String className, String beanName,
                                           String methodName, Object[] args) {
        return service.submitCall(className, beanName, methodName, args);
    }

    @Override
    public void execute(String className, String beanName, String methodName, Object[] args) {
        // Method calls and recursive tasks share the same transport, so this passes straight on
        service.fireAndForget(className, beanName, methodName, args);
    }

    @Override
    public int peerCount() {
        return service.peerCount();
    }

    @Override
    public PoolStats poolStats() {
        return service.poolStats();
    }

    /** Past this depth, nothing is dispatched outward. */
    public int maxDepth() {
        return service.maxDepth();
    }

    @Override
    public String toString() {
        return "ForkJoinMultiProcessingPool{peers=" + peerCount() + ", maxDepth=" + maxDepth() + '}';
    }
}
