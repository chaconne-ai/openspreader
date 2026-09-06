package com.chaconneai.openspreader.sync;

import java.util.concurrent.TimeUnit;

/**
 * A thin {@link ProcessingCountDownLatch}.
 *
 * <p>The real work lives in {@link LatchService} -- gathering counts, pushing, the polling
 * safety net, generation checks. This does two things: remember its own name and initial
 * count, and convert time units to milliseconds.
 *
 * <p><b>It holds no local state.</b> Unlike {@link MultiProcessingMutex}, a latch needs no
 * in-process count because it has no notion of reentrancy, and every operation goes by the
 * leader's register. So constructing one directly, outside the factory, is harmless; the
 * factory caches instances only to check for "same name, different count" along the way.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MultiProcessingCountDownLatch implements ProcessingCountDownLatch {

    private final LatchService service;
    private final String name;
    private final long count;

    public MultiProcessingCountDownLatch(LatchService service, String name, long count) {
        if (count < 0) {
            throw new IllegalArgumentException("a latch's initial count must not be negative: " + count);
        }
        this.service = service;
        this.name = name;
        this.count = count;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long initialCount() {
        return count;
    }

    @Override
    public long countDown(String participantId) {
        if (participantId == null || participantId.isBlank()) {
            throw new IllegalArgumentException(
                    "participantId must not be empty; for a plain count, call the no-argument countDown()");
        }
        // Make sure the latch exists on the leader before counting down: a counter may well
        // arrive before any waiter does
        ensureDeclared();
        return service.countDown(name, participantId);
    }

    @Override
    public void countDown() {
        ensureDeclared();
        service.countDown(name, "");
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return service.await(name, count, unit.toMillis(timeout));
    }

    @Override
    public void await() throws InterruptedException {
        service.await(name, count, -1L);
    }

    @Override
    public long remaining() {
        ensureDeclared();
        return service.remaining(name);
    }

    @Override
    public boolean isSatisfied() {
        long r = remaining();
        return r == 0L;
    }

    /**
     * Declares the latch again before every operation.
     *
     * <p>It looks wasteful, but it is idempotent -- an existing latch merely has its
     * activity time refreshed -- and the step <b>cannot be skipped</b>: after a change of
     * leader the register is empty, and without redeclaring, a count-down would fail
     * outright on "no such latch".
     */
    private void ensureDeclared() {
        String error = service.declare(name, count);
        if (error != null) {
            throw new IllegalStateException(error);
        }
    }

    @Override
    public String toString() {
        return "ProcessingCountDownLatch[" + name + ", count=" + count + "]";
    }
}
