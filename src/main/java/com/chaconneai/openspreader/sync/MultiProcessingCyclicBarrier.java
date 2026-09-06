package com.chaconneai.openspreader.sync;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A thin {@link ProcessingCyclicBarrier}.
 *
 * <p>The real work lives in {@link BarrierService} -- gathering arrivals, generation
 * management, propagating a break, and the push-plus-poll safety net. This forwards the
 * name and the arguments.
 *
 * <p><b>It holds no local state</b>: every {@code await} goes by the leader's register. So
 * one instance may be called from several threads at once, each counting as one
 * participant.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MultiProcessingCyclicBarrier implements ProcessingCyclicBarrier {

    private final BarrierService service;
    private final String name;
    private final int parties;

    public MultiProcessingCyclicBarrier(BarrierService service, String name, int parties) {
        if (parties <= 0) {
            throw new IllegalArgumentException("a barrier needs more than 0 parties: " + parties);
        }
        this.service = service;
        this.name = name;
        this.parties = parties;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int parties() {
        return parties;
    }

    @Override
    public long await() throws InterruptedException, BrokenBarrierException {
        try {
            return service.await(name, parties, -1L);
        } catch (TimeoutException e) {
            // An untimed call cannot time out; reaching here would mean the service layer's
            // semantics had changed
            throw new ProcessingMutexException("an untimed await should never time out", e);
        }
    }

    @Override
    public long await(long timeout, TimeUnit unit)
            throws InterruptedException, BrokenBarrierException, TimeoutException {
        return service.await(name, parties, unit.toMillis(timeout));
    }

    @Override
    public int arrivedCount() {
        SyncMessage r = service.query(name, parties);
        return r != null && r.success() ? (int) r.count() : 0;
    }

    @Override
    public boolean isBroken() {
        SyncMessage r = service.query(name, parties);
        return r != null && r.success() && r.state() == SyncState.BROKEN;
    }

    @Override
    public void reset() {
        service.reset(name, parties);
    }

    @Override
    public String toString() {
        return "ProcessingCyclicBarrier[" + name + ", parties=" + parties + "]";
    }
}
