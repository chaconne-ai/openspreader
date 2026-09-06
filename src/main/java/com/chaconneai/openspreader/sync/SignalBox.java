package com.chaconneai.openspreader.sync;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The local half of "wait for the leader to push".
 *
 * <h2>It waits for pushes and never polls</h2>
 * A waiter parks here until the leader pushes a message or its own timeout expires. There is
 * <b>no periodic polling</b>: once there are many waiters, polling is pure wasted traffic,
 * and the only thing it rescues is a lost push -- which the caller's own timeout already
 * covers.
 *
 * <p>The price, stated plainly: when a push is lost, a waiter waits until <b>its own await
 * times out</b> rather than noticing a few hundred milliseconds earlier. So set
 * {@code await}'s timeout to the worst wait the application can accept.
 *
 * <h2>A change of leader must come from a local event, never from asking</h2>
 * A dead leader pushes nothing at all. So "the register is gone" <b>can only be triggered by
 * a local cluster event</b> -- calling {@link #signalAll} from {@code onLeaderChanged} or
 * {@code onLeaderLeft} -- and cannot depend on asking. Asking would be polling again, and
 * while leadership is vacant there is nobody to ask.
 *
 * <h2>Signals must not be lost: a sequence number, not bare wait/notify</h2>
 * There is a race that has to be handled: the leader's push arrives after a waiter has
 * finished registering but before it has parked. With bare {@code wait/notify} that
 * notification is simply lost and the waiter parks until its timeout -- while the condition
 * was in fact satisfied long before.
 *
 * <p>So each name carries a <b>signal sequence number</b>: a waiter reads it <b>before</b>
 * registering and brings it along when parking, and returns immediately -- never parking at
 * all -- if the number has already moved.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
final class SignalBox {

    /** The signal slot for one name. */
    private static final class Slot {
        /** How many signals have arrived. It is how a waiter tells whether anything happened
         *  while it slept. */
        long seq;
    }

    private final Map<String, Slot> slots = new ConcurrentHashMap<>();

    /**
     * Reads the current signal sequence number.
     *
     * <p><b>It must be called before registering with the leader</b>, or a push arriving
     * between registering and parking is lost.
     */
    long token(String name) {
        Slot slot = slots.computeIfAbsent(name, n -> new Slot());
        synchronized (slot) {
            return slot.seq;
        }
    }

    /**
     * Parks, waiting for a push.
     *
     * @param token     the number read by {@link #token}; if it has already moved, this
     *                  returns immediately
     * @param maxWaitMs the longest to wait, usually the caller's entire remaining timeout
     * @return true when a signal arrived; false when it woke on its own timeout
     */
    boolean await(String name, long token, long maxWaitMs) throws InterruptedException {
        if (maxWaitMs <= 0) {
            return false;
        }
        Slot slot = slots.computeIfAbsent(name, n -> new Slot());
        long deadline = System.currentTimeMillis() + maxWaitMs;
        synchronized (slot) {
            while (slot.seq == token) {
                long left = deadline - System.currentTimeMillis();
                if (left <= 0) {
                    return false;
                }
                slot.wait(left);
            }
            return true;
        }
    }

    /** A push arrived: increments the number and wakes whoever waits on this name. */
    void signal(String name) {
        Slot slot = slots.get(name);
        if (slot == null) {
            // Nobody in this process is waiting on this name. The number need not be kept: a
            // waiter calls token() before registering, so a push aimed at it can only arrive
            // after it has created the slot
            return;
        }
        synchronized (slot) {
            slot.seq++;
            slot.notifyAll();
        }
    }

    /**
     * Wakes every waiter.
     *
     * <p>For a change of leader and for shutdown: in both cases no further push will arrive,
     * and waiters have to wake on this local event to discover that what they were waiting on
     * is gone.
     */
    void signalAll() {
        for (Slot slot : slots.values()) {
            synchronized (slot) {
                slot.seq++;
                slot.notifyAll();
            }
        }
    }

    void clear() {
        signalAll();
        slots.clear();
    }
}
