package com.chaconneai.openspreader.cluster;

/**
 * A marker: this listener manages its own subscription, and auto-configuration should
 * leave it alone.
 *
 * <h2>Why the marker is needed</h2>
 * Auto-configuration attaches every {@code GossipListener} bean in the container to the
 * <b>default channel</b>, so an application need only write a {@code @Component} to
 * receive cluster events, with no manual {@code addListener}.
 *
 * <p>But the services inside this package -- lock, semaphore, cache, latch, barrier,
 * process pool -- are {@code GossipListener}s too, and each subscribes to <b>a channel of
 * its own</b>
 * （{@code spreader.mutex}、{@code spreader.cache} …）。
 * Were auto-configuration to attach them to the default channel as well, two things would
 * happen:
 *
 * <ul>
 *   <li>they would receive <b>the application's business messages</b> and discard them as
 *       malformed -- having parsed them for nothing</li>
 *   <li>the application's listeners would receive <b>their internal traffic</b>, filling
 *       the business log with noise -- exactly what channel isolation was introduced to
 *       prevent</li>
 * </ul>
 *
 * So internal services implement this marker and automatic registration skips them.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface SelfRegisteringListener {
}
