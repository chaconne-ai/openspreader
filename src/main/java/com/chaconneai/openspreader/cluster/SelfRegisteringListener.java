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
