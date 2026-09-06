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

import com.chaconneai.spreader.GossipCluster;
import com.chaconneai.spreader.event.GossipListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;

import java.util.ArrayList;
import java.util.List;

/**
 * Attaches the application's {@code GossipListener} beans to the default channel
 * automatically.
 *
 * <p>So an application need only write a {@code @Component implements GossipListener} to
 * receive cluster events, with no {@code cluster.addListener(...)} to remember somewhere
 * -- one step fewer is one step fewer to forget.
 *
 * <h2>Why SmartInitializingSingleton rather than constructor injection</h2>
 * Injecting and resolving {@code ObjectProvider<GossipListener>} inside the
 * {@code GossipCluster} bean method would <b>create a circular dependency</b>: services
 * such as the lock and the cache are themselves {@code GossipListener}s, and they depend
 * on {@code GossipCluster}.
 *
 * <p>{@link SmartInitializingSingleton} is called back <b>after every singleton has been
 * created</b>, by which point everything is in place and they can simply be fetched and
 * attached.
 *
 * <h2>Internal services are skipped</h2>
 * See {@link SelfRegisteringListener} -- they subscribe to channels of their own, and
 * attaching them to the default channel as well would have internal traffic and business
 * messages pollute each other.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MultiProcessingListenerRegistrar implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(MultiProcessingListenerRegistrar.class);

    private final GossipCluster cluster;
    private final ObjectProvider<GossipListener> listeners;

    public MultiProcessingListenerRegistrar(GossipCluster cluster, ObjectProvider<GossipListener> listeners) {
        this.cluster = cluster;
        this.listeners = listeners;
    }

    @Override
    public void afterSingletonsInstantiated() {
        List<String> registered = new ArrayList<>();
        for (GossipListener listener : listeners) {
            if (listener instanceof SelfRegisteringListener) {
                continue;
            }
            cluster.addListener(listener);
            registered.add(listener.getClass().getSimpleName());
        }
        if (registered.isEmpty()) {
            log.debug("No application-defined cluster event listeners were found");
        } else {
            log.info("Registered {} cluster event listener(s) automatically: {}", registered.size(), registered);
        }
    }
}
