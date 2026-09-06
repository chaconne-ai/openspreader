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
package com.chaconneai.openspreader;

/**
 * The granularity of a coordination object -- a lock, a semaphore, and so on.
 *
 * <p>Granularity decides <b>who contends with whom</b>, and it works by prefixing the name
 * with a different namespace. One name under two granularities is two unrelated objects.
 *
 * <p>It lives at this level because locks and semaphores follow the same granularity rules,
 * and a separate enum in each would only let the two drift apart over time.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum Scope {

    /**
     * Cluster level: the granularity is {@code clusterName}.
     *
     * <p>One instance shared cluster-wide, <b>with no distinction between applications</b> --
     * the order service and the reporting service contend with each other when they use the
     * same name. It suits "only one party cluster-wide may do this" and "only N cluster-wide
     * may proceed": a database migration, or a total call quota against a third party.
     */
    CLUSTER,

    /**
     * Application level: the granularity is {@code clusterName + applicationName}.
     *
     * <p>It acts <b>only among instances of the same application</b>. Five replicas of the
     * order service share one quota, and the reporting service using the same name is
     * unaffected. It suits "all instances of this application together may run only N": a
     * scheduled task, or the consumer concurrency on a message queue.
     */
    APPLICATION;

    /**
     * Assembles the full name.
     *
     * @param clusterName     the cluster name
     * @param applicationName the application name; used only by {@link #APPLICATION}
     * @param key             the application's own name, any combination of strings
     */
    public String qualify(String clusterName, String applicationName, String key) {
        return switch (this) {
            case CLUSTER -> clusterName + "/*/" + key;
            case APPLICATION -> clusterName + "/" + applicationName + "/" + key;
        };
    }
}
