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
package com.chaconneai.openspreader.dag;

import com.chaconneai.openspreader.pooling.MultiProcessingCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The one method the cluster may call. Every node of every graph arrives through here.
 *
 * <h2>Why one entry point and not one per node</h2>
 * {@code ProcessingPool} dispatches by bean name and method name, and the method has to be
 * annotated {@link MultiProcessingCall}, which is the pool's allow-list. Annotating every
 * node class would put every node's method into that allow-list; this way <b>exactly one</b>
 * method is exposed, and the real allow-list becomes {@link #nodesByClassName}, which holds
 * only classes that are {@link GraphNode} beans in this application.
 *
 * <p>That is the same reasoning {@code MapReduceJob} sets out for not annotating {@code map}
 * and {@code reduce}: a type-based allow-list is narrower than a name-based one, and it
 * cannot be widened by accident.
 *
 * <h2>The class name off the wire is never loaded</h2>
 * It is <b>looked up</b>, not resolved. There is no {@code Class.forName} here: a name that
 * is not already a registered node bean is refused, so no message from the cluster can cause
 * a class to be loaded or a static initialiser to run. Had the name been resolved instead of
 * looked up, "run this node" would have become "load this class", which is a different and
 * much larger promise.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class NodeDispatcher {

    /** The bean name the engine dispatches to. Referenced once, from {@link GraphRunner}. */
    public static final String BEAN_NAME = "dagNodeDispatcher";

    /** The method name the engine dispatches to. */
    public static final String METHOD_NAME = "runNode";

    private static final Logger log = LoggerFactory.getLogger(NodeDispatcher.class);

    /**
     * Class name to the bean that implements it. This is the allow-list.
     *
     * <p>Built once at construction from the container's {@link GraphNode} beans. A node
     * class that is not a bean is simply absent, and a request naming it is refused with a
     * message saying so, which is the error somebody actually wants when they forget a
     * {@code @Component}.
     */
    private final Map<String, GraphNode> nodesByClassName = new LinkedHashMap<>();

    /** The label of this process, so a run can report where each node actually ran. */
    private final String selfLabel;

    /**
     * @param nodes     every {@link GraphNode} this process can run. A plain collection
     *                  rather than a Spring type, so that a test, or an application wiring
     *                  this by hand, can build one without a container
     * @param selfLabel how this process identifies itself in a run report
     */
    public NodeDispatcher(Collection<? extends GraphNode> nodes, String selfLabel) {
        this.selfLabel = selfLabel;
        nodes.forEach(node -> nodesByClassName.put(node.getClass().getName(), node));
        log.info("Dagger node dispatcher ready on {}: {} node bean(s) registered",
                selfLabel, nodesByClassName.size());
    }

    /**
     * Runs one node and reports what it changed.
     *
     * <p>It never throws for a node's own failure. A node that throws comes back as a
     * {@link NodeOutcome#failure}, because the exceptional path of the pool's future cannot
     * say <b>which</b> node failed or how long it took, and the coordinator needs both to
     * fill in a {@code RunResult}.
     *
     * @param graphName the graph, for the log line only
     * @param nodeName  the node's name in that graph, which may differ from its class name
     * @param className the node class, looked up in the allow-list
     * @param slice     the channels this node declared it reads, or all of them
     */
    @MultiProcessingCall
    public NodeOutcome runNode(String graphName, String nodeName, String className,
                               GraphState slice) {
        GraphNode node = nodesByClassName.get(className);
        if (node == null) {
            // Not a node bean here. Two ordinary causes, and the message names both because
            // guessing between them costs an afternoon
            return NodeOutcome.failure(nodeName, new DagException(
                    "node class " + className + " is not a GraphNode bean in this process. "
                            + "Either it is missing a @Component, or this replica is running "
                            + "an older build of the application than the one that started "
                            + "the run"), selfLabel, 0L);
        }

        long startedAt = System.nanoTime();
        try {
            Map<String, Object> updates = node.execute(slice == null ? GraphState.empty() : slice);
            long millis = (System.nanoTime() - startedAt) / 1_000_000L;
            if (log.isDebugEnabled()) {
                log.debug("Graph {} node {} finished on {} in {}ms, {} channel(s) changed",
                        graphName, nodeName, selfLabel, millis,
                        updates == null ? 0 : updates.size());
            }
            return NodeOutcome.success(nodeName, updates, selfLabel, millis);
        } catch (Throwable t) {
            long millis = (System.nanoTime() - startedAt) / 1_000_000L;
            log.warn("Graph {} node {} failed on {} after {}ms: {}",
                    graphName, nodeName, selfLabel, millis, t.toString());
            return NodeOutcome.failure(nodeName, t, selfLabel, millis);
        }
    }

    /** For troubleshooting: which node classes this process can run. */
    public java.util.Set<String> registeredNodes() {
        return nodesByClassName.keySet();
    }
}
