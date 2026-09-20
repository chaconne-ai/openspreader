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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * The one method the cluster may call. Every node of every graph arrives through here.
 *
 * <h2>Why one entry point and not one per node</h2>
 * {@code ProcessingPool} dispatches by bean name and method name, and the method has to be
 * annotated {@link MultiProcessingCall}, which is the pool's allow-list. Annotating every
 * node class would put every node's method into that allow-list; this way <b>exactly one</b>
 * method is exposed, and the real allow-list becomes the registry below, which holds only
 * beans that are {@link GraphNode}s in this application.
 *
 * <p>That is the same reasoning {@code MapReduceJob} sets out for not annotating {@code map}
 * and {@code reduce}: a type-based allow-list is narrower than a name-based one, and it
 * cannot be widened by accident.
 *
 * <h2>The name off the wire is never loaded</h2>
 * It is <b>looked up</b>, not resolved. There is no {@code Class.forName} here and no
 * {@code getBean} on an arbitrary string: a name that is not already a registered node bean
 * is refused, so no message from the cluster can cause a class to be loaded or a static
 * initialiser to run. Had the name been resolved instead of looked up, "run this node" would
 * have become "load this class", which is a different and much larger promise.
 *
 * <h2>Two registries, because a class name cannot tell two beans apart</h2>
 * A graph refers to a node either by <b>bean name</b> or by class. The bean name is the one
 * that scales: a class may have several beans, each configured differently, and that is
 * exactly what a graph built from data needs. The class name is kept because an application
 * that never does that should not have to name its beans.
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

    /**
     * Bean name to the bean. The other half of the allow-list, and the half that lets one
     * class be several nodes.
     */
    private final Map<String, GraphNode> nodesByBeanName = new LinkedHashMap<>();

    /** The label of this process, so a run can report where each node actually ran. */
    private final String selfLabel;

    /**
     * How a caller's trace is made current here.
     *
     * <p>On this side of the wire, so it is the executing replica's own configuration that
     * decides: it is the process whose tracer would have to understand the carrier.
     */
    private TracePropagation tracing = TracePropagation.logContext();

    /** Replaces the default, which only restores the run's identity into the log context. */
    public void setTracing(TracePropagation tracing) {
        this.tracing = tracing == null ? TracePropagation.logContext() : tracing;
    }

    /**
     * @param nodes     every {@link GraphNode} this process can run. A plain collection
     *                  rather than a Spring type, so that a test, or an application wiring
     *                  this by hand, can build one without a container
     * @param selfLabel how this process identifies itself in a run report
     */
    public NodeDispatcher(Collection<? extends GraphNode> nodes, String selfLabel) {
        this.selfLabel = selfLabel;
        nodes.forEach(node -> nodesByClassName.put(node.getClass().getName(), node));
        log.info("DAG node dispatcher ready on {}: {} node bean(s) registered by class",
                selfLabel, nodesByClassName.size());
    }

    /**
     * The form the auto-configuration uses: the container's {@link GraphNode} beans, by name.
     *
     * <p>Both registries are filled, so a graph may refer to a node either way. A class with
     * <b>several</b> beans is registered under each of their names, and under its class name
     * only if there is exactly one: two beans of one class make the class name ambiguous, and
     * answering an ambiguous request with whichever came first is how a graph comes to run a
     * step configured for something else.
     *
     * @param nodesByName bean name to bean
     */
    public NodeDispatcher(Map<String, ? extends GraphNode> nodesByName, String selfLabel) {
        this.selfLabel = selfLabel;
        Map<String, Integer> perClass = new LinkedHashMap<>();
        nodesByName.forEach((beanName, node) -> {
            nodesByBeanName.put(beanName, node);
            perClass.merge(node.getClass().getName(), 1, Integer::sum);
        });
        nodesByName.forEach((beanName, node) -> {
            String className = node.getClass().getName();
            if (perClass.get(className) == 1) {
                nodesByClassName.put(className, node);
            }
        });
        log.info("DAG node dispatcher ready on {}: {} node bean(s), {} of them reachable by "
                + "class name", selfLabel, nodesByBeanName.size(), nodesByClassName.size());
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
     * @param nodeName  the node's name in that graph, which may differ from the bean's
     * @param target    the bean name, or the class name, looked up in the allow-list
     * @param slice     the channels this node declared it reads, or all of them
     * @param context   which step this is and what it was configured with. Null from an older
     *                  replica, in which case the node is run as if it had no settings
     */
    @MultiProcessingCall
    public NodeOutcome runNode(String graphName, String nodeName, String target,
                               GraphState slice, NodeContext context) {
        GraphNode node = nodesByBeanName.get(target);
        if (node == null) {
            node = nodesByClassName.get(target);
        }
        if (node == null) {
            // Not a node bean here. The ordinary causes are named because guessing between
            // them costs an afternoon
            return NodeOutcome.failure(nodeName, new DagException(
                    "\"" + target + "\" is not a GraphNode bean in this process. Either it is "
                            + "missing a @Component, or the graph names a bean this replica "
                            + "does not have, or this replica is running an older build than "
                            + "the one that started the run. Registered beans: "
                            + nodesByBeanName.keySet()), selfLabel, 0L);
        }

        long startedAt = System.nanoTime();
        NodeContext ctx = context != null ? context
                : new NodeContext(graphName, null, nodeName, Map.of());
        // Two things restored, and both are undone in the finally: whatever the caller's
        // tracer carried, and the run's identity in the log context. A pooled thread that
        // kept either would put this run's marks on the next run's lines
        try (TracePropagation.Scope traced = tracing.restore(ctx.trace());
             TracePropagation.Scope logged = TracePropagation.inLogContext(ctx)) {
            Map<String, Object> updates =
                    node.execute(slice == null ? GraphState.empty() : slice, ctx);
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

    /** For troubleshooting: which nodes this process can run, by bean name and by class. */
    public Set<String> registeredNodes() {
        Set<String> all = new LinkedHashSet<>(nodesByBeanName.keySet());
        all.addAll(nodesByClassName.keySet());
        return all;
    }
}
