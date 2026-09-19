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

import org.slf4j.MDC;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Carries the caller's trace across a dispatch, so that one run reads as one thing.
 *
 * <h2>The problem it solves</h2>
 * A node runs on another replica, in another process, on a thread that knows nothing about
 * the request that started the run. Without something carried across, a log line from that
 * node cannot be tied to anything: the run is one story told in five places, with no way to
 * put them back together.
 *
 * <h2>What comes for free</h2>
 * The default implementation carries the run's identity and puts it in the logging
 * {@link MDC} on the far side, together with the graph and node names. That costs nothing and
 * no dependency, and it is enough for the question people actually ask a log:
 * <b>what else happened in this run</b>.
 *
 * <pre>
 * # logback pattern, on every replica
 * %d %-5level [%X{dag.run}/%X{dag.node}] %logger{0} - %msg%n
 * </pre>
 *
 * <h2>What needs your tracer</h2>
 * A real distributed trace, where the node's work appears as a child span of the request
 * that started it, needs the trace context in the form your tracer understands: the W3C
 * {@code traceparent} header or whatever else it propagates. This engine does not depend on
 * a tracing library and so cannot produce one, but the two methods below are exactly the
 * shape an injector and an extractor have:
 *
 * <pre>{@code
 * @Bean
 * TracePropagation dagTracePropagation(Propagator propagator, Tracer tracer) {
 *     return new TracePropagation() {
 *         public Map<String, String> capture() {
 *             Map<String, String> carrier = new LinkedHashMap<>();
 *             propagator.inject(tracer.currentTraceContext().context(), carrier, Map::put);
 *             return carrier;
 *         }
 *         public Scope restore(Map<String, String> carrier) {
 *             Span span = propagator.extract(carrier, Map::get).start();
 *             Tracer.SpanInScope scope = tracer.withSpan(span);
 *             return () -> { scope.close(); span.end(); };
 *         }
 *     };
 * }
 * }</pre>
 *
 * <p>Ten lines in the application, none of them in this library, and the reason it is done
 * this way: a tracing library is a choice an application makes, and a workflow engine has no
 * business making it for one.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/09/2026
 */
public interface TracePropagation {

    /** Undoes whatever {@link #restore} put in place. */
    @FunctionalInterface
    interface Scope extends AutoCloseable {
        @Override
        void close();
    }

    /** A scope that has nothing to undo. */
    Scope NOTHING_TO_CLOSE = () -> {
    };

    /**
     * What to carry to whichever replica runs the node.
     *
     * <p>Called on the coordinating instance, before each dispatch. Text only: it travels
     * with the node and has to survive serialisation, and a carrier of plain strings is what
     * every propagation format is defined against.
     */
    Map<String, String> capture();

    /**
     * Makes that context current for as long as the node runs.
     *
     * <p>Called on the executing replica. Whatever is put in place must be undone by the
     * returned scope, or a pooled thread keeps it for the next node that lands on it, which
     * is how one run's identity ends up on another run's log lines.
     */
    Scope restore(Map<String, String> carrier);

    /**
     * The one in use unless the application supplies another: the run's own identity, in the
     * log context.
     *
     * <p>Not a trace, and it does not pretend to be. It answers "what else happened in this
     * run" across every replica, which is the question a log gets asked, and it needs
     * nothing but slf4j.
     */
    static TracePropagation logContext() {
        return new TracePropagation() {

            @Override
            public Map<String, String> capture() {
                // Nothing to capture: what the far side needs is on the NodeContext already,
                // and inventing an identity here would give the run two of them
                return Map.of();
            }

            @Override
            public Scope restore(Map<String, String> carrier) {
                return NOTHING_TO_CLOSE;
            }
        };
    }

    /**
     * Puts the run, graph and node into the logging context for the length of one node.
     *
     * <p>Kept here rather than in the dispatcher so that an application replacing this
     * interface can still call it, and so that the keys are written down in one place.
     */
    static Scope inLogContext(NodeContext context) {
        Map<String, String> restore = new LinkedHashMap<>(3);
        put(restore, "dag.run", context.runId());
        put(restore, "dag.graph", context.graph());
        put(restore, "dag.node", context.node());
        return () -> restore.forEach((key, previous) -> {
            if (previous == null || previous.isEmpty()) {
                MDC.remove(key);
            } else {
                MDC.put(key, previous);
            }
        });
    }

    private static void put(Map<String, String> restore, String key, String value) {
        // Remember what was there, blank included: a pooled thread that keeps one run's id
        // after the node is over puts it on the next run's lines
        restore.put(key, MDC.get(key) == null ? "" : MDC.get(key));
        if (value != null) {
            MDC.put(key, value);
        } else {
            MDC.remove(key);
        }
    }
}
