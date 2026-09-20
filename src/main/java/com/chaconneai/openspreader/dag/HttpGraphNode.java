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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A step that calls an HTTP API, described by configuration rather than by code.
 *
 * <p>One bean, any number of nodes. A graph assembled at run time from rows in a table gets
 * its steps this way: each row is a node name and a handful of settings, and nothing has to
 * be compiled against a class.
 *
 * <pre>{@code
 * for (StepRow row : repository.stepsOf(workflowId)) {
 *     graph.node(row.name(), "httpGraphNode", row.config());
 * }
 *
 * // or written out by hand
 * .node("FetchRate", "httpGraphNode", Map.of(
 *         "url", "https://fx.internal/rates/${currency}",
 *         "method", "GET",
 *         "headers", Map.of("Accept", "application/json"),
 *         "writeTo", "rate"))
 * }</pre>
 *
 * <h2>The settings</h2>
 * <table border="1">
 *   <caption>What a node may be given</caption>
 *   <tr><th>Key</th><th>Meaning</th></tr>
 *   <tr><td>{@code url}</td><td><b>Required.</b> {@code ${channel}} in it is replaced with
 *       that channel's value</td></tr>
 *   <tr><td>{@code method}</td><td>GET by default</td></tr>
 *   <tr><td>{@code headers}</td><td>A map. Values take {@code ${channel}} too</td></tr>
 *   <tr><td>{@code body}</td><td>The request body, with the same substitution. Anything but
 *       a string is sent as its {@code toString}, so build JSON with whatever your
 *       application already uses</td></tr>
 *   <tr><td>{@code writeTo}</td><td>The channel the response body goes into. Without it the
 *       response is discarded, which is right for a step whose point is the side effect</td></tr>
 *   <tr><td>{@code statusTo}</td><td>A channel for the status code, when the graph routes
 *       on it</td></tr>
 *   <tr><td>{@code headersTo}</td><td>A channel for the response headers, as a map of name
 *       to values. For the ones that carry meaning: a pagination cursor, an ETag to send
 *       back on the next call, a rate-limit budget a later step routes on</td></tr>
 *   <tr><td>{@code timeoutMs}</td><td>Per request. Without it, whatever the underlying
 *       client is configured with, which is usually what an application wants: it set those
 *       timeouts for a reason</td></tr>
 *   <tr><td>{@code failOnErrorStatus}</td><td>True by default: 4xx and 5xx throw, so
 *       {@code retry(...)} and {@code onFailure()} work as they do for any other node. Set
 *       false to route on {@code statusTo} instead</td></tr>
 *   <tr><td>{@code idempotencyHeader}</td><td>A header name. Given, the node sends
 *       {@link NodeContext#idempotencyKey()} under it, which is what makes a retried call
 *       safe for the far side to recognise</td></tr>
 * </table>
 *
 * <h2>Why this one is in the library when the rule is to build nothing</h2>
 * Because it builds nothing. It reads settings and hands a request to a client somebody else
 * wrote. There is no connection pooling here, no retry logic and no timeout machinery:
 * retries and compensation belong to the graph, and the timeout is one line of whichever
 * client is underneath.
 *
 * <p>What it does add is the <b>only part an application cannot write for itself without
 * help</b>: a node that knows which step it is. That is {@link NodeContext}, and this class
 * is mostly a demonstration that it is enough.
 *
 * <h2>Which client, and how to use another</h2>
 * This class is abstract, and everything above is common to every implementation: the
 * settings, the placeholders, the idempotency key, what counts as a failure. <b>Only
 * {@link #send} differs</b>, and that is the extension point.
 *
 * <ul>
 *   <li>{@link RestClientGraphNode} is the one that ships, and what the auto-configuration
 *       registers. Spring's {@code RestClient} is <b>synchronous, as a node is</b>, and it
 *       goes through the application's own request factory, so the timeouts, proxy,
 *       interceptors and observations already configured there apply here too</li>
 *   <li><b>Your own</b>: extend this class, implement {@code send}, and declare it as a bean
 *       named {@code httpGraphNode}. The auto-configuration stands aside when one is already
 *       there. That is the way to use OkHttp, Apache HttpClient, a signed-request wrapper,
 *       or a stub in a test, and it is why this class is abstract rather than final</li>
 * </ul>
 *
 * <p>{@code spring-web} is an <b>optional</b> dependency of this library: it is not added to
 * an application that does not already have it. Such an application gets no
 * {@code httpGraphNode} bean, and writing one is the paragraph above.
 *
 * <h2>Declare it local</h2>
 * {@code .local(...)}. The call leaves the process either way, so handing it to a peer first
 * adds a hop, a serialisation and a second thing that can fail. The exception is a step that
 * fans out into many calls at once, where spreading them across the cluster is the point.
 *
 * <h2>What it deliberately does not do</h2>
 * No JSON parsing: the response body reaches a channel as text, and what it means is the
 * application's business. No authentication beyond headers you set. No per-call circuit
 * breaking. Each of those would mean choosing a library on behalf of every user of this
 * engine, and each has a better home in the application.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/09/2026
 */
public abstract class HttpGraphNode extends ExternalNode {

    private static final Logger log = LoggerFactory.getLogger(HttpGraphNode.class);

    /** The bean name the auto-configuration registers an implementation under. */
    public static final String BEAN_NAME = "httpGraphNode";

    /**
     * One request, already resolved: no placeholders left, no settings to interpret.
     *
     * @param headers in iteration order, so a client that cares about ordering keeps it
     */
    public record HttpCall(String method, String url, Map<String, String> headers,
                           String body, long timeoutMs) {
    }

    /**
     * What came back. Nothing is decided here; the base class judges the status.
     *
     * @param headers every response header, values kept as a list because a header may
     *                legitimately appear more than once. {@code Set-Cookie} is the one
     *                everybody meets, and flattening it would lose all but one cookie
     */
    public record HttpAnswer(int status, Map<String, List<String>> headers, String body) {

        public HttpAnswer {
            headers = headers == null ? Map.of() : headers;
        }

        /** Without headers, for an implementation that has none to report. */
        public HttpAnswer(int status, String body) {
            this(status, Map.of(), body);
        }

        /**
         * The first value of a header, or null.
         *
         * <p>Case-insensitive, because HTTP header names are, and an implementation may hand
         * them over in whatever case the server sent.
         */
        public String header(String name) {
            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(name)) {
                    List<String> values = entry.getValue();
                    return values == null || values.isEmpty() ? null : values.get(0);
                }
            }
            return null;
        }
    }

    /**
     * Sends it, and waits.
     *
     * <p>The only thing an implementation has to provide. It must <b>not</b> throw for a 4xx
     * or 5xx: whether that is a failure is the graph's decision, made from the
     * {@code failOnErrorStatus} setting, and an implementation that threw would take it away.
     * Throwing for a connection that could not be made, or a timeout, is right.
     */
    protected abstract HttpAnswer send(HttpCall call) throws Exception;

    /**
     * Never called: this node is configured per step, so the engine always hands it a
     * {@link NodeContext}.
     *
     * <p>It is here because {@link ExternalNode} declares it, and it throws rather than
     * guessing, because a node with no settings has no address to call.
     */
    @Override
    protected final Map<String, Object> call(GraphState state) {
        throw new DagException("an HttpGraphNode is configured per node and cannot run "
                + "without its context. Declare it with node(name, beanName, config), and "
                + "run the graph through the engine rather than calling execute() directly");
    }

    @Override
    protected final Map<String, Object> call(GraphState state, NodeContext context)
            throws Exception {
        String url = resolve(context.require("url"), state);
        String method = context.getString("method", "GET").toUpperCase(Locale.ROOT);
        String body = context.getString("body");
        long timeoutMs = context.getLong("timeoutMs", 0L);

        Map<String, String> headers = new LinkedHashMap<>();
        context.getMap("headers").forEach((name, value) ->
                headers.put(name, resolve(value, state)));

        // The same key on every attempt of this step, which is what lets the far side
        // recognise a retry rather than performing the work twice
        String idempotencyHeader = context.getString("idempotencyHeader");
        if (idempotencyHeader != null && !idempotencyHeader.isBlank()) {
            headers.put(idempotencyHeader, context.idempotencyKey());
        }

        HttpAnswer answer = send(new HttpCall(method, url, headers,
                body == null ? null : resolve(body, state), timeoutMs));

        if (log.isDebugEnabled()) {
            log.debug("Graph {} node {} called {} {} and got {}",
                    context.graph(), context.node(), method, url, answer.status());
        }

        boolean failOnError = context.getBoolean("failOnErrorStatus", true);
        if (failOnError && answer.status() >= 400) {
            // Thrown, so that retry(...) and onFailure() mean the same thing here as they do
            // for every other node. Routing on the status instead is one setting away
            throw new DagException("node " + context.node() + " called " + method + " " + url
                    + " and got " + answer.status() + ": " + brief(answer.body()));
        }

        Map<String, Object> updates = new LinkedHashMap<>(2);
        String writeTo = context.getString("writeTo");
        if (writeTo != null && !writeTo.isBlank()) {
            updates.put(writeTo, answer.body());
        }
        String statusTo = context.getString("statusTo");
        if (statusTo != null && !statusTo.isBlank()) {
            updates.put(statusTo, answer.status());
        }
        String headersTo = context.getString("headersTo");
        if (headersTo != null && !headersTo.isBlank()) {
            // Copied into plain collections: this goes into a channel, and a channel crosses
            // the network to whichever node reads it next
            Map<String, List<String>> received = new LinkedHashMap<>();
            answer.headers().forEach((name, values) ->
                    received.put(name, values == null ? List.of() : new ArrayList<>(values)));
            updates.put(headersTo, received);
        }
        return updates;
    }

    /**
     * Replaces {@code ${channel}} with that channel's value.
     *
     * <p>Deliberately the smallest thing that works, and deliberately not SpEL: a URL or a
     * header is a piece of text with holes in it, and the moment expressions are allowed
     * there, a definition in a database becomes code in a database.
     *
     * <p>An unknown channel is left alone rather than replaced with "null", so a mistake
     * surfaces as a request to a visibly wrong address instead of a plausible one.
     */
    protected String resolve(String template, GraphState state) {
        if (template == null || template.indexOf("${") < 0) {
            return template;
        }
        StringBuilder out = new StringBuilder(template.length());
        int at = 0;
        while (at < template.length()) {
            int open = template.indexOf("${", at);
            if (open < 0) {
                out.append(template, at, template.length());
                break;
            }
            int close = template.indexOf('}', open + 2);
            if (close < 0) {
                out.append(template, at, template.length());
                break;
            }
            out.append(template, at, open);
            String channel = template.substring(open + 2, close);
            Object value = state.get(channel);
            out.append(value == null ? template.substring(open, close + 1) : value);
            at = close + 1;
        }
        return out.toString();
    }

    /** Enough of a failed response to identify it, without putting a page of HTML in a log. */
    private static String brief(String body) {
        if (body == null || body.isEmpty()) {
            return "(no body)";
        }
        String oneLine = body.replaceAll("\\s+", " ").trim();
        return oneLine.length() <= 200 ? oneLine : oneLine.substring(0, 200) + "...";
    }
}
