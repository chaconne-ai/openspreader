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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.server.context.WebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.Environment;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Publishes this process's web address into the node metadata, so that every other node can
 * reach this one's HTTP endpoints -- {@code /actuator/health} above all -- knowing nothing
 * but the member list.
 *
 * <h2>Why it cannot be written by hand</h2>
 * The gossip port is not the HTTP port. Gossip tells everyone {@code host:22000}, while the
 * application answers on {@code 8080} under some context path -- and nothing in the member
 * list says which. So "go and look at that node's health" cannot be done from inside the
 * cluster at all, which is exactly what is wanted when one node is misbehaving and something
 * has to look at it from outside.
 *
 * <p>Writing the port into {@code spring.spreader.metadata} by hand does not work either:
 * with {@code server.port=0}, or several instances on one machine, or a container mapping
 * the port, <b>the real port is not known until the web server has bound</b>. Only the
 * actual bound port is worth publishing.
 *
 * <h2>What is published</h2>
 * <table border="1">
 *   <caption>The metadata keys, named after the Spring properties they come from</caption>
 *   <tr><th>Key</th><th>Meaning</th><th>When it is absent</th></tr>
 *   <tr><td>{@link #SERVER_PORT}</td><td>the port the web server bound to</td>
 *       <td>never, in a web application</td></tr>
 *   <tr><td>{@link #MANAGEMENT_PORT}</td><td>the port the actuator answers on</td>
 *       <td>{@code management.server.port} is negative -- no HTTP actuator at all</td></tr>
 *   <tr><td>{@link #CONTEXT_PATH}</td><td>the servlet context path</td>
 *       <td>deployed at the root</td></tr>
 *   <tr><td>{@link #SERVLET_PATH}</td><td>the dispatcher servlet's path</td>
 *       <td>mapped at the root, which is the default</td></tr>
 *   <tr><td>{@link #ENDPOINTS_BASE_PATH}</td><td>the actuator's base path</td>
 *       <td>left at {@code /actuator}</td></tr>
 *   <tr><td>{@link #MANAGEMENT_BASE_PATH}</td><td>the management server's own base path</td>
 *       <td>no management port of its own, or no base path under it</td></tr>
 * </table>
 *
 * <p>Every path is <b>omitted when it is the root, or the Spring default</b>, so a project
 * that configures none of them -- the great majority -- carries two entries rather than six.
 * A reader builds a URL by treating an absent key as its default:
 *
 * <pre>{@code
 * Node node = ...;
 * Map<String, String> meta = node.metadata();
 * String health = "http://" + node.host() + ":" + meta.get("management.port")
 *         + meta.getOrDefault("server.servlet.context-path", "")
 *         + meta.getOrDefault("management.endpoints.web.base-path", "/actuator")
 *         + "/health";
 * // -> http://10.0.0.7:8081/actuator/health
 * }</pre>
 *
 * <p>Which keys belong in that expression depends on where the actuator sits: on a
 * management port of its own it hangs off {@link #MANAGEMENT_BASE_PATH} and the
 * application's own {@link #CONTEXT_PATH} and {@link #SERVLET_PATH} play no part; sharing
 * the server port, it sits behind both of them. What is published here is the raw
 * configuration -- the assembling is the reader's, because only the reader knows which
 * endpoint it is after.
 *
 * <h2>Why the ports are told apart by event and the paths by configuration</h2>
 * Spring Boot starts a management context of its own only when the management port differs,
 * and that context publishes a {@link WebServerInitializedEvent} of its own, which
 * propagates up to the parent context and so reaches this listener too. The two events are
 * told apart by {@code getApplicationContext().getServerNamespace()} --
 * {@code "management"} for the management context -- and are handled <b>independently of
 * each other's order</b>: each merges its own keys into whatever metadata is already there,
 * because the child context's event usually arrives <i>before</i> the parent's.
 *
 * <p>A port has to come from the event because a bound port of 0 is not the port in use. The
 * paths have no such problem: nothing rewrites them at bind time, so the configuration is
 * the whole truth about them.
 *
 * <h2>Why this goes into metadata rather than a new field on the node</h2>
 * Metadata is precisely the place for "things about this node that the framework itself does
 * not care about". A field would have to travel in the protocol, and every node would carry
 * four values of interest only to whoever is looking at HTTP. Metadata already gossips out,
 * and an application is free to add anything of its own beside these.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 05/09/2026
 */
public class WebAddressMetadataListener implements ApplicationListener<WebServerInitializedEvent> {

    /** The metadata key holding the port the application's web server bound to. */
    public static final String SERVER_PORT = "server.port";

    /** The metadata key holding the port the actuator answers on. */
    public static final String MANAGEMENT_PORT = "management.port";

    /** The metadata key holding the servlet context path; absent means the root. */
    public static final String CONTEXT_PATH = "server.servlet.context-path";

    /** The metadata key holding the dispatcher servlet's path; absent means the root. */
    public static final String SERVLET_PATH = "spring.mvc.servlet.path";

    /** The metadata key holding the actuator's base path; absent means {@code /actuator}. */
    public static final String ENDPOINTS_BASE_PATH = "management.endpoints.web.base-path";

    /** The metadata key holding the management server's own base path; absent means the
     *  root of the management port. */
    public static final String MANAGEMENT_BASE_PATH = "management.server.base-path";

    /** The server namespace Spring Boot gives the management context. */
    private static final String MANAGEMENT_NAMESPACE = "management";

    /** Spring Boot's default for {@link #ENDPOINTS_BASE_PATH}. */
    private static final String DEFAULT_ENDPOINTS_BASE_PATH = "/actuator";

    private static final Logger log = LoggerFactory.getLogger(WebAddressMetadataListener.class);

    private final GossipCluster cluster;
    private final Environment environment;

    public WebAddressMetadataListener(GossipCluster cluster, Environment environment) {
        this.cluster = cluster;
        this.environment = environment;
    }

    @Override
    public void onApplicationEvent(WebServerInitializedEvent event) {
        int port = event.getWebServer().getPort();
        if (port <= 0) {
            // A mock web server bound to nothing. There is no address to publish, and
            // publishing 0 or -1 would only have another node try to connect to it
            return;
        }

        Map<String, String> entries = new LinkedHashMap<>(6);
        if (MANAGEMENT_NAMESPACE.equals(event.getApplicationContext().getServerNamespace())) {
            // A management port of its own. The application's context path and servlet path
            // have nothing to do with it -- the actuator there hangs off
            // management.server.base-path instead
            entries.put(MANAGEMENT_PORT, String.valueOf(port));
            putIfNotRoot(entries, MANAGEMENT_BASE_PATH, MANAGEMENT_BASE_PATH);
        } else {
            entries.put(SERVER_PORT, String.valueOf(port));
            if (managementSharesServerPort(port)) {
                entries.put(MANAGEMENT_PORT, String.valueOf(port));
            }
            putIfNotRoot(entries, CONTEXT_PATH, CONTEXT_PATH);
            putIfNotRoot(entries, SERVLET_PATH, SERVLET_PATH);
        }
        // Published from both branches: it applies wherever the actuator ended up, and
        // whichever event fires first, the merge below makes the second one a no-op
        if (!DEFAULT_ENDPOINTS_BASE_PATH.equals(
                environment.getProperty(ENDPOINTS_BASE_PATH, DEFAULT_ENDPOINTS_BASE_PATH))) {
            putIfNotRoot(entries, ENDPOINTS_BASE_PATH, ENDPOINTS_BASE_PATH);
        }
        publish(entries);
    }

    /**
     * Whether the actuator answers on the server port itself.
     *
     * <p>A management context of its own is started <b>only</b> when
     * {@code management.server.port} names a different port. Left unset -- which is the
     * commonest deployment there is -- or set to the same number, the actuator lives on the
     * server port and <b>no second event will ever arrive</b> to say so. So this case has to
     * be settled from the configuration, or {@code management.port} would be missing on
     * exactly the deployment that configures nothing at all.
     *
     * <p>A negative port switches the actuator's HTTP exposure off altogether; then neither
     * this event nor any later one publishes {@code management.port}, and its absence is the
     * truthful answer.
     */
    private boolean managementSharesServerPort(int serverPort) {
        Integer managementPort = environment.getProperty("management.server.port", Integer.class);
        return managementPort == null || managementPort == serverPort;
    }

    /**
     * Records the path configured under {@code property}, unless it is the root.
     *
     * <p>The key and the property are one and the same name: what is published is the
     * configuration under its own name, so that a reader looking at the metadata and someone
     * looking at {@code application.yml} are reading the same thing.
     *
     * <p>The root is what an unconfigured project has, and a key whose value is empty or
     * {@code "/"} says nothing a reader could not have assumed. Left out, the commonest
     * deployment of all carries two entries instead of six.
     *
     * <p>What is recorded is normalised to "a leading slash and no trailing one", so that
     * concatenating it never produces {@code //} and never runs two segments together --
     * {@code /app}, {@code app/} and {@code /app/} all end up as {@code /app}.
     */
    private void putIfNotRoot(Map<String, String> entries, String key, String property) {
        String path = environment.getProperty(property);
        if (path == null) {
            return;
        }
        String trimmed = path.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        if (trimmed.isEmpty()) {
            return;
        }
        entries.put(key, trimmed.startsWith("/") ? trimmed : "/" + trimmed);
    }

    /**
     * Merges the entries into this node's metadata and gossips the result out.
     *
     * <p>Merged rather than replaced: whatever the application put in
     * {@code spring.spreader.metadata} has to survive, and the two events arrive separately.
     *
     * <p>Unchanged values are dropped without an update. Every update raises the incarnation
     * number and broadcasts, and a context restarted by devtools would otherwise burn a
     * round of gossip on numbers that changed nobody's view of the cluster.
     */
    private synchronized void publish(Map<String, String> entries) {
        if (!cluster.isRunning()) {
            // The cluster stopped between the web server binding and this event, or the
            // application supplied a GossipCluster of its own that was never started. Not
            // worth failing startup over -- this address is a convenience, not a dependency
            log.warn("The cluster is not running; the web address {} was not published to the "
                    + "node metadata", entries);
            return;
        }

        Map<String, String> merged = new LinkedHashMap<>(cluster.self().metadata());
        boolean changed = false;
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            if (!entry.getValue().equals(merged.put(entry.getKey(), entry.getValue()))) {
                changed = true;
            }
        }
        if (!changed) {
            return;
        }

        cluster.updateMetadata(merged);
        log.info("The web address has been published to this node's metadata: {}", entries);
    }
}
