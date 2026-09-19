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

import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link HttpGraphNode} on Spring's {@code RestClient}.
 *
 * <p>Of the three, the one whose shape matches a node's: <b>it is synchronous</b>. A node
 * returns a value, so the call has to be awaited somewhere; here that happens in the client
 * rather than by blocking a reactive chain, as {@link WebClientGraphNode} must.
 *
 * <p>It also lives in {@code spring-web} rather than {@code spring-webflux}, which any Spring
 * Boot web application already has, and it goes through the application's own
 * {@code ClientHttpRequestFactory}: the timeouts, the proxy, the interceptors and the
 * observation registry it has already configured apply here too.
 *
 * <p>Both are optional dependencies of this library, and the default remains the JDK's own
 * client. Choose this one with
 * {@code spring.spreader.multiprocessing.dag.http-client=rest-client}.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/09/2026
 */
public class RestClientGraphNode extends HttpGraphNode {

    private final RestClient client;

    /**
     * One client per explicitly declared timeout.
     *
     * <p>{@code RestClient} takes its timeouts from the request factory, not from the call,
     * so honouring a per-node {@code timeoutMs} means a client built for it. They are cached
     * because a graph has a handful of distinct timeouts at most, and building one per call
     * would throw away connection reuse.
     */
    private final Map<Long, RestClient> byTimeout = new ConcurrentHashMap<>();

    /**
     * @param client the application's own, with whatever it has configured on it
     */
    public RestClientGraphNode(RestClient client) {
        this.client = client;
    }

    @Override
    protected HttpAnswer send(HttpCall call) {
        RestClient.RequestBodySpec request = clientFor(call.timeoutMs())
                .method(HttpMethod.valueOf(call.method()))
                .uri(call.url())
                .headers(headers -> call.headers().forEach(headers::set));

        if (call.body() != null) {
            request.body(call.body());
        }

        // Every status comes back as an answer rather than an exception. RestClient's default
        // is to throw for 4xx and 5xx, which would take the decision away from the graph:
        // routing on a 404 is a legitimate thing for a workflow to do
        ResponseEntity<String> response = request.exchange((req, res) -> ResponseEntity
                .status(res.getStatusCode())
                // The headers have to be copied over explicitly. Building the entity from
                // status and body alone drops them, and an ETag or a paging cursor that
                // silently disappears is the kind of bug that is found in production
                .headers(res.getHeaders())
                .body(new String(res.getBody().readAllBytes(), StandardCharsets.UTF_8)), false);

        Map<String, List<String>> headers = new LinkedHashMap<>();
        response.getHeaders().forEach(headers::put);
        return new HttpAnswer(response.getStatusCode().value(), headers, response.getBody());
    }

    /**
     * The client to use for this call.
     *
     * <p>Without a declared timeout it is the application's own, untouched: whatever proxy,
     * truststore, interceptors and timeouts it configured are what the call gets. With one,
     * a variant carrying that read timeout, because that is the only place
     * {@code RestClient} accepts it.
     */
    private RestClient clientFor(long timeoutMs) {
        if (timeoutMs <= 0L) {
            return client;
        }
        return byTimeout.computeIfAbsent(timeoutMs, ms -> {
            JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory();
            factory.setReadTimeout(Duration.ofMillis(ms));
            return client.mutate().requestFactory(factory).build();
        });
    }
}
