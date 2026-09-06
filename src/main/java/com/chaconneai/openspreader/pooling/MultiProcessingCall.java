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
package com.chaconneai.openspreader.pooling;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Placed on a method, this makes it <b>able to run in another process</b>.
 *
 * <pre>{@code
 * @Component
 * public class ReportService {
 *
 *     @MultiProcessingCall
 *     public String render(String month) { ... }
 * }
 *
 * // The caller changes nothing; it is still an ordinary method call:
 * String html = reportService.render("2026-08");
 * // but this computation may have been done by another replica of the same application
 * }</pre>
 *
 * <h2>It is two things at once</h2>
 * <ol>
 *   <li><b>The calling side's entry point</b> -- a call to this method in this process is
 *       intercepted by the aspect and handed to the process pool, which picks a replica to
 *       execute it. With only itself as a replica it runs locally, with no network and no
 *       serialisation, so introducing it in a single-machine deployment costs nothing
 *       extra</li>
 *   <li><b>The called side's allow-list</b> -- a remote call is, at bottom, "reflectively
 *       invoke a method in this process, named by a string", and without an allow-list any
 *       node in the cluster could call any method of any bean here, {@code shutdown()} and
 *       {@code deleteAll()} included. The annotation is therefore <b>mandatory</b>: a remote
 *       call request for an unannotated method is refused outright</li>
 * </ol>
 *
 * <h2>Synchronous or asynchronous</h2>
 * {@link #sync()} defaults to true -- the caller blocks for the result, no different from an
 * ordinary method call, save that the computation may have happened on another machine.
 *
 * <p>With {@code sync=false} nothing blocks, and the method's return type must be
 * {@link java.util.concurrent.CompletableFuture}, {@link java.util.concurrent.Future} or
 * {@code void}:
 * <pre>{@code
 * @MultiProcessingCall(sync = false)
 * public CompletableFuture<String> renderAsync(String month) { ... }
 *
 * @MultiProcessingCall(sync = false)        // returning void means fire and forget
 * public void warmCache(String key) { ... }
 * }</pre>
 * A return type that does not fit throws {@link ProcessingPoolException} at call time, with a
 * message saying exactly what to change.
 *
 * <h2>Alongside {@code @Async}</h2>
 * The two may be stacked, meaning "do not block the caller, and it may run in another
 * process".
 *
 * <p>{@code sync=false} is then <b>unnecessary</b>: finding {@code @Async} on the method, the
 * aspect treats the call as asynchronous automatically, because not blocking the caller is
 * the whole point of {@code @Async} and synchronous dispatch would cancel it out. This rule
 * makes the two annotations behave alike whichever aspect ends up on the outside -- nothing
 * rests on the accidents of AOP ordering.
 *
 * <p>Stacked, the return type requirements are those of {@code sync=false}, which are
 * {@code @Async}'s own requirements anyway.
 *
 * <h2>Arguments and return values must be serialisable</h2>
 * They travel between processes. Java's native serialisation is the default, so all of them
 * must implement {@code Serializable}; for Kryo instead, see
 * {@link com.chaconneai.openspreader.serialization.SerializationType}. Primitives, String and
 * the collection types are all fine; a Spring bean or a database connection plainly is not.
 *
 * <p><b>With a single replica nothing is serialised</b>, so serialisation problems stay
 * hidden during local development -- always run with two instances before going live.
 *
 * <h2>How overloading is handled</h2>
 * Matching is by method name plus argument count. Where several overloads share a name and an
 * argument count, the first match is taken -- in which case, give them different names rather
 * than gambling.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MultiProcessingCall {

    /**
     * The name published outwards. Left blank, the method's own name is used.
     *
     * <p>Useful for giving remote callers a stable name while the local method name stays free
     * to change.
     */
    String value() default "";

    /**
     * Whether to block for the result; true by default.
     *
     * <p>When false, the method must return {@code CompletableFuture}, {@code Future} or
     * {@code void}. A method carrying {@code @Async} is treated as false; see the class
     * javadoc.
     */
    boolean sync() default true;

    /**
     * How long a synchronous call waits for its result; 60 seconds by default. It matters only
     * where {@link #sync()} is true -- asynchronously, the caller holds the future and decides
     * for itself.
     *
     * <p><b>0 or negative waits indefinitely</b>, equivalent to {@code future.get()} with no
     * timeout; greater than 0 is equivalent to {@code future.get(timeout, timeUnit)}.
     *
     * <p>Waiting without a deadline for a cross-process result deserves thought: a stuck peer
     * becomes a stuck process here, and with a long call chain that is where a cascading
     * failure starts. Unless "too long" genuinely means nothing for this call -- an overnight
     * batch job, say -- give it a deadline.
     *
     * <p>60 seconds is loose enough not to fire wrongly and tight enough not to hang for ever.
     * Where a method is slow by nature -- a report taking minutes -- widen it there rather than
     * changing the global configuration.
     *
     * <p>Note that it is not the same thing as
     * {@code spring.spreader.multiprocessing.pooling.request-timeout-ms}: that governs "how
     * long one network round trip may take before it counts as failed", while this governs "how
     * long I am willing to wait for this business result", and the latter is usually far
     * larger.
     */
    long timeout() default 60;

    /** The unit of {@link #timeout()}; seconds by default. */
    TimeUnit timeUnit() default TimeUnit.SECONDS;
}
