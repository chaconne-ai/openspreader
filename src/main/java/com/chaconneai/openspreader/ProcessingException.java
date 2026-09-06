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
 * The <b>common supertype</b> of every exception the multiprocessing components throw.
 *
 * <h2>Why such a supertype exists</h2>
 * A caller often does not care whether the problem was in the cache or in the task pool,
 * only that it was a failure somewhere along the multiprocessing path rather than a fault
 * in its own business logic. With a common supertype, one {@code catch} or one
 * {@code @ExceptionHandler(ProcessingException.class)} covers the whole path, with no need
 * to enumerate components -- and an enumeration is certain to miss one when a component is
 * added.
 *
 * <p>It is unchecked. Multiprocessing failures mostly arise deep in the call stack -- in an
 * interceptor, mid-serialisation, on a gossip callback -- and forcing every layer to
 * declare {@code throws} would only pollute the signatures with something unrelated to the
 * business.
 *
 * <h2>The subclasses are siblings, not a chain</h2>
 * Each component's exception extends this class <b>directly</b>; none is a parent of
 * another:
 * {@link com.chaconneai.openspreader.cache.ProcessingCacheException}、
 * {@link com.chaconneai.openspreader.pooling.ProcessingPoolException}、
 * {@link com.chaconneai.openspreader.sync.ProcessingMutexException}、
 * {@link com.chaconneai.openspreader.sync.ProcessingSemaphoreException}、
 * {@link com.chaconneai.openspreader.scheduling.ProcessingSchedulingException}、
 * {@link com.chaconneai.openspreader.serialization.SerializationException}、
 * {@link com.chaconneai.openspreader.rpc.RpcException}、
 * {@link com.chaconneai.openspreader.rpc.RpcOverloadException}。
 *
 * <p>The last two deserve particular attention: {@code RpcOverloadException} is
 * <b>deliberately</b> not a subclass of {@code RpcException}, so that narrowing
 * {@code retryableExceptions()} to {@code RpcException} excludes exactly the case where a
 * request was turned away by the rate limiter and retrying would pour fuel on the fire.
 * Do not move it under {@code RpcException} to make the hierarchy look tidier.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 22/08/2026
 */
public class ProcessingException extends RuntimeException {

    public ProcessingException(String message) {
        super(message);
    }

    public ProcessingException(String message, Throwable cause) {
        super(message, cause);
    }

}
