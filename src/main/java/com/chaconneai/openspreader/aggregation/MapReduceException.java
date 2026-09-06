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
package com.chaconneai.openspreader.aggregation;

import com.chaconneai.openspreader.ProcessingException;

/**
 * A distributed aggregation failed.
 *
 * <p>Like every other component's, it sits under {@link ProcessingException}, so a single
 * {@code @ExceptionHandler(ProcessingException.class)} catches it.
 *
 * <h2>It always means the whole job failed</h2>
 * The first version <b>does not retry shards</b>. Retrying would require {@code map} to be
 * idempotent, and the failed shard <b>may already have emitted part of its intermediate
 * results</b>, so a rerun would double-count -- and a wrong answer is far worse than an
 * exception.
 *
 * <p>To retry, {@code submit} the whole job again from the caller.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
public class MapReduceException extends ProcessingException {

    public MapReduceException(String message) {
        super(message);
    }

    public MapReduceException(String message, Throwable cause) {
        super(message, cause);
    }
}
