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

import com.chaconneai.openspreader.ProcessingException;

/**
 * Something went wrong building or running a graph.
 *
 * <h2>What belongs here and what does not</h2>
 * <ul>
 *   <li><b>Here</b> at build time: a cycle, an unreachable node, two parallel branches
 *       writing one channel with no reducer declared. Everything {@code compile()} refuses</li>
 *   <li><b>Here</b> at run time: no coordinator could be taken, a node's bean is missing on
 *       the executing replica, the state could not be read back</li>
 *   <li><b>Not here</b>: the exception a node's own code threw. That is handed back
 *       untouched on {@code RunResult#failure()}, because wrapping an application's own
 *       failure in a framework type only makes it harder to catch what was actually thrown</li>
 * </ul>
 *
 * <p>It extends openspreader's {@link ProcessingException}, so an application that already
 * catches that one catches this too.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class DagException extends ProcessingException {

    public DagException(String message) {
        super(message);
    }

    public DagException(String message, Throwable cause) {
        super(message, cause);
    }
}
