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
package com.chaconneai.openspreader.scheduling;

import com.chaconneai.openspreader.ProcessingException;

/**
 * Something went wrong while cluster mutual exclusion for scheduled tasks was running.
 *
 * <h2>What belongs here and what does not</h2>
 * <ul>
 *   <li><b>Here</b> -- the service is closed, no leader can be reached for a long time, a
 *       protocol response is malformed: cases where the mutual exclusion mechanism itself
 *       has failed to work</li>
 *   <li><b>Not here</b> -- invalid arguments (an empty name, a negative count) still throw
 *       {@link IllegalArgumentException}. That is the caller having written the code
 *       wrongly, has nothing to do with distribution, and a custom exception would only be
 *       one more type to recognise</li>
 * </ul>
 *
 * <p>It is a runtime exception: every one of these needs a code or configuration change to
 * put right, and catching it at the call site opens no other path.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class ProcessingSchedulingException extends ProcessingException {

    public ProcessingSchedulingException(String message) {
        super(message);
    }

    public ProcessingSchedulingException(String message, Throwable cause) {
        super(message, cause);
    }
}
