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

import com.chaconneai.openspreader.ProcessingException;

/**
 * Something went wrong dispatching a task: serialisation failed, the method is not
 * exposed, remote execution failed, or it timed out.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class ProcessingPoolException extends ProcessingException {

    public ProcessingPoolException(String message) {
        super(message);
    }

    public ProcessingPoolException(String message, Throwable cause) {
        super(message, cause);
    }
}
