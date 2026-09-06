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
package com.chaconneai.openspreader.rpc;

import com.chaconneai.openspreader.ProcessingException;

/**
 * A remote call failed <b>at the communication level</b>.
 *
 * <h2>It is a different thing from an exception thrown by the peer's business logic</h2>
 * That distinction is the foundation of the whole retry policy:
 * <ul>
 *   <li><b>This exception</b> -- no usable instance was found, the connection failed, it
 *       timed out, the response would not decode. Trying another instance is worthwhile,
 *       because the problem is most likely in that one instance</li>
 *   <li><b>A business exception</b> -- thrown by the peer's own method, and restored and
 *       rethrown to the caller <b>as it was</b>. Retrying merely repeats the same mistake:
 *       failed argument validation still fails on the eight hundredth attempt</li>
 * </ul>
 *
 * <p>So narrowing {@link RpcClient#retryableExceptions()} to this class is the recommended
 * production setting. The default is {@code Exception}, retrying both kinds -- a
 * conservative "better one attempt too many" choice, not a recommendation.
 *
 * <h2>How business exceptions are restored</h2>
 * The peer's exception class may not exist in this process, since the two sides carry
 * different dependencies. Where it can be restored to its original type it is; where it
 * cannot, it degenerates to this exception with the original class name and description in
 * the message. No information is lost, only the type differs -- so code catching a specific
 * type should be prepared for that.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class RpcException extends ProcessingException {

    public RpcException(String message) {
        super(message);
    }

    public RpcException(String message, Throwable cause) {
        super(message, cause);
    }
}
