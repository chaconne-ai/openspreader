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
 * The request was <b>turned away by the rate limiter</b> and never went out at all -- or
 * the peer never began processing it.
 *
 * <h2>Why it is separate from a plain {@link RpcException}</h2>
 * The two mean entirely different things to a caller:
 * <ul>
 *   <li>{@link RpcException} -- it was attempted and did not succeed. Retrying on another
 *       instance is reasonable</li>
 *   <li><b>This exception</b> -- the system is already at capacity and refused deliberately.
 *       <b>Retrying immediately only makes it worse</b>: it adds pressure to precisely the
 *       thing that is already overloaded</li>
 * </ul>
 *
 * <p>So <b>do not</b> include it in {@link RpcClient#retryableExceptions()}. The default is
 * {@code Exception}, which retries it along with everything else -- the price of a
 * conservative default. Narrowing to {@code RpcException} in production excludes it
 * exactly, since this is deliberately not a subclass of that.
 *
 * <h2>What to do on receiving it</h2>
 * Degrade, queue, tell the user to try again later -- anything that <b>reduces
 * pressure</b>. This exception is a backpressure signal, not a fault: it says the rate
 * limiter is working as designed and holding back requests that would crush what lies
 * downstream.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class RpcOverloadException extends ProcessingException {

    public RpcOverloadException(String message) {
        super(message);
    }
}
