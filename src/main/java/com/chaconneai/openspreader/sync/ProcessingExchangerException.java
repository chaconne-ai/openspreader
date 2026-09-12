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
package com.chaconneai.openspreader.sync;

import com.chaconneai.openspreader.ProcessingException;

/**
 * The exchange could not be completed.
 *
 * <h2>What belongs here and what does not</h2>
 * <ul>
 *   <li><b>Here</b> -- the service is closed, no leader can be reached, the leader changed
 *       while a party was waiting, or a protocol response is malformed: cases where the
 *       mechanism itself failed to work</li>
 *   <li><b>Not here</b> -- running out of time is a {@link java.util.concurrent.TimeoutException},
 *       as it is on {@code Exchanger.exchange(V, long, TimeUnit)}, because waiting longer
 *       than allowed is an ordinary outcome rather than a fault. Invalid arguments still
 *       throw {@link IllegalArgumentException}</li>
 * </ul>
 *
 * <h2>The caller still holds its item</h2>
 * This is the useful guarantee. Every path that throws it is one where <b>no pairing
 * happened</b>, so the item passed to {@code exchange} was never handed to anybody and
 * retrying costs nothing. The one exception is documented on
 * {@link ProcessingExchanger#exchange}: an exchange paired on the old leader and not yet
 * collected is lost, and no exception can give it back.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class ProcessingExchangerException extends ProcessingException {

    public ProcessingExchangerException(String message) {
        super(message);
    }

    public ProcessingExchangerException(String message, Throwable cause) {
        super(message, cause);
    }
}
