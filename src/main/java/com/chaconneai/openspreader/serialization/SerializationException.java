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
package com.chaconneai.openspreader.serialization;

import com.chaconneai.openspreader.ProcessingException;

/**
 * Serialising or deserialising an object failed.
 *
 * <h2>Why it is unchecked</h2>
 * A serialisation failure is almost always <b>a mistake fixed at the point of writing the
 * code</b> rather than something that happens occasionally at runtime: an argument class
 * that forgot to implement {@code Serializable}, a missing no-argument constructor Kryo
 * needs, the two sides configured with different serialisations. All of them need a code or
 * configuration change to put right, and catching one at the call site achieves nothing --
 * there is no other path from there.
 *
 * <p>A checked exception would also spread the pollution: {@code ObjectCodec} is called from
 * task dispatch, RPC and the cache, and each of them would have to either rethrow or wrap,
 * leaving {@code catch (IOException e)} blocks everywhere that can do nothing about it.
 *
 * <h2>The message must name the class</h2>
 * "Serialisation failed" on its own helps nobody. An object graph holds dozens of classes,
 * and the name of the one that went wrong is the only useful piece of information -- so
 * every throw site has to carry it.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class SerializationException extends ProcessingException {

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
