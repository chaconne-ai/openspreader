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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Java's native serialisation. The default implementation, with no dependency.
 *
 * <h2>A note on security</h2>
 * Deserialising arbitrary byte streams is a well-known attack surface. The premise here is
 * that <b>messages come only from members of the same cluster</b>: spreader validates the
 * cluster name, and the work port should never be exposed to the public internet. Even so:
 * <ul>
 *   <li>keep the cluster network internal</li>
 *   <li>expose only methods annotated {@code @MultiProcessingCall} to remote invocation --
 *       this is enforced, not advised</li>
 *   <li>where security matters, prefer the typed {@code decode(byte[], Class)}: it does not
 *       instantiate whatever the byte stream asks it to</li>
 * </ul>
 *
 * <p>{@link #decode(byte[], Class)} checks that the decoded object is of the expected type.
 * That does not stop an attack during construction -- a malicious class can act inside
 * {@code readObject} -- but it does surface a type mismatch on the spot, rather than as a
 * ClassCastException somewhere later.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class JdkObjectCodec implements ObjectCodec {

    /** Stateless, so one shared instance is enough. */
    public static final JdkObjectCodec INSTANCE = new JdkObjectCodec();

    private static final byte[] EMPTY = new byte[0];

    @Override
    public byte[] encode(Object value) {
        if (value == null) {
            return EMPTY;
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream(256);
        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(value);
        } catch (IOException e) {
            throw new SerializationException("serialisation failed: " + value.getClass().getName()
                    + ". It and all its fields must implement Serializable", e);
        }
        return bos.toByteArray();
    }

    @Override
    public Object decode(byte[] data, Class<?> type) {
        Object value = decode(data);
        if (value != null && type != null && !type.isPrimitive() && !type.isInstance(value)) {
            throw new SerializationException("deserialised a " + value.getClass().getName()
                    + ", expected " + type.getName());
        }
        return value;
    }

    @Override
    public Object decode(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            // The peer has this class and this process does not, almost always because the
            // two sides are on different versions
            throw new SerializationException("deserialisation failed: this process has no class "
                    + e.getMessage(), e);
        } catch (IOException e) {
            throw new SerializationException("deserialisation failed: the bytes are not valid Java "
                    + "serialisation. Is spring.spreader.multiprocessing.serialization set the "
                    + "same on both sides?", e);
        }
    }

    @Override
    public SerializationType type() {
        return SerializationType.JDK;
    }
}
