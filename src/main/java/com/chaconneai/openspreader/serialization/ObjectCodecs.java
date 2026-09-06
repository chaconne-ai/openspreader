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

/**
 * Builds an {@link ObjectCodec} from configuration.
 *
 * <p>Kryo's library is {@code optional}, so without it Kryo cannot be used. This says so
 * <b>at construction time</b> rather than letting a {@code NoClassDefFoundError} surface
 * on the first serialisation -- by which point the error is a long way from the
 * configuration that caused it.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class ObjectCodecs {

    private ObjectCodecs() {
    }

    /**
     * Builds one.
     *
     * @throws SerializationException when the corresponding library is not on the classpath
     */
    public static ObjectCodec create(SerializationType type) {
        if (type == null) {
            return JdkObjectCodec.INSTANCE;
        }
        return switch (type) {
            case JDK -> JdkObjectCodec.INSTANCE;
            case KRYO -> {
                require("com.esotericsoftware.kryo.Kryo", type,
                        "com.esotericsoftware:kryo");
                yield new KryoObjectCodec();
            }
        };
    }

    /** Whether the library this serialisation needs is on the classpath. */
    public static boolean available(SerializationType type) {
        return switch (type) {
            case JDK -> true;
            case KRYO -> present("com.esotericsoftware.kryo.Kryo");
        };
    }

    private static void require(String className, SerializationType type, String coordinates) {
        if (!present(className)) {
            throw new SerializationException("serialization is configured as " + type
                    + ", but its library is not on the classpath. Add " + coordinates
                    + ", or set spring.spreader.multiprocessing.serialization back to JDK");
        }
    }

    private static boolean present(String className) {
        try {
            Class.forName(className, false, ObjectCodecs.class.getClassLoader());
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
