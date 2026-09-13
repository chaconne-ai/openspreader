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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.util.Pool;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Kryo serialisation. The smallest and the fastest of the options.
 *
 * <h2>A Kryo instance is not thread-safe</h2>
 * This is the easiest trap to fall into with Kryo: {@link Kryo} carries mutable state --
 * reference tables, the class registry -- and sharing one across threads produces
 * deserialisation errors that make <b>no sense whatsoever</b>. Intermittently, and the more
 * so under load.
 *
 * <p>The official {@link Pool} is used here, borrowing and returning as needed. Its advantage
 * over a ThreadLocal is that once a thread pool's threads have turned over, the old Kryo
 * instances can be collected rather than held indefinitely.
 *
 * <h2>Class registration is not enabled</h2>
 * {@code setRegistrationRequired(false)}: there is no need to register every class in
 * advance. The price is that each message carries fully-qualified class names, making it
 * slightly larger than in registered mode, and that -- as with JDK serialisation -- <b>it
 * instantiates whatever class the byte stream names</b>.
 *
 * <p>Registration would save size and be safer, but it requires every node in the cluster to
 * register <b>exactly the same classes in exactly the same order</b> -- adding one class
 * would mean restarting everything. That is far too steep a price for a setting whose whole
 * point is "switch serialisation and go faster", so it is off by default. Where it is
 * genuinely wanted, construct a configured Kryo factory and pass it in.
 *
 * <h2>Why it falls back to Objenesis</h2>
 * Kryo's default instantiation strategy requires a <b>no-argument constructor</b> and throws
 * "Class cannot be created" at decode time without one. JDK serialisation <b>never</b>
 * requires that: it runs the no-argument constructor of the nearest non-serialisable
 * superclass, bypassing the class's own.
 *
 * <p>That difference matters a great deal here, because Kryo is a <b>setting</b>: change
 * {@code spring.spreader.multiprocessing.serialization=KRYO} and every object crossing a
 * process boundary switches to Kryo. Without this fallback, a user would have to go back and
 * add a no-argument constructor to every DTO and every task class -- and a switch that merely
 * changes a value should not carry a contagious requirement like that.
 *
 * <p>More insidiously, <b>the failure appears on the decoding side</b>: the sender encodes
 * perfectly well and the error surfaces at the peer, with nothing in the stack but
 * "deserialisation failed". That is exactly how it was found: {@code SumTask(int,int)} has no
 * no-argument constructor, JDK serialisation was entirely green, and switching to Kryo blew
 * up at the far end.
 *
 * <p>{@link StdInstantiatorStrategy} allocates the object through JVM internals and <b>calls
 * no constructor at all</b>, which aligns the semantics with JDK serialisation. Note it
 * equally means constructor validation and default assignments do not run -- but that is the
 * existing behaviour of every serialisation framework anyway: field values come from the byte
 * stream, not from the constructor.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class KryoObjectCodec implements ObjectCodec {

    private static final byte[] EMPTY = new byte[0];

    /** Initial output buffer size per message; it grows as needed. */
    private static final int BUFFER_SIZE = 512;

    private final Pool<Kryo> pool;

    public KryoObjectCodec() {
        this(defaultPool());
    }

    public KryoObjectCodec(Pool<Kryo> pool) {
        this.pool = pool;
    }

    /** The default Kryo pool: soft references, so idle instances can be collected under
     *  memory pressure. */
    public static Pool<Kryo> defaultPool() {
        return new Pool<>(true, true) {
            @Override
            protected Kryo create() {
                Kryo kryo = new Kryo();
                // No advance class registration; the class documentation says why
                kryo.setRegistrationRequired(false);
                // Handles circular references. Turning it off is slightly faster, but an object
                // graph containing a cycle would blow the stack outright
                kryo.setReferences(true);
                // Falls back to Objenesis without a no-argument constructor; see the class documentation
                kryo.setInstantiatorStrategy(
                        new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
                useJavaSerialisationFor(kryo);
                return kryo;
            }
        };
    }


    /**
     * Hands a few shapes back to JDK serialisation, because Kryo rebuilds them wrongly.
     *
     * <p>Kryo's own map and collection serialisers work by <b>creating an empty instance and
     * adding to it</b>. That cannot work for anything unmodifiable: the instance is created
     * and the first add throws. Kryo cannot rebuild a {@link Throwable} either, whose state
     * lives in fields the JVM fills in.
     *
     * <p>Both shapes are ordinary in a payload rather than exotic, which is what makes this
     * worth doing. A pooled method returning {@code Map.of(...)} or a collection wrapped in
     * {@code Collections.unmodifiableList} is idiomatic Java; so is a task whose result
     * carries the exception it caught. Every one of them failed under Kryo and worked under
     * JDK serialisation, which is precisely the kind of difference that has someone change
     * one configuration line and meet a bug in production.
     *
     * <p>They are all {@link java.io.Serializable}, so Kryo's own {@code JavaSerializer}
     * handles them correctly. It is slower than Kryo's native path, and that is the right
     * trade: these are small parts of a payload, and correct beats fast.
     *
     * <p>Registered from sample instances rather than by class name, because the classes are
     * package-private JDK internals whose names are not a stable interface.
     */
    private static void useJavaSerialisationFor(Kryo kryo) {
        // Anything that throws when something is added to it
        Object[] samples = {
                Collections.unmodifiableCollection(new ArrayList<>()),
                Collections.unmodifiableList(new ArrayList<>()),
                Collections.unmodifiableList(new LinkedList<>()),
                Collections.unmodifiableSet(new HashSet<>()),
                Collections.unmodifiableSortedSet(new TreeSet<>()),
                Collections.unmodifiableMap(new HashMap<>()),
                Collections.unmodifiableSortedMap(new TreeMap<>()),
                Collections.emptyList(), Collections.emptySet(), Collections.emptyMap(),
                Collections.singletonList(""), Collections.singleton(""),
                Collections.singletonMap("", ""),
                Arrays.asList("", ""),
                List.of(), List.of(""), List.of("", "", ""),
                Set.of(), Set.of(""), Set.of("", "x", "y"),
                Map.of(), Map.of("", ""), Map.of("", "", "x", "y", "z", "w")
        };
        for (Object sample : samples) {
            kryo.addDefaultSerializer(sample.getClass(), JavaSerializer.class);
        }
        // Subclasses included, so an application's own exception type is covered too
        kryo.addDefaultSerializer(Throwable.class, JavaSerializer.class);
    }

    @Override
    public byte[] encode(Object value) {
        if (value == null) {
            return EMPTY;
        }
        Kryo kryo = pool.obtain();
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(BUFFER_SIZE);
             Output output = new Output(bos)) {
            // writeClassAndObject rather than writeObject, so the decoding side learns the
            // concrete type -- which is what keeps a polymorphic argument, declared as an
            // interface and passed an implementation, from being mangled
            kryo.writeClassAndObject(output, value);
            output.flush();
            return bos.toByteArray();
        } catch (Exception e) {
            throw new SerializationException("Kryo serialisation failed: "
                    + value.getClass().getName(), e);
        } finally {
            pool.free(kryo);
        }
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
        Kryo kryo = pool.obtain();
        try (Input input = new Input(data)) {
            return kryo.readClassAndObject(input);
        } catch (Exception e) {
            throw new SerializationException("Kryo deserialisation failed. Is "
                    + "spring.spreader.multiprocessing.serialization set the same on both sides?", e);
        } finally {
            pool.free(kryo);
        }
    }

    @Override
    public SerializationType type() {
        return SerializationType.KRYO;
    }
}
