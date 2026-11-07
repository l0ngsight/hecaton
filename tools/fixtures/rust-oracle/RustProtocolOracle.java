/*
 * Copyright (c) 2026, The Hecaton Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.matcher.ElementMatchers;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.ClientMessageReader;
import uk.org.hecaton.client.impl.protocol.ClientMessageWriter;
import uk.org.hecaton.client.impl.protocol.codec.builtin.CustomTypeFactory;
import uk.org.hecaton.client.impl.protocol.codec.builtin.ErrorsCodec;
import uk.org.hecaton.client.impl.protocol.codec.holder.VectorPairHolder;
import uk.org.hecaton.internal.json.Json;
import uk.org.hecaton.internal.json.JsonArray;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.json.JsonValue;
import uk.org.hecaton.vector.VectorValues;

/**
 * The Java oracle for the Rust client-protocol codecs (hecaton-rust, plan §9.1).
 * <p>
 * hecaton-rust writes a sample of every request, response and event, plus a manifest
 * ({@code cargo run -p hecaton-protocol --features samples --example write-samples -- DIR}). This
 * program decodes each sample with the Java codec and encodes the decoded values again. If the
 * bytes are equal, the Rust encoding is the one Java produces and reads; the Java bytes are
 * written to the output directory and become the fixtures hecaton-rust tests against. Samples
 * that differ or that Java cannot decode are reported, and the program exits with status 1.
 * <p>
 * Usage: {@code java -cp <hecaton jar>:<byte-buddy jar> RustProtocolOracle.java SAMPLES OUT}
 * (see rust-oracle.sh).
 */
public class RustProtocolOracle {

    private static final String CODEC_PACKAGE = "uk.org.hecaton.client.impl.protocol.codec.";

    public static void main(String[] args) throws Exception {
        Path in = Path.of(args[0]);
        Path out = Path.of(args[1]);
        Files.createDirectories(out);
        JsonArray manifest = Json.parse(Files.readString(in.resolve("manifest.json"))).asArray();
        List<String> failures = new ArrayList<>();
        int ok = 0;
        for (JsonValue v : manifest) {
            JsonObject entry = v.asObject();
            String file = entry.getString("file", null);
            byte[] sample = Files.readAllBytes(in.resolve(file));
            try {
                byte[] java = toBytes(roundtrip(entry, fromBytes(sample)));
                Files.write(out.resolve(file), java);
                if (Arrays.equals(sample, java)) {
                    ok++;
                } else {
                    failures.add(file + ": Java re-encodes to different bytes (" + firstDifference(sample, java) + ")");
                }
            } catch (Throwable t) {
                Throwable cause = t;
                while (cause instanceof java.lang.reflect.InvocationTargetException && cause.getCause() != null) {
                    cause = cause.getCause();
                }
                failures.add(file + ": " + cause);
            }
        }
        Files.copy(in.resolve("manifest.json"), out.resolve("manifest.json"),
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        failures.forEach(f -> System.out.println("FAIL " + f));
        System.out.println(ok + " of " + manifest.size() + " samples match the Java codecs");
        System.exit(failures.isEmpty() ? 0 : 1);
    }

    private static ClientMessage roundtrip(JsonObject entry, ClientMessage message) throws Exception {
        String part = entry.getString("part", null);
        if (part.equals("errors")) {
            return ErrorsCodec.encode(ErrorsCodec.decode(message));
        }
        Class<?> codec = Class.forName(CODEC_PACKAGE + entry.getString("codec", null));
        List<String> params = strings(entry.get("params"));
        List<String> newParams = strings(entry.get("newParams"));
        switch (part) {
            case "request":
                return encodeDecoded(codec, "Request", message, params, newParams);
            case "response":
                return encodeDecoded(codec, "Response", message, params, newParams);
            case "event":
                return encodeEvent(codec, entry.getString("event", null), message, params, newParams);
            default:
                throw new IllegalArgumentException("unknown part " + part);
        }
    }

    /** Mirrors the shapes of the generated decoders: no value, a single value, or a parameters class. */
    private static ClientMessage encodeDecoded(Class<?> codec, String part, ClientMessage message,
                                               List<String> params, List<String> newParams) throws Exception {
        Method encode = method(codec, "encode" + part);
        if (params.isEmpty()) {
            return (ClientMessage) encode.invoke(null);
        }
        Object decoded = codec.getMethod("decode" + part, ClientMessage.class).invoke(null, message);
        if (params.size() == 1 && newParams.isEmpty()) {
            return (ClientMessage) encode.invoke(null, decoded);
        }
        Object[] values = new Object[params.size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = adapt(encode.getParameterTypes()[i], decoded.getClass().getField(params.get(i)).get(decoded));
        }
        return (ClientMessage) encode.invoke(null, values);
    }

    /**
     * Converts a decoded value to the type the encoder takes where the two differ: vector search
     * requests decode their vectors as pairs but encode them from {@code VectorValues}.
     */
    @SuppressWarnings("unchecked")
    private static Object adapt(Class<?> target, Object value) {
        if (target == VectorValues.class && value instanceof List) {
            return CustomTypeFactory.toVectorValues((List<VectorPairHolder>) value);
        }
        return value;
    }

    /**
     * Decodes an event through the codec's AbstractEventHandler (subclassed with ByteBuddy) and
     * encodes the handler's arguments again. Handlers receive an "exists" flag before each
     * parameter added after the event, which the encoder does not take.
     */
    private static ClientMessage encodeEvent(Class<?> codec, String event, ClientMessage message,
                                             List<String> params, List<String> newParams) throws Exception {
        Class<?> handlerType = Class.forName(codec.getName() + "$AbstractEventHandler");
        Method encode = method(codec, "encode" + event + "Event");
        AtomicReference<ClientMessage> result = new AtomicReference<>();
        String handlerName = "handle" + event + "Event";
        Object handler = new ByteBuddy()
                .subclass(handlerType)
                .method(ElementMatchers.isAbstract())
                .intercept(InvocationHandlerAdapter.of((proxy, m, args) -> {
                    if (!m.getName().equals(handlerName)) {
                        throw new IllegalStateException("decoded as " + m.getName());
                    }
                    Object[] values = new Object[params.size()];
                    int arg = 0;
                    for (int i = 0; i < values.length; i++) {
                        if (newParams.contains(params.get(i))) {
                            arg++;
                        }
                        values[i] = args[arg++];
                    }
                    result.set((ClientMessage) encode.invoke(null, values));
                    return null;
                }))
                .make()
                .load(handlerType.getClassLoader())
                .getLoaded()
                .getDeclaredConstructor()
                .newInstance();
        handlerType.getMethod("handle", ClientMessage.class).invoke(handler, message);
        if (result.get() == null) {
            throw new IllegalStateException("the handler was not called");
        }
        return result.get();
    }

    private static Method method(Class<?> type, String name) {
        Method found = null;
        for (Method m : type.getMethods()) {
            if (m.getName().equals(name) && Modifier.isStatic(m.getModifiers())) {
                if (found != null) {
                    throw new IllegalStateException("overloaded " + type.getSimpleName() + "." + name);
                }
                found = m;
            }
        }
        if (found == null) {
            throw new IllegalStateException("no " + type.getSimpleName() + "." + name);
        }
        return found;
    }

    private static List<String> strings(JsonValue value) {
        List<String> out = new ArrayList<>();
        for (JsonValue v : value.asArray()) {
            out.add(v.asString());
        }
        return out;
    }

    private static ClientMessage fromBytes(byte[] bytes) {
        ClientMessageReader reader = new ClientMessageReader(Integer.MAX_VALUE);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if (!reader.readFrom(buffer, true) || buffer.hasRemaining()) {
            throw new IllegalArgumentException("not exactly one message");
        }
        return reader.getClientMessage();
    }

    private static byte[] toBytes(ClientMessage message) {
        ByteBuffer buffer = ByteBuffer.allocate(message.getBufferLength());
        if (!new ClientMessageWriter().writeTo(buffer, message)) {
            throw new IllegalStateException("message did not fit");
        }
        return buffer.array();
    }

    private static String firstDifference(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            if (a[i] != b[i]) {
                return "first at byte " + i + " of " + a.length + "/" + b.length;
            }
        }
        return "lengths " + a.length + "/" + b.length;
    }
}
