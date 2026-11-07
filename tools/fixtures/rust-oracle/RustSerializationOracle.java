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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import uk.org.hecaton.core.HecatonJsonValue;
import uk.org.hecaton.internal.json.Json;
import uk.org.hecaton.internal.json.JsonArray;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.json.JsonValue;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.serialization.impl.compact.FieldDescriptor;
import uk.org.hecaton.internal.serialization.impl.compact.Schema;
import uk.org.hecaton.internal.serialization.impl.compact.SchemaService;
import uk.org.hecaton.internal.util.HashUtil;
import uk.org.hecaton.nio.serialization.ClassDefinition;
import uk.org.hecaton.nio.serialization.ClassDefinitionBuilder;
import uk.org.hecaton.nio.serialization.FieldDefinition;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecord;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecordBuilder;
import uk.org.hecaton.partition.PartitionAwareKey;
import uk.org.hecaton.partition.strategy.DefaultPartitioningStrategy;

/**
 * Writes the serialization fixtures of hecaton-rust (plan §9.1): a corpus of values serialized by
 * the Java serialization service, each with its {@code Data} bytes, partition hash, partition id
 * (271 partitions) and a typed JSON description of the value that the Rust decoder must produce.
 * The Rust encoder must also reproduce the bytes from the description. Compact schemas are
 * written alongside, because Compact data carries only the schema id.
 * <p>
 * Usage: {@code java -cp <hecaton jar> RustSerializationOracle.java OUT} (see rust-oracle.sh).
 */
@SuppressWarnings("removal")
public class RustSerializationOracle {

    private static final int PARTITION_COUNT = 271;
    private static final HexFormat HEX = HexFormat.of();

    private final Map<Long, Schema> schemas = new LinkedHashMap<>();
    private final InternalSerializationService ss = new DefaultSerializationServiceBuilder()
            .setSchemaService(new SchemaService() {
                @Override
                public Schema get(long schemaId) {
                    return schemas.get(schemaId);
                }

                @Override
                public void put(Schema schema) {
                    schemas.put(schema.getSchemaId(), schema);
                }

                @Override
                public void putLocal(Schema schema) {
                    put(schema);
                }
            })
            // As members and clients do: PartitionAware keys carry their partition key's hash.
            .setPartitioningStrategy(new DefaultPartitioningStrategy())
            .build();
    private final JsonArray values = new JsonArray();

    public static void main(String[] args) throws Exception {
        Path out = Path.of(args[0]);
        Files.createDirectories(out);
        RustSerializationOracle oracle = new RustSerializationOracle();
        oracle.builtins();
        oracle.collections();
        oracle.compact();
        oracle.compactArrayOffsets();
        oracle.portable();
        oracle.partitionAware();
        JsonArray schemaJson = new JsonArray();
        for (Schema schema : oracle.schemas.values()) {
            JsonArray fields = new JsonArray();
            for (FieldDescriptor f : schema.getFields()) {
                fields.add(new JsonObject().add("name", f.getFieldName()).add("kind", f.getKind().getId()));
            }
            schemaJson.add(new JsonObject()
                    .add("id", Long.toString(schema.getSchemaId()))
                    .add("typeName", schema.getTypeName())
                    .add("fields", fields));
        }
        JsonObject manifest = new JsonObject()
                .add("partitionCount", PARTITION_COUNT)
                .add("schemas", schemaJson)
                .add("values", oracle.values);
        Files.writeString(out.resolve("manifest.json"), manifest.toString(uk.org.hecaton.internal.json.PrettyPrint.PRETTY_PRINT) + "\n");
        System.out.println("wrote " + oracle.values.size() + " values and " + oracle.schemas.size() + " schemas");
    }

    private void add(String name, Object value, JsonValue expect) {
        Data data = ss.toData(value);
        Object back = ss.toObject(data);
        if (!Arrays.equals(data.toByteArray(), ss.toData(back).toByteArray())) {
            throw new IllegalStateException(name + " does not round-trip in Java: " + back);
        }
        values.add(new JsonObject()
                .add("name", name)
                .add("data", HEX.formatHex(data.toByteArray()))
                .add("partitionHash", data.getPartitionHash())
                .add("partitionId", HashUtil.hashToIndex(data.getPartitionHash(), PARTITION_COUNT))
                .add("expect", expect));
    }

    // -----------------------------------------------------------------------------------------
    // Typed JSON descriptions
    // -----------------------------------------------------------------------------------------

    private static JsonObject t(String type) {
        return new JsonObject().add("t", type);
    }

    private static JsonObject t(String type, JsonValue v) {
        return t(type).add("v", v);
    }

    private static JsonObject t(String type, String v) {
        return t(type).add("v", v);
    }

    private static JsonValue str(String s) {
        return s == null ? Json.NULL : Json.value(s);
    }

    static JsonObject date(LocalDate d) {
        return t("localdate").add("y", d.getYear()).add("m", d.getMonthValue()).add("d", d.getDayOfMonth());
    }

    static JsonObject time(LocalTime t) {
        return t("localtime").add("h", t.getHour()).add("mi", t.getMinute()).add("s", t.getSecond()).add("n", t.getNano());
    }

    static JsonObject dateTime(LocalDateTime dt) {
        return t("localdatetime").add("date", date(dt.toLocalDate())).add("time", time(dt.toLocalTime()));
    }

    static JsonObject offsetDateTime(OffsetDateTime odt) {
        return t("offsetdatetime").add("datetime", dateTime(odt.toLocalDateTime()))
                .add("offset", odt.getOffset().getTotalSeconds());
    }

    static JsonObject decimal(BigDecimal d) {
        return t("decimal").add("unscaled", HEX.formatHex(d.unscaledValue().toByteArray())).add("scale", d.scale());
    }

    static JsonValue floatBits(float f) {
        return Json.value(Integer.toHexString(Float.floatToRawIntBits(f)));
    }

    static JsonValue doubleBits(double d) {
        return Json.value(Long.toHexString(Double.doubleToRawLongBits(d)));
    }

    // -----------------------------------------------------------------------------------------
    // Builtin types
    // -----------------------------------------------------------------------------------------

    private void builtins() {
        add("true", true, t("bool", Json.TRUE));
        add("false", false, t("bool", Json.FALSE));
        for (byte b : new byte[]{0, 1, -1, Byte.MIN_VALUE, Byte.MAX_VALUE}) {
            add("byte " + b, b, t("byte", Json.value(b)));
        }
        for (char c : new char[]{'a', 'é', '￿', '\ud83d'}) {
            add("char " + (int) c, c, t("char", Json.value((int) c)));
        }
        for (short s : new short[]{0, -1, Short.MIN_VALUE, Short.MAX_VALUE}) {
            add("short " + s, s, t("short", Json.value(s)));
        }
        for (int i : new int[]{0, 1, -1, 42, 271, Integer.MIN_VALUE, Integer.MAX_VALUE, 123456789}) {
            add("int " + i, i, t("int", Json.value(i)));
        }
        for (long l : new long[]{0, 1, -1, Long.MIN_VALUE, Long.MAX_VALUE, 1L << 40}) {
            add("long " + l, l, t("long", Long.toString(l)));
        }
        for (float f : new float[]{0f, -0f, 1.5f, Float.NaN, Float.POSITIVE_INFINITY, Float.MIN_VALUE}) {
            add("float " + f, f, t("float", floatBits(f)));
        }
        for (double d : new double[]{0d, -0d, Math.PI, Double.NaN, Double.NEGATIVE_INFINITY, Double.MAX_VALUE}) {
            add("double " + d, d, t("double", doubleBits(d)));
        }
        for (String s : new String[]{"", "a", "hello world", "key-1", "café", "日本語",
                "emoji 😀", "x".repeat(300)}) {
            add("string " + (s.length() > 20 ? s.substring(0, 20) + "..." : s), s, t("string", s));
        }
        add("byte[]", new byte[]{1, 2, 3, -1}, t("bytes", "010203ff"));
        add("byte[] empty", new byte[0], t("bytes", ""));
        add("boolean[]", new boolean[]{true, false, true},
                t("bool[]", new JsonArray().add(true).add(false).add(true)));
        add("char[]", new char[]{'a', 'é'}, t("char[]", new JsonArray().add((int) 'a').add(0xe9)));
        add("short[]", new short[]{1, -2}, t("short[]", new JsonArray().add(1).add(-2)));
        add("int[]", new int[]{1, -2, Integer.MAX_VALUE}, t("int[]", new JsonArray().add(1).add(-2).add(Integer.MAX_VALUE)));
        add("long[]", new long[]{1, Long.MIN_VALUE}, t("long[]", new JsonArray().add("1").add(Long.toString(Long.MIN_VALUE))));
        add("float[]", new float[]{1.5f, -0f}, t("float[]", new JsonArray().add(floatBits(1.5f)).add(floatBits(-0f))));
        add("double[]", new double[]{Math.E}, t("double[]", new JsonArray().add(doubleBits(Math.E))));
        add("String[]", new String[]{"a", null, "é"}, t("string[]", new JsonArray().add("a").add(Json.NULL).add("é")));
        UUID uuid = new UUID(0x0123456789abcdefL, 0xfedcba9876543210L);
        add("uuid", uuid, t("uuid", uuid.toString()));
        add("entry", new AbstractMap.SimpleEntry<>("k", 1),
                t("entry").add("k", t("string", "k")).add("v", t("int", Json.value(1))));
        add("immutable entry", new AbstractMap.SimpleImmutableEntry<>(1L, "v"),
                t("immutable-entry").add("k", t("long", "1")).add("v", t("string", "v")));
        for (BigInteger b : new BigInteger[]{BigInteger.ZERO, BigInteger.valueOf(-129), new BigInteger("123456789012345678901234567890")}) {
            add("biginteger " + b, b, t("biginteger", HEX.formatHex(b.toByteArray())));
        }
        for (BigDecimal d : new BigDecimal[]{new BigDecimal("0"), new BigDecimal("-12.345"), new BigDecimal("1E+10")}) {
            add("bigdecimal " + d, d, decimal(d));
        }
        add("date", new Date(1_700_000_000_123L), t("date", "1700000000123"));
        LocalDate ld = LocalDate.of(2026, 11, 7);
        LocalTime lt = LocalTime.of(13, 45, 30, 123_456_789);
        add("localdate", ld, date(ld));
        add("localdate negative year", LocalDate.of(-50, 1, 31), date(LocalDate.of(-50, 1, 31)));
        add("localtime", lt, time(lt));
        add("localdatetime", LocalDateTime.of(ld, lt), dateTime(LocalDateTime.of(ld, lt)));
        OffsetDateTime odt = OffsetDateTime.of(LocalDateTime.of(ld, lt), ZoneOffset.ofHoursMinutes(-5, -30));
        add("offsetdatetime", odt, offsetDateTime(odt));
        add("class", String.class, t("class", "java.lang.String"));
        // Enums use Java serialization outside compatibility mode: opaque to a member without Java.
        add("enum", java.util.concurrent.TimeUnit.SECONDS, t("opaque"));
        add("optional", Optional.of("x"), t("optional", t("string", "x")));
        add("optional empty", Optional.empty(), t("optional", Json.NULL));
        add("json", new HecatonJsonValue("{\"name\":\"a\",\"age\":3}"), t("json", "{\"name\":\"a\",\"age\":3}"));
    }

    // -----------------------------------------------------------------------------------------
    // Collections and nested objects
    // -----------------------------------------------------------------------------------------

    private void collections() {
        add("Object[]", new Object[]{1, "a", null}, t("array",
                new JsonArray().add(t("int", Json.value(1))).add(t("string", "a")).add(t("null"))));
        add("ArrayList", new ArrayList<>(List.of(1, 2)), list(-29, t("int", Json.value(1)), t("int", Json.value(2))));
        add("LinkedList", new LinkedList<>(List.of("a")), list(-30, t("string", "a")));
        add("LinkedHashSet", new LinkedHashSet<>(List.of(3L)), list(-39, t("long", "3")));
        Map<String, Integer> lhm = new LinkedHashMap<>();
        lhm.put("one", 1);
        lhm.put("two", 2);
        add("LinkedHashMap", lhm, t("map").add("type", -35).add("v", new JsonArray()
                .add(new JsonArray().add(t("string", "one")).add(t("int", Json.value(1))))
                .add(new JsonArray().add(t("string", "two")).add(t("int", Json.value(2))))));
        add("TreeMap", new TreeMap<>(Map.of(1, "x")), t("map").add("type", -36).add("v", new JsonArray()
                .add(new JsonArray().add(t("int", Json.value(1))).add(t("string", "x")))));
        add("ConcurrentHashMap", new ConcurrentHashMap<>(Map.of("k", true)), t("map").add("type", -34).add("v", new JsonArray()
                .add(new JsonArray().add(t("string", "k")).add(t("bool", Json.TRUE)))));
        add("ArrayList of nested", new ArrayList<>(List.of(new UUID(1, 2), new ArrayList<>(), new byte[]{7})),
                list(-29, t("uuid", new UUID(1, 2).toString()), list(-29), t("bytes", "07")));
        // List.of() uses Java serialization, which has no length prefix: without Java, nothing
        // after it can be read, so the whole value is opaque.
        add("ArrayList with opaque", new ArrayList<>(List.of(List.of(1), "after")), t("opaque"));
    }

    private static JsonObject list(int type, JsonValue... items) {
        JsonArray a = new JsonArray();
        for (JsonValue i : items) {
            a.add(i);
        }
        return t("list").add("type", type).add("v", a);
    }

    // -----------------------------------------------------------------------------------------
    // Compact
    // -----------------------------------------------------------------------------------------

    private void compact() {
        LocalDate ld = LocalDate.of(2026, 11, 7);
        LocalTime lt = LocalTime.of(1, 2, 3, 4);
        LocalDateTime ldt = LocalDateTime.of(ld, lt);
        OffsetDateTime odt = OffsetDateTime.of(ldt, ZoneOffset.ofHours(2));
        BigDecimal dec = new BigDecimal("3.14159");
        GenericRecord address = GenericRecordBuilder.compact("Address")
                .setString("street", "Main St")
                .setInt32("number", 12)
                .build();
        CompactJson addressJson = new CompactJson("Address")
                .field("street", 17, t("string", "Main St"))
                .field("number", 9, t("int", Json.value(12)));
        add("compact address", address, addressJson.json(address));

        GenericRecordBuilder all = GenericRecordBuilder.compact("AllKinds");
        CompactJson j = new CompactJson("AllKinds");
        all.setBoolean("bool", true);
        j.field("bool", 1, t("bool", Json.TRUE));
        all.setBoolean("bool2", false);
        j.field("bool2", 1, t("bool", Json.FALSE));
        for (int i = 0; i < 9; i++) {
            all.setBoolean("flag" + i, i % 3 == 0);
            j.field("flag" + i, 1, t("bool", Json.value(i % 3 == 0)));
        }
        all.setArrayOfBoolean("bools", new boolean[]{true, false, true, true, false, false, true, true, true});
        j.field("bools", 2, t("bool[]", jsonArray(true, false, true, true, false, false, true, true, true)));
        all.setInt8("i8", (byte) -7);
        j.field("i8", 3, t("byte", Json.value(-7)));
        all.setArrayOfInt8("i8s", new byte[]{1, -1});
        j.field("i8s", 4, t("bytes", "01ff"));
        all.setInt16("i16", (short) 300);
        j.field("i16", 7, t("short", Json.value(300)));
        all.setArrayOfInt16("i16s", new short[]{-300});
        j.field("i16s", 8, t("short[]", new JsonArray().add(-300)));
        all.setInt32("i32", -100000);
        j.field("i32", 9, t("int", Json.value(-100000)));
        all.setArrayOfInt32("i32s", new int[]{1, 2, 3});
        j.field("i32s", 10, t("int[]", new JsonArray().add(1).add(2).add(3)));
        all.setInt64("i64", Long.MAX_VALUE);
        j.field("i64", 11, t("long", Long.toString(Long.MAX_VALUE)));
        all.setArrayOfInt64("i64s", new long[]{-1});
        j.field("i64s", 12, t("long[]", new JsonArray().add("-1")));
        all.setFloat32("f32", 2.5f);
        j.field("f32", 13, t("float", floatBits(2.5f)));
        all.setArrayOfFloat32("f32s", new float[]{1f});
        j.field("f32s", 14, t("float[]", new JsonArray().add(floatBits(1f))));
        all.setFloat64("f64", -1.25);
        j.field("f64", 15, t("double", doubleBits(-1.25)));
        all.setArrayOfFloat64("f64s", new double[]{});
        j.field("f64s", 16, t("double[]", new JsonArray()));
        all.setString("str", "café");
        j.field("str", 17, t("string", "café"));
        all.setString("nullStr", null);
        j.field("nullStr", 17, t("null"));
        all.setArrayOfString("strs", new String[]{"a", null});
        j.field("strs", 18, t("array", new JsonArray().add(t("string", "a")).add(t("null"))));
        all.setDecimal("dec", dec);
        j.field("dec", 19, decimal(dec));
        all.setArrayOfDecimal("decs", new BigDecimal[]{null, BigDecimal.TEN});
        j.field("decs", 20, t("array", new JsonArray().add(t("null")).add(decimal(BigDecimal.TEN))));
        all.setTime("time", lt);
        j.field("time", 21, time(lt));
        all.setArrayOfTime("times", new LocalTime[]{lt});
        j.field("times", 22, t("array", new JsonArray().add(time(lt))));
        all.setDate("date", ld);
        j.field("date", 23, date(ld));
        all.setArrayOfDate("dates", new LocalDate[]{ld, null});
        j.field("dates", 24, t("array", new JsonArray().add(date(ld)).add(t("null"))));
        all.setTimestamp("ts", ldt);
        j.field("ts", 25, dateTime(ldt));
        all.setArrayOfTimestamp("tss", new LocalDateTime[]{});
        j.field("tss", 26, t("array", new JsonArray()));
        all.setTimestampWithTimezone("tstz", odt);
        j.field("tstz", 27, offsetDateTime(odt));
        all.setArrayOfTimestampWithTimezone("tstzs", new OffsetDateTime[]{odt});
        j.field("tstzs", 28, t("array", new JsonArray().add(offsetDateTime(odt))));
        all.setGenericRecord("address", address);
        j.field("address", 29, addressJson.json(address));
        all.setGenericRecord("noAddress", null);
        j.field("noAddress", 29, t("null"));
        all.setArrayOfGenericRecord("addresses", new GenericRecord[]{address, null, address});
        j.field("addresses", 30, t("array", new JsonArray().add(addressJson.json(address)).add(t("null")).add(addressJson.json(address))));
        all.setNullableBoolean("nBool", null);
        j.field("nBool", 33, t("null"));
        all.setArrayOfNullableBoolean("nBools", new Boolean[]{true, null});
        j.field("nBools", 34, t("array", new JsonArray().add(t("bool", Json.TRUE)).add(t("null"))));
        all.setNullableInt8("nI8", (byte) 5);
        j.field("nI8", 35, t("byte", Json.value(5)));
        all.setArrayOfNullableInt8("nI8s", new Byte[]{null});
        j.field("nI8s", 36, t("array", new JsonArray().add(t("null"))));
        all.setNullableInt16("nI16", (short) 6);
        j.field("nI16", 37, t("short", Json.value(6)));
        all.setArrayOfNullableInt16("nI16s", new Short[]{7});
        j.field("nI16s", 38, t("array", new JsonArray().add(t("short", Json.value(7)))));
        all.setNullableInt32("nI32", 8);
        j.field("nI32", 39, t("int", Json.value(8)));
        all.setArrayOfNullableInt32("nI32s", new Integer[]{9, null, 10});
        j.field("nI32s", 40, t("array", new JsonArray().add(t("int", Json.value(9))).add(t("null")).add(t("int", Json.value(10)))));
        all.setNullableInt64("nI64", null);
        j.field("nI64", 41, t("null"));
        all.setArrayOfNullableInt64("nI64s", null);
        j.field("nI64s", 42, t("null"));
        all.setNullableFloat32("nF32", 1.5f);
        j.field("nF32", 43, t("float", floatBits(1.5f)));
        all.setArrayOfNullableFloat32("nF32s", new Float[]{2.5f});
        j.field("nF32s", 44, t("array", new JsonArray().add(t("float", floatBits(2.5f)))));
        all.setNullableFloat64("nF64", 3.5);
        j.field("nF64", 45, t("double", doubleBits(3.5)));
        all.setArrayOfNullableFloat64("nF64s", new Double[]{null, 4.5});
        j.field("nF64s", 46, t("array", new JsonArray().add(t("null")).add(t("double", doubleBits(4.5)))));
        GenericRecord allKinds = all.build();
        add("compact all kinds", allKinds, j.json(allKinds));

        GenericRecord empty = GenericRecordBuilder.compact("Empty").build();
        add("compact empty", empty, new CompactJson("Empty").json(empty));
        GenericRecord fixedOnly = GenericRecordBuilder.compact("FixedOnly").setInt64("id", 99).setBoolean("ok", true).build();
        add("compact fixed only", fixedOnly, new CompactJson("FixedOnly")
                .field("id", 11, t("long", "99")).field("ok", 1, t("bool", Json.TRUE)).json(fixedOnly));

        // Offsets switch from bytes to shorts at 255 bytes of data and to ints at 65535 bytes.
        // The data of "Big" is 13 bytes plus the text: 241 and 242 give 254 and 255 bytes,
        // 65521 and 65522 give 65534 and 65535.
        for (int size : new int[]{200, 241, 242, 300, 65_521, 65_522, 70_000}) {
            String text = "y".repeat(size);
            GenericRecord big = GenericRecordBuilder.compact("Big").setString("a", "x").setString("b", text).setInt32("c", 1).build();
            add("compact offsets " + size, big, new CompactJson("Big")
                    .field("a", 17, t("string", "x")).field("b", 17, t("string", text)).field("c", 9, t("int", Json.value(1)))
                    .json(big));
        }
    }

    private void compactArrayOffsets() {
        // An array of strings "x" and k "y"s holds 9 + k bytes of items: 245 and 246 give 254
        // and 255, where the item offsets switch from bytes to shorts.
        for (int k : new int[]{245, 246}) {
            String text = "y".repeat(k);
            GenericRecord r = GenericRecordBuilder.compact("Strings").setArrayOfString("s", new String[]{"x", text}).build();
            add("compact array offsets " + k, r, new CompactJson("Strings")
                    .field("s", 18, t("array", new JsonArray().add(t("string", "x")).add(t("string", text)))).json(r));
        }
    }

    private static JsonArray jsonArray(boolean... values) {
        JsonArray a = new JsonArray();
        for (boolean v : values) {
            a.add(v);
        }
        return a;
    }

    /** Builds the description of a Compact record; fields are listed in any order. */
    private final class CompactJson {
        private final String typeName;
        private final JsonObject fields = new JsonObject();

        CompactJson(String typeName) {
            this.typeName = typeName;
        }

        CompactJson field(String name, int kind, JsonValue value) {
            fields.add(name, new JsonObject().add("kind", kind).add("value", value));
            return this;
        }

        JsonObject json(GenericRecord record) {
            // The schema is put into the schema service when the record is serialized.
            Schema schema = new Schema(typeName, descriptors(record));
            return t("compact").add("schema", Long.toString(schema.getSchemaId())).add("type", typeName).add("fields", fields);
        }

        private List<FieldDescriptor> descriptors(GenericRecord record) {
            List<FieldDescriptor> out = new ArrayList<>();
            for (String name : record.getFieldNames()) {
                out.add(new FieldDescriptor(name, record.getFieldKind(name)));
            }
            return out;
        }
    }

    // -----------------------------------------------------------------------------------------
    // Portable
    // -----------------------------------------------------------------------------------------

    private void portable() {
        ClassDefinition inner = new ClassDefinitionBuilder(7, 2, 1).addStringField("name").build();
        ClassDefinition outer = new ClassDefinitionBuilder(7, 1, 3)
                .addIntField("id")
                .addStringField("title")
                .addBooleanField("active")
                .addByteField("b")
                .addCharField("c")
                .addShortField("s")
                .addLongField("l")
                .addFloatField("f")
                .addDoubleField("d")
                .addDecimalField("dec")
                .addTimeField("time")
                .addDateField("date")
                .addTimestampField("ts")
                .addTimestampWithTimezoneField("tstz")
                .addIntArrayField("ints")
                .addStringArrayField("strs")
                .addByteArrayField("bytes")
                .addPortableField("child", inner)
                .addPortableField("noChild", inner)
                .addPortableArrayField("children", inner)
                .addDateArrayField("dates")
                .build();
        LocalDate ld = LocalDate.of(2026, 11, 7);
        LocalTime lt = LocalTime.of(23, 59, 59, 999);
        OffsetDateTime odt = OffsetDateTime.of(LocalDateTime.of(ld, lt), ZoneOffset.UTC);
        GenericRecord child = GenericRecordBuilder.portable(inner).setString("name", "kid").build();
        GenericRecord record = GenericRecordBuilder.portable(outer)
                .setInt32("id", 5)
                .setString("title", "hello")
                .setBoolean("active", true)
                .setInt8("b", (byte) 1)
                .setChar("c", 'z')
                .setInt16("s", (short) -2)
                .setInt64("l", 1L << 50)
                .setFloat32("f", 0.5f)
                .setFloat64("d", 1e-3)
                .setDecimal("dec", new BigDecimal("-0.5"))
                .setTime("time", lt)
                .setDate("date", ld)
                .setTimestamp("ts", LocalDateTime.of(ld, lt))
                .setTimestampWithTimezone("tstz", odt)
                .setArrayOfInt32("ints", new int[]{4, 5})
                .setArrayOfString("strs", new String[]{"p", "q"})
                .setArrayOfInt8("bytes", new byte[]{9})
                .setGenericRecord("child", child)
                .setGenericRecord("noChild", null)
                .setArrayOfGenericRecord("children", new GenericRecord[]{child, child})
                .setArrayOfDate("dates", new LocalDate[]{ld})
                .build();
        JsonObject childJson = portableJson(inner, new JsonArray().add(field("name", 9, t("string", "kid"))));
        JsonArray fields = new JsonArray()
                .add(field("id", 5, t("int", Json.value(5))))
                .add(field("title", 9, t("string", "hello")))
                .add(field("active", 2, t("bool", Json.TRUE)))
                .add(field("b", 1, t("byte", Json.value(1))))
                .add(field("c", 3, t("char", Json.value((int) 'z'))))
                .add(field("s", 4, t("short", Json.value(-2))))
                .add(field("l", 6, t("long", Long.toString(1L << 50))))
                .add(field("f", 7, t("float", floatBits(0.5f))))
                .add(field("d", 8, t("double", doubleBits(1e-3))))
                .add(field("dec", 20, decimal(new BigDecimal("-0.5"))))
                .add(field("time", 22, time(lt)))
                .add(field("date", 24, date(ld)))
                .add(field("ts", 26, dateTime(LocalDateTime.of(ld, lt))))
                .add(field("tstz", 28, offsetDateTime(odt)))
                .add(field("ints", 15, t("int[]", new JsonArray().add(4).add(5))))
                .add(field("strs", 19, t("string[]", new JsonArray().add("p").add("q"))))
                .add(field("bytes", 11, t("bytes", "09")))
                .add(field("child", 0, childJson).add("factoryId", 7).add("classId", 2))
                .add(field("noChild", 0, t("null")).add("factoryId", 7).add("classId", 2))
                .add(field("children", 10, t("array", new JsonArray().add(childJson).add(childJson))).add("factoryId", 7).add("classId", 2))
                .add(field("dates", 25, t("array", new JsonArray().add(date(ld)))));
        add("portable", record, portableJson(outer, fields));
        ClassDefinition empty = new ClassDefinitionBuilder(8, 1, 0).build();
        add("portable empty", GenericRecordBuilder.portable(empty).build(), portableJson(empty, new JsonArray()));
    }

    private static JsonObject field(String name, int type, JsonValue value) {
        return new JsonObject().add("name", name).add("type", type).add("value", value);
    }

    private static JsonObject portableJson(ClassDefinition cd, JsonArray fields) {
        return t("portable").add("factoryId", cd.getFactoryId()).add("classId", cd.getClassId())
                .add("version", cd.getVersion()).add("fields", fields);
    }

    // -----------------------------------------------------------------------------------------
    // Partition-aware keys: the Data carries the hash of the partition key
    // -----------------------------------------------------------------------------------------

    private void partitionAware() {
        for (Object partitionKey : new Object[]{"user-1", 42, 7L}) {
            PartitionAwareKey<String, Object> key = new PartitionAwareKey<>("order-" + partitionKey, partitionKey);
            Data data = ss.toData(key);
            Data partitionKeyData = ss.toData(partitionKey);
            if (!data.hasPartitionHash() || data.getPartitionHash() != partitionKeyData.getPartitionHash()) {
                throw new IllegalStateException("partition hash of " + key);
            }
            values.add(new JsonObject()
                    .add("name", "partition-aware " + partitionKey)
                    .add("data", HEX.formatHex(data.toByteArray()))
                    .add("partitionHash", data.getPartitionHash())
                    .add("partitionId", HashUtil.hashToIndex(data.getPartitionHash(), PARTITION_COUNT))
                    .add("expect", t("opaque")));
        }
    }
}
