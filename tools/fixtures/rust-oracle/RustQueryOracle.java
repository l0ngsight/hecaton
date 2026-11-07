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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import uk.org.hecaton.core.HecatonJsonValue;
import uk.org.hecaton.internal.json.JsonArray;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.json.PrettyPrint;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.serialization.impl.compact.FieldDescriptor;
import uk.org.hecaton.internal.serialization.impl.compact.Schema;
import uk.org.hecaton.internal.serialization.impl.compact.SchemaService;
import uk.org.hecaton.nio.serialization.ClassDefinition;
import uk.org.hecaton.nio.serialization.ClassDefinitionBuilder;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecordBuilder;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.query.impl.CachedQueryEntry;
import uk.org.hecaton.query.impl.getters.Extractors;

/**
 * Writes the query fixtures of hecaton-rust: a corpus of map entries, a list of predicates as
 * clients serialize them, and the result of every predicate on every entry as Java's query
 * engine computes it ({@code true}, {@code false}, or the exception class). The Rust member must
 * agree on each. Each entry is evaluated with a freshly deserialized predicate, as a member that
 * holds one value type per map would see it.
 * <p>
 * Usage: {@code java -cp <hecaton jar> RustQueryOracle.java OUT} (see rust-oracle.sh).
 */
@SuppressWarnings("removal")
public class RustQueryOracle {

    private static final HexFormat HEX = HexFormat.of();
    private static final Map<Long, Schema> SCHEMAS = new LinkedHashMap<>();
    private static final InternalSerializationService SS = new DefaultSerializationServiceBuilder()
            .setSchemaService(new SchemaService() {
                @Override
                public Schema get(long schemaId) {
                    return SCHEMAS.get(schemaId);
                }

                @Override
                public void put(Schema schema) {
                    SCHEMAS.put(schema.getSchemaId(), schema);
                }

                @Override
                public void putLocal(Schema schema) {
                    put(schema);
                }
            })
            .build();

    record Entry(String name, Data key, Data value) {
    }

    public static void main(String[] args) throws Exception {
        Path out = Path.of(args[0]);
        Files.createDirectories(out);
        List<Entry> entries = entries();
        Map<String, Predicate<?, ?>> predicates = predicates();
        Extractors extractors = Extractors.newBuilder(SS).build();

        JsonArray entryJson = new JsonArray();
        for (Entry e : entries) {
            entryJson.add(new JsonObject().add("name", e.name()).add("key", HEX.formatHex(e.key().toByteArray()))
                    .add("value", HEX.formatHex(e.value().toByteArray())));
        }
        JsonArray predicateJson = new JsonArray();
        for (var p : predicates.entrySet()) {
            JsonArray results = new JsonArray();
            Data predicateData = SS.toData(p.getValue());
            for (Entry e : entries) {
                try {
                    // A fresh copy per entry: a predicate caches the attribute type of the first entry it
                    // sees, which would make results depend on the order of a mixed corpus.
                    Predicate<?, ?> fresh = SS.toObject(predicateData);
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    boolean r = ((Predicate) fresh).apply(new CachedQueryEntry<>(SS, e.key(), e.value(), extractors));
                    results.add(r);
                } catch (RuntimeException ex) {
                    results.add("error: " + ex.getClass().getSimpleName());
                }
            }
            predicateJson.add(new JsonObject().add("name", p.getKey())
                    .add("data", HEX.formatHex(SS.toData(p.getValue()).toByteArray())).add("results", results));
        }
        JsonArray schemaJson = new JsonArray();
        for (Schema schema : SCHEMAS.values()) {
            JsonArray fields = new JsonArray();
            for (FieldDescriptor f : schema.getFields()) {
                fields.add(new JsonObject().add("name", f.getFieldName()).add("kind", f.getKind().getId()));
            }
            schemaJson.add(new JsonObject().add("id", Long.toString(schema.getSchemaId()))
                    .add("typeName", schema.getTypeName()).add("fields", fields));
        }
        JsonObject manifest = new JsonObject().add("schemas", schemaJson).add("entries", entryJson)
                .add("predicates", predicateJson);
        Files.writeString(out.resolve("manifest.json"), manifest.toString(PrettyPrint.PRETTY_PRINT) + "\n");
        System.out.println("wrote " + predicates.size() + " predicates over " + entries.size() + " entries");
    }

    static List<Entry> entries() {
        List<Entry> out = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            out.add(new Entry("k" + i + "=" + i, SS.toData("k" + i), SS.toData(i)));
        }
        out.add(new Entry("long key, double value", SS.toData(42L), SS.toData(2.5)));
        out.add(new Entry("string value", SS.toData("name"), SS.toData("Ann")));
        out.add(new Entry("boolean value", SS.toData("flag"), SS.toData(true)));
        out.add(new Entry("uuid value", SS.toData("id"), SS.toData(new UUID(1, 2))));
        out.add(new Entry("decimal value", SS.toData("price"), SS.toData(new BigDecimal("19.99"))));
        for (Object[] o : new Object[][]{{1L, "alice", 10.5}, {2L, "bob", 20.0}, {3L, null, 0.0}}) {
            out.add(new Entry("compact order " + o[0], SS.toData(o[0]), SS.toData(GenericRecordBuilder.compact("Order")
                    .setInt64("id", (Long) o[0]).setString("customer", (String) o[1]).setFloat64("amount", (Double) o[2])
                    .setArrayOfInt32("scores", new int[]{(int) (long) (Long) o[0], 7})
                    .setGenericRecord("address", GenericRecordBuilder.compact("Address")
                            .setString("city", o[0].equals(2L) ? "Paris" : "Rome").build())
                    .build())));
        }
        ClassDefinition person = new ClassDefinitionBuilder(1, 1, 0).addStringField("name").addIntField("age").build();
        out.add(new Entry("portable person", SS.toData("p1"),
                SS.toData(GenericRecordBuilder.portable(person).setString("name", "carl").setInt32("age", 44).build())));
        out.add(new Entry("json ann", SS.toData(1), SS.toData(new HecatonJsonValue(
                "{\"name\":\"ann\",\"age\":31,\"tags\":[\"a\",\"b\"],\"address\":{\"city\":\"Paris\"},\"active\":true}"))));
        out.add(new Entry("json ben", SS.toData(2), SS.toData(new HecatonJsonValue(
                "{\"name\":\"ben\",\"age\":25.5,\"tags\":[],\"address\":null,\"active\":false}"))));
        return out;
    }

    static Map<String, Predicate<?, ?>> predicates() {
        Map<String, Predicate<?, ?>> p = new LinkedHashMap<>();
        p.put("this > 6", Predicates.greaterThan("this", 6));
        p.put("this >= 6", Predicates.greaterEqual("this", 6));
        p.put("this < 2", Predicates.lessThan("this", 2));
        p.put("this <= 2L", Predicates.lessEqual("this", 2L));
        p.put("this = 4", Predicates.equal("this", 4));
        p.put("this != 4", Predicates.notEqual("this", 4));
        p.put("this > 5.5", Predicates.greaterThan("this", 5.5));
        p.put("this = '3' (string operand)", Predicates.equal("this", "3"));
        p.put("__key = k4", Predicates.equal("__key", "k4"));
        p.put("__key like k1%", Predicates.like("__key", "k1%"));
        p.put("__key ilike K_", Predicates.ilike("__key", "K_"));
        p.put("__key regex", Predicates.regex("__key", "k[2-3]"));
        p.put("between 3 and 5", Predicates.between("this", 3, 5));
        p.put("in 1, 5, 42", Predicates.in("this", 1, 5, 42));
        p.put("and", Predicates.and(Predicates.greaterEqual("this", 3), Predicates.lessEqual("this", 4)));
        p.put("or", Predicates.or(Predicates.equal("this", 1), Predicates.equal("this", 8)));
        p.put("not", Predicates.not(Predicates.lessThan("this", 9)));
        p.put("true", Predicates.alwaysTrue());
        p.put("false", Predicates.alwaysFalse());
        p.put("sql: this < 2", Predicates.sql("this < 2"));
        p.put("sql: this between 2 and 3 or this = 9", Predicates.sql("this between 2 and 3 or this = 9"));
        p.put("sql: __key in ('k1', 'k2')", Predicates.sql("__key in ('k1', 'k2')"));
        p.put("sql: not (this > 1)", Predicates.sql("not (this > 1)"));
        p.put("sql: this = 2.5", Predicates.sql("this = 2.5"));
        p.put("sql: this = 'Ann'", Predicates.sql("this = 'Ann'"));
        p.put("sql: this = true", Predicates.sql("this = true"));
        p.put("customer = alice", Predicates.equal("customer", "alice"));
        p.put("customer = null", Predicates.equal("customer", null));
        p.put("amount > 15", Predicates.greaterThan("amount", 15.0));
        p.put("amount > 15 (int operand)", Predicates.greaterThan("amount", 15));
        p.put("this.customer like a%", Predicates.like("this.customer", "a%"));
        p.put("scores[0] = 2", Predicates.equal("scores[0]", 2));
        p.put("scores[any] = 7", Predicates.equal("scores[any]", 7));
        p.put("address.city = Paris", Predicates.equal("address.city", "Paris"));
        p.put("sql: customer = 'bob' and amount >= 20", Predicates.sql("customer = 'bob' and amount >= 20"));
        p.put("portable age > 40", Predicates.greaterThan("age", 40));
        p.put("name = carl", Predicates.equal("name", "carl"));
        p.put("json age > 30", Predicates.greaterThan("age", 30));
        p.put("json name = ann", Predicates.equal("name", "ann"));
        p.put("json tags[any] = b", Predicates.equal("tags[any]", "b"));
        p.put("json address.city = Paris", Predicates.equal("address.city", "Paris"));
        p.put("json active = true", Predicates.equal("active", true));
        p.put("sql: age < 30", Predicates.sql("age < 30"));
        p.put("this = uuid", Predicates.equal("this", new UUID(1, 2)));
        p.put("this > 19.5 (decimal attribute)", Predicates.greaterThan("this", 19.5));
        return p;
    }
}
