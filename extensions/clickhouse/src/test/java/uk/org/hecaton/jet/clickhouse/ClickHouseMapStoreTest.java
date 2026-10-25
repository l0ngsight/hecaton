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

package uk.org.hecaton.jet.clickhouse;

import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecord;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecordBuilder;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class ClickHouseMapStoreTest extends ClickHouseTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initializeMembers();
    }

    private static String createPeopleTable() {
        String table = newTableName("people");
        execute("CREATE TABLE " + table + " (id Int64, name String, age Nullable(Int32), "
                + "version UInt64, is_deleted UInt8) ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY id");
        return table;
    }

    private static <K, V> IMap<K, V> mapWithStore(String mapName, Properties extra) {
        return mapWithStore(mapName, extra, 0);
    }

    private static <K, V> IMap<K, V> mapWithStore(String mapName, Properties extra, int writeDelaySeconds) {
        Properties properties = new Properties();
        properties.setProperty(ClickHouseMapStore.DATA_CONNECTION_REF_PROPERTY, DATA_CONNECTION);
        properties.putAll(extra);
        instance().getConfig().addMapConfig(new MapConfig(mapName).setMapStoreConfig(new MapStoreConfig()
                .setEnabled(true)
                .setClassName(ClickHouseMapStore.class.getName())
                .setWriteDelaySeconds(writeDelaySeconds)
                .setProperties(properties)));
        return instance().getMap(mapName);
    }

    private static GenericRecord person(String typeName, String name, Integer age) {
        return GenericRecordBuilder.compact(typeName).setString("name", name).setNullableInt32("age", age).build();
    }

    @Test
    public void storesLoadsAndDeletes() {
        String table = createPeopleTable();
        IMap<Long, GenericRecord> map = mapWithStore(table, new Properties());

        map.put(1L, person(table, "Ann", 31));
        map.put(2L, person(table, "Bob", null));
        map.put(1L, person(table, "Ann", 32));
        map.remove(2L);

        assertThat(query("SELECT id, name, age FROM " + table + " FINAL WHERE is_deleted = 0"))
                .containsExactly(List.of(1L, "Ann", 32));

        map.evictAll();
        GenericRecord ann = map.get(1L);
        assertThat(ann.getString("name")).isEqualTo("Ann");
        assertThat(ann.getNullableInt32("age")).isEqualTo(32);
        assertThat(map.get(2L)).isNull();
    }

    @Test
    public void loadsExistingRowsOnFirstUse() {
        String table = createPeopleTable();
        execute("INSERT INTO " + table + " VALUES (1, 'Ann', 31, 1, 0), (2, 'Bob', 40, 1, 0), (3, 'Cid', 50, 1, 0), "
                + "(2, 'Bob', 41, 2, 0), (3, 'Cid', 50, 2, 1)");

        IMap<Long, GenericRecord> map = mapWithStore(table, new Properties());

        assertThat(map.size()).isEqualTo(2);
        assertThat(map.get(2L).getNullableInt32("age")).isEqualTo(41);
        assertThat(map.containsKey(3L)).isFalse();
    }

    @Test
    public void storesAndLoadsAllAtOnce() {
        String table = createPeopleTable();
        IMap<Long, GenericRecord> map = mapWithStore(table, new Properties());
        Map<Long, GenericRecord> people = new java.util.HashMap<>();
        for (long i = 0; i < 2500; i++) {
            people.put(i, person(table, "person-" + i, (int) i));
        }

        map.putAll(people);
        map.evictAll();

        assertThat(count("SELECT count() FROM " + table + " FINAL WHERE is_deleted = 0")).isEqualTo(2500);
        Map<Long, GenericRecord> loaded = map.getAll(people.keySet());
        assertThat(loaded).hasSize(2500);
        assertThat(loaded.get(2499L).getString("name")).isEqualTo("person-2499");
    }

    @Test
    public void writesBehind() {
        String table = createPeopleTable();
        IMap<Long, GenericRecord> map = mapWithStore(table, new Properties(), 1);

        for (long i = 0; i < 1000; i++) {
            map.put(i, person(table, "person-" + i, (int) i));
        }
        for (long i = 0; i < 1000; i += 2) {
            map.put(i, person(table, "updated-" + i, (int) i));
        }
        map.delete(999L);

        assertTrueEventually(() -> assertThat(count("SELECT count() FROM " + table + " FINAL WHERE is_deleted = 0"))
                .isEqualTo(999));
        assertThat(count("SELECT countIf(startsWith(name, 'updated-')) FROM " + table + " FINAL WHERE is_deleted = 0"))
                .isEqualTo(500);
    }

    @Test
    public void storesASingleColumnAsTheValue() {
        String table = newTableName("names");
        execute("CREATE TABLE " + table + " (key String, name String, version UInt64, is_deleted UInt8) "
                + "ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY key");
        Properties properties = new Properties();
        properties.setProperty(ClickHouseMapStore.ID_COLUMN_PROPERTY, "key");
        properties.setProperty(ClickHouseMapStore.SINGLE_COLUMN_AS_VALUE_PROPERTY, "true");
        IMap<String, String> map = mapWithStore(table, properties);
        // a key that would break SQL and the escaping of query parameters
        String key = "it's a \\ key\t{k0:String}";

        map.put(key, "value");
        map.evictAll();

        assertThat(map.get(key)).isEqualTo("value");
        assertThat(query("SELECT key, name FROM " + table + " FINAL")).containsExactly(List.of(key, "value"));
    }

    @Test
    public void convertsColumnTypes() {
        String table = newTableName("types");
        execute("CREATE TABLE " + table + " (id UUID, flag Bool, tiny Int8, small UInt8, count UInt32, "
                + "big UInt64, ratio Float32, amount Decimal(18, 4), label LowCardinality(String), "
                + "kind Enum8('a' = 1, 'b' = 2), address IPv4, day Date, at DateTime64(3, 'UTC'), "
                + "note Nullable(String), version UInt64, is_deleted UInt8) "
                + "ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY id");
        IMap<String, GenericRecord> map = mapWithStore(table, new Properties());
        String id = "0b0e3c6e-3f5e-4c39-9d2f-4b3c1c7d2a10";
        OffsetDateTime at = OffsetDateTime.of(2026, 10, 25, 12, 30, 0, 123_000_000, ZoneOffset.UTC);
        GenericRecord value = GenericRecordBuilder.compact(table)
                .setBoolean("flag", true)
                .setInt8("tiny", (byte) -3)
                .setInt32("small", 200)
                .setInt64("count", 4_000_000_000L)
                .setDecimal("big", new BigDecimal("18446744073709551615"))
                .setFloat32("ratio", 0.5f)
                .setDecimal("amount", new BigDecimal("12.3456"))
                .setString("label", "red")
                .setString("kind", "b")
                .setString("address", "10.1.2.3")
                .setDate("day", LocalDate.of(2026, 10, 25))
                .setTimestampWithTimezone("at", at)
                .setString("note", null)
                .build();

        map.put(id, value);
        map.evictAll();

        GenericRecord loaded = map.get(id);
        assertThat(loaded.getBoolean("flag")).isTrue();
        assertThat(loaded.getInt8("tiny")).isEqualTo((byte) -3);
        assertThat(loaded.getInt16("small")).isEqualTo((short) 200);
        assertThat(loaded.getInt64("count")).isEqualTo(4_000_000_000L);
        assertThat(loaded.getDecimal("big")).isEqualTo(new BigDecimal("18446744073709551615"));
        assertThat(loaded.getFloat32("ratio")).isEqualTo(0.5f);
        assertThat(loaded.getDecimal("amount")).isEqualTo(new BigDecimal("12.3456"));
        assertThat(loaded.getString("label")).isEqualTo("red");
        assertThat(loaded.getString("kind")).isEqualTo("b");
        assertThat(loaded.getString("address")).isEqualTo("10.1.2.3");
        assertThat(loaded.getDate("day")).isEqualTo(LocalDate.of(2026, 10, 25));
        assertThat(loaded.getTimestampWithTimezone("at")).isEqualTo(at);
        assertThat(loaded.getString("note")).isNull();
        assertThat(map.keySet()).containsExactly(id);
    }

    @Test
    public void loadsKeysThatClickHouseReturnsInAnotherForm() {
        String decimals = newTableName("decimals");
        execute("CREATE TABLE " + decimals + " (id Decimal(18, 4), name String, version UInt64, is_deleted UInt8) "
                + "ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY id");
        Properties singleColumn = new Properties();
        singleColumn.setProperty(ClickHouseMapStore.SINGLE_COLUMN_AS_VALUE_PROPERTY, "true");
        IMap<BigDecimal, String> byDecimal = mapWithStore(decimals, singleColumn);
        byDecimal.put(new BigDecimal("1.5"), "one and a half");
        byDecimal.evictAll();
        assertThat(byDecimal.get(new BigDecimal("1.5"))).isEqualTo("one and a half");

        String times = newTableName("times");
        execute("CREATE TABLE " + times + " (id DateTime64(3, 'UTC'), name String, version UInt64, is_deleted UInt8) "
                + "ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY id");
        IMap<OffsetDateTime, String> byTime = mapWithStore(times, singleColumn);
        OffsetDateTime at = OffsetDateTime.of(2026, 10, 25, 14, 0, 0, 0, ZoneOffset.ofHours(2));
        byTime.put(at, "two o'clock");
        byTime.evictAll();
        assertThat(byTime.get(at)).isEqualTo("two o'clock");
        assertThat(byTime.get(at.withOffsetSameInstant(ZoneOffset.UTC))).isEqualTo("two o'clock");

        String addresses = newTableName("addresses");
        execute("CREATE TABLE " + addresses + " (id IPv4, name String, version UInt64, is_deleted UInt8) "
                + "ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY id");
        IMap<String, String> byAddress = mapWithStore(addresses, singleColumn);
        byAddress.put("10.1.2.3", "host");
        byAddress.evictAll();
        assertThat(byAddress.get("10.1.2.3")).isEqualTo("host");
        assertThat(byAddress.keySet()).containsExactly("10.1.2.3");
    }

    @Test
    public void refusesATableWithoutReplacingMergeTree() {
        String table = newTableName("plain");
        execute("CREATE TABLE " + table + " (id Int64, name String, version UInt64, is_deleted UInt8) "
                + "ENGINE = MergeTree ORDER BY id");
        IMap<Long, GenericRecord> map = mapWithStore(table, new Properties());

        assertThatThrownBy(() -> map.put(1L, GenericRecordBuilder.compact(table).setString("name", "x").build()))
                .hasStackTraceContaining("needs a table with the engine ReplacingMergeTree(version, is_deleted)")
                .hasStackTraceContaining("its engine is MergeTree");
    }

    @Test
    public void refusesATableOrderedByMoreThanTheKey() {
        String table = newTableName("ordered");
        execute("CREATE TABLE " + table + " (id Int64, name String, version UInt64, is_deleted UInt8) "
                + "ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY (name, id)");
        IMap<Long, GenericRecord> map = mapWithStore(table, new Properties());

        assertThatThrownBy(() -> map.put(1L, GenericRecordBuilder.compact(table).setString("name", "x").build()))
                .hasStackTraceContaining("it is ordered by 'name, id'");
    }

    @Test
    public void refusesAValueWithoutAColumnsField() {
        String table = createPeopleTable();
        IMap<Long, GenericRecord> map = mapWithStore(table, new Properties());

        assertThatThrownBy(() -> map.put(1L, GenericRecordBuilder.compact(table).setString("name", "x").build()))
                .hasStackTraceContaining("The record has no field 'age'");
    }
}
