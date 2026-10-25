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

import uk.org.hecaton.collection.IList;
import uk.org.hecaton.jet.datamodel.Tuple2;
import uk.org.hecaton.jet.pipeline.BatchSource;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sink;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.test.TestSources;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static uk.org.hecaton.jet.datamodel.Tuple2.tuple2;
import static uk.org.hecaton.jet.pipeline.DataConnectionRef.dataConnectionRef;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class ClickHouseSourcesAndSinksTest extends ClickHouseTestSupport {

    private static final int ITEM_COUNT = 25_000;

    @BeforeClass
    public static void beforeClass() {
        initializeMembers();
    }

    private static String createEventsTable() {
        String table = newTableName("events");
        execute("CREATE TABLE " + table + " (id Int64, kind String, amount Nullable(Float64), version UInt64, "
                + "is_deleted UInt8) ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY id");
        return table;
    }

    private static List<Integer> items() {
        return IntStream.range(0, ITEM_COUNT).boxed().collect(Collectors.toList());
    }

    private static Object[] row(int i) {
        return new Object[]{i, i % 2 == 0 ? "even" : "odd", i % 10 == 0 ? null : i * 1.5, 1, 0};
    }

    @Test
    public void sinkInsertsAllItems() {
        String table = createEventsTable();
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemsDistributed(items()))
         .writeTo(ClickHouseSinks.<Integer>builder("events", table, ClickHouseSourcesAndSinksTest::row)
                                 .dataConnectionRef(dataConnectionRef(DATA_CONNECTION))
                                 .columns("id", "kind", "amount", "version", "is_deleted")
                                 .batchSize(1000)
                                 .build());

        instance().getJet().newJob(p).join();

        assertThat(count("SELECT count() FROM " + table)).isEqualTo(ITEM_COUNT);
        assertThat(count("SELECT countIf(amount IS NULL) FROM " + table)).isEqualTo(ITEM_COUNT / 10);
        assertThat(query("SELECT kind, amount FROM " + table + " WHERE id = 7")).containsExactly(List.of("odd", 10.5));
    }

    @Test
    public void sinkWithAllColumnsByDefault() {
        String table = createEventsTable();
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1, 2, 3))
         .writeTo(ClickHouseSinks.<Integer>builder("events", table, ClickHouseSourcesAndSinksTest::row)
                                 .clientFn(() -> newClient(endpoint(clickhouse)))
                                 .build());

        instance().getJet().newJob(p).join();

        assertThat(count("SELECT count() FROM " + table)).isEqualTo(3);
    }

    @Test
    public void sinkFailsTheJobForAValueThatDoesNotFit() {
        String table = createEventsTable();
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1))
         .writeTo(ClickHouseSinks.insert("events", dataConnectionRef(DATA_CONNECTION), table, "id, kind",
                 (Integer i) -> new Object[]{i, null}));

        assertThatThrownBy(() -> instance().getJet().newJob(p).join())
                .hasStackTraceContaining("Column 'kind' is not Nullable, but the value is null");
    }

    @Test
    public void tableSourceReadsInParallelWithFinal() {
        String table = createEventsTable();
        execute("INSERT INTO " + table + " SELECT number, 'old', number, 1, 0 FROM numbers(" + ITEM_COUNT + ")");
        execute("INSERT INTO " + table + " SELECT number, 'new', number, 2, 0 FROM numbers(" + ITEM_COUNT + ")");
        execute("INSERT INTO " + table + " SELECT number, 'new', number, 3, 1 FROM numbers(10)");
        IList<String> result = instance().getList(newTableName("result"));

        Pipeline p = Pipeline.create();
        BatchSource<String> source = ClickHouseSources
                .builder("events", row -> row.getLong("id") + ":" + row.getString("kind"))
                .dataConnectionRef(dataConnectionRef(DATA_CONNECTION))
                .table(table)
                .columns("id", "kind")
                .filter("is_deleted = 0")
                .useFinal(true)
                .splitColumn("id")
                .localParallelism(3)
                .build();
        p.readFrom(source).writeTo(Sinks.list(result));

        instance().getJet().newJob(p).join();

        assertThat(result).hasSize(ITEM_COUNT - 10)
                          .doesNotHaveDuplicates()
                          .allMatch(s -> s.endsWith(":new"))
                          .doesNotContain("3:new");
    }

    @Test
    public void querySourceReadsOnOneProcessor() {
        String table = createEventsTable();
        execute("INSERT INTO " + table + " SELECT number, if(number % 2 = 0, 'even', 'odd'), number, 1, 0 "
                + "FROM numbers(1000)");
        IList<Tuple2<String, Long>> result = instance().getList(newTableName("result"));

        Pipeline p = Pipeline.create();
        p.readFrom(ClickHouseSources.query("kinds", dataConnectionRef(DATA_CONNECTION),
                 "SELECT kind, count() AS c FROM " + table + " GROUP BY kind ORDER BY kind",
                 row -> tuple2(row.getString("kind"), row.getLong("c"))))
         .writeTo(Sinks.list(result));

        instance().getJet().newJob(p).join();

        assertThat(result).containsExactly(tuple2("even", 500L), tuple2("odd", 500L));
    }

    @Test
    public void sinkAndSourceRoundTrip() {
        String table = createEventsTable();
        Pipeline write = Pipeline.create();
        Sink<Integer> sink = ClickHouseSinks.insert("events", dataConnectionRef(DATA_CONNECTION), table,
                "id, kind, amount, version, is_deleted", ClickHouseSourcesAndSinksTest::row);
        write.readFrom(TestSources.items(items())).writeTo(sink);
        instance().getJet().newJob(write).join();

        IList<Long> ids = instance().getList(newTableName("ids"));
        Pipeline read = Pipeline.create();
        read.readFrom(ClickHouseSources.table("events", dataConnectionRef(DATA_CONNECTION), table, "id", true,
                    row -> row.getLong("id")))
            .writeTo(Sinks.list(ids));
        instance().getJet().newJob(read).join();

        assertThat(ids).hasSize(ITEM_COUNT).doesNotHaveDuplicates();
    }
}
