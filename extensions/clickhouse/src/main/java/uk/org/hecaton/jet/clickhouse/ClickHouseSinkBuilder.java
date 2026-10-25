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

import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.clickhouse.impl.ClientSource;
import uk.org.hecaton.jet.clickhouse.impl.ColumnType;
import uk.org.hecaton.jet.clickhouse.impl.RowInserter;
import uk.org.hecaton.jet.clickhouse.impl.TableMetadata;
import uk.org.hecaton.jet.clickhouse.impl.TableName;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.pipeline.DataConnectionRef;
import uk.org.hecaton.jet.pipeline.Sink;
import uk.org.hecaton.jet.pipeline.SinkBuilder;
import com.clickhouse.client.api.Client;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static uk.org.hecaton.internal.util.Preconditions.checkPositive;
import static uk.org.hecaton.internal.util.Preconditions.checkState;

/**
 * Builds a sink that inserts items into a ClickHouse table, one row per item, in batches of up
 * to {@linkplain #batchSize batch size} rows. A batch is also inserted when the sink has no more
 * items for the moment and before each snapshot of the job.
 * <p>
 * Each batch is one insert with its own deduplication token ({@code insert_deduplication_token}),
 * and a failed insert is retried with the same token: on a replicated table, or a table with
 * {@code non_replicated_deduplication_window}, ClickHouse does not insert a retried batch twice.
 * The sink inserts at least once: after a job restarts it can insert rows again, which a
 * {@code ReplacingMergeTree} table with a version column collapses.
 * <pre>{@code
 * Sink<Person> sink = ClickHouseSinks
 *         .<Person>builder("people", "people", p -> new Object[]{p.id(), p.name(), p.version(), 0})
 *         .dataConnectionRef(dataConnectionRef("clickhouse"))
 *         .columns("id", "name", "version", "is_deleted")
 *         .build();
 * }</pre>
 *
 * @param <T> the type of the items
 * @since 1.0
 */
public final class ClickHouseSinkBuilder<T> {

    private static final int DEFAULT_BATCH_SIZE = 10_000;
    private static final int DEFAULT_INSERT_ATTEMPTS = 3;
    private static final int DEFAULT_LOCAL_PARALLELISM = 2;

    private final String name;
    private final String table;
    private final FunctionEx<? super T, Object[]> toRowFn;
    private ClientSource clientSource;
    private List<String> columns = List.of();
    private int batchSize = DEFAULT_BATCH_SIZE;
    private int insertAttempts = DEFAULT_INSERT_ATTEMPTS;
    private int localParallelism = DEFAULT_LOCAL_PARALLELISM;
    private final Map<String, String> settings = new HashMap<>();

    ClickHouseSinkBuilder(@Nonnull String name, @Nonnull String table, @Nonnull FunctionEx<? super T, Object[]> toRowFn) {
        this.name = checkNotNull(name, "name");
        this.table = checkNotNull(table, "table");
        this.toRowFn = checkNotNull(toRowFn, "toRowFn");
    }

    /**
     * Inserts through the clients of a {@link ClickHouseDataConnection}.
     */
    @Nonnull
    public ClickHouseSinkBuilder<T> dataConnectionRef(@Nonnull DataConnectionRef dataConnectionRef) {
        clientSource = ClientSource.of(checkNotNull(dataConnectionRef, "dataConnectionRef"));
        return this;
    }

    /**
     * Inserts through clients the function creates; each processor creates one and closes it
     * at the end.
     */
    @Nonnull
    public ClickHouseSinkBuilder<T> clientFn(@Nonnull SupplierEx<Client> clientFn) {
        clientSource = ClientSource.of(checkNotNull(clientFn, "clientFn"));
        return this;
    }

    /**
     * The columns to insert, in the order of the values {@code toRowFn} returns (default: all
     * columns of the table, in their order).
     */
    @Nonnull
    public ClickHouseSinkBuilder<T> columns(@Nonnull String... columns) {
        List<String> stripped = new ArrayList<>();
        for (String column : columns) {
            stripped.add(column.strip());
        }
        this.columns = List.copyOf(stripped);
        return this;
    }

    /**
     * The largest number of rows in one insert (default 10,000).
     */
    @Nonnull
    public ClickHouseSinkBuilder<T> batchSize(int batchSize) {
        this.batchSize = checkPositive("batchSize", batchSize);
        return this;
    }

    /**
     * How many times a batch is tried before the job fails (default 3).
     */
    @Nonnull
    public ClickHouseSinkBuilder<T> insertAttempts(int insertAttempts) {
        this.insertAttempts = checkPositive("insertAttempts", insertAttempts);
        return this;
    }

    /**
     * A ClickHouse setting of each insert, for example {@code insert_quorum}.
     */
    @Nonnull
    public ClickHouseSinkBuilder<T> setting(@Nonnull String setting, @Nonnull String value) {
        settings.put(checkNotNull(setting, "setting"), checkNotNull(value, "value"));
        return this;
    }

    /**
     * The processors of the sink on each member (default 2).
     */
    @Nonnull
    public ClickHouseSinkBuilder<T> localParallelism(int localParallelism) {
        this.localParallelism = checkPositive("localParallelism", localParallelism);
        return this;
    }

    /**
     * Builds the sink.
     */
    @Nonnull
    public Sink<T> build() {
        checkState(clientSource != null, "Set dataConnectionRef or clientFn");
        ClientSource source = clientSource;
        String sinkTable = table;
        List<String> sinkColumns = columns;
        Map<String, String> sinkSettings = Map.copyOf(settings);
        int sinkBatchSize = batchSize;
        int attempts = insertAttempts;
        FunctionEx<? super T, Object[]> rowFn = toRowFn;
        return SinkBuilder
                .sinkBuilder(name, context -> new Writer<T>(source, context, sinkTable, sinkColumns, sinkSettings,
                        sinkBatchSize, attempts, rowFn))
                .<T>receiveFn(Writer::add)
                .flushFn(Writer::flush)
                .destroyFn(Writer::close)
                .preferredLocalParallelism(localParallelism)
                .build();
    }

    /**
     * The state of one processor: the rows of the current batch.
     */
    private static final class Writer<T> {

        private final ClientSource.Lease lease;
        private final RowInserter inserter;
        private final int batchSize;
        private final FunctionEx<? super T, Object[]> toRowFn;
        private final List<Object[]> rows = new ArrayList<>();

        @SuppressWarnings("checkstyle:ParameterNumber")
        Writer(ClientSource source, Processor.Context context, String table, List<String> columns,
               Map<String, String> settings, int batchSize, int insertAttempts, FunctionEx<? super T, Object[]> toRowFn) {
            this.batchSize = batchSize;
            this.toRowFn = toRowFn;
            this.lease = source.open(context.dataConnectionService());
            try {
                TableMetadata metadata = TableMetadata.read(lease.client(), TableName.parse(table));
                List<ColumnType> types = new ArrayList<>();
                for (String column : columns.isEmpty() ? metadata.columns().keySet() : columns) {
                    types.add(metadata.column(column));
                }
                inserter = new RowInserter(metadata.name(), types, settings, insertAttempts);
            } catch (RuntimeException e) {
                lease.close();
                throw e;
            }
        }

        void add(T item) {
            rows.add(inserter.convert(toRowFn.apply(item)));
            if (rows.size() >= batchSize) {
                flush();
            }
        }

        void flush() {
            if (!rows.isEmpty()) {
                inserter.insert(lease.client(), rows);
                rows.clear();
            }
        }

        void close() {
            lease.close();
        }
    }
}
