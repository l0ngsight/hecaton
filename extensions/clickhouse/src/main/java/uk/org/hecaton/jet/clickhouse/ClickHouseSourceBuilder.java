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

import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.clickhouse.impl.ClientSource;
import uk.org.hecaton.jet.clickhouse.impl.TableName;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.pipeline.BatchSource;
import uk.org.hecaton.jet.pipeline.DataConnectionRef;
import uk.org.hecaton.jet.pipeline.SourceBuilder;
import uk.org.hecaton.jet.pipeline.SourceBuilder.SourceBuffer;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static uk.org.hecaton.internal.util.Preconditions.checkPositive;
import static uk.org.hecaton.internal.util.Preconditions.checkState;

/**
 * Builds a batch source that reads from ClickHouse, either
 * <ul>
 *     <li>a {@linkplain #query query}, read by one processor in the cluster, or</li>
 *     <li>a {@linkplain #table table}, with optional {@linkplain #columns columns},
 *     {@linkplain #filter filter} and {@linkplain #useFinal FINAL}. With a
 *     {@linkplain #splitColumn split column}, every member reads the table with
 *     {@linkplain #localParallelism several processors}, each the rows whose split column hashes
 *     to it ({@code cityHash64(column) % total = index}); without one, a single processor reads
 *     it.</li>
 * </ul>
 * The client comes from a {@linkplain #dataConnectionRef data connection} or a
 * {@linkplain #clientFn function}.
 * <pre>{@code
 * BatchSource<Person> people = ClickHouseSources
 *         .builder("people", row -> new Person(row.getLong("id"), row.getString("name")))
 *         .dataConnectionRef(dataConnectionRef("clickhouse"))
 *         .table("people")
 *         .columns("id", "name")
 *         .filter("is_deleted = 0")
 *         .useFinal(true)
 *         .splitColumn("id")
 *         .build();
 * }</pre>
 *
 * @param <T> the type of the items
 * @since 1.0
 */
public final class ClickHouseSourceBuilder<T> {

    private static final int DEFAULT_LOCAL_PARALLELISM = 2;
    private static final int MAX_ITEMS_PER_CALL = 1024;

    private final String name;
    private final FunctionEx<? super ClickHouseBinaryFormatReader, ? extends T> mapFn;
    private ClientSource clientSource;
    private String query;
    private String table;
    private List<String> columns = List.of();
    private String filter;
    private boolean useFinal;
    private String splitColumn;
    private int localParallelism = DEFAULT_LOCAL_PARALLELISM;

    ClickHouseSourceBuilder(@Nonnull String name,
                            @Nonnull FunctionEx<? super ClickHouseBinaryFormatReader, ? extends T> mapFn) {
        this.name = checkNotNull(name, "name");
        this.mapFn = checkNotNull(mapFn, "mapFn");
    }

    /**
     * Reads through the clients of a {@link ClickHouseDataConnection}.
     */
    @Nonnull
    public ClickHouseSourceBuilder<T> dataConnectionRef(@Nonnull DataConnectionRef dataConnectionRef) {
        clientSource = ClientSource.of(checkNotNull(dataConnectionRef, "dataConnectionRef"));
        return this;
    }

    /**
     * Reads through clients the function creates; each processor creates one and closes it at
     * the end.
     */
    @Nonnull
    public ClickHouseSourceBuilder<T> clientFn(@Nonnull SupplierEx<Client> clientFn) {
        clientSource = ClientSource.of(checkNotNull(clientFn, "clientFn"));
        return this;
    }

    /**
     * Reads the result of a query, with one processor.
     */
    @Nonnull
    public ClickHouseSourceBuilder<T> query(@Nonnull String query) {
        this.query = checkNotNull(query, "query");
        return this;
    }

    /**
     * Reads a table, {@code table} or {@code database.table}.
     */
    @Nonnull
    public ClickHouseSourceBuilder<T> table(@Nonnull String table) {
        this.table = checkNotNull(table, "table");
        return this;
    }

    /**
     * Reads these columns of the table (default: all).
     */
    @Nonnull
    public ClickHouseSourceBuilder<T> columns(@Nonnull String... columns) {
        this.columns = List.of(columns);
        return this;
    }

    /**
     * Reads only the rows of the table that match a condition, a ClickHouse SQL expression.
     */
    @Nonnull
    public ClickHouseSourceBuilder<T> filter(@Nonnull String condition) {
        this.filter = checkNotNull(condition, "condition");
        return this;
    }

    /**
     * Reads the table with {@code FINAL} (default {@code false}).
     */
    @Nonnull
    public ClickHouseSourceBuilder<T> useFinal(boolean useFinal) {
        this.useFinal = useFinal;
        return this;
    }

    /**
     * Reads the table in parallel, split by the hash of this column.
     */
    @Nonnull
    public ClickHouseSourceBuilder<T> splitColumn(@Nonnull String splitColumn) {
        this.splitColumn = checkNotNull(splitColumn, "splitColumn");
        return this;
    }

    /**
     * The processors on each member that read a table split by a column (default 2).
     */
    @Nonnull
    public ClickHouseSourceBuilder<T> localParallelism(int localParallelism) {
        this.localParallelism = checkPositive("localParallelism", localParallelism);
        return this;
    }

    /**
     * Builds the source.
     */
    @Nonnull
    public BatchSource<T> build() {
        checkState(clientSource != null, "Set dataConnectionRef or clientFn");
        checkState((query == null) != (table == null), "Set either query or table");
        checkState(query == null || (columns.isEmpty() && filter == null && !useFinal && splitColumn == null),
                "columns, filter, useFinal and splitColumn apply to a table, not to a query");
        String sql = query != null ? query : tableQuery();
        boolean split = splitColumn != null;
        ClientSource source = clientSource;
        FunctionEx<? super ClickHouseBinaryFormatReader, ? extends T> mapFn = this.mapFn;
        String sourceName = name;
        SourceBuilder<Reader<T>>.Batch<T> builder = SourceBuilder
                .batch(name, context -> new Reader<T>(sourceName, source, context, sql, split, mapFn))
                .<T>fillBufferFn(Reader::fill)
                .destroyFn(Reader::close);
        if (split) {
            builder = builder.distributed(localParallelism);
        }
        return builder.build();
    }

    private String tableQuery() {
        StringBuilder sql = new StringBuilder("SELECT ");
        if (columns.isEmpty()) {
            sql.append('*');
        } else {
            for (int i = 0; i < columns.size(); i++) {
                sql.append(i == 0 ? "" : ", ").append(TableName.quote(columns.get(i)));
            }
        }
        sql.append(" FROM ").append(TableName.parse(table).sql());
        if (useFinal) {
            sql.append(" FINAL");
        }
        if (filter != null) {
            sql.append(" WHERE (").append(filter).append(')');
        }
        if (splitColumn != null) {
            sql.append(filter != null ? " AND " : " WHERE ")
               .append("cityHash64(").append(TableName.quote(splitColumn)).append(") % {total:UInt32} = {index:UInt32}");
        }
        return sql.toString();
    }

    /**
     * The state of one processor: the query's response, read as the buffer is filled.
     */
    private static final class Reader<T> {

        private final String name;
        private final FunctionEx<? super ClickHouseBinaryFormatReader, ? extends T> mapFn;
        private final ClientSource.Lease lease;
        private final QueryResponse response;
        private final ClickHouseBinaryFormatReader reader;

        Reader(String name, ClientSource source, Processor.Context context, String sql, boolean split,
               FunctionEx<? super ClickHouseBinaryFormatReader, ? extends T> mapFn) {
            this.name = name;
            this.mapFn = mapFn;
            this.lease = source.open(context.dataConnectionService());
            try {
                Client client = lease.client();
                if (split) {
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put("total", context.totalParallelism());
                    parameters.put("index", context.globalProcessorIndex());
                    response = client.query(sql, parameters).get();
                } else {
                    response = client.query(sql).get();
                }
                reader = client.newBinaryFormatReader(response);
            } catch (Exception e) {
                lease.close();
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new HecatonException("The ClickHouse source '" + name + "' could not run its query: "
                        + e.getMessage(), e);
            }
        }

        void fill(SourceBuffer<T> buffer) {
            for (int i = 0; i < MAX_ITEMS_PER_CALL; i++) {
                if (!reader.hasNext()) {
                    buffer.close();
                    return;
                }
                reader.next();
                buffer.add(mapFn.apply(reader));
            }
        }

        void close() {
            try {
                reader.close();
                response.close();
            } catch (Exception e) {
                throw new HecatonException("The ClickHouse source '" + name + "' could not close its query", e);
            } finally {
                lease.close();
            }
        }
    }
}
