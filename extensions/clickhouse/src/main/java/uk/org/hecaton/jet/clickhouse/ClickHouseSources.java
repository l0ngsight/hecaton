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
import uk.org.hecaton.jet.pipeline.BatchSource;
import uk.org.hecaton.jet.pipeline.DataConnectionRef;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;

import javax.annotation.Nonnull;

/**
 * Batch sources that read from ClickHouse.
 *
 * @since 1.0
 */
public final class ClickHouseSources {

    private ClickHouseSources() {
    }

    /**
     * Returns a builder of a source that reads a query or a table. See
     * {@link ClickHouseSourceBuilder}.
     *
     * @param name  the name of the source
     * @param mapFn creates an item from the current row of the reader; it reads the row with the
     *              reader's getters, by column name or position (from 1), and must not move it
     * @param <T>   the type of the items
     */
    @Nonnull
    public static <T> ClickHouseSourceBuilder<T> builder(
            @Nonnull String name,
            @Nonnull FunctionEx<? super ClickHouseBinaryFormatReader, ? extends T> mapFn
    ) {
        return new ClickHouseSourceBuilder<>(name, mapFn);
    }

    /**
     * Returns a source that runs a query, on one member, through a {@link ClickHouseDataConnection}.
     *
     * @param name              the name of the source
     * @param dataConnectionRef the data connection
     * @param query             the query
     * @param mapFn             creates an item from the current row of the reader
     * @param <T>               the type of the items
     */
    @Nonnull
    public static <T> BatchSource<T> query(
            @Nonnull String name,
            @Nonnull DataConnectionRef dataConnectionRef,
            @Nonnull String query,
            @Nonnull FunctionEx<? super ClickHouseBinaryFormatReader, ? extends T> mapFn
    ) {
        return ClickHouseSources.<T>builder(name, mapFn).dataConnectionRef(dataConnectionRef).query(query).build();
    }

    /**
     * Returns a source that reads a table in parallel on all members, through a
     * {@link ClickHouseDataConnection}: each processor reads the rows whose {@code splitColumn}
     * hashes to it. With {@code useFinal}, the table is read with {@code FINAL}, for example to
     * read only the latest rows of a {@code ReplacingMergeTree} table.
     *
     * @param name              the name of the source
     * @param dataConnectionRef the data connection
     * @param table             the table, {@code table} or {@code database.table}
     * @param splitColumn       the column that splits the rows between the processors, best the
     *                          table's key
     * @param useFinal          whether to read with {@code FINAL}
     * @param mapFn             creates an item from the current row of the reader
     * @param <T>               the type of the items
     */
    @Nonnull
    public static <T> BatchSource<T> table(
            @Nonnull String name,
            @Nonnull DataConnectionRef dataConnectionRef,
            @Nonnull String table,
            @Nonnull String splitColumn,
            boolean useFinal,
            @Nonnull FunctionEx<? super ClickHouseBinaryFormatReader, ? extends T> mapFn
    ) {
        return ClickHouseSources.<T>builder(name, mapFn).dataConnectionRef(dataConnectionRef).table(table).splitColumn(splitColumn)
                                   .useFinal(useFinal).build();
    }
}
