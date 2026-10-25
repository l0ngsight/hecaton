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
import uk.org.hecaton.jet.pipeline.DataConnectionRef;
import uk.org.hecaton.jet.pipeline.Sink;

import javax.annotation.Nonnull;

/**
 * Sinks that insert into ClickHouse tables.
 *
 * @since 1.0
 */
public final class ClickHouseSinks {

    private ClickHouseSinks() {
    }

    /**
     * Returns a builder of a sink that inserts into a table. See {@link ClickHouseSinkBuilder}.
     *
     * @param name    the name of the sink
     * @param table   the table, {@code table} or {@code database.table}
     * @param toRowFn returns the values of the columns for an item, in the order of the
     *                {@linkplain ClickHouseSinkBuilder#columns columns}
     * @param <T>     the type of the items
     */
    @Nonnull
    public static <T> ClickHouseSinkBuilder<T> builder(
            @Nonnull String name,
            @Nonnull String table,
            @Nonnull FunctionEx<? super T, Object[]> toRowFn
    ) {
        return new ClickHouseSinkBuilder<>(name, table, toRowFn);
    }

    /**
     * Returns a sink that inserts into some columns of a table, through a
     * {@link ClickHouseDataConnection}.
     *
     * @param name              the name of the sink
     * @param dataConnectionRef the data connection
     * @param table             the table, {@code table} or {@code database.table}
     * @param columns           the columns to insert, separated by commas
     * @param toRowFn           returns the values of the columns for an item, in their order
     * @param <T>               the type of the items
     */
    @Nonnull
    public static <T> Sink<T> insert(
            @Nonnull String name,
            @Nonnull DataConnectionRef dataConnectionRef,
            @Nonnull String table,
            @Nonnull String columns,
            @Nonnull FunctionEx<? super T, Object[]> toRowFn
    ) {
        return ClickHouseSinks.<T>builder(name, table, toRowFn).dataConnectionRef(dataConnectionRef)
                                            .columns(columns.split(","))
                                            .build();
    }
}
