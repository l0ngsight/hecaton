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

package uk.org.hecaton.jet.clickhouse.impl;

import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.internal.util.UuidUtil;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.util.concurrent.ExecutionException;

/**
 * Inserts rows into some columns of a table in the {@code RowBinary} format. Each call is one
 * insert with its own deduplication token; a failed insert is retried with the same token, so
 * that ClickHouse does not insert the rows twice when the first attempt did reach the table.
 */
public final class RowInserter {

    private static final long FIRST_BACKOFF_MILLIS = 100;

    private final TableName table;
    private final List<ColumnType> columns;
    private final List<String> quotedColumnNames = new ArrayList<>();
    private final TableSchema schema;
    private final Map<String, String> serverSettings;
    private final int maxAttempts;

    /**
     * @param serverSettings ClickHouse settings of each insert, for example {@code insert_quorum}
     * @param maxAttempts    how many times an insert is tried before it fails
     */
    public RowInserter(TableName table, List<ColumnType> columns, Map<String, String> serverSettings,
                       int maxAttempts) {
        this.table = table;
        this.columns = List.copyOf(columns);
        List<ClickHouseColumn> schemaColumns = new ArrayList<>();
        for (ColumnType column : columns) {
            schemaColumns.add(column.column());
            quotedColumnNames.add(TableName.quote(column.name()));
        }
        this.schema = new TableSchema(schemaColumns);
        this.serverSettings = Map.copyOf(serverSettings);
        this.maxAttempts = maxAttempts;
    }

    public List<ColumnType> columns() {
        return columns;
    }

    /**
     * Converts the values of a row, in the order of the columns, to the values the client writes.
     *
     * @throws IllegalArgumentException if a value does not fit its column
     */
    public Object[] convert(Object[] row) {
        if (row.length != columns.size()) {
            throw new IllegalArgumentException("A row for " + table + " has " + row.length + " values, but "
                    + columns.size() + " columns are written: " + quotedColumnNames);
        }
        Object[] converted = new Object[row.length];
        for (int i = 0; i < row.length; i++) {
            converted[i] = columns.get(i).toClickHouse(row[i]);
        }
        return converted;
    }

    /**
     * Inserts rows already {@linkplain #convert converted}, in one insert.
     */
    public void insert(Client client, List<Object[]> convertedRows) {
        if (convertedRows.isEmpty()) {
            return;
        }
        InsertSettings settings = new InsertSettings().setDeduplicationToken(UuidUtil.newUnsecureUuidString());
        serverSettings.forEach(settings::serverSetting);
        Exception failure = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try (InsertResponse ignored = client.insert(table.sql(), quotedColumnNames, out -> {
                RowBinaryFormatWriter writer = new RowBinaryFormatWriter(out, schema, ClickHouseFormat.RowBinary);
                for (Object[] row : convertedRows) {
                    for (int i = 0; i < row.length; i++) {
                        writer.setValue(i + 1, row[i]);
                    }
                    writer.commitRow();
                }
            }, ClickHouseFormat.RowBinary, settings).get()) {
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new HecatonException("Interrupted while inserting into " + table, e);
            } catch (ExecutionException | RuntimeException e) {
                failure = e;
                if (attempt < maxAttempts) {
                    sleep(FIRST_BACKOFF_MILLIS << (attempt - 1));
                }
            }
        }
        Throwable cause = failure instanceof ExecutionException && failure.getCause() != null
                ? failure.getCause() : failure;
        throw new HecatonException("Inserting " + convertedRows.size() + " rows into " + table + " failed after "
                + maxAttempts + " attempts: " + cause.getMessage(), cause);
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HecatonException("Interrupted while inserting into " + table, e);
        }
    }
}
