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
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseColumn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The columns and the engine of a ClickHouse table.
 */
public final class TableMetadata {

    private static final Pattern ENGINE_ARGUMENTS = Pattern.compile("^\\w+\\(([^)]*)\\)");

    private final TableName name;
    private final Map<String, ClickHouseColumn> columns;
    private final String engine;
    private final String sortingKey;
    private final List<String> engineArguments;

    private TableMetadata(TableName name, Map<String, ClickHouseColumn> columns, String engine, String sortingKey,
                          List<String> engineArguments) {
        this.name = name;
        this.columns = columns;
        this.engine = engine;
        this.sortingKey = sortingKey;
        this.engineArguments = engineArguments;
    }

    /**
     * Reads the metadata of a table.
     *
     * @throws HecatonException if the table does not exist or cannot be read
     */
    public static TableMetadata read(Client client, TableName name) {
        Map<String, ClickHouseColumn> columns = new LinkedHashMap<>();
        String engine;
        String sortingKey;
        String engineFull;
        try {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("table", name.table());
            String database = "currentDatabase()";
            if (name.database() != null) {
                parameters.put("database", name.database());
                database = "{database:String}";
            }
            try (QueryResponse response = client.query("SELECT engine, sorting_key, engine_full FROM system.tables "
                    + "WHERE database = " + database + " AND name = {table:String}", parameters).get();
                 ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
                if (!reader.hasNext()) {
                    throw new HecatonException("The ClickHouse table " + name + " does not exist");
                }
                reader.next();
                engine = reader.getString(1);
                sortingKey = reader.getString(2);
                engineFull = reader.getString(3);
            }
            TableSchema schema = name.database() == null
                    ? client.getTableSchema(name.table())
                    : client.getTableSchema(name.table(), name.database());
            for (ClickHouseColumn column : schema.getColumns()) {
                columns.put(column.getColumnName(), column);
            }
        } catch (HecatonException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HecatonException("Interrupted while reading the ClickHouse table " + name, e);
        } catch (Exception e) {
            throw new HecatonException("Unable to read the ClickHouse table " + name + ": " + e.getMessage(), e);
        }
        return new TableMetadata(name, columns, engine, unquoteAll(sortingKey), engineArguments(engineFull));
    }

    private static List<String> engineArguments(String engineFull) {
        List<String> arguments = new ArrayList<>();
        Matcher matcher = ENGINE_ARGUMENTS.matcher(engineFull == null ? "" : engineFull);
        if (matcher.find()) {
            for (String argument : matcher.group(1).split(",")) {
                String stripped = argument.strip();
                // the ZooKeeper path and replica name of a replicated engine are string literals
                if (!stripped.isEmpty() && !stripped.startsWith("'")) {
                    arguments.add(unquoteAll(stripped));
                }
            }
        }
        return arguments;
    }

    private static String unquoteAll(String identifier) {
        return identifier == null ? "" : identifier.replace("`", "").strip();
    }

    public TableName name() {
        return name;
    }

    /**
     * Returns the columns by name, in the order of the table.
     */
    public Map<String, ClickHouseColumn> columns() {
        return columns;
    }

    /**
     * Returns the type of a column, or fails if the table has no such column or its type is not
     * supported.
     */
    public ColumnType column(String columnName) {
        ClickHouseColumn column = columns.get(columnName);
        if (column == null) {
            throw new HecatonException("The ClickHouse table " + name + " has no column '" + columnName
                    + "'; its columns are " + columns.keySet());
        }
        try {
            return ColumnType.of(column);
        } catch (IllegalArgumentException e) {
            throw new HecatonException("ClickHouse table " + name + ": " + e.getMessage(), e);
        }
    }

    public String engine() {
        return engine;
    }

    public String sortingKey() {
        return sortingKey;
    }

    /**
     * Returns the column arguments of the engine, for example {@code [version, is_deleted]} for
     * {@code ReplicatedReplacingMergeTree('/path', '{replica}', version, is_deleted)}.
     */
    public List<String> engineArguments() {
        return engineArguments;
    }
}
