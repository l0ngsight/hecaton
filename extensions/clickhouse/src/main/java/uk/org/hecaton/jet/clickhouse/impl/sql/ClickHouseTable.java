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

package uk.org.hecaton.jet.clickhouse.impl.sql;

import uk.org.hecaton.jet.clickhouse.impl.TableName;
import uk.org.hecaton.jet.sql.impl.connector.SqlConnector;
import uk.org.hecaton.jet.sql.impl.connector.SqlConnector.SqlExternalResource;
import uk.org.hecaton.jet.sql.impl.schema.JetTable;
import uk.org.hecaton.sql.impl.optimizer.PlanObjectKey;
import uk.org.hecaton.sql.impl.schema.ConstantTableStatistics;
import uk.org.hecaton.sql.impl.schema.TableField;
import uk.org.hecaton.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_PREFERRED_LOCAL_PARALLELISM;

/**
 * A ClickHouse table mapped in SQL.
 */
final class ClickHouseTable extends JetTable {

    private static final String DEFAULT_LOCAL_PARALLELISM = "2";
    private static final String DEFAULT_INSERT_BATCH_SIZE = "10000";

    private final TableName tableName;
    private final String dataConnection;
    private final Map<String, String> options;
    private final List<String> columns = new ArrayList<>();
    private final List<QueryDataType> types = new ArrayList<>();

    ClickHouseTable(SqlConnector connector, List<TableField> fields, String schemaName, String mappingName,
                    SqlExternalResource resource) {
        super(connector, fields, schemaName, mappingName, new ConstantTableStatistics(0), resource.objectType(), false);
        this.tableName = ClickHouseSqlConnector.tableName(resource.externalName());
        this.dataConnection = resource.dataConnection();
        this.options = resource.options();
        for (TableField field : fields) {
            columns.add(((Field) field).column());
            types.add(field.getType());
        }
    }

    TableName tableName() {
        return tableName;
    }

    String dataConnection() {
        return dataConnection;
    }

    /**
     * Returns the ClickHouse column of each field.
     */
    List<String> columns() {
        return columns;
    }

    /**
     * Returns the SQL type of each field.
     */
    List<QueryDataType> types() {
        return types;
    }

    boolean useFinal() {
        return Boolean.parseBoolean(options.getOrDefault(ClickHouseSqlConnector.OPTION_USE_FINAL, "false"));
    }

    @Nullable
    String splitColumn() {
        return options.get(ClickHouseSqlConnector.OPTION_SPLIT_COLUMN);
    }

    int localParallelism() {
        return Integer.parseInt(options.getOrDefault(OPTION_PREFERRED_LOCAL_PARALLELISM, DEFAULT_LOCAL_PARALLELISM));
    }

    int insertBatchSize() {
        return Integer.parseInt(options.getOrDefault(ClickHouseSqlConnector.OPTION_INSERT_BATCH_SIZE,
                DEFAULT_INSERT_BATCH_SIZE));
    }

    @Nullable
    String insertQuorum() {
        return options.get(ClickHouseSqlConnector.OPTION_INSERT_QUORUM);
    }

    @Override
    public PlanObjectKey getObjectKey() {
        return new Key(getSchemaName(), getSqlName(), tableName, dataConnection, getFields(), options);
    }

    /**
     * Identifies the plans of the table, for the plan cache.
     */
    record Key(String schemaName, String mappingName, TableName tableName, String dataConnection,
               List<TableField> fields, Map<String, String> options) implements PlanObjectKey {
    }

    /**
     * A field of the table: a ClickHouse column.
     */
    static final class Field extends TableField {

        private final String column;

        Field(String name, QueryDataType type, String column) {
            super(name, type, false);
            this.column = column;
        }

        String column() {
            return column;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Field other && super.equals(other) && column.equals(other.column);
        }

        @Override
        public int hashCode() {
            return 31 * super.hashCode() + column.hashCode();
        }
    }
}
