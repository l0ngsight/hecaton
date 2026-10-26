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

import uk.org.hecaton.dataconnection.DataConnectionService;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.jet.clickhouse.ClickHouseDataConnection;
import uk.org.hecaton.jet.clickhouse.impl.ClientSource;
import uk.org.hecaton.jet.clickhouse.impl.ColumnType;
import uk.org.hecaton.jet.clickhouse.impl.TableMetadata;
import uk.org.hecaton.jet.clickhouse.impl.TableName;
import uk.org.hecaton.jet.core.EventTimePolicy;
import uk.org.hecaton.jet.core.ProcessorMetaSupplier;
import uk.org.hecaton.jet.core.Vertex;
import uk.org.hecaton.jet.sql.impl.connector.HecatonRexNode;
import uk.org.hecaton.jet.sql.impl.connector.SqlConnector;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.sql.impl.QueryException;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.row.JetSqlRow;
import uk.org.hecaton.sql.impl.schema.MappingField;
import uk.org.hecaton.sql.impl.schema.Table;
import uk.org.hecaton.sql.impl.schema.TableField;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import com.clickhouse.data.ClickHouseColumn;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static uk.org.hecaton.jet.pipeline.DataConnectionRef.dataConnectionRef;

/**
 * The SQL connector of type {@code ClickHouse}: mappings of ClickHouse tables, through a
 * {@link ClickHouseDataConnection}, for {@code SELECT}, {@code INSERT INTO} and
 * {@code SINK INTO}.
 * <p>
 * A scan sends the parts of the {@code WHERE} clause that mean the same in ClickHouse to
 * ClickHouse, with their values as query parameters, and reads only the columns the query
 * uses; it also evaluates the whole predicate and projection on the rows it receives, so the
 * results are those of Hecaton's SQL. Without the {@code splitColumn} option one processor in
 * the cluster runs the query; with it, every member runs it with several processors, each
 * reading the rows whose split column hashes to it.
 */
public class ClickHouseSqlConnector implements SqlConnector {

    /**
     * The type of the connector in {@code CREATE MAPPING}.
     */
    public static final String TYPE_NAME = "ClickHouse";
    /**
     * Reads the table with {@code FINAL}.
     */
    public static final String OPTION_USE_FINAL = "useFinal";
    /**
     * Reads the table in parallel, split by the hash of this column.
     */
    public static final String OPTION_SPLIT_COLUMN = "splitColumn";
    /**
     * The largest number of rows in one insert.
     */
    public static final String OPTION_INSERT_BATCH_SIZE = "insertBatchSize";
    /**
     * The {@code insert_quorum} setting of inserts.
     */
    public static final String OPTION_INSERT_QUORUM = "insertQuorum";

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull
    @Override
    public String defaultObjectType() {
        return "Table";
    }

    static TableName tableName(String[] externalName) {
        if (externalName.length == 1) {
            return new TableName(null, externalName[0]);
        } else if (externalName.length == 2) {
            return new TableName(externalName[0], externalName[1]);
        }
        throw QueryException.error("Invalid external name " + String.join(".", externalName)
                + ": a ClickHouse table is named 'table' or 'database.table'");
    }

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> userFields
    ) {
        if (externalResource.dataConnection() == null) {
            throw QueryException.error("A ClickHouse mapping needs a data connection: CREATE MAPPING ... "
                    + "DATA CONNECTION <name>");
        }
        TableName tableName = tableName(externalResource.externalName());
        TableMetadata metadata = readMetadata(nodeEngine.getDataConnectionService(), externalResource.dataConnection(),
                tableName);
        List<MappingField> fields = new ArrayList<>();
        if (userFields.isEmpty()) {
            for (ClickHouseColumn column : metadata.columns().values()) {
                QueryDataType type = sqlType(column);
                if (type != null) {
                    fields.add(new MappingField(column.getColumnName(), type));
                }
            }
            if (fields.isEmpty()) {
                throw QueryException.error("The ClickHouse table " + tableName + " has no column of a supported type");
            }
            return fields;
        }
        for (MappingField field : userFields) {
            String columnName = field.externalName() != null ? field.externalName() : field.name();
            ClickHouseColumn column = metadata.columns().get(columnName);
            if (column == null) {
                throw QueryException.error("The ClickHouse table " + tableName + " has no column '" + columnName + "'");
            }
            QueryDataType type = sqlType(column);
            if (type == null) {
                throw QueryException.error("The column '" + columnName + "' of the ClickHouse table " + tableName
                        + " has the type " + column.getOriginalTypeName() + ", which SQL does not support");
            }
            if (!type.equals(field.type()) && !type.getConverter().canConvertTo(field.type().getTypeFamily())) {
                throw QueryException.error("The field '" + field.name() + "' has the type "
                        + field.type().getTypeFamily() + ", but the column '" + columnName + "' is "
                        + column.getOriginalTypeName() + ", read as " + type.getTypeFamily());
            }
            fields.add(field);
        }
        return fields;
    }

    private static TableMetadata readMetadata(DataConnectionService dataConnectionService, String dataConnection,
                                              TableName tableName) {
        try (ClientSource.Lease lease = ClientSource.of(dataConnectionRef(dataConnection))
                                                    .open(dataConnectionService)) {
            return TableMetadata.read(lease.client(), tableName);
        }
    }

    /**
     * Returns the SQL type of a column, or {@code null} if SQL does not support its type.
     */
    @Nullable
    static QueryDataType sqlType(ClickHouseColumn column) {
        ColumnType type;
        try {
            type = ColumnType.of(column);
        } catch (IllegalArgumentException e) {
            return null;
        }
        return switch (type.kind()) {
            case BOOLEAN -> QueryDataType.BOOLEAN;
            case INT8 -> QueryDataType.TINYINT;
            case INT16 -> QueryDataType.SMALLINT;
            case INT32 -> QueryDataType.INT;
            case INT64 -> QueryDataType.BIGINT;
            case FLOAT32 -> QueryDataType.REAL;
            case FLOAT64 -> QueryDataType.DOUBLE;
            case DECIMAL -> QueryDataType.DECIMAL;
            case STRING -> QueryDataType.VARCHAR;
            case DATE -> QueryDataType.DATE;
            case TIMESTAMP_WITH_TIMEZONE -> QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
        };
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> resolvedFields
    ) {
        List<TableField> fields = new ArrayList<>(resolvedFields.size());
        for (MappingField field : resolvedFields) {
            String column = field.externalName() != null ? field.externalName() : field.name();
            fields.add(new ClickHouseTable.Field(field.name(), field.type(), column));
        }
        return new ClickHouseTable(this, fields, schemaName, mappingName, externalResource);
    }

    @Override
    public boolean supportsExpression(@Nonnull HecatonRexNode expression) {
        return PredicateTranslator.supports(expression.unwrap(RexNode.class));
    }

    @Nonnull
    @Override
    public Vertex fullScanReader(
            @Nonnull DagBuildContext context,
            @Nullable HecatonRexNode predicate,
            @Nonnull List<HecatonRexNode> projection,
            @Nullable List<Map<String, Expression<?>>> partitionPruningCandidates,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider
    ) {
        if (eventTimePolicyProvider != null) {
            throw QueryException.error("Ordering functions are not supported on top of " + TYPE_NAME + " mappings");
        }
        ClickHouseTable table = context.getTable();
        RexNode rexPredicate = predicate == null ? null : predicate.unwrap(RexNode.class);
        List<RexNode> rexProjection = new ArrayList<>();
        for (HecatonRexNode node : projection) {
            rexProjection.add(node.unwrap(RexNode.class));
        }

        // read only the columns of the fields that the query uses
        int[] selectedFields = RelOptUtil.InputFinder.bits(rexProjection, rexPredicate).toArray();
        StringBuilder sql = new StringBuilder("SELECT ");
        if (selectedFields.length == 0) {
            sql.append('1');
        }
        for (int i = 0; i < selectedFields.length; i++) {
            sql.append(i == 0 ? "" : ", ").append(TableName.quote(table.columns().get(selectedFields[i])));
        }
        sql.append(" FROM ").append(table.tableName().sql());
        if (table.useFinal()) {
            sql.append(" FINAL");
        }
        List<String> conditions = new ArrayList<>();
        List<PushedParameter> parameters = new ArrayList<>();
        if (rexPredicate != null) {
            for (RexNode conjunction : RelOptUtil.conjunctions(rexPredicate)) {
                PredicateTranslator.Condition condition = PredicateTranslator.translate(conjunction, table.columns(),
                        parameters.size());
                if (condition != null) {
                    conditions.add(condition.sql());
                    parameters.addAll(condition.parameters());
                }
            }
        }
        String splitColumn = table.splitColumn();
        if (splitColumn != null) {
            conditions.add("cityHash64(" + TableName.quote(splitColumn) + ") % {total:UInt32} = {index:UInt32}");
        }
        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }

        SelectProcessorSupplier supplier = new SelectProcessorSupplier(table.dataConnection(), sql.toString(),
                parameters, splitColumn != null, selectedFields, table.types(), context.convertFilter(predicate),
                context.convertProjection(projection));
        ProcessorMetaSupplier metaSupplier = splitColumn != null
                ? ProcessorMetaSupplier.of(table.localParallelism(), supplier)
                : ProcessorMetaSupplier.forceTotalParallelismOne(supplier);
        return context.getDag().newUniqueVertex("Select(" + table.tableName() + ")", metaSupplier);
    }

    @Nonnull
    @Override
    public VertexWithInputConfig insertProcessor(@Nonnull DagBuildContext context) {
        return new VertexWithInputConfig(writeVertex(context, "Insert"));
    }

    @Nonnull
    @Override
    public Vertex sinkProcessor(@Nonnull DagBuildContext context) {
        return writeVertex(context, "Sink");
    }

    private static Vertex writeVertex(DagBuildContext context, String name) {
        ClickHouseTable table = context.getTable();
        Map<String, String> settings = new HashMap<>();
        if (table.insertQuorum() != null) {
            settings.put("insert_quorum", table.insertQuorum());
        }
        return context.getDag().newUniqueVertex(name + "(" + table.tableName() + ")",
                new InsertProcessorSupplier(table.dataConnection(), table.tableName(), table.columns(),
                        table.insertBatchSize(), settings)).localParallelism(1);
    }

    @Nonnull
    @Override
    public List<String> getPrimaryKey(Table table) {
        // the planner asks for the key of the rows to change before it builds an UPDATE or DELETE
        throw QueryException.error("UPDATE and DELETE are not supported on ClickHouse mappings: insert new rows "
                + "with INSERT INTO or SINK INTO, for example into a ReplacingMergeTree table");
    }

    @Override
    public Set<String> nonSensitiveConnectorOptions() {
        Set<String> options = SqlConnector.super.nonSensitiveConnectorOptions();
        options.add(OPTION_USE_FINAL);
        options.add(OPTION_SPLIT_COLUMN);
        options.add(OPTION_INSERT_BATCH_SIZE);
        options.add(OPTION_INSERT_QUORUM);
        options.add(OPTION_PREFERRED_LOCAL_PARALLELISM);
        return options;
    }
}
