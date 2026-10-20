/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.jet.sql.impl.connector.test;

import com.google.common.collect.ImmutableMap;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.jet.core.EventTimePolicy;
import uk.org.hecaton.jet.core.ProcessorMetaSupplier;
import uk.org.hecaton.jet.core.Vertex;
import uk.org.hecaton.jet.impl.pipeline.transform.BatchSourceTransform;
import uk.org.hecaton.jet.pipeline.BatchSource;
import uk.org.hecaton.jet.pipeline.SourceBuilder;
import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.jet.sql.impl.ExpressionUtil;
import uk.org.hecaton.jet.sql.impl.connector.HecatonRexNode;
import uk.org.hecaton.jet.sql.impl.connector.SqlConnector;
import uk.org.hecaton.jet.sql.impl.schema.JetTable;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.sql.SqlService;
import uk.org.hecaton.sql.impl.QueryException;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.optimizer.PlanObjectKey;
import uk.org.hecaton.sql.impl.row.JetSqlRow;
import uk.org.hecaton.sql.impl.schema.ConstantTableStatistics;
import uk.org.hecaton.sql.impl.schema.MappingField;
import uk.org.hecaton.sql.impl.schema.Table;
import uk.org.hecaton.sql.impl.schema.TableField;
import uk.org.hecaton.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static uk.org.hecaton.jet.impl.util.Util.toList;
import static uk.org.hecaton.jet.sql.SqlTestSupport.TEST_SS;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;

/**
 * A SQL source yielding a single row with all supported types.
 */
public class TestAllTypesSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "AllTypes";

    public static final List<MappingField> FIELD_LIST = asList(
            new MappingField("string", QueryDataType.VARCHAR),
            new MappingField("boolean", QueryDataType.BOOLEAN),
            new MappingField("byte", QueryDataType.TINYINT),
            new MappingField("short", QueryDataType.SMALLINT),
            new MappingField("int", QueryDataType.INT),
            new MappingField("long", QueryDataType.BIGINT),
            new MappingField("float", QueryDataType.REAL),
            new MappingField("double", QueryDataType.DOUBLE),
            new MappingField("decimal", QueryDataType.DECIMAL),
            new MappingField("time", QueryDataType.TIME),
            new MappingField("date", QueryDataType.DATE),
            new MappingField("timestamp", QueryDataType.TIMESTAMP),
            new MappingField("timestampTz", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME),
            new MappingField("map", QueryDataType.OBJECT),
            new MappingField("object", QueryDataType.OBJECT)
    );

    private static final List<TableField> FIELD_LIST2 = toList(FIELD_LIST, f -> new TableField(f.name(), f.type(), false));

    private static final JetSqlRow VALUES = new JetSqlRow(TEST_SS, new Object[]{
            "string",
            true,
            (byte) 127,
            (short) 32767,
            2147483647,
            9223372036854775807L,
            1234567890.1f,
            123451234567890.1,
            new BigDecimal("9223372036854775.123"),
            LocalTime.of(12, 23, 34),
            LocalDate.of(2020, 4, 15),
            LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000),
            OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC),
            ImmutableMap.of(42, 43),
            null
    });

    public static final SqlTestSupport.Row ALL_TYPES_ROW = new SqlTestSupport.Row(VALUES.getValues());

    public static void create(SqlService sqlService, String tableName) {
        sqlService.execute("CREATE MAPPING " + tableName + " TYPE " + TestAllTypesSqlConnector.TYPE_NAME);
    }

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull
    @Override
    public String defaultObjectType() {
        return "Dummy";
    }

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> userFields) {
        if (!userFields.isEmpty()) {
            throw QueryException.error("Don't specify external fields, they are fixed");
        }
        return FIELD_LIST;
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> resolvedFields) {
        return new TestAllTypesTable(this, schemaName, mappingName);
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
            throw QueryException.error("Ordering function are not supported for " + TYPE_NAME + " mappings");
        }
        Expression<Boolean> convertedPredicate = context.convertFilter(predicate);
        List<Expression<?>> convertedProjection = context.convertProjection(projection);

        BatchSource<JetSqlRow> source = SourceBuilder
                .batch("batch", ExpressionEvalContext::from)
                .<JetSqlRow>fillBufferFn((ctx, buf) -> {
                    JetSqlRow row = ExpressionUtil.evaluate(convertedPredicate, convertedProjection, VALUES, ctx);
                    if (row != null) {
                        buf.add(row);
                    }
                    buf.close();
                })
                .build();
        ProcessorMetaSupplier pms = ((BatchSourceTransform<JetSqlRow>) source).metaSupplier;
        return context.getDag().newUniqueVertex(context.getTable().toString(), pms);
    }

    private static final class TestAllTypesTable extends JetTable {

        private TestAllTypesTable(
                @Nonnull SqlConnector sqlConnector,
                @Nonnull String schemaName,
                @Nonnull String name
        ) {
            super(sqlConnector, FIELD_LIST2, schemaName, name, new ConstantTableStatistics(1), null, false);
        }

        @Override
        public PlanObjectKey getObjectKey() {
            return new TestAllTypesPlanObjectKey(getSchemaName(), getSqlName());
        }
    }

    private static final class TestAllTypesPlanObjectKey implements PlanObjectKey {

        private final String schemaName;
        private final String name;

        private TestAllTypesPlanObjectKey(String schemaName, String name) {
            this.schemaName = schemaName;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestAllTypesPlanObjectKey that = (TestAllTypesPlanObjectKey) o;
            return Objects.equals(schemaName, that.schemaName) && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaName, name);
        }
    }
}
