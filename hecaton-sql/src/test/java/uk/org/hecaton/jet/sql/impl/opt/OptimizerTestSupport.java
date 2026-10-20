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

package uk.org.hecaton.jet.sql.impl.opt;

import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.jet.sql.impl.CalciteSqlOptimizerImpl;
import uk.org.hecaton.jet.sql.impl.OptimizerContext;
import uk.org.hecaton.jet.sql.impl.connector.generator.StreamSqlConnector;
import uk.org.hecaton.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import uk.org.hecaton.jet.sql.impl.opt.logical.LogicalRel;
import uk.org.hecaton.jet.sql.impl.opt.logical.LogicalRules;
import uk.org.hecaton.jet.sql.impl.opt.logical.SelectByKeyMapLogicalRule;
import uk.org.hecaton.jet.sql.impl.opt.physical.PhysicalRel;
import uk.org.hecaton.jet.sql.impl.opt.physical.PhysicalRules;
import uk.org.hecaton.jet.sql.impl.parse.QueryParseResult;
import uk.org.hecaton.jet.sql.impl.schema.HecatonSchema;
import uk.org.hecaton.jet.sql.impl.schema.HecatonSchemaUtils;
import uk.org.hecaton.jet.sql.impl.schema.HecatonTable;
import uk.org.hecaton.jet.sql.impl.schema.HecatonTableStatistic;
import uk.org.hecaton.jet.sql.impl.validate.param.StrictParameterConverter;
import uk.org.hecaton.sql.impl.ParameterConverter;
import uk.org.hecaton.sql.impl.QueryParameterMetadata;
import uk.org.hecaton.sql.impl.QueryUtils;
import uk.org.hecaton.sql.impl.SqlServiceImpl;
import uk.org.hecaton.sql.impl.extract.GenericQueryTargetDescriptor;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import uk.org.hecaton.sql.impl.schema.ConstantTableStatistics;
import uk.org.hecaton.sql.impl.schema.Table;
import uk.org.hecaton.sql.impl.schema.TableField;
import uk.org.hecaton.sql.impl.schema.map.MapTableField;
import uk.org.hecaton.sql.impl.schema.map.MapTableIndex;
import uk.org.hecaton.sql.impl.schema.map.PartitionedMapTable;
import uk.org.hecaton.sql.impl.security.NoOpSqlSecurityContext;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RuleSets;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static uk.org.hecaton.sql.impl.QueryUtils.SCHEMA_NAME_PUBLIC;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonSerialClassRunner.class)
public abstract class OptimizerTestSupport extends SqlTestSupport {

    protected RelNode preOptimize(String sql, HecatonTable... tables) {
        HecatonSchema schema = schema(tables);
        OptimizerContext context = context(schema);
        return preOptimizeInternal(sql, context);
    }

    protected LogicalRel optimizeLogical(String sql, HecatonTable... tables) {
        HecatonSchema schema = schema(tables);
        OptimizerContext context = context(schema);
        return optimizeLogicalInternal(sql, context);
    }

    protected LogicalRel optimizeLogical(String sql, boolean requiresJob, HecatonTable... tables) {
        HecatonSchema schema = schema(tables);
        OptimizerContext context = context(schema);
        context.setRequiresJob(requiresJob);
        return optimizeLogicalInternal(sql, context);
    }

    protected Result optimizePhysical(String sql, List<QueryDataType> parameterTypes, HecatonTable... tables) {
        HecatonSchema schema = schema(tables);
        OptimizerContext context = context(schema, parameterTypes.toArray(new QueryDataType[0]));
        return optimizePhysicalInternal(sql, context);
    }

    static RelNode preOptimizeInternal(String sql, OptimizerContext context) {
        QueryParseResult parseResult = context.parse(sql);
        return context.convert(parseResult.getNode()).getRel();
    }

    private static LogicalRel optimizeLogicalInternal(String sql, OptimizerContext context) {
        RelNode rel = preOptimizeInternal(sql, context);

        LogicalRel optimizedLogicalRel = (LogicalRel) context
                .optimize(rel, LogicalRules.getRuleSet(), OptUtils.toLogicalConvention(rel.getTraitSet()));

        // IMap keyed access optimization
        return (LogicalRel) context
                .optimize(
                        optimizedLogicalRel,
                        RuleSets.ofList(SelectByKeyMapLogicalRule.INSTANCE),
                        OptUtils.toLogicalConvention(rel.getTraitSet()));
    }

    private static Result optimizePhysicalInternal(String sql, OptimizerContext context) {
        LogicalRel logicalRel = optimizeLogicalInternal(sql, context);
        PhysicalRel physicalRel = (PhysicalRel) context
                .optimize(logicalRel, PhysicalRules.getRuleSet(), OptUtils.toPhysicalConvention(logicalRel.getTraitSet()));
        physicalRel = CalciteSqlOptimizerImpl.postOptimizationRewrites(physicalRel);
        return new Result(logicalRel, physicalRel);
    }

    private static HecatonSchema schema(HecatonTable... tables) {
        return new HecatonSchema(stream(tables).collect(toMap(table -> table.getTarget().getSqlName(), identity())));
    }

    private static OptimizerContext context(HecatonSchema schema, QueryDataType... parameterTypes) {
        SqlServiceImpl sql = (SqlServiceImpl) (instance().getSql());
        HepProgram subqueryRewriterProgram = sql.getOptimizer().getSubqueryRewriterProgram();
        OptimizerContext context = OptimizerContext.create(
                HecatonSchemaUtils.createCatalog(schema),
                QueryUtils.prepareSearchPaths(null, null),
                emptyList(),
                name -> null,
                subqueryRewriterProgram,
                NoOpSqlSecurityContext.INSTANCE
        );

        ParameterConverter[] parameterConverters = IntStream.range(0, parameterTypes.length)
                .mapToObj(i -> new StrictParameterConverter(i, SqlParserPos.ZERO, parameterTypes[i]))
                .toArray(ParameterConverter[]::new);
        QueryParameterMetadata parameterMetadata = new QueryParameterMetadata(parameterConverters);
        context.setParameterMetadata(parameterMetadata);

        return context;
    }

    protected static HecatonTable partitionedTable(String name, List<TableField> fields, long rowCount) {
        return partitionedTable(name, fields, emptyList(), rowCount);
    }

    protected static HecatonTable partitionedTable(
            String name,
            List<TableField> fields,
            List<MapTableIndex> indexes,
            long rowCount
    ) {
        return partitionedTable(name, fields, indexes, rowCount, emptyList(), false);
    }

    // TODO: migrate this code to builder
    protected static HecatonTable partitionedTable(
            String name,
            List<TableField> fields,
            List<MapTableIndex> indexes,
            long rowCount,
            List<String> partitioningAttributes,
            boolean supportsPartitionPruning
    ) {
        PartitionedMapTable table = new PartitionedMapTable(
                SCHEMA_NAME_PUBLIC,
                name,
                name,
                fields,
                new ConstantTableStatistics(rowCount),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                indexes,
                false,
                partitioningAttributes,
                supportsPartitionPruning);
        return new HecatonTable(table, new HecatonTableStatistic(rowCount));
    }

    protected static HecatonTable streamingTable(Table table, long rowCount) {
        return new HecatonTable(table, new HecatonTableStatistic(rowCount));
    }

    protected static HecatonTable streamGeneratorTable(String name, int rowCount) {
        return new HecatonTable(
                StreamSqlConnector.createTable(SCHEMA_NAME_PUBLIC, name, emptyList()),
                new HecatonTableStatistic(rowCount));
    }

    protected static TableField field(String name, QueryDataType type) {
        return new Field(name, type, false);
    }

    protected static MapTableField mapField(String name, QueryDataType type, QueryPath queryPath) {
        return new MapTableField(name, type, false, queryPath);
    }

    protected static void assertPlan(RelNode rel, PlanRows expected) {
        BufferedReader reader = new BufferedReader(new StringReader(RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES)));
        List<PlanRow> rows = reader.lines()
                .map(PlanRow::parse)
                .collect(toList());
        assertPlan(new PlanRows(rows), expected);
    }

    private static void assertPlan(PlanRows actual, PlanRows expected) {
        int expectedRowCount = expected.getRowCount();
        int actualRowCount = actual.getRowCount();
        assertThat(actualRowCount)
                .as("Plan are different%n%n>>> EXPECTED PLAN:%n%s%n>>> ACTUAL PLAN:%n%s", expected, actual)
                .isEqualTo(expectedRowCount);

        for (int i = 0; i < expectedRowCount; i++) {
            PlanRow expectedRow = expected.getRow(i);
            PlanRow actualRow = actual.getRow(i);
            assertThat(actualRow)
                    .as("Plan rows are different at %s%n%n>>> EXPECTED PLAN:%n%s%n>>> ACTUAL PLAN:%n%s", i + 1, expected, actual)
                    .isEqualTo(expectedRow);
        }
    }

    protected static PlanRows plan(PlanRow... rows) {
        return new PlanRows(asList(rows));
    }

    protected static PlanRow planRow(int level, Class<? extends RelNode> node) {
        return new PlanRow(level, node);
    }

    private static class Field extends TableField {
        private Field(String name, QueryDataType type, boolean hidden) {
            super(name, type, hidden);
        }
    }

    protected static class Result {

        private final LogicalRel logical;
        private final PhysicalRel physical;

        private Result(LogicalRel logical, PhysicalRel physical) {
            this.logical = logical;
            this.physical = physical;
        }

        public LogicalRel getLogical() {
            return logical;
        }

        public PhysicalRel getPhysical() {
            return physical;
        }
    }

    protected static class PlanRows {

        private final List<PlanRow> rows;

        private PlanRows(List<PlanRow> rows) {
            this.rows = rows;
        }

        private int getRowCount() {
            return rows.size();
        }

        private PlanRow getRow(int index) {
            return rows.get(index);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < rows.size(); i++) {
                PlanRow row = rows.get(i);
                builder.append(String.format("%02d", i)).append(": ").append(row).append("\n");
            }
            return builder.toString();
        }
    }

    protected static class PlanRow {

        private final int level;
        private final String node;

        protected PlanRow(int level, Class<? extends RelNode> nodeClass) {
            this(level, nodeClass.getSimpleName());
        }

        protected PlanRow(int level, String node) {
            this.level = level;
            this.node = node;
        }

        protected static PlanRow parse(String input) {
            // level
            int level = 0;
            while (input.charAt(level * 2) == ' ') {
                level++;
            }

            // node
            String nodeAndSignature = input.substring(0, input.lastIndexOf(":")).trim();
            String node = input.contains("(")
                    ? nodeAndSignature.substring(0, nodeAndSignature.indexOf('('))
                    : nodeAndSignature;

            return new PlanRow(level, node);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < level; i++) {
                builder.append("  ");
            }
            builder.append(node);
            return builder.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PlanRow planRow = (PlanRow) o;
            return level == planRow.level && Objects.equals(node, planRow.node);
        }

        @Override
        public int hashCode() {
            return Objects.hash(level, node);
        }
    }
}
