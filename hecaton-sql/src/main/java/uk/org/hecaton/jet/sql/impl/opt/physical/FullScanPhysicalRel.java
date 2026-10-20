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

package uk.org.hecaton.jet.sql.impl.opt.physical;

import uk.org.hecaton.function.BiFunctionEx;
import uk.org.hecaton.jet.core.EventTimePolicy;
import uk.org.hecaton.jet.core.WatermarkPolicy;
import uk.org.hecaton.jet.sql.impl.CalciteSqlOptimizerImpl;
import uk.org.hecaton.jet.sql.impl.HecatonPhysicalScan;
import uk.org.hecaton.jet.sql.impl.aggregate.WindowUtils;
import uk.org.hecaton.jet.sql.impl.opt.FullScan;
import uk.org.hecaton.jet.sql.impl.opt.OptUtils;
import uk.org.hecaton.jet.sql.impl.opt.cost.CostUtils;
import uk.org.hecaton.jet.sql.impl.schema.HecatonTable;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonTypeUtils;
import uk.org.hecaton.sql.impl.QueryParameterMetadata;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.plan.node.PlanNodeSchema;
import uk.org.hecaton.sql.impl.row.JetSqlRow;
import uk.org.hecaton.sql.impl.schema.TableField;
import uk.org.hecaton.sql.impl.schema.map.PartitionedMapTable;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static uk.org.hecaton.jet.impl.util.Util.toList;
import static uk.org.hecaton.jet.sql.impl.opt.cost.CostUtils.TABLE_SCAN_CPU_MULTIPLIER;

public class FullScanPhysicalRel extends FullScan implements HecatonPhysicalScan {

    /**
     * See {@link CalciteSqlOptimizerImpl#postOptimizationRewrites(PhysicalRel)}.
     */
    private final int discriminator;

    FullScanPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            @Nullable Expression<?> lagExpression,
            int watermarkedColumnIndex,
            int discriminator
    ) {
        super(cluster, traitSet, table, lagExpression, watermarkedColumnIndex);
        this.discriminator = discriminator;
    }

    @Override
    public RexNode filter() {
        return getTable().unwrap(HecatonTable.class).getFilter();
    }

    @Override
    public List<RexNode> projection() {
        return getTable().unwrap(HecatonTable.class).getProjects();
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        List<QueryDataType> fieldTypes = toList(projection(), rexNode -> HecatonTypeUtils.toHecatonType(rexNode.getType()));
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public <V> V accept(CreateDagVisitor<V> visitor) {
        return visitor.onFullScan(this);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        HecatonTable table = getTable().unwrap(HecatonTable.class);
        double totalRowCount = table.getStatistic().getRowCount() != null
                ? table.getTotalRowCount()
                : getTable().getRowCount();

        double filterRowCount = totalRowCount;

        if (table.getFilter() != null) {
            filterRowCount = CostUtils.adjustFilteredRowCount(totalRowCount, RelMdUtil.guessSelectivity(table.getFilter()));
        }

        return computeSelfCost(
                planner,
                totalRowCount,
                table.getFilter() != null,
                filterRowCount,
                table.getProjects().size()
        );
    }

    private static RelOptCost computeSelfCost(
            RelOptPlanner planner,
            double scanRowCount,
            boolean hasFilter,
            double filterRowCount,
            int projectCount
    ) {
        // 1. Get cost of the scan itself.
        double scanCpu = scanRowCount * TABLE_SCAN_CPU_MULTIPLIER;

        // 2. Get cost of the filter, if any.
        double filterCpu = hasFilter ? CostUtils.adjustCpuForConstrainedScan(scanCpu) : 0;

        // 3. Get cost of the project taking into account the filter and number of expressions. Project never produces IO.
        double projectCpu = CostUtils.adjustCpuForConstrainedScan(CostUtils.getProjectCpu(filterRowCount, projectCount));

        // 4. Finally, return sum of both scan and project.
        return planner.getCostFactory().makeCost(
                filterRowCount,
                scanCpu + filterCpu + projectCpu,
                0
        );
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        final HecatonTable hecatonTable = OptUtils.extractHecatonTable(this);
        final var candidates = OptUtils.metadataQuery(this).extractPrunability(this);
        final boolean isPrunable = !candidates.isEmpty();
        String partitioningKey = "";
        String partitioningKeyValues = "";
        if (hecatonTable.getTarget() instanceof PartitionedMapTable && isPrunable) {
            final PartitionedMapTable target = hecatonTable.getTarget();
            final List<Integer> fieldIndexes = target.partitioningAttributes().isEmpty() ?
                    target.keyFields()
                            .map(f -> target.getFieldIndex(f.getName()))
                            .collect(Collectors.toList())
                    : target.keyFields()
                            .filter(kf -> target.partitioningAttributes().contains(kf.getPath().getPath()))
                            .map(TableField::getName)
                            .map(target::getFieldIndex)
                            .collect(Collectors.toList());

            partitioningKey = fieldIndexes.stream()
                    .map(index -> "$" + index)
                    .collect(Collectors.joining(", "));

            final List<String> fieldNames = fieldIndexes.stream()
                    .map(fieldIndex -> (TableField) target.getField(fieldIndex))
                    .map(TableField::getName)
                    .collect(Collectors.toList());

            partitioningKeyValues = candidates.get(target.getSqlName()).stream()
                    .map(candidate -> fieldNames.stream()
                            .map(candidate::get)
                            .filter(Objects::nonNull)
                            .map(RexNode::toString)
                            .collect(Collectors.joining(", ")))
                    .map(s -> "(" + s + ")")
                    .collect(Collectors.joining(", "));
        }
        return super.explainTerms(pw)
                .item("discriminator", discriminator)
                .itemIf("partitioningKey", partitioningKey, isPrunable)
                .itemIf("partitioningKeyValues", partitioningKeyValues, isPrunable);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new FullScanPhysicalRel(getCluster(), traitSet, getTable(), lagExpression(),
                watermarkedColumnIndex(), discriminator());
    }

    public RelNode copy(RelTraitSet traitSet, int discriminator) {
        return new FullScanPhysicalRel(getCluster(), traitSet, getTable(), lagExpression(),
                watermarkedColumnIndex(), discriminator);
    }

    public int discriminator() {
        return discriminator;
    }

    public List<RexNode> getProjects() {
        HecatonTable table = getTable().unwrap(HecatonTable.class);
        return table.getProjects();
    }

    public BiFunctionEx<ExpressionEvalContext, Byte, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider(
            int wmColumnIndex, @Nullable Expression<?> lagExpression, long throttlingFrameSize) {
        if (lagExpression == null) {
            return null;
        }
        return (context, watermarkKey) -> {
            long lagMs = WindowUtils.extractMillis(lagExpression, context);
            return EventTimePolicy.eventTimePolicy(
                    row -> WindowUtils.extractMillis(row.get(wmColumnIndex)),
                    (row, timestamp) -> row,
                    WatermarkPolicy.limitingLag(lagMs),
                    throttlingFrameSize,
                    0,
                    EventTimePolicy.DEFAULT_IDLE_TIMEOUT,
                    watermarkKey);
        };
    }
}
