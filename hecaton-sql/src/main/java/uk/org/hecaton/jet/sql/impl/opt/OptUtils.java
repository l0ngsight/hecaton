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

import com.google.common.collect.ImmutableList;
import uk.org.hecaton.jet.datamodel.Tuple2;
import uk.org.hecaton.jet.impl.util.Util;
import uk.org.hecaton.jet.sql.impl.connector.SqlConnectorUtil;
import uk.org.hecaton.jet.sql.impl.opt.metadata.Boundedness;
import uk.org.hecaton.jet.sql.impl.opt.metadata.HecatonRelMetadataQuery;
import uk.org.hecaton.jet.sql.impl.opt.physical.visitor.RexToExpressionVisitor;
import uk.org.hecaton.jet.sql.impl.schema.HecatonRelOptTable;
import uk.org.hecaton.jet.sql.impl.schema.HecatonTable;
import uk.org.hecaton.jet.sql.impl.schema.JetTable;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonJsonType;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonObjectType;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonTypeUtils;
import uk.org.hecaton.sql.impl.QueryParameterMetadata;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import uk.org.hecaton.sql.impl.plan.node.PlanNodeSchema;
import uk.org.hecaton.sql.impl.schema.Table;
import uk.org.hecaton.sql.impl.schema.TableField;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.sql.impl.type.QueryDataType.QueryDataTypeField;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.HecatonRelOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.HecatonRelSubsetUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static uk.org.hecaton.jet.impl.util.Util.arrayIndexOf;
import static uk.org.hecaton.jet.sql.impl.opt.Conventions.LOGICAL;
import static uk.org.hecaton.jet.sql.impl.opt.Conventions.PHYSICAL;

/**
 * Static utility classes for rules.
 */
public final class OptUtils {

    private OptUtils() {
    }

    /**
     * Convert the given trait set to logical convention.
     *
     * @param traitSet Original trait set.
     * @return New trait set with logical convention.
     */
    public static RelTraitSet toLogicalConvention(RelTraitSet traitSet) {
        return traitPlus(traitSet, LOGICAL);
    }

    /**
     * Convert the given input into logical input.
     *
     * @param rel Original input.
     * @return Logical input.
     */
    public static RelNode toLogicalInput(RelNode rel) {
        return RelOptRule.convert(rel, toLogicalConvention(rel.getTraitSet()));
    }

    /**
     * Convert the given trait set to physical convention.
     *
     * @param traitSet Original trait set.
     * @return New trait set with physical convention and provided distribution.
     */
    public static RelTraitSet toPhysicalConvention(RelTraitSet traitSet) {
        return traitPlus(traitSet, PHYSICAL);
    }

    /**
     * Convert the given input into physical input.
     *
     * @param rel Original input.
     * @return Logical input.
     */
    public static RelNode toPhysicalInput(RelNode rel) {
        if (rel == null) {
            return null;
        }
        return RelOptRule.convert(rel, toPhysicalConvention(rel.getTraitSet()));
    }

    /**
     * Add a single trait to the trait set.
     *
     * @param traitSet Original trait set.
     * @param trait    Trait to add.
     * @return Resulting trait set.
     */
    public static RelTraitSet traitPlus(RelTraitSet traitSet, RelTrait trait) {
        return traitSet.plus(trait).simplify();
    }

    public static LogicalTableScan createLogicalScan(
            RelOptCluster cluster,
            HecatonTable hecatonTable
    ) {
        JetTable table = hecatonTable.getTarget();

        HecatonRelOptTable relTable = createRelTable(
                table.getQualifiedName(),
                hecatonTable,
                cluster.getTypeFactory()
        );
        return LogicalTableScan.create(cluster, relTable, ImmutableList.of());
    }

    public static LogicalTableScan createLogicalScan(
            TableScan originalScan,
            HecatonTable newHecatonTable
    ) {
        HecatonRelOptTable originalRelTable = (HecatonRelOptTable) originalScan.getTable();

        HecatonRelOptTable newTable = createRelTable(
                originalRelTable,
                newHecatonTable,
                originalScan.getCluster().getTypeFactory()
        );

        return LogicalTableScan.create(
                originalScan.getCluster(),
                newTable,
                originalScan.getHints()
        );
    }

    public static HecatonRelOptTable createRelTable(
            List<String> names,
            HecatonTable hecatonTable,
            RelDataTypeFactory typeFactory
    ) {
        RelDataType rowType = hecatonTable.getRowType(typeFactory);

        RelOptTableImpl relTable = RelOptTableImpl.create(
                null,
                rowType,
                names,
                hecatonTable,
                (org.apache.calcite.linq4j.tree.Expression) null
        );
        return new HecatonRelOptTable(relTable);
    }

    public static HecatonRelOptTable createRelTable(
            HecatonRelOptTable originalRelTable,
            HecatonTable newHecatonTable,
            RelDataTypeFactory typeFactory
    ) {
        RelOptTableImpl newTable = RelOptTableImpl.create(
                originalRelTable.getRelOptSchema(),
                newHecatonTable.getRowType(typeFactory),
                originalRelTable.getDelegate().getQualifiedName(),
                newHecatonTable,
                (org.apache.calcite.linq4j.tree.Expression) null
        );

        return new HecatonRelOptTable(newTable);
    }

    /**
     * Finds a set for the given RelNode, and return subsets that have the
     * physical trait. Every returned input is guaranteed to have a unique trait
     * set.
     *
     * @return Physical rels.
     */
    public static Collection<RelNode> extractPhysicalRelsFromSubset(RelNode input) {
        return extractRelsFromSubset(input, OptUtils::isPhysical);
    }

    private static boolean isPhysical(RelNode rel) {
        return rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE).equals(Conventions.PHYSICAL);
    }

    /**
     * Finds a set for the given RelNode, and return subsets that have the
     * logical trait. Every returned input is guaranteed to have a unique trait
     * set.
     *
     * @return Logical rels.
     */
    public static Collection<RelNode> extractLogicalRelsFromSubset(RelNode input) {
        return extractRelsFromSubset(input, OptUtils::isLogical);
    }

    private static boolean isLogical(RelNode rel) {
        return rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE).equals(Conventions.LOGICAL);
    }

    private static Collection<RelNode> extractRelsFromSubset(RelNode input, Predicate<RelNode> predicate) {
        Set<RelTraitSet> traitSets = new HashSet<>();

        Set<RelNode> res = Collections.newSetFromMap(new IdentityHashMap<>());

        for (RelNode rel : HecatonRelSubsetUtil.getSubsets(input)) {
            if (!predicate.test(rel)) {
                continue;
            }

            if (traitSets.add(rel.getTraitSet())) {
                res.add(rel);
            }
        }

        return res;
    }

    public static boolean isBounded(RelNode rel) {
        return metadataQuery(rel).extractBoundedness(rel) == Boundedness.BOUNDED;
    }

    public static boolean isUnbounded(RelNode rel) {
        return metadataQuery(rel).extractBoundedness(rel) == Boundedness.UNBOUNDED;
    }

    public static HecatonRelMetadataQuery metadataQuery(RelNode rel) {
        return HecatonRelMetadataQuery.reuseOrCreate(rel.getCluster().getMetadataQuery());
    }

    public static HecatonRelOptCluster getCluster(RelNode rel) {
        assert rel.getCluster() instanceof HecatonRelOptCluster;

        return (HecatonRelOptCluster) rel.getCluster();
    }

    /**
     * If the {@code node} is a {@link RelSubset}, finds the subset matching
     * the {@code operandPredicate}.
     * If multiple matches are found, it throws. If no match is found, it returns null.
     * <p>
     * If the {@code node} isn't a {@code RelSubset} and it matches the
     * predicate, the {@code node} is returned. If it doesn't match,
     * {@code null} is returned.
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public static <T> T findMatchingRel(RelNode node, RelOptRuleOperand operandPredicate) {
        if (node instanceof RelSubset subset) {
            RelNode res = null;
            for (RelNode rel : subset.getRels()) {
                if (operandPredicate.matches(rel)) {
                    if (res != null) {
                        throw new RuntimeException("multiple matches found");
                    }
                    res = rel;
                }
            }
            return (T) res;
        } else if (operandPredicate.matches(node)) {
            return (T) node;
        }
        return null;
    }

    public static PlanNodeSchema schema(RelDataType rowType) {
        return new PlanNodeSchema(extractFieldTypes(rowType));
    }

    public static PlanNodeSchema schema(RelOptTable relTable) {
        Table table = relTable.unwrap(HecatonTable.class).getTarget();
        return schema(table);
    }

    public static PlanNodeSchema schema(Table table) {
        List<QueryDataType> fieldTypes = new ArrayList<>();
        for (TableField field : table.getFields()) {
            fieldTypes.add(field.getType());
        }
        return new PlanNodeSchema(fieldTypes);
    }

    public static RexVisitor<Expression<?>> createRexToExpressionVisitor(
            PlanNodeFieldTypeProvider schema,
            QueryParameterMetadata parameterMetadata) {
        return new RexToExpressionVisitor(schema, parameterMetadata);
    }

    /**
     * Converts a {@link TableField} to {@link RelDataType}.
     */
    public static RelDataType convert(TableField field, RelDataTypeFactory typeFactory) {
        QueryDataType fieldType = field.getType();

        SqlTypeName sqlTypeName = HecatonTypeUtils.toCalciteType(fieldType);
        if (sqlTypeName == null) {
            throw new IllegalStateException("Unsupported type family: " + fieldType
                    + ", getSqlTypeName should never return null.");
        }

        if (sqlTypeName == SqlTypeName.OTHER) {
            return convertOtherType(fieldType);
        } else if (fieldType.isCustomType()) {
            return convertCustomType(fieldType, typeFactory);
        } else {
            RelDataType relType = typeFactory.createSqlType(sqlTypeName);
            return typeFactory.createTypeWithNullability(relType, true);
        }
    }

    private static RelDataType convertCustomType(QueryDataType type, RelDataTypeFactory typeFactory) {
        Map<String, HecatonObjectType> typeMap = new HashMap<>();
        HecatonObjectType converted = convertCustomType(type, typeFactory, typeMap);
        HecatonObjectType.finalizeFields(typeMap.values());
        return converted;
    }

    private static HecatonObjectType convertCustomType(
            QueryDataType type,
            RelDataTypeFactory typeFactory,
            Map<String, HecatonObjectType> typeMap
    ) {
        HecatonObjectType converted = typeMap.get(type.getObjectTypeName());
        if (converted != null) {
            return converted;
        }

        converted = new HecatonObjectType(type.getObjectTypeName());
        typeMap.put(type.getObjectTypeName(), converted);

        for (int i = 0; i < type.getObjectFields().size(); i++) {
            QueryDataTypeField field = type.getObjectFields().get(i);
            QueryDataType fieldType = field.getType();

            RelDataType convertedFieldType = fieldType.isCustomType()
                    ? convertCustomType(fieldType, typeFactory, typeMap)
                    : typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(HecatonTypeUtils.toCalciteType(fieldType)),
                            true
                    );
            converted.addField(new HecatonObjectType.Field(field.getName(), i, convertedFieldType));
        }

        return converted;
    }

    private static RelDataType convertOtherType(QueryDataType fieldType) {
        switch (fieldType.getTypeFamily()) {
            case JSON:
                return HecatonJsonType.create(true);
            default:
                throw new IllegalStateException("Unexpected type family: " + fieldType);
        }
    }

    private static List<QueryDataType> extractFieldTypes(RelDataType rowType) {
        return Util.toList(rowType.getFieldList(),
                f -> HecatonTypeUtils.toHecatonType(f.getType()));
    }

    public static boolean requiresJob(RelNode rel) {
        return ((HecatonRelOptCluster) rel.getCluster()).requiresJob();
    }

    public static boolean hasTableType(RelNode rel, Class<? extends Table> tableClass) {
        if (rel.getTable() == null) {
            return false;
        }

        HecatonTable table = rel.getTable().unwrap(HecatonTable.class);
        return table != null && tableClass.isAssignableFrom(table.getTarget().getClass());
    }

    public static HecatonTable extractHecatonTable(RelNode rel) {
        HecatonTable table = rel.getTable().unwrap(HecatonTable.class);
        assert table != null;
        return table;
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    public static RexNode extractKeyConstantExpression(RelOptTable relTable, RexBuilder rexBuilder) {
        HecatonTable table = relTable.unwrap(HecatonTable.class);

        RexNode filter = table.getFilter();
        if (filter == null) {
            return null;
        }

        int keyIndex = findPrimaryKeyIndex(table.getTarget());
        switch (filter.getKind()) {
            // WHERE __key = true, calcite simplifies to just `WHERE __key`
            case INPUT_REF: {
                return ((RexInputRef) filter).getIndex() == keyIndex
                        ? rexBuilder.makeLiteral(true)
                        : null;
            }
            // WHERE __key = false, calcite simplifies to `WHERE NOT __key`
            case NOT: {
                RexNode operand = ((RexCall) filter).getOperands().get(0);
                return operand.getKind() == SqlKind.INPUT_REF && ((RexInputRef) operand).getIndex() == keyIndex
                        ? rexBuilder.makeLiteral(false)
                        : null;
            }
            // __key = ...
            case EQUALS: {
                Tuple2<Integer, RexNode> constantExpressionByIndex = extractConstantExpression((RexCall) filter);
                //noinspection ConstantConditions
                return constantExpressionByIndex != null && constantExpressionByIndex.getKey() == keyIndex
                        ? constantExpressionByIndex.getValue()
                        : null;
            }
            default:
                return null;
        }
    }

    /**
     * Returns the index of the primary key field in the given `table`. If
     * there's no primary key, or if there's more than ona primary key field, it
     * returns -1.
     */
    public static int findPrimaryKeyIndex(Table table) {
        List<String> primaryKey = SqlConnectorUtil.getJetSqlConnector(table).getPrimaryKey(table);
        if (primaryKey.size() != 1) {
            return -1;
        }

        int keyIndex = table.getFieldIndex(primaryKey.get(0));
        assert keyIndex >= 0;

        return keyIndex;
    }

    private static Tuple2<Integer, RexNode> extractConstantExpression(RexCall condition) {
        Tuple2<Integer, RexNode> constantExpression = extractConstantExpression(condition, 0);
        return constantExpression != null ? constantExpression : extractConstantExpression(condition, 1);
    }

    private static Tuple2<Integer, RexNode> extractConstantExpression(RexCall condition, int i) {
        RexNode firstOperand = condition.getOperands().get(i);
        if (firstOperand.getKind() == SqlKind.INPUT_REF) {
            int index = ((RexInputRef) firstOperand).getIndex();
            RexNode secondOperand = condition.getOperands().get(1 - i);
            if (RexUtil.isConstant(secondOperand)) {
                return Tuple2.tuple2(index, secondOperand);
            }
        }
        return null;
    }

    /**
     * Return true if the `expression` contains any input reference to a field
     * with index in `indexes`.
     */
    public static boolean hasInputRef(RexNode expression, int... indexes) {
        boolean[] res = {false};
        expression.accept(new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitInputRef(RexInputRef inputRef) {
                if (arrayIndexOf(inputRef.getIndex(), indexes) >= 0) {
                    res[0] = true;
                }
                return null;
            }
        });
        return res[0];
    }

    /**
     * Inlines `inlinedExpressions` into `expr` and returns the modified expression.
     * <p>
     * Example:
     * {@code
     * inlinedExpressions: [UPPER($1), LOWER($0)]
     * expr: $1 || $0
     * result: LOWER($0) || UPPER($1)
     * }
     */
    @SuppressWarnings("checkstyle:AnonInnerLength")
    public static RexNode inlineExpression(List<RexNode> inlinedExpressions, RexNode expr) {
        return expr.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                return inlinedExpressions.get(inputRef.getIndex());
            }
        });
    }

    /**
     * Same as {@link #inlineExpression(List, RexNode)}, but applied to all
     * expressions in {@code exprs}.
     */
    public static List<RexNode> inlineExpressions(List<RexNode> inlinedExpressions, List<RexNode> exprs) {
        List<RexNode> res = new ArrayList<>(exprs.size());
        for (RexNode expr : exprs) {
            res.add(inlineExpression(inlinedExpressions, expr));
        }
        return res;
    }

    /**
     * Return the index at which a {@link Calc} program projects the input's
     * field at index {@code inputFieldIndex}. If the program doesn't project
     * that field directly, returns -1. If it projects it multiple times,
     * returns the first occurrence.
     * <p>
     * For example, if the input has fields `a, b, c` and the projection is `c,
     * a, b+1`, then:
     * <ul>
     *     <li>getTargetField(0) = 1
     *     <li>getTargetField(1) = -1
     *     <li>getTargetField(2) = 0
     * </ul>
     *
     * The method is named analogously to {@link
     * RexProgram#getSourceField(int)}, which finds the opposite mapping.
     *
     * @param calcProgram     The calc program (the projection)
     * @param inputFieldIndex The index of the input field
     * @return The position of the input field in the output, or -1, if it's not in the output.
     */
    public static int getTargetField(RexProgram calcProgram, int inputFieldIndex) {
        for (int i = 0; i < calcProgram.getProjectList().size(); i++) {
            int expressionIndex = calcProgram.getProjectList().get(i).getIndex();
            RexNode expr = calcProgram.getExprList().get(expressionIndex);
            if (expr instanceof RexInputRef ref && ref.getIndex() == inputFieldIndex) {
                return i;
            }
        }
        return -1;
    }
}
