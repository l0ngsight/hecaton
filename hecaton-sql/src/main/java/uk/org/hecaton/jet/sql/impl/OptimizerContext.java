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

package uk.org.hecaton.jet.sql.impl;

import com.google.common.collect.ImmutableList;
import uk.org.hecaton.jet.sql.impl.opt.cost.CostFactory;
import uk.org.hecaton.jet.sql.impl.opt.metadata.HecatonRelMdBoundedness;
import uk.org.hecaton.jet.sql.impl.opt.metadata.HecatonRelMdPrunability;
import uk.org.hecaton.jet.sql.impl.opt.metadata.HecatonRelMdRowCount;
import uk.org.hecaton.jet.sql.impl.opt.metadata.HecatonRelMdWatermarkedFields;
import uk.org.hecaton.jet.sql.impl.parse.QueryConvertResult;
import uk.org.hecaton.jet.sql.impl.parse.QueryConverter;
import uk.org.hecaton.jet.sql.impl.parse.QueryParseResult;
import uk.org.hecaton.jet.sql.impl.parse.QueryParser;
import uk.org.hecaton.jet.sql.impl.schema.HecatonCalciteCatalogReader;
import uk.org.hecaton.jet.sql.impl.schema.HecatonSchema;
import uk.org.hecaton.jet.sql.impl.schema.HecatonSchemaUtils;
import uk.org.hecaton.jet.sql.impl.validate.HecatonSqlValidator;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonTypeFactory;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.sql.impl.QueryParameterMetadata;
import uk.org.hecaton.sql.impl.optimizer.PlanObjectKey;
import uk.org.hecaton.sql.impl.schema.IMapResolver;
import uk.org.hecaton.sql.impl.schema.SqlCatalog;
import uk.org.hecaton.sql.impl.security.SqlSecurityContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.HecatonRootCalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.HecatonRelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RuleSet;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Optimizer context which holds the whole environment for the given optimization session.
 * Should not be re-used between optimization sessions.
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public final class OptimizerContext {

    private static final ThreadLocal<OptimizerContext> THREAD_CONTEXT = new ThreadLocal<>();

    private static final RelMetadataProvider METADATA_PROVIDER = ChainedRelMetadataProvider.of(ImmutableList.of(
            HecatonRelMdRowCount.SOURCE,
            HecatonRelMdBoundedness.SOURCE,
            HecatonRelMdWatermarkedFields.SOURCE,
            HecatonRelMdPrunability.SOURCE,
            DefaultRelMetadataProvider.INSTANCE
    ));

    private static final CalciteConnectionConfig CONNECTION_CONFIG = CalciteConfiguration.DEFAULT.toConnectionConfig();

    private final HecatonRelOptCluster cluster;
    private final QueryParser parser;
    private final QueryConverter converter;
    private final QueryPlanner planner;
    private final Set<PlanObjectKey> usedViews = new HashSet<>();
    private final Deque<String> viewExpansionStack = new ArrayDeque<>();

    private OptimizerContext(
            HecatonRelOptCluster cluster,
            QueryParser parser,
            QueryConverter converter,
            QueryPlanner planner
    ) {
        this.cluster = cluster;
        this.parser = parser;
        this.converter = converter;
        this.planner = planner;
    }

    /**
     * Create the optimization context.
     *
     * @param searchPaths Search paths to support "current schema" feature.
     * @return Context.
     */
    public static OptimizerContext create(
            SqlCatalog schema,
            List<List<String>> searchPaths,
            List<Object> arguments,
            IMapResolver iMapResolver,
            HepProgram subqueryRewriterProgram,
            SqlSecurityContext securityContext
    ) {
        // Resolve tables.
        HecatonSchema rootSchema = HecatonSchemaUtils.createRootSchema(schema);

        return create(
                rootSchema,
                searchPaths,
                arguments,
                iMapResolver,
                subqueryRewriterProgram,
                securityContext);
    }

    public static OptimizerContext create(
            HecatonSchema rootSchema,
            List<List<String>> schemaPaths,
            List<Object> arguments,
            IMapResolver iMapResolver,
            HepProgram subqueryRewriterProgram,
            SqlSecurityContext ssc
    ) {
        Prepare.CatalogReader catalogReader = createCatalogReader(rootSchema, schemaPaths);
        HecatonSqlValidator validator = new HecatonSqlValidator(catalogReader, arguments, iMapResolver, ssc);
        VolcanoPlanner volcanoPlanner = createPlanner();

        HecatonRelOptCluster cluster = createCluster(volcanoPlanner, ssc);

        QueryParser parser = new QueryParser(validator);
        QueryConverter converter = new QueryConverter(validator, catalogReader, cluster, subqueryRewriterProgram);
        QueryPlanner planner = new QueryPlanner(volcanoPlanner);

        return new OptimizerContext(cluster, parser, converter, planner);
    }

    public static void setThreadContext(OptimizerContext context) {
        THREAD_CONTEXT.set(context);
    }

    public static OptimizerContext getThreadContext() {
        return THREAD_CONTEXT.get();
    }

    /**
     * Parse SQL statement.
     *
     * @param sql SQL string.
     * @return SQL tree.
     */
    public QueryParseResult parse(String sql) {
        return parser.parse(sql, cluster.getSecurityContext());
    }

    /**
     * Perform initial conversion of an SQL tree to a relational tree.
     *
     * @param node Query parse result.
     * @return Relational tree.
     */
    public QueryConvertResult convert(SqlNode node) {
        return converter.convert(node);
    }

    public RelNode convertView(SqlNode node) {
        return converter.convertView(node);
    }

    /**
     * Apply the given rules to the node.
     *
     * @param node     Node.
     * @param rules    Rules.
     * @param traitSet Required trait set.
     * @return Optimized node.
     */
    public RelNode optimize(RelNode node, RuleSet rules, RelTraitSet traitSet) {
        return planner.optimize(node, rules, traitSet);
    }

    public void setParameterMetadata(QueryParameterMetadata parameterMetadata) {
        cluster.setParameterMetadata(parameterMetadata);
    }

    public void setRequiresJob(boolean requiresJob) {
        cluster.setRequiresJob(requiresJob);
    }

    private static Prepare.CatalogReader createCatalogReader(HecatonSchema rootSchema, List<List<String>> searchPaths) {
        assert searchPaths != null;

        return new HecatonCalciteCatalogReader(
                new HecatonRootCalciteSchema(rootSchema),
                searchPaths,
                HecatonTypeFactory.INSTANCE,
                CONNECTION_CONFIG);
    }

    private static VolcanoPlanner createPlanner() {
        VolcanoPlanner planner = new VolcanoPlanner(
                CostFactory.INSTANCE,
                Contexts.of(CONNECTION_CONFIG)
        );

        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        return planner;
    }

    private static HecatonRelOptCluster createCluster(
            VolcanoPlanner planner,
            SqlSecurityContext securityContext) {
        HecatonRelOptCluster cluster = HecatonRelOptCluster.create(
                planner,
                HecatonRexBuilder.INSTANCE,
                securityContext);

        // Wire up custom metadata providers.
        cluster.setMetadataProvider(JaninoRelMetadataProvider.of(METADATA_PROVIDER));

        return cluster;
    }

    public Deque<String> getViewExpansionStack() {
        return viewExpansionStack;
    }

    public Set<PlanObjectKey> getUsedViews() {
        return usedViews;
    }

    public SqlSecurityContext getSecurityContext() {
        return cluster.getSecurityContext();
    }

    public void dump(ILogger logger) {
        StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        planner.dump(pw);
        pw.flush();
        logger.info(sw.toString());
    }
}
