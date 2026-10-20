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

package org.apache.calcite.plan;

import uk.org.hecaton.sql.impl.QueryParameterMetadata;
import uk.org.hecaton.sql.impl.security.SqlSecurityContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extended {@link RelOptCluster} with Hecaton-specific data.
 * <p>
 * Located in the Calcite package because the required super constructor is package-private.
 */
public final class HecatonRelOptCluster extends RelOptCluster {

    /**
     * Metadata about parameters.
     */
    private QueryParameterMetadata parameterMetadata;

    /**
     * Whether 'CREATE JOB' is used
     */
    private boolean requiresJob;

    /** SQL security context */
    private final SqlSecurityContext securityContext;

    private HecatonRelOptCluster(
            RelOptPlanner planner,
            RelDataTypeFactory typeFactory,
            RexBuilder rexBuilder,
            AtomicInteger nextCorrel,
            Map<String, RelNode> mapCorrelToRel,
            SqlSecurityContext securityContext
    ) {
        super(planner, typeFactory, rexBuilder, nextCorrel, mapCorrelToRel);
        this.securityContext = securityContext;
    }

    public static HecatonRelOptCluster create(RelOptPlanner planner, RexBuilder rexBuilder, SqlSecurityContext ssc) {
        return new HecatonRelOptCluster(
                planner,
                rexBuilder.getTypeFactory(),
                rexBuilder,
                new AtomicInteger(),
                new HashMap<>(),
                ssc
        );
    }

    public QueryParameterMetadata getParameterMetadata() {
        return parameterMetadata;
    }

    public void setParameterMetadata(QueryParameterMetadata parameterMetadata) {
        this.parameterMetadata = parameterMetadata;
    }

    public boolean requiresJob() {
        return requiresJob;
    }

    public void setRequiresJob(boolean requiresJob) {
        this.requiresJob = requiresJob;
    }

    public SqlSecurityContext getSecurityContext() {
        return securityContext;
    }
}
