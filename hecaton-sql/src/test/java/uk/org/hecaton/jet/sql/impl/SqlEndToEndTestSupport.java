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

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.PartitioningAttributeConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.cluster.MemberInfo;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.datamodel.Tuple2;
import uk.org.hecaton.jet.impl.JobCoordinationService;
import uk.org.hecaton.jet.impl.JobInvocationObserver;
import uk.org.hecaton.jet.impl.execution.init.ExecutionPlan;
import uk.org.hecaton.jet.impl.util.Util;
import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.partition.PartitionService;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.sql.SqlExpectedResultType;
import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.sql.SqlRow;
import uk.org.hecaton.sql.SqlStatement;
import uk.org.hecaton.sql.impl.QueryId;
import uk.org.hecaton.sql.impl.SqlServiceImpl;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.optimizer.SqlPlan;
import uk.org.hecaton.sql.impl.security.NoOpSqlSecurityContext;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static uk.org.hecaton.internal.util.PartitioningStrategyUtil.constructAttributeBasedKey;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class SqlEndToEndTestSupport extends SqlTestSupport {
    protected ExpressionEvalContext eec;

    protected NodeEngineImpl nodeEngine;
    protected SqlServiceImpl sqlService;
    protected PlanExecutor planExecutor;
    protected JobCoordinationService jobCoordinationService;
    protected SqlJobInvocationObserverImpl sqlJobInvocationObserver;
    protected JobInvocationObserverImpl jobInvocationObserver;


    @Before
    public void setUp() throws Exception {
        nodeEngine = Accessors.getNodeEngineImpl(instance());
        sqlJobInvocationObserver = new SqlJobInvocationObserverImpl();
        jobInvocationObserver = new JobInvocationObserverImpl();
        sqlService = (SqlServiceImpl) instance().getSql();
        planExecutor = sqlService.getOptimizer().getPlanExecutor();
        jobCoordinationService = getJetServiceBackend(instance()).getJobCoordinationService();

        planExecutor.registerJobInvocationObserver(sqlJobInvocationObserver);
        jobCoordinationService.registerInvocationObserver(jobInvocationObserver);

        eec = ExpressionEvalContext.createContext(
                emptyList(),
                instance(),
                Util.getSerializationService(instance()),
                null
        );
    }

    @After
    public void teardown() {
        jobCoordinationService.unregisterInvocationObserver(jobInvocationObserver);
    }

    SqlPlanImpl.SelectPlan assertQueryPlan(String query) {
        SqlStatement sql = new SqlStatement(query);
        SqlPlan plan = sqlService.prepare(
                sql.getSchema(),
                query,
                sql.getParameters(),
                SqlExpectedResultType.ROWS,
                NoOpSqlSecurityContext.INSTANCE);

        assertInstanceOf(SqlPlanImpl.SelectPlan.class, plan);
        return (SqlPlanImpl.SelectPlan) plan;
    }

    SqlPlanImpl.DmlPlan assertDmlQueryPlan(String query) {
        SqlStatement sql = new SqlStatement(query);
        SqlPlan plan = sqlService.prepare(
                sql.getSchema(),
                query,
                sql.getParameters(),
                SqlExpectedResultType.UPDATE_COUNT,
                NoOpSqlSecurityContext.INSTANCE);

        assertInstanceOf(SqlPlanImpl.DmlPlan.class, plan);
        return (SqlPlanImpl.DmlPlan) plan;
    }

    void assertQueryResult(SqlPlanImpl.SelectPlan selectPlan, Collection<Row> expectedResults, Object... args) {
        List<Object> arguments = Collections.emptyList();
        if (args.length > 0) {
            arguments = Arrays.asList(args);
            eec = ExpressionEvalContext.createContext(
                    arguments,
                    instance(),
                    Util.getSerializationService(instance()),
                    null
            );
        }
        QueryId queryId = QueryId.create(UUID.randomUUID());
        SqlResult result = planExecutor.execute(selectPlan,
                queryId, arguments, 0L, NoOpSqlSecurityContext.INSTANCE);
        assertCollection(expectedResults, collectResult(result));
    }

    @Nonnull
    protected static ArrayList<Row> collectResult(SqlResult result) {
        var actualRows = new ArrayList<Row>();
        for (SqlRow r : result) {
            actualRows.add(new Row(r));
        }
        return actualRows;
    }

    @Nonnull
    protected ObjectAssert<DAG> assertThatDag() {
        return Assertions.<DAG>assertThat(sqlJobInvocationObserver.dag);
    }

    protected void assertInvokedOnlyOnMembers(HecatonInstance... members) {
        Set<Address> expectedMembers = Arrays.stream(members)
                .map(Accessors::getAddress)
                .collect(Collectors.toSet());
        assertEquals(expectedMembers, jobInvocationObserver.getMembers());
    }

    protected void configureMapWithAttributes(String mapName, String... attributes) {
        MapConfig mc = new MapConfig(mapName);
        for (String attribute : attributes) {
            mc.getPartitioningAttributeConfigs()
                    .add(new PartitioningAttributeConfig(attribute));
        }
        instance().getConfig().addMapConfig(mc);
    }

    protected static class SqlJobInvocationObserverImpl implements SqlJobInvocationObserver {
        public DAG dag;
        public JobConfig jobConfig;

        @Override
        public void onJobInvocation(DAG dag, JobConfig config) {
            this.dag = dag;
            this.jobConfig = config;
        }
    }

    protected static class JobInvocationObserverImpl implements JobInvocationObserver {
        public long jobId;
        public Set<MemberInfo> members;
        public DAG dag;
        public JobConfig jobConfig;

        @Override
        public void onJobInvocation(long jobId, Map<MemberInfo, ExecutionPlan> planMap, DAG dag, JobConfig jobConfig) {
            this.jobId = jobId;
            this.members = planMap.keySet();
            this.dag = dag;
            this.jobConfig = jobConfig;
        }

        @Override
        public void onLightJobInvocation(long jobId, Set<MemberInfo> members, DAG dag, JobConfig jobConfig) {
            this.jobId = jobId;
            this.members = members;
            this.dag = dag;
            this.jobConfig = jobConfig;
        }

        public Set<Address> getMembers() {
            return members.stream().map(MemberInfo::getAddress).collect(toSet());
        }
    }

    @SuppressWarnings({"SameParameterValue", "DanglingJavadoc"})
    /**
     * Calculates expected partitions and members to participate in the query execution.
     *
     * @param shouldUseCoordinator           whether coordinator should be included in the expected members
     * @param arity                          number of fields in partitioning attribute key
     *                                       (e.g. 2 for f0, f1)
     * @param partitionedPredicateConstants  constants used in the predicate in query
     */
    static Tuple2<Set<Address>, Set<Integer>> calculateExpectedPartitions(
            NodeEngine nodeEngine,
            Map<Address, int[]> partitionAssignment,
            boolean shouldUseCoordinator,
            int arity,
            int... partitionedPredicateConstants) {
        HecatonInstance hz = nodeEngine.getHecatonInstance();
        PartitionService partitionService = hz.getPartitionService();
        Map<Integer, Address> reversedPartitionAssignment = new HashMap<>();
        for (Map.Entry<Address, int[]> entry : partitionAssignment.entrySet()) {
            for (int partitionId : entry.getValue()) {
                reversedPartitionAssignment.put(partitionId, entry.getKey());
            }
        }

        Set<Integer> expectedPartitionsToParticipate = new HashSet<>();
        Set<Address> expectedMembersToParticipate = new HashSet<>();
        for (int equalityConstants : partitionedPredicateConstants) {
            Object[] constants = new Object[arity];
            Arrays.fill(constants, equalityConstants);
            Data keyData = nodeEngine.getSerializationService().toData(constructAttributeBasedKey(constants), v -> v);
            int partitionId = nodeEngine.getPartitionService().getPartitionId(keyData);
            assertTrue(reversedPartitionAssignment.containsKey(partitionId));

            expectedPartitionsToParticipate.add(partitionId);
            expectedMembersToParticipate.add(reversedPartitionAssignment.get(partitionId));
        }

        if (shouldUseCoordinator) {
            expectedMembersToParticipate.add(hz.getCluster().getLocalMember().getAddress());
            expectedPartitionsToParticipate.add(partitionService.getPartition("").getPartitionId());
        }

        return Tuple2.tuple2(expectedMembersToParticipate, expectedPartitionsToParticipate);
    }
}
