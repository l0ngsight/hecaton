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

package uk.org.hecaton.jet.sql.impl.connector.map;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.ProcessorMetaSupplier;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.core.Vertex;
import uk.org.hecaton.jet.core.test.TestProcessorMetaSupplierContext;
import uk.org.hecaton.jet.core.test.TestProcessorSupplierContext;
import uk.org.hecaton.jet.impl.connector.ReadMapOrCacheP;
import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.sql.impl.expression.ConstantExpression;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static uk.org.hecaton.jet.core.Edge.between;
import static uk.org.hecaton.jet.core.processor.SinkProcessors.writeMapP;
import static uk.org.hecaton.jet.sql.impl.connector.map.SpecificPartitionsImapReaderPms.mapReader;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings({"rawtypes", "unchecked"})
public class SpecificPartitionsImapReaderPmsTest extends SqlTestSupport {
    private static final int MEMBERS = 5;
    private static final int ITERATIONS = 1000;

    private int coordinatorOwnedPartitionKey;
    private int coordinatorOwnedPartitionId;
    // Non-coordinator-owned partitions keys and ids, one per member in cluster.
    // Coordinator is not included!
    private int[] perMemberOwnedPKey;
    private int[] perMemberOwnedPId;

    private String mapName;
    private String sinkName;

    private IMap<Integer, Integer> sourceMap;
    private IMap<Integer, Integer> sinkMap;

    Map<Address, int[]> partitionAssignment;
    Map<Integer, Address> reversedPartitionAssignment;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(MEMBERS, null);
    }

    @Before
    public void before() throws Exception {
        partitionAssignment = getPartitionAssignment(instance());
        reversedPartitionAssignment = new HashMap<>();
        for (Map.Entry<Address, int[]> entry : partitionAssignment.entrySet()) {
            for (int pId : entry.getValue()) {
                reversedPartitionAssignment.put(pId, entry.getKey());
            }
        }

        mapName = randomName();
        sinkName = randomName();
        sourceMap = instance().getMap(mapName);
        sinkMap = instance().getMap(sinkName);

        perMemberOwnedPKey = new int[MEMBERS - 1];
        perMemberOwnedPId = new int[MEMBERS - 1];

        Address coordinatorAddress = Accessors.getAddress(instance());
        for (int i = 1; i < ITERATIONS; ++i) {
            int pIdCandidate = instance().getPartitionService().getPartition(i).getPartitionId();
            if (reversedPartitionAssignment.get(pIdCandidate).equals(coordinatorAddress)) {
                coordinatorOwnedPartitionKey = i;
                coordinatorOwnedPartitionId = pIdCandidate;
                break;
            }
        }

        Set<Address> clusterAddresses = new HashSet<>(partitionAssignment.keySet());
        clusterAddresses.remove(coordinatorAddress);
        int j = 0;
        for (int i = 1; i < ITERATIONS && !clusterAddresses.isEmpty(); ++i) {
            int pIdCandidate = Accessors.getNodeEngineImpl(instance()).getPartitionService().getPartitionId(i);
            Address address = reversedPartitionAssignment.get(pIdCandidate);
            if (clusterAddresses.contains(address)) {
                perMemberOwnedPKey[j] = i;
                perMemberOwnedPId[j++] = pIdCandidate;
                clusterAddresses.remove(address);
            }
        }
    }

    @Test
    public void test_nonPrunableScan_unitTest() throws Exception {
        SpecificPartitionsImapReaderPms readPms = (SpecificPartitionsImapReaderPms) mapReader(mapName, null, null);

        readPms.init(getMetaSupplierContext());

        // Ensure that we scan all partitions.
        assertNull(readPms.partitionsToScan);
        Function<Address, ProcessorSupplier> psf = readPms.get(new ArrayList<>(partitionAssignment.keySet()));

        for (HecatonInstance hz : instances()) {
            TestProcessorSupplierContext psCtx = new TestProcessorSupplierContext().setHecatonInstance(hz);
            Address address = Accessors.getAddress(hz);
            ReadMapOrCacheP.LocalProcessorSupplier ps = (ReadMapOrCacheP.LocalProcessorSupplier) psf.apply(address);
            ps.init(psCtx);
            assertThat(ps.getPartitionsToScan()).as("Should scan all partitions owned by member")
                    .containsExactly(psCtx.memberPartitions());
        }
    }

    @Test
    public void test_prunableScan_unitTest() throws Exception {
        for (int partitionCountToUse = 0; partitionCountToUse < perMemberOwnedPKey.length + 1; ++partitionCountToUse) {
            // given
            List<List<Expression<?>>> expressions = new ArrayList<>();
            if (partitionCountToUse > 0) {
                expressions.add(List.of(ConstantExpression.create(coordinatorOwnedPartitionKey, QueryDataType.INT)));
            }
            for (int i = 0; i < partitionCountToUse - 1; ++i) {
                expressions.add(List.of(ConstantExpression.create(perMemberOwnedPKey[i], QueryDataType.INT)));
            }
            SpecificPartitionsImapReaderPms readPms = (SpecificPartitionsImapReaderPms) mapReader(mapName, null, expressions);

            // when
            readPms.init(getMetaSupplierContext());

            // then
            int[] expected = partitionCountToUse > 0
                    ? ArrayUtils.add(Arrays.copyOf(perMemberOwnedPId, partitionCountToUse - 1), coordinatorOwnedPartitionId)
                    : new int[]{};
            assertThat(readPms.partitionsToScan)
                    .as("Should scan expected partitions").containsExactlyInAnyOrder(expected)
                    .as("Partitions list should be sorted").isSorted();
        }
    }

    @Test
    public void test_prunableScan_psUnitTest() throws Exception {
        // given
        List<List<Expression<?>>> expressions = new ArrayList<>();
        expressions.add(List.of(ConstantExpression.create(coordinatorOwnedPartitionKey, QueryDataType.INT)));
        SpecificPartitionsImapReaderPms readPms = (SpecificPartitionsImapReaderPms) mapReader(mapName, null, expressions);

        TestProcessorSupplierContext context = new TestProcessorSupplierContext().setHecatonInstance(instance());
        readPms.init(context);

        Address coordinatorAddress = Accessors.getAddress(instance());
        ReadMapOrCacheP.LocalProcessorSupplier ps = (ReadMapOrCacheP.LocalProcessorSupplier)
                readPms.get(List.of(coordinatorAddress)).apply(coordinatorAddress);

        // when
        ps.init(context);

        // then
        assertThat(ps.getPartitionsToScan())
                .as("Should scan only expected partitions").containsExactly(coordinatorOwnedPartitionId);
    }

    @Test
    public void test_prunableScanDuplicates_unitTest() throws Exception {
        // given
        List<List<Expression<?>>> expressions = new ArrayList<>();
        // simulate situation when many expressions produce the same partition
        for (int i = 0; i < 5; ++i) {
            expressions.add(List.of(ConstantExpression.create(coordinatorOwnedPartitionKey, QueryDataType.INT)));
        }
        SpecificPartitionsImapReaderPms readPms = (SpecificPartitionsImapReaderPms) mapReader(mapName, null, expressions);

        // when
        readPms.init(getMetaSupplierContext());

        // then
        assertThat(readPms.partitionsToScan)
                .as("Should scan partition once").containsExactly(coordinatorOwnedPartitionId);
    }

    // We test basic code path for IMap scan
    @Test
    public void test_nonPrunableScan() {
        // Basic test is performed as submitting job by DAG.
        sourceMap.put(0, 0);

        DAG dag = new DAG();
        ProcessorMetaSupplier readPms = mapReader(mapName, null, null);
        Vertex source = dag.newVertex("source", readPms);
        Vertex sink = dag.newVertex("sink", writeMapP(sinkName));

        dag.edge(between(source, sink));
        instance().getJet().newLightJob(dag).join();

        assertEquals(0, instance().getMap(sinkName).get(0));
    }

    @Test
    public void test_prunableSinglePartition() {
        // Given
        int partitionsToUse = 1;
        DAG dag = setupPrunableDag(sourceMap, partitionsToUse);

        // When
        instance().getJet().newLightJob(dag).join();

        // Then
        assertPrunability(partitionsToUse);
    }

    @Test
    public void test_prunableMultiplePartitions() {
        // Given
        int partitionsToUse = 3;
        DAG dag = setupPrunableDag(sourceMap, partitionsToUse);

        // When
        instance().getJet().newLightJob(dag).join();

        // Then
        assertPrunability(partitionsToUse);
    }

    @Test
    public void test_prunableNoPartitions() {
        // Given
        int partitionsToUse = 0;
        DAG dag = setupPrunableDag(sourceMap, partitionsToUse);

        // When
        instance().getJet().newLightJob(dag).join();

        // Then
        assertPrunability(partitionsToUse);
    }

    private DAG setupPrunableDag(IMap<Integer, Integer> map, int partitionCountToUse) {
        List<List<Expression<?>>> expressions = new ArrayList<>();
        if (partitionCountToUse > 0) {
            map.put(coordinatorOwnedPartitionKey, coordinatorOwnedPartitionKey);
            expressions.add(List.of(ConstantExpression.create(coordinatorOwnedPartitionKey, QueryDataType.INT)));
        }
        for (int i = 0; i < partitionCountToUse - 1; ++i) {
            map.put(perMemberOwnedPKey[i], perMemberOwnedPKey[i]);
            expressions.add(List.of(ConstantExpression.create(perMemberOwnedPKey[i], QueryDataType.INT)));
        }

        assertEquals(partitionCountToUse, expressions.size());

        DAG dag = new DAG();
        ProcessorMetaSupplier mapReader = mapReader(mapName, null, expressions);
        Vertex source = dag.newVertex("source", mapReader);
        Vertex sink = dag.newVertex("sink", writeMapP(sinkName));
        dag.edge(between(source, sink));

        return dag;
    }

    private void assertPrunability(int partitionCountUsed) {
        // Assert sink map read all results
        assertEquals(partitionCountUsed, sinkMap.size());
        if (partitionCountUsed > 0) {
            assertEquals(coordinatorOwnedPartitionKey, instance().getMap(sinkName).get(coordinatorOwnedPartitionKey));
        }
        for (int i = 0; i < partitionCountUsed - 1; i++) {
            assertEquals(perMemberOwnedPKey[i], instance().getMap(sinkName).get(perMemberOwnedPKey[i]));
        }
    }

    @Nonnull
    private static TestProcessorMetaSupplierContext getMetaSupplierContext() {
        return new TestProcessorMetaSupplierContext().setHecatonInstance(instance());
    }
}
