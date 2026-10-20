/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.internal.util;

import uk.org.hecaton.collection.impl.CollectionTxnUtil;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.nio.BufferObjectDataInput;
import uk.org.hecaton.internal.nio.BufferObjectDataOutput;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CollectionTxnUtilTest extends HecatonTestSupport {

    private NodeEngine nodeEngine = mock(NodeEngine.class);

    private RemoteService remoteService = mock(RemoteService.class);

    private String serviceName;

    private UUID callerUuid;

    private int partitionId;

    private List<Operation> operationList;

    private Operation wrapper;

    @Before
    public void setup() {
        serviceName = randomString();
        when(nodeEngine.getService(serviceName)).thenReturn(remoteService);

        callerUuid = UuidUtil.newUnsecureUUID();
        partitionId = RandomPicker.getInt(271);
        operationList = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            operationList.add(new TestOperation(i));
        }
        wrapper = new TestOperation(-1);
        wrapper.setService(remoteService);
        wrapper.setServiceName(serviceName);
        wrapper.setCallerUuid(callerUuid);
        wrapper.setNodeEngine(nodeEngine);
        wrapper.setPartitionId(partitionId);
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(CollectionTxnUtil.class);
    }

    @Test
    public void testBefore() throws Exception {
        CollectionTxnUtil.before(operationList, wrapper);
        for (Operation operation : operationList) {
            TestOperation op = (TestOperation) operation;
            assertTrue(op.beforeCalled);
            assertEquals(remoteService, op.getService());
            assertEquals(serviceName, op.getServiceName());
            assertEquals(callerUuid, op.getCallerUuid());
            assertEquals(nodeEngine, op.getNodeEngine());
            assertEquals(partitionId, op.getPartitionId());
        }
    }

    @Test
    public void testRun() throws Exception {
        List<Operation> backupList = CollectionTxnUtil.run(operationList);
        assertEquals(1, backupList.size());
        TestOperation operation = (TestOperation) backupList.get(0);
        assertEquals(-3, operation.i);
    }

    @Test
    public void testAfter() throws Exception {
        CollectionTxnUtil.after(operationList);
        for (Operation operation : operationList) {
            TestOperation op = (TestOperation) operation;
            assertTrue(op.afterCalled);
        }
    }

    @Test
    public void testWriteRead() throws IOException {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        BufferObjectDataOutput out = ss.createObjectDataOutput();
        CollectionTxnUtil.write(out, operationList);
        BufferObjectDataInput in = ss.createObjectDataInput(out.toByteArray());
        List<Operation> resultList = CollectionTxnUtil.read(in);
        assertEquals(operationList.size(), resultList.size());
        for (int i = 0; i < operationList.size(); i++) {
            assertEquals(operationList.get(i), resultList.get(i));
        }
    }

    static class TestOperation extends Operation implements BackupAwareOperation {

        int i;

        transient boolean beforeCalled;

        transient boolean runCalled;

        transient boolean afterCalled;

        TestOperation() {
        }

        TestOperation(int i) {
            this.i = i;
        }

        @Override
        public void beforeRun() throws Exception {
            beforeCalled = true;
        }

        @Override
        public void run() throws Exception {
            runCalled = true;
        }

        @Override
        public void afterRun() throws Exception {
            afterCalled = true;
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }

        @Override
        public boolean shouldBackup() {
            return i == 3;
        }

        @Override
        public int getSyncBackupCount() {
            return 0;
        }

        @Override
        public int getAsyncBackupCount() {
            return 0;
        }

        @Override
        public Operation getBackupOperation() {
            return new TestOperation(-i);
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            out.writeInt(i);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            i = in.readInt();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestOperation)) {
                return false;
            }

            TestOperation that = (TestOperation) o;
            return i == that.i;
        }

        @Override
        public int hashCode() {
            return i;
        }
    }
}
