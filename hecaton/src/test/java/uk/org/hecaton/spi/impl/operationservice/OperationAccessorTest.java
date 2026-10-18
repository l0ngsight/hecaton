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

package uk.org.hecaton.spi.impl.operationservice;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.spi.impl.operationservice.impl.DummyOperation;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static uk.org.hecaton.spi.impl.operationservice.OperationAccessor.deactivate;
import static uk.org.hecaton.spi.impl.operationservice.OperationAccessor.setCallId;
import static uk.org.hecaton.spi.impl.operationservice.OperationAccessor.setCallTimeout;
import static uk.org.hecaton.spi.impl.operationservice.OperationAccessor.setCallerAddress;
import static uk.org.hecaton.spi.impl.operationservice.OperationAccessor.setInvocationTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationAccessorTest {

    @Mock
    private ServerConnection serverConnection;
    @Mock
    private Address address;

    @Test
    public void testCloneAndResetOperation() {
        Operation operation = new DummyOperation();

        setCallId(operation, 1);
        setInvocationTime(operation, 1000);
        Operation clone = OperationAccessor.cloneAndReset(operation, new DefaultSerializationServiceBuilder().build());
        assertEquals(0, clone.getCallId());
        assertEquals(-1, clone.getInvocationTime());
    }

    @Test
    public void testSetCallId() {
        Operation operation = new DummyOperation();
        setCallId(operation, 10);
        assertEquals(10, operation.getCallId());
    }

    @Test
    public void testSetInvocationTime() {
        Operation operation = new DummyOperation();
        setInvocationTime(operation, 10);
        assertEquals(10, operation.getInvocationTime());
    }

    @Test
    public void testSetCallerAddress() {
        Operation operation = new DummyOperation();
        setCallerAddress(operation, address);
        assertEquals(address, operation.getCallerAddress());
    }

    @Test
    public void testDeactivate() {
        Operation operation = new DummyOperation();
        setCallId(operation, 10);
        assertTrue(deactivate(operation));
        assertFalse(operation.isActive());
    }

    @Test
    public void testSetCallTimeout() {
        Operation operation = new DummyOperation();
        setCallTimeout(operation, 10);
        assertEquals(10, operation.getCallTimeout());
    }

    @Test
    public void testSetConnection() {
        Operation operation = new DummyOperation();
        OperationAccessor.setConnection(operation, serverConnection);
        assertEquals(serverConnection, operation.getConnection());
    }
}
