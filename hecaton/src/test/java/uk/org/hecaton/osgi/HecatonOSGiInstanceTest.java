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

package uk.org.hecaton.osgi;

import uk.org.hecaton.client.ClientService;
import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.cluster.Endpoint;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.DistributedObjectListener;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.core.LifecycleService;
import uk.org.hecaton.cp.CPSubsystem;
import uk.org.hecaton.cp.IAtomicLong;
import uk.org.hecaton.cp.IAtomicReference;
import uk.org.hecaton.cp.ICountDownLatch;
import uk.org.hecaton.cp.ISemaphore;
import uk.org.hecaton.cp.lock.FencedLock;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.multimap.MultiMap;
import uk.org.hecaton.partition.PartitionService;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.topic.ITopic;
import uk.org.hecaton.transaction.HecatonXAResource;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionOptions;
import uk.org.hecaton.transaction.TransactionalTask;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.osgi.impl.HecatonOSGiTestUtil.createHecatonOSGiInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonOSGiInstanceTest {

    @Test
    @SuppressWarnings("EqualsWithItself")
    public void equalsReturnsTrueForSameOSGiInstances() {
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        assertEquals(hecatonOSGiInstance, hecatonOSGiInstance);
    }

    @Test
    @SuppressWarnings("ObjectEqualsNull")
    public void equalsReturnsFalseForNullObject() {
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        assertNotEquals(null, hecatonOSGiInstance);
    }

    @Test
    public void equalsReturnsFalseForDifferentTypedObject() {
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        assertNotEquals(hecatonOSGiInstance, new Object());
    }

    @Test
    public void equalsReturnsFalseForDifferentOSGiInstancesWithDifferentDelegatedInstanceAndSameService() {
        HecatonInstance mockHecatonInstance1 = mock(HecatonInstance.class);
        HecatonInstance mockHecatonInstance2 = mock(HecatonInstance.class);
        HecatonOSGiService mockService = mock(HecatonOSGiService.class);
        HecatonOSGiInstance hecatonOSGiInstance1 = createHecatonOSGiInstance(mockHecatonInstance1, mockService);
        HecatonOSGiInstance hecatonOSGiInstance2 = createHecatonOSGiInstance(mockHecatonInstance2, mockService);

        assertNotEquals(hecatonOSGiInstance1, hecatonOSGiInstance2);
    }

    @Test
    public void equalsReturnsFalseForDifferentOSGiInstancesWithSameDelegatedInstanceAndDifferentService() {
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiService mockService1 = mock(HecatonOSGiService.class);
        HecatonOSGiService mockService2 = mock(HecatonOSGiService.class);
        HecatonOSGiInstance hecatonOSGiInstance1 = createHecatonOSGiInstance(mockHecatonInstance, mockService1);
        HecatonOSGiInstance hecatonOSGiInstance2 = createHecatonOSGiInstance(mockHecatonInstance, mockService2);

        assertNotEquals(hecatonOSGiInstance1, hecatonOSGiInstance2);
    }

    @Test
    public void equalsReturnsFalseForDifferentOSGiInstancesWithDifferentDelegatedInstanceAndDifferentService() {
        HecatonInstance mockHecatonInstance1 = mock(HecatonInstance.class);
        HecatonInstance mockHecatonInstance2 = mock(HecatonInstance.class);
        HecatonOSGiService mockService1 = mock(HecatonOSGiService.class);
        HecatonOSGiService mockService2 = mock(HecatonOSGiService.class);
        HecatonOSGiInstance hecatonOSGiInstance1 = createHecatonOSGiInstance(mockHecatonInstance1, mockService1);
        HecatonOSGiInstance hecatonOSGiInstance2 = createHecatonOSGiInstance(mockHecatonInstance2, mockService2);

        assertNotEquals(hecatonOSGiInstance1, hecatonOSGiInstance2);
    }

    @Test
    public void equalsReturnsTrueForDifferentOSGiInstancesWithSameDelegatedInstanceAndSameService() {
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiService mockService = mock(HecatonOSGiService.class);
        HecatonOSGiInstance hecatonOSGiInstance1 = createHecatonOSGiInstance(mockHecatonInstance, mockService);
        HecatonOSGiInstance hecatonOSGiInstance2 = createHecatonOSGiInstance(mockHecatonInstance, mockService);

        assertEquals(hecatonOSGiInstance1, hecatonOSGiInstance2);
    }

    @Test
    public void getDelegatedInstanceCalledSuccessfullyOverOSGiInstance() {
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        assertEquals(mockHecatonInstance, hecatonOSGiInstance.getDelegatedInstance());
    }

    @Test
    public void getOwnerServiceCalledSuccessfullyOverOSGiInstance() {
        HecatonOSGiService mockHecatonOSGiService = mock(HecatonOSGiService.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonOSGiService);

        assertEquals(mockHecatonOSGiService, hecatonOSGiInstance.getOwnerService());
    }

    @Test
    public void getNameCalledSuccessfullyOverOSGiInstance() {
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getName()).thenReturn("my-name");

        assertEquals("my-name", hecatonOSGiInstance.getName());

        verify(mockHecatonInstance).getName();
    }

    @Test
    public void getQueueCalledSuccessfullyOverOSGiInstance() {
        IQueue<Object> mockQueue = mock();
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getQueue("my-queue")).thenReturn(mockQueue);

        assertEquals(mockQueue, hecatonOSGiInstance.getQueue("my-queue"));

        verify(mockHecatonInstance).getQueue("my-queue");
    }

    @Test
    public void getTopicCalledSuccessfullyOverOSGiInstance() {
        ITopic<Object> mockTopic = mock();
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getTopic("my-topic")).thenReturn(mockTopic);

        assertEquals(mockTopic, hecatonOSGiInstance.getTopic("my-topic"));

        verify(mockHecatonInstance).getTopic("my-topic");
    }

    @Test
    public void getSetCalledSuccessfullyOverOSGiInstance() {
        ISet<Object> mockSet = mock();
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getSet("my-set")).thenReturn(mockSet);

        assertEquals(mockSet, hecatonOSGiInstance.getSet("my-set"));

        verify(mockHecatonInstance).getSet("my-set");
    }

    @Test
    public void getListCalledSuccessfullyOverOSGiInstance() {
        IList<Object> mockList = mock();
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getList("my-list")).thenReturn(mockList);

        assertEquals(mockList, hecatonOSGiInstance.getList("my-list"));

        verify(mockHecatonInstance).getList("my-list");
    }

    @Test
    public void getMapCalledSuccessfullyOverOSGiInstance() {
        IMap<Object, Object> mockMap = mock();
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getMap("my-map")).thenReturn(mockMap);

        assertEquals(mockMap, hecatonOSGiInstance.getMap("my-map"));

        verify(mockHecatonInstance).getMap("my-map");
    }

    @Test
    public void getReplicatedMapCalledSuccessfullyOverOSGiInstance() {
        ReplicatedMap<Object, Object> mockReplicatedMap = mock();
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getReplicatedMap("my-replicatedmap")).thenReturn(mockReplicatedMap);

        assertEquals(mockReplicatedMap, hecatonOSGiInstance.getReplicatedMap("my-replicatedmap"));

        verify(mockHecatonInstance).getReplicatedMap("my-replicatedmap");
    }

    @Test
    public void getMultiMapCalledSuccessfullyOverOSGiInstance() {
        MultiMap<Object, Object> mockMultiMap = mock();
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getMultiMap("my-multimap")).thenReturn(mockMultiMap);

        assertEquals(mockMultiMap, hecatonOSGiInstance.getMultiMap("my-multimap"));

        verify(mockHecatonInstance).getMultiMap("my-multimap");
    }

    @Test
    public void getLockCalledSuccessfullyOverOSGiInstance() {
        FencedLock mockLock = mock(FencedLock.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        CPSubsystem cpSubsystem = mock(CPSubsystem.class);
        when(mockHecatonInstance.getCPSubsystem()).thenReturn(cpSubsystem);

        when(cpSubsystem.getLock("my-lock")).thenReturn(mockLock);

        assertEquals(mockLock, hecatonOSGiInstance.getCPSubsystem().getLock("my-lock"));

        verify(cpSubsystem).getLock("my-lock");
    }

    @Test
    public void getRingbufferCalledSuccessfullyOverOSGiInstance() {
        Ringbuffer<Object> mockRingbuffer = mock();
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getRingbuffer("my-ringbuffer")).thenReturn(mockRingbuffer);

        assertEquals(mockRingbuffer, hecatonOSGiInstance.getRingbuffer("my-ringbuffer"));

        verify(mockHecatonInstance).getRingbuffer("my-ringbuffer");
    }

    @Test
    public void getReliableTopicCalledSuccessfullyOverOSGiInstance() {
        ITopic<Object> mockReliableTopic = mock();
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getReliableTopic("my-reliabletopic")).thenReturn(mockReliableTopic);

        assertEquals(mockReliableTopic, hecatonOSGiInstance.getReliableTopic("my-reliabletopic"));

        verify(mockHecatonInstance).getReliableTopic("my-reliabletopic");
    }

    @Test
    public void getClusterCalledSuccessfullyOverOSGiInstance() {
        Cluster mockCluster = mock(Cluster.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getCluster()).thenReturn(mockCluster);

        assertEquals(mockCluster, hecatonOSGiInstance.getCluster());

        verify(mockHecatonInstance).getCluster();
    }

    @Test
    public void getLocalEndpointCalledSuccessfullyOverOSGiInstance() {
        Endpoint mockEndpoint = mock(Endpoint.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getLocalEndpoint()).thenReturn(mockEndpoint);

        assertEquals(mockEndpoint, hecatonOSGiInstance.getLocalEndpoint());

        verify(mockHecatonInstance).getLocalEndpoint();
    }

    @Test
    public void getExecutorServiceCalledSuccessfullyOverOSGiInstance() {
        IExecutorService mockExecutorService = mock(IExecutorService.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getExecutorService("my-executorservice")).thenReturn(mockExecutorService);

        assertEquals(mockExecutorService, hecatonOSGiInstance.getExecutorService("my-executorservice"));

        verify(mockHecatonInstance).getExecutorService("my-executorservice");
    }

    @Test
    public void executeTransactionCalledSuccessfullyOverOSGiInstance() {
        Object result = new Object();
        TransactionalTask mockTransactionalTask = mock(TransactionalTask.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.executeTransaction(mockTransactionalTask)).thenReturn(result);

        assertEquals(result, hecatonOSGiInstance.executeTransaction(mockTransactionalTask));

        verify(mockHecatonInstance).executeTransaction(mockTransactionalTask);
    }

    @Test
    public void executeTransactionWithOptionsCalledSuccessfullyOverOSGiInstance() {
        Object result = new Object();
        TransactionOptions transactionOptions = new TransactionOptions();
        TransactionalTask mockTransactionalTask = mock(TransactionalTask.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.executeTransaction(transactionOptions, mockTransactionalTask)).thenReturn(result);

        assertEquals(result, hecatonOSGiInstance.executeTransaction(transactionOptions, mockTransactionalTask));

        verify(mockHecatonInstance).executeTransaction(transactionOptions, mockTransactionalTask);
    }

    @Test
    public void newTransactionContextCalledSuccessfullyOverOSGiInstance() {
        TransactionContext mockTransactionContext = mock(TransactionContext.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.newTransactionContext()).thenReturn(mockTransactionContext);

        assertEquals(mockTransactionContext, hecatonOSGiInstance.newTransactionContext());

        verify(mockHecatonInstance).newTransactionContext();
    }

    @Test
    public void newTransactionContextWithOptionsCalledSuccessfullyOverOSGiInstance() {
        TransactionOptions transactionOptions = new TransactionOptions();
        TransactionContext mockTransactionContext = mock(TransactionContext.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.newTransactionContext(transactionOptions)).thenReturn(mockTransactionContext);

        assertEquals(mockTransactionContext, hecatonOSGiInstance.newTransactionContext(transactionOptions));

        verify(mockHecatonInstance).newTransactionContext(transactionOptions);
    }

    @Test
    public void getAtomicLongCalledSuccessfullyOverOSGiInstance() {
        IAtomicLong mockAtomicLong = mock(IAtomicLong.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        CPSubsystem cpSubsystem = mock(CPSubsystem.class);
        when(mockHecatonInstance.getCPSubsystem()).thenReturn(cpSubsystem);
        when(mockHecatonInstance.getCPSubsystem().getAtomicLong("my-atomiclong")).thenReturn(mockAtomicLong);

        assertEquals(mockAtomicLong, hecatonOSGiInstance.getCPSubsystem().getAtomicLong("my-atomiclong"));

        verify(cpSubsystem).getAtomicLong("my-atomiclong");
    }

    @Test
    public void getAtomicReferenceCalledSuccessfullyOverOSGiInstance() {
        IAtomicReference<Object> mockAtomicReference = mock();
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        CPSubsystem cpSubsystem = mock(CPSubsystem.class);
        when(mockHecatonInstance.getCPSubsystem()).thenReturn(cpSubsystem);
        when(cpSubsystem.getAtomicReference("my-atomicreference")).thenReturn(mockAtomicReference);

        assertEquals(mockAtomicReference, hecatonOSGiInstance.getCPSubsystem().getAtomicReference("my-atomicreference"));

        verify(cpSubsystem).getAtomicReference("my-atomicreference");
    }

    @Test
    public void getCountDownLatchCalledSuccessfullyOverOSGiInstance() {
        ICountDownLatch mockCountDownLatch = mock(ICountDownLatch.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        CPSubsystem cpSubsystem = mock(CPSubsystem.class);
        when(mockHecatonInstance.getCPSubsystem()).thenReturn(cpSubsystem);
        when(cpSubsystem.getCountDownLatch("my-countdownlatch")).thenReturn(mockCountDownLatch);

        assertEquals(mockCountDownLatch, hecatonOSGiInstance.getCPSubsystem().getCountDownLatch("my-countdownlatch"));

        verify(cpSubsystem).getCountDownLatch("my-countdownlatch");
    }

    @Test
    public void getSemaphoreCalledSuccessfullyOverOSGiInstance() {
        ISemaphore mockSemaphore = mock(ISemaphore.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        CPSubsystem cpSubsystem = mock(CPSubsystem.class);
        when(mockHecatonInstance.getCPSubsystem()).thenReturn(cpSubsystem);
        when(cpSubsystem.getSemaphore("my-semaphore")).thenReturn(mockSemaphore);

        assertEquals(mockSemaphore, hecatonOSGiInstance.getCPSubsystem().getSemaphore("my-semaphore"));

        verify(cpSubsystem).getSemaphore("my-semaphore");
    }

    @Test
    public void getDistributedObjectsCalledSuccessfullyOverOSGiInstance() {
        Collection<DistributedObject> mockDistributedObjects = mock();
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getDistributedObjects()).thenReturn(mockDistributedObjects);

        assertEquals(mockDistributedObjects, hecatonOSGiInstance.getDistributedObjects());

        verify(mockHecatonInstance).getDistributedObjects();
    }

    @Test
    public void getDistributedObjectCalledSuccessfullyOverOSGiInstance() {
        DistributedObject mockDistributedObject = mock(DistributedObject.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance =
                createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getDistributedObject("my-service", "my-name")).thenReturn(mockDistributedObject);

        assertEquals(mockDistributedObject, hecatonOSGiInstance.getDistributedObject("my-service", "my-name"));

        verify(mockHecatonInstance).getDistributedObject("my-service", "my-name");
    }

    @Test
    public void addDistributedObjectListenerCalledSuccessfullyOverOSGiInstance() {
        DistributedObjectListener mockDistributedObjectListener = mock(DistributedObjectListener.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance =
                createHecatonOSGiInstance(mockHecatonInstance);

        UUID registrationId = UuidUtil.newUnsecureUUID();
        when(mockHecatonInstance.addDistributedObjectListener(mockDistributedObjectListener)).thenReturn(registrationId);

        assertEquals(registrationId, hecatonOSGiInstance.addDistributedObjectListener(mockDistributedObjectListener));

        verify(mockHecatonInstance).addDistributedObjectListener(mockDistributedObjectListener);
    }

    @Test
    public void removeDistributedObjectListenerCalledSuccessfullyOverOSGiInstance() {
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        UUID registrationId = UuidUtil.newUnsecureUUID();

        when(mockHecatonInstance.removeDistributedObjectListener(registrationId)).thenReturn(true);

        assertTrue(hecatonOSGiInstance.removeDistributedObjectListener(registrationId));

        verify(mockHecatonInstance).removeDistributedObjectListener(registrationId);
    }

    @Test
    public void getPartitionServiceCalledSuccessfullyOverOSGiInstance() {
        PartitionService mockPartitionService = mock(PartitionService.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getPartitionService()).thenReturn(mockPartitionService);

        assertEquals(mockPartitionService, hecatonOSGiInstance.getPartitionService());

        verify(mockHecatonInstance).getPartitionService();
    }

    @Test
    public void getSplitBrainProtectionServiceCalledSuccessfullyOverOSGiInstance() {
        SplitBrainProtectionService mockSplitBrainProtectionService = mock(SplitBrainProtectionService.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getSplitBrainProtectionService()).thenReturn(mockSplitBrainProtectionService);

        assertEquals(mockSplitBrainProtectionService, hecatonOSGiInstance.getSplitBrainProtectionService());

        verify(mockHecatonInstance).getSplitBrainProtectionService();
    }

    @Test
    public void getClientServiceCalledSuccessfullyOverOSGiInstance() {
        ClientService mockClientService = mock(ClientService.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getClientService()).thenReturn(mockClientService);

        assertEquals(mockClientService, hecatonOSGiInstance.getClientService());

        verify(mockHecatonInstance).getClientService();
    }

    @Test
    public void getLoggingServiceCalledSuccessfullyOverOSGiInstance() {
        LoggingService mockLoggingService = mock(LoggingService.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getLoggingService()).thenReturn(mockLoggingService);

        assertEquals(mockLoggingService, hecatonOSGiInstance.getLoggingService());

        verify(mockHecatonInstance).getLoggingService();
    }

    @Test
    public void getLifecycleServiceCalledSuccessfullyOverOSGiInstance() {
        LifecycleService mockLifecycleService = mock(LifecycleService.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getLifecycleService()).thenReturn(mockLifecycleService);

        assertEquals(mockLifecycleService, hecatonOSGiInstance.getLifecycleService());

        verify(mockHecatonInstance).getLifecycleService();
    }

    @Test
    public void getUserContextCalledSuccessfullyOverOSGiInstance() {
        ConcurrentMap<String, Object> mockUserContext = new ConcurrentHashMap<>(1);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getUserContext()).thenReturn(mockUserContext);

        assertEquals(mockUserContext, hecatonOSGiInstance.getUserContext());

        verify(mockHecatonInstance).getUserContext();
    }

    @Test
    public void getXAResourceCalledSuccessfullyOverOSGiInstance() {
        HecatonXAResource mockXAResource = mock(HecatonXAResource.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getXAResource()).thenReturn(mockXAResource);

        assertEquals(mockXAResource, hecatonOSGiInstance.getXAResource());

        verify(mockHecatonInstance).getXAResource();
    }

    @Test
    public void getConfigCalledSuccessfullyOverOSGiInstance() {
        Config mockConfig = mock(Config.class);
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        when(mockHecatonInstance.getConfig()).thenReturn(mockConfig);

        assertEquals(mockConfig, hecatonOSGiInstance.getConfig());

        verify(mockHecatonInstance).getConfig();
    }

    @Test
    public void shutdownCalledSuccessfullyOverOSGiInstance() {
        HecatonInstance mockHecatonInstance = mock(HecatonInstance.class);
        HecatonOSGiInstance hecatonOSGiInstance = createHecatonOSGiInstance(mockHecatonInstance);

        hecatonOSGiInstance.shutdown();

        verify(mockHecatonInstance).shutdown();
    }
}
