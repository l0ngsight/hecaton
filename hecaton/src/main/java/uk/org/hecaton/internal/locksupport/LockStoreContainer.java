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

package uk.org.hecaton.internal.locksupport;

import uk.org.hecaton.internal.partition.impl.NameSpaceUtil;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.internal.util.ConcurrencyUtil;
import uk.org.hecaton.internal.util.ConstructorFunction;
import uk.org.hecaton.internal.util.scheduler.EntryTaskScheduler;
import uk.org.hecaton.internal.util.scheduler.EntryTaskSchedulerFactory;
import uk.org.hecaton.internal.util.scheduler.ScheduleType;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.executionservice.TaskScheduler;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class LockStoreContainer {

    private final LockSupportServiceImpl lockService;
    private final int partitionId;

    private final ConcurrentMap<ObjectNamespace, LockStoreImpl> lockStores = new ConcurrentHashMap<>();
    private final ConstructorFunction<ObjectNamespace, LockStoreImpl> lockStoreConstructor =
            new ConstructorFunction<>() {
                public LockStoreImpl createNew(ObjectNamespace namespace) {
                    final ConstructorFunction<ObjectNamespace, LockStoreInfo> ctor =
                            lockService.getConstructor(namespace.getServiceName());
                    if (ctor != null) {
                        LockStoreInfo info = ctor.createNew(namespace);
                        if (info != null) {
                            int backupCount = info.getBackupCount();
                            int asyncBackupCount = info.getAsyncBackupCount();
                            EntryTaskScheduler<Data, Integer> entryTaskScheduler = createScheduler(namespace);
                            return new LockStoreImpl(lockService, namespace, entryTaskScheduler, backupCount, asyncBackupCount);
                        }
                    }
                    throw new IllegalArgumentException("No LockStore constructor is registered!");
                }
            };

    public LockStoreContainer(LockSupportServiceImpl lockService, int partitionId) {
        this.lockService = lockService;
        this.partitionId = partitionId;
    }

    void clearLockStore(ObjectNamespace namespace) {
        LockStoreImpl lockStore = lockStores.remove(namespace);
        if (lockStore != null) {
            lockStore.clear();
        }
    }

    LockStoreImpl getOrCreateLockStore(ObjectNamespace namespace) {
        return ConcurrencyUtil.getOrPutIfAbsent(lockStores, namespace, lockStoreConstructor);
    }

    public LockStoreImpl getLockStore(ObjectNamespace namespace) {
        return lockStores.get(namespace);
    }

    public Collection<LockStoreImpl> getLockStores() {
        return Collections.unmodifiableCollection(lockStores.values());
    }

    void clear() {
        for (LockStoreImpl lockStore : lockStores.values()) {
            lockStore.clear();
        }
        lockStores.clear();
    }

    int getPartitionId() {
        return partitionId;
    }

    public void put(LockStoreImpl ls) {
        ls.setLockService(lockService);
        ls.setEntryTaskScheduler(createScheduler(ls.getNamespace()));
        lockStores.put(ls.getNamespace(), ls);
    }

    private EntryTaskScheduler<Data, Integer> createScheduler(ObjectNamespace namespace) {
        NodeEngine nodeEngine = lockService.getNodeEngine();
        LockEvictionProcessor entryProcessor = new LockEvictionProcessor(nodeEngine, namespace);
        TaskScheduler globalScheduler = nodeEngine.getExecutionService().getGlobalTaskScheduler();
        return EntryTaskSchedulerFactory.newScheduler(globalScheduler, entryProcessor, ScheduleType.FOR_EACH);
    }

    public Collection<ServiceNamespace> getAllNamespaces(int replicaIndex) {
        return NameSpaceUtil.getAllNamespaces(lockStores,
                lockStore -> lockStore.getTotalBackupCount() >= replicaIndex,
                LockStoreImpl::getNamespace);
    }
}
