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

package uk.org.hecaton.collection.impl;

import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.collection.impl.collection.CollectionContainer;
import uk.org.hecaton.collection.impl.collection.CollectionItem;
import uk.org.hecaton.collection.impl.collection.CollectionService;
import uk.org.hecaton.collection.impl.list.ListService;
import uk.org.hecaton.collection.impl.queue.QueueContainer;
import uk.org.hecaton.collection.impl.queue.QueueService;
import uk.org.hecaton.collection.impl.set.SetService;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.Accessors;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static uk.org.hecaton.test.Accessors.getFirstBackupInstance;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getPartitionIdViaReflection;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public final class CollectionTestUtil {

    private CollectionTestUtil() {
    }

    /**
     * Returns the backup instance of an {@link IList} by a given list instance.
     * <p>
     * Note: Returns the backups from the first replica index.
     *
     * @param instances the {@link HecatonInstance} array to gather the data from
     * @param list      the {@link IList} to retrieve the backup from
     * @return a {@link List} with the backup items
     */
    public static <E> List<E> getBackupList(HecatonInstance[] instances, IList list) {
        int partitionId = getPartitionIdViaReflection(list);
        HecatonInstance backupInstance = getFirstBackupInstance(instances, partitionId);
        return getBackupList(backupInstance, list.getName());
    }

    /**
     * Returns all backup items of an {@link IList} by a given list name.
     * <p>
     * Note: You have to provide the {@link HecatonInstance} you want to retrieve the backups from.
     * Use {@link Accessors#getBackupInstance(HecatonInstance[], int, int)} to retrieve the backup instance for a given replica index.
     *
     * @param backupInstance the {@link HecatonInstance} to retrieve the backups from
     * @param listName       the list name
     * @return a {@link List} with the backup items
     */
    public static <E> List<E> getBackupList(HecatonInstance backupInstance, String listName) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(backupInstance);
        CollectionService service = nodeEngine.getService(ListService.SERVICE_NAME);
        CollectionContainer collectionContainer = service.getContainerMap().get(listName);
        if (collectionContainer == null) {
            return emptyList();
        }
        // the replica items are retrieved via `getMap()`, the primary items via `getCollection()`
        Map<Long, CollectionItem> map = collectionContainer.getMap();

        List<E> backupList = new ArrayList<>(map.size());
        SerializationService serializationService = nodeEngine.getSerializationService();
        for (CollectionItem collectionItem : map.values()) {
            E value = serializationService.toObject(collectionItem.getValue());
            backupList.add(value);
        }
        return backupList;
    }

    /**
     * Returns the backup instance of an {@link IQueue} by a given queue instance.
     * <p>
     * Note: Returns the backups from the first replica index.
     *
     * @param instances the {@link HecatonInstance} array to gather the data from
     * @param queue     the {@link IQueue} to retrieve the backup from
     * @return a {@link Queue} with the backup items
     */
    public static <E> Queue<E> getBackupQueue(HecatonInstance[] instances, IQueue queue) {
        int partitionId = getPartitionIdViaReflection(queue);
        HecatonInstance backupInstance = getFirstBackupInstance(instances, partitionId);
        return getBackupQueue(backupInstance, queue.getName());
    }

    /**
     * Returns all backup items of an {@link IQueue} by a given queue name.
     * <p>
     * Note: You have to provide the {@link HecatonInstance} you want to retrieve the backups from.
     * Use {@link Accessors#getBackupInstance(HecatonInstance[], int, int)} to retrieve the backup instance for a given replica index.
     *
     * @param backupInstance the {@link HecatonInstance} to retrieve the backups from
     * @param queueName      the queue name
     * @return a {@link Queue} with the backup items
     */
    public static <E> Queue<E> getBackupQueue(HecatonInstance backupInstance, String queueName) {
        Queue<E> backupQueue = new LinkedList<>();

        NodeEngineImpl nodeEngine = getNodeEngineImpl(backupInstance);
        QueueService service = nodeEngine.getService(QueueService.SERVICE_NAME);
        QueueContainer container = service.getExistingContainerOrNull(queueName);
        if (container == null) {
            return backupQueue;
        }

        container.scanBackupItems(queueItem -> {
            E value = nodeEngine.getSerializationService().toObject(queueItem.getSerializedObject());
            backupQueue.add(value);
        });

        return backupQueue;
    }

    /**
     * Returns the backup instance of an {@link ISet} by a given set instance.
     * <p>
     * Note: Returns the backups from the first replica index.
     *
     * @param instances the {@link HecatonInstance} array to gather the data from
     * @param set       the {@link ISet} to retrieve the backup from
     * @return a {@link Set} with the backup items
     */
    public static <E> Set<E> getBackupSet(HecatonInstance[] instances, ISet set) {
        int partitionId = getPartitionIdViaReflection(set);
        HecatonInstance backupInstance = getFirstBackupInstance(instances, partitionId);
        return getBackupSet(backupInstance, set.getName());
    }

    /**
     * Returns all backup items of an {@link ISet} by a given set name.
     * <p>
     * Note: You have to provide the {@link HecatonInstance} you want to retrieve the backups from.
     * Use {@link Accessors#getBackupInstance(HecatonInstance[], int, int)} to retrieve the backup instance for a given replica index.
     *
     * @param backupInstance the {@link HecatonInstance} to retrieve the backups from
     * @param setName        the set name
     * @return a {@link Set} with the backup items
     */
    public static <E> Set<E> getBackupSet(HecatonInstance backupInstance, String setName) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(backupInstance);
        CollectionService service = nodeEngine.getService(SetService.SERVICE_NAME);
        CollectionContainer collectionContainer = service.getContainerMap().get(setName);
        if (collectionContainer == null) {
            return emptySet();
        }
        // the replica items are retrieved via `getMap()`, the primary items via `getCollection()`
        Map<Long, CollectionItem> map = collectionContainer.getMap();

        Set<E> backupSet = new HashSet<>();
        SerializationService serializationService = nodeEngine.getSerializationService();
        for (CollectionItem collectionItem : map.values()) {
            E value = serializationService.toObject(collectionItem.getValue());
            backupSet.add(value);
        }
        return backupSet;
    }
}
