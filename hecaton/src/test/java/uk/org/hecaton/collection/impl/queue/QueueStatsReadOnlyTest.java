/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.collection.impl.queue;

import uk.org.hecaton.collection.impl.queue.model.VersionedObject;
import uk.org.hecaton.collection.impl.queue.model.VersionedObjectComparator;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;

/**
 * Queue statistics are read outside the partition thread (metrics collection,
 * {@code getLocalQueueStats()}). Reading them must not move the items between the container's
 * primary queue and its backup map, which only the partition thread may do: done concurrently,
 * it can lose or duplicate items (a promoted priority queue was seen with an extra item).
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueStatsReadOnlyTest extends HecatonTestSupport {

    @Test
    public void statsOfABackupReplicaOnTheOwnerDoNotTouchItems() {
        String name = "priority";
        Config config = smallInstanceConfig();
        config.getQueueConfig(name).setPriorityComparatorClassName(VersionedObjectComparator.class.getName());
        HecatonInstance instance = createHecatonInstance(config);
        warmUpPartitions(instance);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        QueueService service = nodeEngine.getService(QueueService.SERVICE_NAME);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(name);

        // backup items on the member that now owns the partition, as right after a promotion
        QueueContainer container = service.getOrCreateContainer(name, true);
        for (int i = 0; i < 3; i++) {
            container.offerBackup(nodeEngine.toData(new VersionedObject<>(i, i)), i);
        }
        QueueContainer spied = spy(container);
        service.addContainer(name, spied);

        assertThat(service.createLocalQueueStats(name, partitionId).getOwnedItemCount()).isEqualTo(3);
        verify(spied, never()).getItemQueue();
        verify(spied, never()).getBackupMap();
    }

    @Test
    public void statsOfThePrimaryOnAMemberThatLostThePartitionDoNotTouchItems() {
        String name = "plain";
        HecatonInstance[] instances = createHecatonInstanceFactory(2).newInstances(smallInstanceConfig());
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instances[0]);
        QueueService service = nodeEngine.getService(QueueService.SERVICE_NAME);
        String queueName = randomNameOwnedBy(instances[1]);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(queueName);

        // primary items on a member that is now only a backup
        QueueContainer container = service.getOrCreateContainer(queueName, false);
        for (int i = 0; i < 3; i++) {
            container.offer(nodeEngine.toData(i));
        }
        QueueContainer spied = spy(container);
        service.addContainer(queueName, spied);

        assertThat(service.createLocalQueueStats(queueName, partitionId).getBackupItemCount()).isEqualTo(3);
        verify(spied, never()).getItemQueue();
        verify(spied, never()).getBackupMap();
    }
}
