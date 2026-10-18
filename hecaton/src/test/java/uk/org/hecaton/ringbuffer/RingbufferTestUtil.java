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

package uk.org.hecaton.ringbuffer;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.TestUtil;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.ringbuffer.impl.RingbufferContainer;
import uk.org.hecaton.ringbuffer.impl.RingbufferService;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.Accessors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static uk.org.hecaton.ringbuffer.impl.RingbufferService.getRingbufferNamespace;
import static uk.org.hecaton.test.Accessors.getFirstBackupInstance;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getPartitionIdViaReflection;
import static java.util.Collections.emptyList;

final class RingbufferTestUtil {

    private RingbufferTestUtil() {
    }

    /**
     * Returns all backup items of a {@link Ringbuffer} by a given ringbuffer instance.
     * <p>
     * Note: Returns the backups from the first replica index.
     *
     * @param instances  the {@link HecatonInstance} array to gather the data from
     * @param ringbuffer the {@link Ringbuffer} to retrieve the backups from
     * @return a {@link Collection} with the backup items
     */
    static Collection<Object> getBackupRingbuffer(HecatonInstance[] instances, Ringbuffer ringbuffer) {
        int partitionId = getPartitionIdViaReflection(ringbuffer);
        HecatonInstance backupInstance = getFirstBackupInstance(instances, partitionId);
        return getBackupRingbuffer(backupInstance, partitionId, ringbuffer.getName());
    }

    /**
     * Returns all backup items of a {@link Ringbuffer} by a given ringbuffer name.
     * <p>
     * Note: You have to provide the {@link HecatonInstance} you want to retrieve the backups from.
     * Use {@link Accessors#getBackupInstance(HecatonInstance[], int, int)} to retrieve the backup instance for a given replica index.
     *
     * @param backupInstance the {@link HecatonInstance} to retrieve the backups from
     * @param partitionId    the partition ID of the ringbuffer
     * @param ringbufferName the ringbuffer name
     * @return a {@link Collection} with the backup items
     */
    static Collection<Object> getBackupRingbuffer(HecatonInstance backupInstance, int partitionId, String ringbufferName) {
        // Backup may be owned by instance with previous version, need to setup namespaces for it as well.
        // It does not really matter which particular NodeEngine is used as long as it is from the same Hecaton version.
        TestUtil.setupNamespacesForCurrentThread(backupInstance);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(backupInstance);
        RingbufferService service = nodeEngine.getService(RingbufferService.SERVICE_NAME);
        RingbufferContainer container = service.getContainerOrNull(partitionId, getRingbufferNamespace(ringbufferName));
        if (container == null) {
            return emptyList();
        }

        SerializationService serializationService = nodeEngine.getSerializationService();
        List<Object> backupRingbuffer = new ArrayList<>((int) container.size());
        for (long sequence = container.headSequence(); sequence <= container.tailSequence(); sequence++) {
            backupRingbuffer.add(serializationService.toObject(container.readAsData(sequence)));
        }
        return backupRingbuffer;
    }
}
