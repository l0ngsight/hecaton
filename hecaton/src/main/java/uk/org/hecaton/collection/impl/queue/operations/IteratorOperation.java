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

package uk.org.hecaton.collection.impl.queue.operations;

import uk.org.hecaton.collection.impl.queue.QueueContainer;
import uk.org.hecaton.collection.impl.queue.QueueDataSerializerHook;
import uk.org.hecaton.internal.monitor.impl.LocalQueueStatsImpl;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;
import uk.org.hecaton.spi.impl.SerializableList;

import java.util.List;

/**
 * Provides iterator functionality for Queue.
 */
public class IteratorOperation extends QueueOperation implements ReadonlyOperation {

    public IteratorOperation() {
    }

    public IteratorOperation(String name) {
        super(name);
    }

    @Override
    public void run() {
        QueueContainer queueContainer = getContainer();
        List<Data> dataList = queueContainer.getAsDataList();
        response = new SerializableList(dataList);
    }

    @Override
    public void afterRun() throws Exception {
        LocalQueueStatsImpl stats = getQueueService().getLocalQueueStatsImpl(name);
        stats.incrementOtherOperations();
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.ITERATOR;
    }
}
