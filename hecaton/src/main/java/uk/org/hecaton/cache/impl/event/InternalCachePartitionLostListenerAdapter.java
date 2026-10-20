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

package uk.org.hecaton.cache.impl.event;

import uk.org.hecaton.cache.CacheEventType;
import uk.org.hecaton.cache.impl.CacheEventListener;
import uk.org.hecaton.cache.impl.CachePartitionEventData;
import uk.org.hecaton.spi.annotation.PrivateApi;

@PrivateApi
public class InternalCachePartitionLostListenerAdapter implements CacheEventListener {

    private final CachePartitionLostListener partitionLostListener;

    public InternalCachePartitionLostListenerAdapter(CachePartitionLostListener partitionLostListener) {
        this.partitionLostListener = partitionLostListener;
    }

    @Override
    public void handleEvent(Object eventObject) {
        final CachePartitionEventData eventData = (CachePartitionEventData) eventObject;
        final CachePartitionLostEvent event = new CachePartitionLostEvent(eventData.getName(),
                eventData.getMember(), CacheEventType.PARTITION_LOST.getType(),
                    eventData.getPartitionId());
        partitionLostListener.partitionLost(event);
    }
}
