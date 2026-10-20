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

package uk.org.hecaton.map.impl.querycache.subscriber;

import uk.org.hecaton.map.IMapEvent;
import uk.org.hecaton.map.impl.ListenerAdapter;
import uk.org.hecaton.map.impl.querycache.QueryCacheContext;
import uk.org.hecaton.map.impl.querycache.accumulator.Accumulator;
import uk.org.hecaton.map.impl.querycache.accumulator.AccumulatorInfo;
import uk.org.hecaton.map.impl.querycache.event.BatchEventData;
import uk.org.hecaton.map.impl.querycache.event.BatchIMapEvent;
import uk.org.hecaton.map.impl.querycache.event.QueryCacheEventData;
import uk.org.hecaton.map.impl.querycache.event.SingleIMapEvent;
import uk.org.hecaton.internal.serialization.SerializationService;

import java.util.Collection;

/**
 * Subscriber side listener per {@code QueryCache} which listens events sent by publisher-sides.
 */
public class SubscriberListener implements ListenerAdapter<IMapEvent> {

    private final AccumulatorInfo info;
    private final Accumulator accumulator;
    private final SubscriberContext subscriberContext;
    private final SerializationService serializationService;

    public SubscriberListener(QueryCacheContext context, AccumulatorInfo info) {
        this.info = info;
        this.subscriberContext = context.getSubscriberContext();
        this.accumulator = createAccumulator();
        this.serializationService = context.getSerializationService();
    }

    @Override
    public void onEvent(IMapEvent iMapEvent) {
        if (iMapEvent instanceof SingleIMapEvent event) {
            QueryCacheEventData eventData = event.getEventData();
            eventData.setSerializationService(serializationService);
            accumulator.accumulate(eventData);
            return;
        }

        if (iMapEvent instanceof BatchIMapEvent batchIMapEvent) {
            BatchEventData batchEventData = batchIMapEvent.getBatchEventData();
            Collection<QueryCacheEventData> events = batchEventData.getEvents();
            for (QueryCacheEventData eventData : events) {
                eventData.setSerializationService(serializationService);
                accumulator.accumulate(eventData);
            }
            return;
        }
    }

    private Accumulator createAccumulator() {
        MapSubscriberRegistry mapSubscriberRegistry = subscriberContext.getMapSubscriberRegistry();
        SubscriberRegistry subscriberRegistry = mapSubscriberRegistry.getOrCreate(info.getMapName());
        return subscriberRegistry.getOrCreate(info.getCacheId());
    }
}
