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

package uk.org.hecaton.map.impl.journal;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.journal.AbstractEventJournalExpiringTest;
import uk.org.hecaton.journal.EventJournalTestContext;
import uk.org.hecaton.map.EventJournalMapEvent;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.ringbuffer.impl.RingbufferContainer;
import uk.org.hecaton.ringbuffer.impl.RingbufferService;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.jet.impl.util.Util.getNodeEngine;

@RunWith(HecatonSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class MapEventJournalExpiringTest<K, V> extends AbstractEventJournalExpiringTest<EventJournalMapEvent> {

    private static final String MAP_NAME = "mappy";

    @Override
    protected Config getConfig() {
        Config defConfig = super.getConfig();
        defConfig.getMapConfig(MAP_NAME)
                .setBackupCount(1)
                .setInMemoryFormat(getInMemoryFormat())
                .setEventJournalConfig(getEventJournalConfig());
        return defConfig;
    }

    protected InMemoryFormat getInMemoryFormat() {
        return MapConfig.DEFAULT_IN_MEMORY_FORMAT;
    }

    @Override
    protected EventJournalTestContext<K, V, EventJournalMapEvent<K, V>> createContext() {
        return new EventJournalTestContext<>(
                new EventJournalMapDataStructureAdapter<>(getRandomInstance().getMap(MAP_NAME)),
                null,
                new EventJournalMapEventAdapter<>()
        );
    }

    protected String getName() {
        return MAP_NAME;
    }

    protected  RingbufferContainer<?, ?> getRingBufferContainer(String name, int partitionId, HecatonInstance instance) {
        var serviceName = RingbufferService.SERVICE_NAME;
        final RingbufferService service = getNodeEngine(instance).getService(serviceName);
        final ObjectNamespace ns = MapService.getObjectNamespace(name);

        RingbufferContainer<?, ?> ringbuffer = service.getContainerOrNull(partitionId, ns);
        if (ringbuffer == null) {
            throw new IllegalStateException("No ringbuffer container for partition " + partitionId);
        }
        return ringbuffer;
    }
}
