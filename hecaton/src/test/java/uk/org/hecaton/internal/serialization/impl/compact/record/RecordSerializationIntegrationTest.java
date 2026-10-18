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

package uk.org.hecaton.internal.serialization.impl.compact.record;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.CompactSerializationConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class RecordSerializationIntegrationTest extends HecatonTestSupport {

    protected final TestHecatonFactory factory = new TestHecatonFactory();

    @Before
    public abstract void setUp();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    public abstract HecatonInstance getDriver();

    public abstract HecatonInstance getDriverWithConfig(CompactSerializationConfig compactSerializationConfig);

    @Test
    public void shouldSerializeAndDeserializeRecord() {
        HecatonInstance instance = getDriver();

        AllTypesRecord allTypesRecord = AllTypesRecord.create();
        IMap<Integer, AllTypesRecord> map = instance.getMap(randomMapName());

        map.put(1, allTypesRecord);

        AllTypesRecord loaded = map.get(1);
        assertThat(loaded).isEqualTo(allTypesRecord);
    }

    @Test
    public void shouldSerializeAndDeserializeRecordWhenRegisteredExplicitly() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig
                .addClass(AllTypesRecord.class);

        HecatonInstance instance = getDriverWithConfig(compactSerializationConfig);

        AllTypesRecord allTypesRecord = AllTypesRecord.create();
        IMap<Integer, AllTypesRecord> map = instance.getMap(randomMapName());

        map.put(1, allTypesRecord);

        AllTypesRecord loaded = map.get(1);
        assertThat(loaded).isEqualTo(allTypesRecord);
    }
}
