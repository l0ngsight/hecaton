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

package uk.org.hecaton.client.map;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.IndeterminateOperationStateException;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.impl.SpiDataSerializerHook;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.spi.properties.ClusterProperty.FAIL_ON_INDETERMINATE_OPERATION_STATE;
import static uk.org.hecaton.spi.properties.ClusterProperty.OPERATION_BACKUP_TIMEOUT_MILLIS;
import static uk.org.hecaton.test.PacketFiltersUtil.dropOperationsBetween;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientIndeterminateOperationStateExceptionTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    private HecatonInstance instance1;

    private HecatonInstance instance2;

    private HecatonInstance client;

    @Before
    public void init() {
        Config config = new Config();
        config.setProperty(OPERATION_BACKUP_TIMEOUT_MILLIS.getName(), String.valueOf(1000));
        config.setProperty(FAIL_ON_INDETERMINATE_OPERATION_STATE.getName(), String.valueOf(true));

        instance1 = factory.newHecatonInstance(config);
        instance2 = factory.newHecatonInstance(config);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.FAIL_ON_INDETERMINATE_OPERATION_STATE.getName(), String.valueOf(true));
        client = factory.newHecatonClient(clientConfig);

        warmUpPartitions(instance1, instance2);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void shouldFail_whenBackupAckMissed() {
        dropOperationsBetween(instance1, instance2, SpiDataSerializerHook.F_ID, singletonList(SpiDataSerializerHook.BACKUP));

        String key = generateKeyOwnedBy(instance1);

        IMap<Object, Object> map = client.getMap(randomMapName());
        assertThatThrownBy(() -> map.put(key, key)).isInstanceOf(IndeterminateOperationStateException.class);

        assertEquals(key, map.get(key));
    }
}
