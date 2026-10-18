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

package uk.org.hecaton.map;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.backup.BackupAccessor;
import uk.org.hecaton.test.environment.RuntimeAvailableProcessorsRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.test.backup.TestBackupUtils.assertBackupEntryEqualsEventually;
import static uk.org.hecaton.test.backup.TestBackupUtils.assertBackupEntryNullEventually;
import static uk.org.hecaton.test.backup.TestBackupUtils.newMapAccessor;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapSetTtlBackupTest extends HecatonTestSupport {

    private static final int CLUSTER_SIZE = 3;

    @Parameters(name = "inMemoryFormat:{0}")
    public static Object[] memoryFormat() {
        return new Object[]{
                InMemoryFormat.BINARY,
                InMemoryFormat.OBJECT,
        };
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Rule
    public RuntimeAvailableProcessorsRule runtimeAvailableProcessorsRule = new RuntimeAvailableProcessorsRule(4);

    protected HecatonInstance[] instances;
    private TestHecatonInstanceFactory factory;

    @Before
    public void setup() {
        MapConfig mapConfig = new MapConfig("default")
                .setBackupCount(CLUSTER_SIZE - 1)
                .setInMemoryFormat(inMemoryFormat);

        Config config = getConfig()
                .addMapConfig(mapConfig);

        factory = createHecatonInstanceFactory(CLUSTER_SIZE);
        instances = factory.newInstances(config);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testBackups() {
        String mapName = randomMapName();
        HecatonInstance instance = instances[0];

        putKeys(instance, mapName, 0, 1000, 0, null);
        setTtl(instance, mapName, 0, 1000, 1, TimeUnit.SECONDS);

        sleepAtLeastMillis(1001);
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            assertKeysNotPresent(instances, mapName, 0, 1000);
        }
    }

    @Test
    public void testMakesTempBackupEntriesUnlimited() {
        String mapName = randomMapName();
        HecatonInstance instance = instances[0];

        putKeys(instance, mapName, 0, 1000, 15, TimeUnit.SECONDS);
        setTtl(instance, mapName, 0, 1000, 0, TimeUnit.SECONDS);
        sleepAtLeastSeconds(16);
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            assertKeys(instances, mapName, 0, 1);
        }
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @SuppressWarnings("SameParameterValue")
    private static void putKeys(HecatonInstance instance, String mapName,
                                int from, int to, int ttl, TimeUnit timeUnit) {
        IMap<Integer, Integer> map = instance.getMap(mapName);
        for (int i = from; i < to; i++) {
            if (timeUnit == null) {
                map.put(i, i);
            } else {
                map.put(i, i, ttl, timeUnit);
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void setTtl(HecatonInstance instance, String mapName, int from, int to, long ttl, TimeUnit timeUnit) {
        IMap<Integer, Integer> map = instance.getMap(mapName);
        for (int i = from; i < to; i++) {
            map.setTtl(i, ttl, timeUnit);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void assertKeysNotPresent(HecatonInstance[] instances, String mapName, int from, int to) {
        for (int replicaIndex = 1; replicaIndex < CLUSTER_SIZE; replicaIndex++) {
            BackupAccessor<Integer, Integer> backupAccessor = newMapAccessor(instances, mapName, replicaIndex);
            for (int i = from; i < to; i++) {
                assertBackupEntryNullEventually(i, backupAccessor);
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void assertKeys(HecatonInstance[] instances, String mapName, int from, int to) {
        for (int replicaIndex = 1; replicaIndex < CLUSTER_SIZE; replicaIndex++) {
            BackupAccessor<Integer, Integer> backupAccessor = newMapAccessor(instances, mapName, replicaIndex);
            for (int i = from; i < to; i++) {
                assertBackupEntryEqualsEventually(i, i, backupAccessor);
            }
        }
    }
}
