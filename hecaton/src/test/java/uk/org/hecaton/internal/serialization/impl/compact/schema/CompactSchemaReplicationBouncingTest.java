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

package uk.org.hecaton.internal.serialization.impl.compact.schema;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.TestUtil;
import uk.org.hecaton.internal.serialization.impl.compact.Schema;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecord;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecordBuilder;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.SlowTest;
import uk.org.hecaton.test.bounce.BounceMemberRule;
import uk.org.hecaton.test.bounce.BounceTestConfiguration;
import uk.org.hecaton.test.bounce.DriverFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class CompactSchemaReplicationBouncingTest extends HecatonTestSupport {

    private static final int DURATION_SECONDS = 30;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
            .clusterSize(4)
            .driverCount(1)
            .useTerminate(true)
            .driverType(getDriverType())
            .driverFactory(getDriverFactory())
            .build();

    @Test
    public void testSchemaReplicatedSuccessfully() {
        HecatonInstance driver = bounceMemberRule.getNextTestDriver();
        NewSchemaRegisterer runnable = new NewSchemaRegisterer(driver);
        bounceMemberRule.testRepeatedly(new Runnable[]{runnable}, DURATION_SECONDS);

        int registeredSchemaCount = runnable.getCounterValue();
        HecatonInstance steadyMember = bounceMemberRule.getSteadyMember();
        Collection<Schema> schemas = TestUtil.getNode(steadyMember).getSchemaService().getAllSchemas();
        assertEquals(registeredSchemaCount, schemas.size());
    }

    protected DriverFactory getDriverFactory() {
        return null;
    }

    protected BounceTestConfiguration.DriverType getDriverType() {
        return BounceTestConfiguration.DriverType.MEMBER;
    }

    public Config getConfig() {
        return smallInstanceConfig();
    }

    static class NewSchemaRegisterer implements Runnable {

        private final AtomicInteger counter = new AtomicInteger();
        private final HecatonInstance driver;

        NewSchemaRegisterer(HecatonInstance driver) {
            this.driver = driver;
        }

        @Override
        public void run() {
            int value = counter.getAndIncrement();

            // Each compact object will have a different schema
            // due to different field names
            GenericRecord record = GenericRecordBuilder.compact("foo")
                    .setInt32(Integer.toString(value), value)
                    .build();

            driver.getMap("foo").set(value, record);
        }

        public int getCounterValue() {
            return counter.get();
        }
    }
}
