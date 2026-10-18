/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.jet.elastic;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.impl.util.Util;
import uk.org.hecaton.jet.test.IgnoreInJenkinsOnWindows;
import uk.org.hecaton.jet.test.SerialTest;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.AfterClass;
import org.junit.experimental.categories.Category;

import java.util.function.Supplier;

/**
 * Test running 3 local Jet members in a cluster and Elastic in docker
 */
@Category({NightlyTest.class, SerialTest.class, IgnoreInJenkinsOnWindows.class})
public class LocalClusterElasticSinksTest extends CommonElasticSinksTest {

    private static HecatonInstance[] instances;

    // Cluster startup takes >1s, reusing the cluster between tests
    private static Supplier<HecatonInstance> hzSupplier = Util.memoize(() -> {
        TestHecatonFactory factory = new TestHecatonFactory();
        instances = factory.newInstances(config(), 3);
        return instances[0];
    });


    @AfterClass
    public static void afterClass() {
        if (instances != null) {
            for (HecatonInstance instance : instances) {
                instance.shutdown();
            }
            instances = null;
        }
    }

    @Override
    protected HecatonInstance createHecatonInstance() {
        return hzSupplier.get();
    }
}
