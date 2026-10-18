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

package uk.org.hecaton.internal.serialization.impl.portable.integration;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.SerializationConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GenericRecordIntegrationTest extends AbstractGenericRecordIntegrationTest {

    private TestHecatonInstanceFactory factory;

    @Before
    public void before() {
        factory = createHecatonInstanceFactory();
    }

    @After
    public void after() {
        factory.terminateAll();
    }


    @Override
    protected HecatonInstance createAccessorInstance(SerializationConfig serializationConfig) {
        Config config = new Config();
        config.setLiteMember(true);
        config.setSerializationConfig(serializationConfig);
        return factory.newHecatonInstance(config);
    }

    @Override
    protected HecatonInstance[] createCluster() {
        return factory.newInstances(new Config(), 2);
    }
}
