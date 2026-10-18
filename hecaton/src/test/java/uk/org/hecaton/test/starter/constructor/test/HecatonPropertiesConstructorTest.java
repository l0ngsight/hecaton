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

package uk.org.hecaton.test.starter.constructor.test;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.starter.constructor.HecatonPropertiesConstructor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonPropertiesConstructorTest {

    @Test
    public void testConstructor() {
        Config config = new Config()
                .setProperty("myProperty", "myValue");
        HecatonProperties properties = new HecatonProperties(config);

        HecatonPropertiesConstructor constructor = new HecatonPropertiesConstructor(HecatonProperties.class);
        HecatonProperties clonedProperties = (HecatonProperties) constructor.createNew(properties);

        assertEquals(properties.get("myProperty"), clonedProperties.get("myProperty"));
        assertEquals(properties.get("invalid"), clonedProperties.get("invalid"));
    }
}
