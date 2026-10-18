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

package uk.org.hecaton.config;

import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NearCachePreloaderConfigTest {

    private NearCachePreloaderConfig config = new NearCachePreloaderConfig();

    @Test
    public void testConstructor_withDirectory() {
        config = new NearCachePreloaderConfig("myParentDirectory");

        assertTrue(config.isEnabled());
        assertEquals("myParentDirectory", config.getDirectory());
    }

    @Test
    public void setDirectory() {
        config.setDirectory("myParentDirectory");

        assertEquals("myParentDirectory", config.getDirectory());
    }

    @Test(expected = NullPointerException.class)
    public void setDirectory_withNull() {
        config.setDirectory(null);
    }

    @Test
    public void setStoreInitialDelaySeconds() {
        config.setStoreInitialDelaySeconds(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setStoreInitialDelaySeconds_withZero() {
        config.setStoreInitialDelaySeconds(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setStoreInitialDelaySeconds_withNegative() {
        config.setStoreInitialDelaySeconds(-1);
    }

    @Test
    public void setStoreIntervalSeconds() {
        config.setStoreIntervalSeconds(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setStoreIntervalSeconds_withZero() {
        config.setStoreIntervalSeconds(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setStoreIntervalSeconds_withNegative() {
        config.setStoreIntervalSeconds(-1);
    }

    @Test
    public void testSerialization() {
        config.setEnabled(true);
        config.setDirectory("myParentDirectory");
        config.setStoreInitialDelaySeconds(23);
        config.setStoreIntervalSeconds(42);

        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(config);
        NearCachePreloaderConfig deserialized = serializationService.toObject(serialized);

        assertEquals(config.isEnabled(), deserialized.isEnabled());
        assertEquals(config.getDirectory(), deserialized.getDirectory());
        assertEquals(config.getStoreInitialDelaySeconds(), deserialized.getStoreInitialDelaySeconds());
        assertEquals(config.getStoreIntervalSeconds(), deserialized.getStoreIntervalSeconds());
        assertEquals(config.toString(), deserialized.toString());
    }
}
