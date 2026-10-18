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

package uk.org.hecaton.jet.config;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JetConfigTest {


    @Test
    public void when_setInstanceConfig_thenReturnsInstanceConfig() {
        // When
        JetConfig config = new JetConfig();
        InstanceConfig instanceConfig = new InstanceConfig();
        config.setInstanceConfig(instanceConfig);

        // Then
        assertEquals(instanceConfig, config.getInstanceConfig());
    }

    @Test
    public void when_setEdgeConfig_thenReturnsEdgeConfig() {
        // When
        JetConfig config = new JetConfig();
        EdgeConfig edgeConfig = new EdgeConfig();
        config.setDefaultEdgeConfig(edgeConfig);

        // Then
        assertEquals(edgeConfig, config.getDefaultEdgeConfig());
    }

    @Test
    public void testJetIsDisabledByDefault() {
        assertFalse(new JetConfig().isEnabled());
        assertFalse(new Config().getJetConfig().isEnabled());
    }
}
