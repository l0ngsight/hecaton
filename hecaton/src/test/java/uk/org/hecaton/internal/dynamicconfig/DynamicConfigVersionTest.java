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

package uk.org.hecaton.internal.dynamicconfig;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DeviceConfig;
import uk.org.hecaton.config.DynamicConfigurationConfig;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.SplitBrainProtectionConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import static uk.org.hecaton.internal.dynamicconfig.ClusterWideConfigurationService.CONFIG_TO_VERSION;
import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigVersionTest {

    // config classes not supported by dynamic data structure config
    private static final Set<Class<?>> NON_DYNAMIC_CONFIG_CLASSES;

    static {
        Set<Class<?>> nonDynamicConfigClasses = new HashSet<>();
        nonDynamicConfigClasses.add(WanReplicationConfig.class);
        nonDynamicConfigClasses.add(SplitBrainProtectionConfig.class);
        nonDynamicConfigClasses.add(ListenerConfig.class);
        nonDynamicConfigClasses.add(DeviceConfig.class);
        nonDynamicConfigClasses.add(DynamicConfigurationConfig.class);
        NON_DYNAMIC_CONFIG_CLASSES = nonDynamicConfigClasses;
    }

    @Test
    public void test_allConfigClasses_areAssignedToVersion() {
        Class<Config> topLevelConfigClass = Config.class;
        Method[] allConfigMethods = topLevelConfigClass.getDeclaredMethods();
        for (Method method : allConfigMethods) {
            String methodName = method.getName();
            if (methodName.startsWith("add") && methodName.endsWith("Config")) {
                assert method.getParameterCount() == 1;
                Class klass = method.getParameterTypes()[0];
                boolean isMappedToVersion = CONFIG_TO_VERSION.get(klass) != null
                        || NON_DYNAMIC_CONFIG_CLASSES.contains(klass);
                assertTrue(format("Config class %s does not have a minimum Version set", klass.getName()), isMappedToVersion);
            }
        }
    }
}
