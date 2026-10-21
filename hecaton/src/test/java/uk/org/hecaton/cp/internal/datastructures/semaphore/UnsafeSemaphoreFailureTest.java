/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.cp.internal.datastructures.semaphore;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.cp.SemaphoreConfig;
import uk.org.hecaton.core.HecatonInstance;

public abstract class UnsafeSemaphoreFailureTest extends AbstractSemaphoreFailureTest {

    @Override
    protected HecatonInstance getPrimaryInstance() {
        return instances[0];
    }

    @Override
    protected HecatonInstance getProxyInstance() {
        return instances[1];
    }

    @Override
    protected String getProxyName() {
        warmUpPartitions(instances);
        String group = generateKeyOwnedBy(primaryInstance);
        return objectName + "@" + group;
    }

    @Override
    protected HecatonInstance[] createInstances() {
        Config config = new Config();
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig(objectName, isJDKCompatible(), 0);
        config.getCPSubsystemConfig().addSemaphoreConfig(semaphoreConfig);
        return factory.newInstances(config, 2);
    }
}
