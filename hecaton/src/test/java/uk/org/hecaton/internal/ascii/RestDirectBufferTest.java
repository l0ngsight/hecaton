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

package uk.org.hecaton.internal.ascii;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.spi.properties.ClusterProperty.SOCKET_BUFFER_DIRECT;

@RunWith(HecatonParallelClassRunner.class)
@Category(QuickTest.class)
public class RestDirectBufferTest extends RestTest {

    @Override
    public Config getConfig() {
        Config config = super.getConfig();
        config.setProperty(SOCKET_BUFFER_DIRECT.getName(), "true");
        return config;
    }
}
