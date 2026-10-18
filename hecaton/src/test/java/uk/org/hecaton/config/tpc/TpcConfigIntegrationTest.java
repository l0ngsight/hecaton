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

package uk.org.hecaton.config.tpc;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.OverridePropertyRule;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.internal.tpc.TpcServerBootstrap.TPC_ENABLED;
import static uk.org.hecaton.test.OverridePropertyRule.clear;
import static org.junit.Assert.assertThrows;


@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class TpcConfigIntegrationTest {

    @Rule
    public final OverridePropertyRule ruleSysPropTpcEnabled = clear(TPC_ENABLED.getName());

    @Test
    public void whenTpcEnabled_fromSystemProperty() {
        System.setProperty(TPC_ENABLED.getName(), "true");

        assertThrows(IllegalStateException.class, Hecaton::newHecatonInstance);
    }

    @Test
    public void whenTpcEnabled_fromConfig() {
        Config config = new Config();
        config.getTpcConfig().setEnabled(true);

        assertThrows(IllegalStateException.class, () -> Hecaton.newHecatonInstance(config));
    }
}
