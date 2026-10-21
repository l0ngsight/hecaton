/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.map.tiered;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.TieredStoreConfig;
import uk.org.hecaton.internal.util.HecatonFeature;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Without the {@code hecaton-tiered} module, maps with a tiered store are refused with a clear message. */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TieredStoreNotAvailableTest extends HecatonTestSupport {

    @Test
    public void refusedWithoutTheModule() {
        assertThat(HecatonFeature.TIERED_STORE.isAvailable()).isFalse();
        Config config = smallInstanceConfig().addMapConfig(new MapConfig("tiered").setInMemoryFormat(InMemoryFormat.NATIVE)
                .setTieredStoreConfig(new TieredStoreConfig().setEnabled(true)));
        assertThatThrownBy(() -> createHecatonInstance(config).getMap("tiered").put(1, 1))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessage(HecatonFeature.TIERED_STORE.notAvailableMessage());
        assertThat(HecatonFeature.TIERED_STORE.notAvailableMessage()).contains("hecaton-tiered");
    }
}
