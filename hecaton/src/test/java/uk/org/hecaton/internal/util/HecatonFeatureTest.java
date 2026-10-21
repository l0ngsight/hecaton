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

package uk.org.hecaton.internal.util;

import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonFeatureTest {

    @Test
    public void plannedFeatureNamesItsMilestone() {
        assertThat(HecatonFeature.TLS.notAvailableMessage())
                .isEqualTo("TLS/SSL (F1) is not available in Hecaton yet; it is planned for milestone M3"
                        + " (see the implementation plan).");
    }

    @Test
    public void unsupportedFeatureNamesItsAlternative() {
        assertThat(HecatonFeature.SYMMETRIC_ENCRYPTION.notAvailableMessage())
                .isEqualTo("Symmetric encryption (F6) is not supported by Hecaton; use TLS/SSL (F1) instead.");
        assertThat(HecatonFeature.THREAD_PER_CORE.notAvailableMessage())
                .isEqualTo("Thread-per-core (TPC) is not supported by Hecaton.");
    }

    @Test
    public void moduleFeatureNamesItsArtifact() {
        assertThat(HecatonFeature.HIGH_DENSITY_MEMORY.isAvailable()).isFalse();
        assertThat(HecatonFeature.HIGH_DENSITY_MEMORY.notAvailableMessage())
                .isEqualTo("High-Density memory (NATIVE in-memory format) (F11) requires uk.org.hecaton:hecaton-hd"
                        + " on the member's classpath.");
    }

    @Test
    public void noMessageMentionsAnEnterpriseEdition() {
        for (HecatonFeature feature : HecatonFeature.values()) {
            assertThat(feature.notAvailableMessage()).doesNotContainIgnoringCase("enterprise");
        }
    }
}
