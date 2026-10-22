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

package uk.org.hecaton.internal.nio.ssl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.SSLConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** TLS checks made when a member with a real network starts (so not in a parallel JVM). */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class SSLMemberStartTest {

    @Test
    public void memberWithoutKeyStoreDoesNotStart() {
        Config config = new Config();
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true));

        assertThatThrownBy(() -> Hecaton.newHecatonInstance(config))
                .hasStackTraceContaining("TLS on a member needs a certificate");
    }
}
