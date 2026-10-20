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

package uk.org.hecaton.internal.namespace.impl;

import uk.org.hecaton.internal.util.HecatonFeature;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.UserCodeNamespaceConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static uk.org.hecaton.test.HecatonTestSupport.randomString;
import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfigWithoutJetAndMetrics;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
public class UserCodeNamespaceServiceLicenseTest {

    @Test
    public void testNamespaceServiceOnlyAvailableFromEE() {
        try {
            Config config = smallInstanceConfigWithoutJetAndMetrics();
            config.getNamespacesConfig().setEnabled(true);
            config.getNamespacesConfig().addNamespaceConfig(new UserCodeNamespaceConfig(randomString()));
            Hecaton.newHecatonInstance(config);
            throw new AssertionError("NamespaceService was created from OS Edition");
        } catch (IllegalStateException ex) {
            assertTrue("NamespaceService creation does not fail with expected IllegalStateException from OS",
                    ex.getMessage().contains(HecatonFeature.USER_CODE_NAMESPACES.notAvailableMessage()));
        }
    }
}
