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

package uk.org.hecaton.query.impl.getters.policy;

import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static uk.org.hecaton.query.impl.getters.policy.ReflectiveAttributeLookupPolicy.REFLECTIVE_ATTRIBUTE_LOOKUP_POLICY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReflectiveAttributeLookupPolicySelectionTest {

    @Test
    public void getPolicy_whenPropertyNotConfigured_thenReturnsControlledAccessPolicy() {
        ReflectiveAttributeLookupPolicy policy = getPolicy(new Properties());
        assertThat(policy).isSameAs(DefaultReflectiveAttributeLookupPolicy.INSTANCE);
    }

    @Test
    public void getPolicy_whenConfiguredAsFullAccess_thenReturnsFullAccessPolicy() {
        ReflectiveAttributeLookupPolicy policy = getPolicy("full-access");
        assertThat(policy).isSameAs(FullAccessReflectiveAttributeLookupPolicy.INSTANCE);
    }

    @Test
    public void getPolicy_whenConfiguredAsControlledAccess_thenReturnsControlledAccessPolicy() {
        ReflectiveAttributeLookupPolicy policy = getPolicy("controlled-access");
        assertThat(policy).isSameAs(DefaultReflectiveAttributeLookupPolicy.INSTANCE);
    }

    @Test
    public void getPolicy_whenConfiguredAsPublicFieldsOnly_thenReturnsPublicFieldsOnlyPolicy() {
        ReflectiveAttributeLookupPolicy policy = getPolicy("public-fields-only");
        assertThat(policy).isSameAs(PublicFieldReflectiveAttributeLookupPolicy.INSTANCE);
    }

    @Test
    public void getPolicy_whenConfiguredWithDifferentCase_thenResolvesPolicy() {
        ReflectiveAttributeLookupPolicy policy = getPolicy("FULL-ACCESS");
        assertThat(policy).isSameAs(FullAccessReflectiveAttributeLookupPolicy.INSTANCE);
    }

    @Test
    public void getPolicy_whenConfiguredWithUnknownValue_thenThrows() {
        assertThatThrownBy(() -> getPolicy("unknown-policy"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown reflective lookup policy name: unknown-policy");
    }

    private ReflectiveAttributeLookupPolicy getPolicy(String policyName) {
        Properties properties = new Properties();
        properties.setProperty(REFLECTIVE_ATTRIBUTE_LOOKUP_POLICY.getName(), policyName);
        return getPolicy(properties);
    }

    private ReflectiveAttributeLookupPolicy getPolicy(Properties properties) {
        return ReflectiveAttributeLookupPolicy.getPolicy(new HecatonProperties(properties));
    }
}
