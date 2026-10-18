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

package uk.org.hecaton.internal.nio.ascii;

import uk.org.hecaton.config.RestEndpointGroup;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

/**
 * Tests if HTTP REST URLs are protected by the correct REST endpoint groups.
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category(SlowTest.class)
public class HttpRestEndpointGroupsTest extends RestApiConfigTestBase {

    @Parameter
    public RestEndpointGroup restEndpointGroup;

    @Parameters(name = "restEndpointGroup:{0}")
    public static RestEndpointGroup[] parameters() {
        return RestEndpointGroup.values();
    }

    @Test
    public void testGroupEnabled() throws Exception {
        HecatonInstance hz = factory.newHecatonInstance(createConfigWithEnabledGroups(restEndpointGroup));
        for (TestUrl testUrl : TEST_URLS) {
            if (restEndpointGroup.getCode() == testUrl.restEndpointGroup.getCode()) {
                assertTextProtocolResponse(hz, testUrl);
            }
        }
    }

    @Test
    public void testGroupDisabled() throws Exception {
        HecatonInstance hz = factory.newHecatonInstance(createConfigWithDisabledGroups(restEndpointGroup));
        for (TestUrl testUrl : TEST_URLS) {
            if (restEndpointGroup.getCode() == testUrl.restEndpointGroup.getCode()) {
                assertNoTextProtocolResponse(hz, testUrl);
            }
        }
    }

    @Test
    public void testOthersWhenGroupEnabled() throws Exception {
        HecatonInstance hz = factory.newHecatonInstance(createConfigWithEnabledGroups(restEndpointGroup));
        for (TestUrl testUrl : TEST_URLS) {
            if (restEndpointGroup.getCode() != testUrl.restEndpointGroup.getCode()) {
                assertNoTextProtocolResponse(hz, testUrl);
            }
        }
    }

    @Test
    public void testOthersWhenGroupDisabled() throws Exception {
        HecatonInstance hz = factory.newHecatonInstance(createConfigWithDisabledGroups(restEndpointGroup));
        for (TestUrl testUrl : TEST_URLS) {
            if (restEndpointGroup.getCode() != testUrl.restEndpointGroup.getCode()) {
                assertTextProtocolResponse(hz, testUrl);
            }
        }
    }
}
