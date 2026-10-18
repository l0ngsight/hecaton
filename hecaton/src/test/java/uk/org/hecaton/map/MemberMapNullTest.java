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

package uk.org.hecaton.map;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

/**
 * Member implementation for basic map methods nullability tests
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberMapNullTest extends AbstractMapNullTest {

    private HecatonInstance instance;

    @Before
    public void setup() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        instance = factory.newHecatonInstance();
    }

    @Override
    protected boolean isNotClient() {
        return true;
    }

    @Override
    protected HecatonInstance getDriver() {
        return instance;
    }

    @Override
    @Test
    public void testNullability() {
        super.testNullability();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void testNullability(Object key, Object value) {
        super.testNullability(key, value);

        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync(key, ""));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync("", value));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync(key, "", -1, TimeUnit.SECONDS));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync("", value, -1, TimeUnit.SECONDS));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync("", "", -1, null));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync(key, "", -1, TimeUnit.SECONDS, -1, TimeUnit.SECONDS));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync("", value, -1, TimeUnit.SECONDS, -1, TimeUnit.SECONDS));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync("", "", -1, null, -1, TimeUnit.SECONDS));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync("", "", -1, TimeUnit.SECONDS, -1, null));
    }

    @Test
    @Ignore("Needs additional validation in member proxy - HZG-446")
    @Override
    public void testNullabilityNullHeapData() {
        super.testNullabilityNullHeapData();
    }

    @Test
    @Ignore("Needs additional validation in member proxy - HZG-446")
    @Override
    public void testNullabilityEmptyHeapData() {
        super.testNullabilityEmptyHeapData();
    }
}
