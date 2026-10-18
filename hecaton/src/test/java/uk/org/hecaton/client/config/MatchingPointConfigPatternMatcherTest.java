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

package uk.org.hecaton.client.config;

import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.config.matcher.MatchingPointConfigPatternMatcher;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.HecatonTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MatchingPointConfigPatternMatcherTest {

    @Test
    public void testNearCacheConfigWithoutWildcard() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig().setName("someNearCache");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig);

        assertEquals(nearCacheConfig, config.getNearCacheConfig("someNearCache"));

        // non-matching name
        assertNotEquals(nearCacheConfig, config.getNearCacheConfig("doesNotExist"));
        // non-matching case
        assertNotEquals(nearCacheConfig, config.getNearCacheConfig("SomeNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcard1() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig().setName("*hecaton.test.myNearCache");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig);

        assertEquals(nearCacheConfig, config.getNearCacheConfig("uk.org.hecaton.test.myNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcard2() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig().setName("uk.org.hecaton.*.myNearCache");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig);

        assertEquals(nearCacheConfig, config.getNearCacheConfig("uk.org.hecaton.test.myNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcard3() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig().setName("uk.org.hecaton.test.*");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig);

        assertEquals(nearCacheConfig, config.getNearCacheConfig("uk.org.hecaton.test.myNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcardMultipleConfigs() {
        NearCacheConfig nearCacheConfig1 = new NearCacheConfig().setName("uk.org.hecaton.*");
        NearCacheConfig nearCacheConfig2 = new NearCacheConfig().setName("uk.org.hecaton.test.*");
        NearCacheConfig nearCacheConfig3 = new NearCacheConfig().setName("uk.org.hecaton.test.sub.*");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig1);
        config.addNearCacheConfig(nearCacheConfig2);
        config.addNearCacheConfig(nearCacheConfig3);

        // we should get the best matching result
        assertEquals(nearCacheConfig1, config.getNearCacheConfig("uk.org.hecaton.myNearCache"));
        assertEquals(nearCacheConfig2, config.getNearCacheConfig("uk.org.hecaton.test.myNearCache"));
        assertEquals(nearCacheConfig3, config.getNearCacheConfig("uk.org.hecaton.test.sub.myNearCache"));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testMapConfigWildcardMultipleAmbiguousConfigs() {
        NearCacheConfig nearCacheConfig1 = new NearCacheConfig().setName("uk.org.hecaton*");
        NearCacheConfig nearCacheConfig2 = new NearCacheConfig().setName("*uk.org.hecaton");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig1);
        config.addNearCacheConfig(nearCacheConfig2);

        config.getNearCacheConfig("uk.org.hecaton");
    }

    @Test
    public void testNearCacheConfigWildcardMatchingPointStartsWith() {
        NearCacheConfig nearCacheConfig1 = new NearCacheConfig().setName("hecaton.*");
        NearCacheConfig nearCacheConfig2 = new NearCacheConfig().setName("hecaton.test.*");
        NearCacheConfig nearCacheConfig3 = new NearCacheConfig().setName("hecaton.test.sub.*");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig1);
        config.addNearCacheConfig(nearCacheConfig2);
        config.addNearCacheConfig(nearCacheConfig3);

        // we should not match any of the configs (startsWith)
        assertNull(config.getNearCacheConfig("uk.org.hecaton.myNearCache"));
        assertNull(config.getNearCacheConfig("uk.org.hecaton.test.myNearCache"));
        assertNull(config.getNearCacheConfig("uk.org.hecaton.test.sub.myNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcardMatchingPointEndsWith() {
        NearCacheConfig nearCacheConfig1 = new NearCacheConfig().setName("*.sub");
        NearCacheConfig nearCacheConfig2 = new NearCacheConfig().setName("*.test.sub");
        NearCacheConfig nearCacheConfig3 = new NearCacheConfig().setName("*.hecaton.test.sub");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig1);
        config.addNearCacheConfig(nearCacheConfig2);
        config.addNearCacheConfig(nearCacheConfig3);

        // we should not match any of the configs (endsWith)
        assertNull(config.getNearCacheConfig("com.hazelFast.Fast.sub.myNearCache"));
        assertNull(config.getNearCacheConfig("hazelFast.test.sub.myNearCache"));
        assertNull(config.getNearCacheConfig("test.sub.myNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcardOnly() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig().setName("*");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig);

        assertEquals(nearCacheConfig, config.getNearCacheConfig("uk.org.hecaton.myNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcardOnlyMultipleConfigs() {
        NearCacheConfig nearCacheConfig1 = new NearCacheConfig().setName("*");
        NearCacheConfig nearCacheConfig2 = new NearCacheConfig().setName("uk.org.hecaton.*");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig1);
        config.addNearCacheConfig(nearCacheConfig2);

        // we should get the best matching result
        assertEquals(nearCacheConfig2, config.getNearCacheConfig("uk.org.hecaton.myNearCache"));
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(MatchingPointConfigPatternMatcher.class)
                .usingGetClass()
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }
}
