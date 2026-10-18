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

package uk.org.hecaton.map.impl.querycache;

import uk.org.hecaton.config.ClasspathXmlConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheXmlConfigWildcardTest extends HecatonTestSupport {
    @Test
    public void testQueryCacheXmlConfigWithWildCard() {
        HecatonInstance hecatonInstance = createInstance();
        IMap<Integer, Integer> map = hecatonInstance.getMap("testQueryCacheXmlConfigWithWildCardMap1");
        QueryCache<Integer, Integer> queryCache = map.getQueryCache("queryCacheName1");

        assertNotNull(queryCache);
    }

    protected HecatonInstance createInstance() {
        ClasspathXmlConfig config = new ClasspathXmlConfig("hecaton-querycache-xml-config-wildcard-test.xml");
        return createHecatonInstance(config);
    }
}
