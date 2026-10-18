/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.jet.sql.impl.index;

import uk.org.hecaton.jet.sql.impl.JetSqlSerializerHook;
import uk.org.hecaton.sql.impl.exec.scan.index.IndexCompositeFilter;
import uk.org.hecaton.sql.impl.exec.scan.index.IndexEqualsFilter;
import uk.org.hecaton.sql.impl.exec.scan.index.IndexFilter;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertSame;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexCompositeFilterTest extends IndexFilterTestSupport {
    @Test
    public void testContent() {
        List<IndexFilter> filters = Collections.singletonList(new IndexEqualsFilter(intValue(1)));

        IndexCompositeFilter filter = new IndexCompositeFilter(filters);

        assertSame(filters, filter.getFilters());
    }

    @Test
    public void testEquals() {
        IndexCompositeFilter filter = new IndexCompositeFilter(Collections.singletonList(new IndexEqualsFilter(intValue(1))));

        checkEquals(filter, new IndexCompositeFilter(Collections.singletonList(new IndexEqualsFilter(intValue(1)))), true);
        checkEquals(filter, new IndexCompositeFilter(Collections.singletonList(new IndexEqualsFilter(intValue(2)))), false);
    }

    @Test
    public void testSerialization() {
        IndexCompositeFilter original = new IndexCompositeFilter(Collections.singletonList(new IndexEqualsFilter(intValue(1))));
        IndexCompositeFilter restored = serializeAndCheck(original, JetSqlSerializerHook.INDEX_FILTER_IN);

        checkEquals(original, restored, true);
    }
}
