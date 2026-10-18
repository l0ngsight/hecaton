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

package uk.org.hecaton.collection.impl.set;

import uk.org.hecaton.collection.impl.collection.CollectionItem;
import uk.org.hecaton.internal.serialization.SerializationServiceBuilder;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SetEqualsHashTest extends HecatonTestSupport {

    @Test
    public void testCollectionItem_equalsAndHash() {
        SerializationServiceBuilder serializationServiceBuilder = new DefaultSerializationServiceBuilder();
        SerializationService build = serializationServiceBuilder.build();
        Data value = build.toData(randomString());
        CollectionItem firstItem = new CollectionItem(1, value);
        CollectionItem secondItem = new CollectionItem(2, value);
        assertEquals(firstItem, secondItem);
        assertEquals(firstItem.hashCode(), secondItem.hashCode());
    }
}
