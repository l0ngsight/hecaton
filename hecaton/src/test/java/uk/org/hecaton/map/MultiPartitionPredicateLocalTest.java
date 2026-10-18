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

import static uk.org.hecaton.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.query.PartitionPredicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiPartitionPredicateLocalTest extends MultiPartitionPredicateTestSupport {

    private HecatonInstance instance;

    @Override
    protected IMap<String, Integer> getMap(String name) {
        return instance.getMap(name);
    }

    @Override
    protected void setupInternal() {
        instance = getFactory().getAllHecatonInstances().stream().findFirst().get();
    }

    @Override
    protected HecatonInstance getInstance() {
        return instance;
    }

    @Test
    public void testSerialization() {
        SerializationService serializationService = getSerializationService(getInstance());
        Data serialized = serializationService.toData(getPredicate());
        PartitionPredicate deserialized = serializationService.toObject(serialized);

        assertEquals(getPartitionKeys(), deserialized.getPartitionKeys());
        assertEquals(Predicates.alwaysTrue(), deserialized.getTarget());
    }
}
