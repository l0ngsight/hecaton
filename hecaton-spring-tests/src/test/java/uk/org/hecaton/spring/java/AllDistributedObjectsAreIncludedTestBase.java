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
package uk.org.hecaton.spring.java;

import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.spring.HecatonObjectExtractionConfiguration;
import uk.org.hecaton.transaction.TransactionalObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Set;

import static uk.org.hecaton.test.ReflectionsHelper.REFLECTIONS;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AllDistributedObjectsAreIncludedTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(AllDistributedObjectsAreIncludedTestBase.class);

    /**
     * Classes that are either abstract/base interfaces or structures that doesn't have configuration.
     */
    private static final Set<Class<?>> EXCEPTIONS = Set.of(
            uk.org.hecaton.collection.ICollection.class,
            uk.org.hecaton.cp.IAtomicReference.class,
            uk.org.hecaton.cp.ICountDownLatch.class,
            uk.org.hecaton.cp.IAtomicLong.class,
            uk.org.hecaton.core.PrefixedDistributedObject.class,
            uk.org.hecaton.multimap.BaseMultiMap.class,
            uk.org.hecaton.collection.BaseQueue.class,
            uk.org.hecaton.transaction.HecatonXAResource.class,
            uk.org.hecaton.map.BaseMap.class,
            uk.org.hecaton.cache.impl.ICacheInternal.class);

    @Autowired
    private HecatonObjectExtractionConfiguration conf;

    @AfterAll
    public static void afterAll() {
        HecatonInstanceFactory.terminateAll();
    }

    @Test
    void checkAllDistributedObjectsAreIncluded() {
        Set<Class<?>> typesOfBeans = conf.registeredTypesOfBeans();
        LOGGER.debug("List of all registered: {}\n",  typesOfBeans.stream()
                                                                  .map(c -> " - " + c.getName())
                                                                  .collect(joining(System.lineSeparator())));

        Set<Class<? extends  DistributedObject>> typesOfDistributedObjects = REFLECTIONS.getSubTypesOf(DistributedObject.class);
        typesOfDistributedObjects.removeIf(c -> !c.isInterface());
        typesOfDistributedObjects.removeIf(c -> c.getName().startsWith("Base"));
        typesOfDistributedObjects.removeIf(c -> c.getName().endsWith("Internal"));
        typesOfDistributedObjects.removeIf(EXCEPTIONS::contains);
        typesOfDistributedObjects.removeIf(TransactionalObject.class::isAssignableFrom);

        assertThat(typesOfBeans)
                .as("Some classes are not added as bean by Hecaton configuration")
                .containsAll(typesOfDistributedObjects);
    }
}
