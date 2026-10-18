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

package uk.org.hecaton.sql.impl;

import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.jet.sql.impl.JetSqlSerializerHook;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.OverridePropertyRule;
import org.junit.ClassRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Helper test classes.
 */
public class CoreSqlTestSupport extends HecatonTestSupport {

    @ClassRule
    public static OverridePropertyRule enableJetRule = OverridePropertyRule.set("hc.jet.enabled", "true");

    /**
     * Check object equality with additional hash code check.
     *
     * @param first    First object.
     * @param second   Second object.
     * @param expected Expected result.
     */
    public static void checkEquals(Object first, Object second, boolean expected) {
        if (expected) {
            assertEquals(first, second);
            assertEquals(first.hashCode(), second.hashCode());
        } else {
            assertNotEquals(first, second);
        }
    }

    public static <T> T serializeAndCheck(Object original, int expectedClassId) {
        return serializeAndCheck(original, JetSqlSerializerHook.F_ID, expectedClassId);
    }

    public static <T> T serializeAndCheck(Object original, int expectedFactoryId, int expectedClassId) {
        assertInstanceOf(IdentifiedDataSerializable.class, original);
        IdentifiedDataSerializable original0 = (IdentifiedDataSerializable) original;

        assertEquals(expectedFactoryId, original0.getFactoryId());
        assertEquals(expectedClassId, original0.getClassId());

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        return ss.toObject(ss.toData(original));
    }
}
