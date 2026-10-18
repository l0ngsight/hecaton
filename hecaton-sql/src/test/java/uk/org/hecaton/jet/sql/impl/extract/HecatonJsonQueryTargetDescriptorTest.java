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

package uk.org.hecaton.jet.sql.impl.extract;

import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.sql.impl.extract.QueryTarget;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class HecatonJsonQueryTargetDescriptorTest {

    private static final InternalSerializationService SERIALIZATION_SERVICE =
            new DefaultSerializationServiceBuilder().build();

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void test_create(boolean key) {
        Extractors extractors = Extractors.newBuilder(SERIALIZATION_SERVICE).build();

        HecatonJsonQueryTargetDescriptor descriptor = HecatonJsonQueryTargetDescriptor.INSTANCE;

        // when
        QueryTarget target = descriptor.create(SERIALIZATION_SERVICE, extractors, key);

        // then
        assertThat(target).isInstanceOf(HecatonJsonQueryTarget.class);
    }

    @Test
    public void test_serialization() {
        HecatonJsonQueryTargetDescriptor original = HecatonJsonQueryTargetDescriptor.INSTANCE;

        // when
        HecatonJsonQueryTargetDescriptor serialized =
                SERIALIZATION_SERVICE.toObject(SERIALIZATION_SERVICE.toData(original));

        // then
        assertThat(serialized).isEqualTo(original);
    }
}
