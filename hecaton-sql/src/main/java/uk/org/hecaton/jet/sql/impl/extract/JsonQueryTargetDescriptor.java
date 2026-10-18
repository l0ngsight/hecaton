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
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.sql.impl.extract.QueryTarget;
import uk.org.hecaton.sql.impl.extract.QueryTargetDescriptor;

public final class JsonQueryTargetDescriptor implements QueryTargetDescriptor {

    public static final JsonQueryTargetDescriptor INSTANCE = new JsonQueryTargetDescriptor();

    private JsonQueryTargetDescriptor() {
    }

    @Override
    public QueryTarget create(InternalSerializationService serializationService, Extractors extractors, boolean isKey) {
        return new JsonQueryTarget();
    }

    @Override
    public void writeData(ObjectDataOutput out) {
    }

    @Override
    public void readData(ObjectDataInput in) {
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof JsonQueryTargetDescriptor;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
