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

package uk.org.hecaton.sql.impl.extract;

import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.query.impl.getters.Extractors;

/**
 * Descriptor of the target object (key or value from map record). Used to generate the proper attribute extraction strategy
 * and to impose additional restrictions on data that is used for compilation. For example, expected class name or expected
 * portable class ID.
 * <p>
 * The principal difference to {@link QueryTarget} is that descriptor is transferred over a wire, while the target object
 * may contain non-serializable internals.
 */
public interface QueryTargetDescriptor extends DataSerializable {
    /**
     * Create the target that will be used for extraction.
     *
     * @param serializationService Serialization service.
     * @param extractors Extractors associated with the map.
     * @param isKey Whether this target extract data from key or value. Used mainly for proper error reporting.
     * @return Target object.
     */
    QueryTarget create(InternalSerializationService serializationService, Extractors extractors, boolean isKey);
}
