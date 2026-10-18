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

package uk.org.hecaton.sql.impl.schema;

import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.sql.impl.SqlDataSerializerHook;

import javax.annotation.Nonnull;

/**
 * Interface for catalog objects which support unparsing to DDL.
 */
public interface SqlCatalogObject extends IdentifiedDataSerializable {

    /**
     * @return DDL statement to create this object.
     */
    @Nonnull
    String unparse();

    /**
     * @return name of sql catalog object.
     */
    String name();

    @Override
    default int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }
}
