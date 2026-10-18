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

package uk.org.hecaton.sql.impl.type.converter;

import uk.org.hecaton.core.HecatonJsonValue;
import uk.org.hecaton.internal.serialization.SerializableByConvention;

/**
 * Converter for {@link java.lang.String} type.
 */
@SerializableByConvention
public final class StringConverter extends AbstractStringConverter {

    public static final StringConverter INSTANCE = new StringConverter();

    private StringConverter() {
        super(ID_STRING);
    }

    @Override
    public Class<?> getValueClass() {
        return String.class;
    }

    @Override
    protected String cast(Object val) {
        return (String) val;
    }

    @Override
    public HecatonJsonValue asJson(final Object val) {
        return new HecatonJsonValue(cast(val));
    }
}
