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
import uk.org.hecaton.sql.impl.type.QueryDataTypeFamily;

@SerializableByConvention
public class JsonConverter extends Converter {
    public static final JsonConverter INSTANCE = new JsonConverter();

    public JsonConverter() {
        super(ID_JSON, QueryDataTypeFamily.JSON);
    }

    @Override
    public Class<?> getValueClass() {
        return HecatonJsonValue.class;
    }

    @Override
    public Object convertToSelf(final Converter converter, final Object val) {
        return converter.asJson(val);
    }

    @Override
    public String asVarchar(final Object val) {
        final HecatonJsonValue jsonValue = (HecatonJsonValue) val;
        return jsonValue.toString();
    }

    @Override
    public HecatonJsonValue asJson(final Object val) {
        return (HecatonJsonValue) val;
    }
}
