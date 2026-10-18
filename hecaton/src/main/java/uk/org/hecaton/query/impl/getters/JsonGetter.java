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

package uk.org.hecaton.query.impl.getters;

import tools.jackson.core.json.JsonFactory;
import tools.jackson.core.JsonParser;
import uk.org.hecaton.core.HecatonJsonValue;
import uk.org.hecaton.internal.serialization.impl.NavigableJsonInputAdapter;
import uk.org.hecaton.internal.serialization.impl.StringNavigableJsonAdapter;
import uk.org.hecaton.query.QueryException;

public final class JsonGetter extends AbstractJsonGetter {

    public static final JsonGetter INSTANCE = new JsonGetter();

    private final JsonFactory factory = new JsonFactory();

    protected JsonGetter() {
        super(null);
    }

    @Override
    protected NavigableJsonInputAdapter annotate(Object object) {
        HecatonJsonValue hecatonJson = (HecatonJsonValue) object;
        return new StringNavigableJsonAdapter(hecatonJson.toString(), 0);
    }

    @Override
    JsonParser createParser(Object obj) {
        if (obj instanceof HecatonJsonValue) {
            return factory.createParser(obj.toString());
        } else {
            throw new QueryException("Queried object is not of HecatonJsonValue type. It is " + obj.getClass());
        }
    }
}
