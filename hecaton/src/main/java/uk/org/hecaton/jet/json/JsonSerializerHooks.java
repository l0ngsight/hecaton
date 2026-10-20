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

package uk.org.hecaton.jet.json;

import tools.jackson.jr.ob.impl.DeferredMap;
import uk.org.hecaton.jet.impl.serialization.SerializerHookConstants;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.Serializer;
import uk.org.hecaton.nio.serialization.SerializerHook;
import uk.org.hecaton.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.Map;

/**
 * Hecaton serializer hooks for the classes in the {@code
 * uk.org.hecaton.jet.json} package. This is not a public-facing API.
 */
class JsonSerializerHooks {

    public static final class DeferredMapHook implements SerializerHook<DeferredMap> {

        @Override
        public Class<DeferredMap> getSerializationType() {
            return DeferredMap.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<DeferredMap>() {

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.DEFERRED_MAP;
                }

                @Override
                public void write(ObjectDataOutput out, DeferredMap deferredMap) throws IOException {
                    out.writeInt(deferredMap.size());
                    for (Map.Entry<String, Object> entry : deferredMap.entrySet()) {
                        out.writeString(entry.getKey());
                        out.writeObject(entry.getValue());
                    }
                }

                @Override
                public DeferredMap read(ObjectDataInput in) throws IOException {
                    int size = in.readInt();
                    DeferredMap deferredMap = new DeferredMap(false, size);
                    for (int i = 0; i < size; i++) {
                        deferredMap.put(in.readString(), in.readObject());
                    }
                    return deferredMap;
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }
}
