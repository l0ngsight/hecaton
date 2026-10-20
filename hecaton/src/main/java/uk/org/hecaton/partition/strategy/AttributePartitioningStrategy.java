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

package uk.org.hecaton.partition.strategy;

import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonJsonValue;
import uk.org.hecaton.internal.serialization.SerializableByConvention;
import uk.org.hecaton.internal.serialization.impl.GenericRecordQueryReader;
import uk.org.hecaton.internal.serialization.impl.InternalGenericRecord;
import uk.org.hecaton.internal.util.PartitioningStrategyUtil;
import uk.org.hecaton.jet.impl.util.ReflectionUtils;
import uk.org.hecaton.partition.PartitioningStrategy;
import uk.org.hecaton.query.impl.getters.JsonGetter;

import javax.annotation.Nonnull;

@SerializableByConvention
public final class AttributePartitioningStrategy implements PartitioningStrategy<Object> {

    private final String[] attributes;

    public AttributePartitioningStrategy(String... attributes) {
        checkAttributesAreValid(attributes);
        this.attributes = attributes;
    }

    @Override
    public Object getPartitionKey(final Object key) {
        final Object[] result;

        if (key instanceof InternalGenericRecord internalGenericRecord) {
            result = extractFromGenericRecord(internalGenericRecord);
        } else if (key instanceof HecatonJsonValue) {
            result = extractFromJson(key);
        } else  {
            result = extractFromPojo(key);
        }

        return PartitioningStrategyUtil.constructAttributeBasedKey(result);
    }

    @Nonnull
    private Object[] extractFromPojo(final Object key) {
        final Object[] values = new Object[attributes.length];
        for (int i = 0; i < attributes.length; i++) {
            final String attribute = attributes[i];
            final Object value = ReflectionUtils.getFieldValue(attribute, key);
            checkNull(value, attribute);
            values[i] = value;
        }

        return values;
    }

    @Nonnull
    private Object[] extractFromJson(final Object key) {
        final Object[] values = new Object[attributes.length];
        for (int i = 0; i < attributes.length; i++) {
            final Object value = JsonGetter.INSTANCE.getValue(key, attributes[i]);
            checkNull(value, attributes[i]);
            values[i] = value;
        }

        return values;
    }

    @Nonnull
    private Object[] extractFromGenericRecord(final InternalGenericRecord key) {
        final Object[] values = new Object[attributes.length];
        final GenericRecordQueryReader reader = new GenericRecordQueryReader(key);
        for (int i = 0; i < attributes.length; i++) {
            final String attribute = attributes[i];
            final Object value = reader.read(attribute);
            checkNull(value, attributes[i]);
            values[i] = value;
        }

        return values;
    }

    public String[] getPartitioningAttributes() {
        return attributes;
    }

    private static void checkNull(Object extracted, String attributeName) {
        if (extracted == null) {
            throw new HecatonException("Cannot extract '" + attributeName + "' from the key");
        }
    }

    private static void checkAttributesAreValid(final String[] attributes) {
        if (attributes == null) {
            throw new NullPointerException("Attributes names array must not be null.");
        }

        if (attributes.length == 0) {
            throw new IllegalArgumentException("Attributes names array must not be empty.");
        }

        for (final String attribute : attributes) {
            if (attribute == null || attribute.isEmpty()) {
                throw new IllegalArgumentException("Attribute name must not be empty.");
            }
        }
    }
}
