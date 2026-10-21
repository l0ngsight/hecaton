/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.persistence.internal.map;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.HeapData;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.expiry.ExpiryMetadata;
import uk.org.hecaton.map.impl.recordstore.expiry.ExpiryMetadataImpl;

import java.nio.ByteBuffer;

/**
 * The persisted form of a map entry's value: format, version, creation and last update times,
 * TTL, max idle, expiration time, then the serialized value.
 */
final class MapRecordCodec {

    private static final byte FORMAT = 1;
    private static final int HEADER_SIZE = 1 + Integer.BYTES + 5 * Long.BYTES;

    private MapRecordCodec() {
    }

    static byte[] encode(Record<?> record, ExpiryMetadata expiry, SerializationService serializationService) {
        Data value = serializationService.toData(record.getValue());
        byte[] valueBytes = value == null ? new byte[0] : value.toByteArray();
        return ByteBuffer.allocate(HEADER_SIZE + valueBytes.length)
                .put(FORMAT)
                .putInt(record.getVersion())
                .putLong(record.getCreationTime())
                .putLong(record.getLastUpdateTime())
                .putLong(expiry.getTtl())
                .putLong(expiry.getMaxIdle())
                .putLong(expiry.getExpirationTime())
                .put(valueBytes)
                .array();
    }

    static Decoded decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte format = buffer.get();
        if (format != FORMAT) {
            throw new IllegalStateException("Unknown format of a persisted map entry: " + format);
        }
        int version = buffer.getInt();
        long creationTime = buffer.getLong();
        long lastUpdateTime = buffer.getLong();
        long ttl = buffer.getLong();
        long maxIdle = buffer.getLong();
        long expirationTime = buffer.getLong();
        byte[] value = new byte[buffer.remaining()];
        buffer.get(value);
        ExpiryMetadata expiry = ttl == Long.MAX_VALUE && maxIdle == Long.MAX_VALUE
                ? ExpiryMetadata.NULL
                : new ExpiryMetadataImpl(ttl, maxIdle, expirationTime, lastUpdateTime);
        return new Decoded(version, creationTime, lastUpdateTime, expiry, new HeapData(value));
    }

    /** A decoded entry value. */
    record Decoded(int version, long creationTime, long lastUpdateTime, ExpiryMetadata expiry, Data value) {
    }
}
