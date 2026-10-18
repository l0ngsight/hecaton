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

package uk.org.hecaton.jet.hadoop.impl;

import uk.org.hecaton.internal.nio.ClassLoaderUtil;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.Serializer;
import uk.org.hecaton.nio.serialization.SerializerHook;
import uk.org.hecaton.nio.serialization.StreamSerializer;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

import static uk.org.hecaton.jet.impl.util.ExceptionUtil.rethrow;

final class WritableSerializerHooks {

    private static final int DEFAULT = -380;
    private static final int INT = -381;
    private static final int LONG = -382;
    private static final int BOOLEAN = -383;
    private static final int FLOAT = -384;
    private static final int DOUBLE = -385;
    private static final int BYTE = -386;
    private static final int TEXT = -387;

    private WritableSerializerHooks() {
    }

    static final class Boolean extends WritableSerializerHook<BooleanWritable> {
        Boolean() {
            super(BooleanWritable.class, BooleanWritable::new, BOOLEAN);
        }
    }

    static final class Byte extends WritableSerializerHook<ByteWritable> {
        Byte() {
            super(ByteWritable.class, ByteWritable::new, BYTE);
        }
    }

    static final class Double extends WritableSerializerHook<DoubleWritable> {
        Double() {
            super(DoubleWritable.class, DoubleWritable::new, DOUBLE);
        }
    }

    static final class Float extends WritableSerializerHook<FloatWritable> {
        Float() {
            super(FloatWritable.class, FloatWritable::new, FLOAT);
        }
    }

    static final class Int extends WritableSerializerHook<IntWritable> {
        Int() {
            super(IntWritable.class, IntWritable::new, INT);
        }
    }

    static final class Long extends WritableSerializerHook<LongWritable> {
        Long() {
            super(LongWritable.class, LongWritable::new, LONG);
        }
    }

    static final class Text extends WritableSerializerHook<org.apache.hadoop.io.Text> {
        Text() {
            super(org.apache.hadoop.io.Text.class, org.apache.hadoop.io.Text::new, TEXT);
        }
    }

    static final class Default implements SerializerHook<Writable> {

        @Override
        public Class<Writable> getSerializationType() {
            return Writable.class;
        }

        @Override
        public Serializer createSerializer() {
            return new WritableStreamSerializer();
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    private static class WritableStreamSerializer implements StreamSerializer<Writable> {

        private static final ILogger LOGGER = Logger.getLogger(WritableStreamSerializer.class);

        private boolean warnedOnce;

        @Override
        public int getTypeId() {
            return DEFAULT;
        }

        @Override
        public void write(ObjectDataOutput out, Writable writable) throws IOException {
            if (!warnedOnce) {
                LOGGER.warning("Using default Writable serialization without explicit type registration. " +
                        "Consider explicit type registration using WritableSerializerHook.");
                warnedOnce = true;
            }
            out.writeString(writable.getClass().getName());
            writable.write(out);
        }

        @Override
        public Writable read(ObjectDataInput in) throws IOException {
            if (!warnedOnce) {
                LOGGER.warning("Using default Writable serialization without explicit type registration. " +
                        "Consider explicit type registration using WritableSerializerHook.");
                warnedOnce = true;
            }
            String className = in.readString();
            try {
                Writable instance = ClassLoaderUtil.newInstance(Thread.currentThread().getContextClassLoader(), className);
                instance.readFields(in);
                return instance;
            } catch (Exception e) {
                throw rethrow(e);
            }
        }
    }
}

