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

package uk.org.hecaton.jet.sql.impl.connector.test;

import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.jet.core.Processor.Context;
import uk.org.hecaton.jet.core.ProcessorMetaSupplier;
import uk.org.hecaton.jet.impl.pipeline.transform.StreamSourceTransform;
import uk.org.hecaton.jet.pipeline.SourceBuilder;
import uk.org.hecaton.jet.pipeline.StreamSource;
import uk.org.hecaton.sql.SqlService;
import uk.org.hecaton.sql.impl.type.QueryDataTypeFamily;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

/**
 * A test stream-data connector. It emits rows of provided types and values.
 */
public class TestStreamSqlConnector extends TestAbstractSqlConnector {

    public static final String TYPE_NAME = "TestStream";

    public static void create(
            SqlService sqlService,
            String tableName,
            List<String> names,
            List<QueryDataTypeFamily> types,
            Object[]... values
    ) {
        List<String[]> stringValues = stream(values)
                .map(row -> stream(row).map(value -> value == null ? null : value.toString()).toArray(String[]::new))
                .collect(Collectors.toList());
        TestAbstractSqlConnector.create(sqlService, TYPE_NAME, tableName, names, types, stringValues, true);
    }

    @Override
    protected ProcessorMetaSupplier createProcessorSupplier(FunctionEx<Context, TestDataGenerator> createContextFn) {
        StreamSource<Object> source = SourceBuilder
                .stream("stream", createContextFn)
                .fillBufferFn(TestDataGenerator::fillBuffer)
                .build();
        return ((StreamSourceTransform<Object>) source).metaSupplierFn.apply(null);
    }

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull
    @Override
    public String defaultObjectType() {
        return "stream";
    }

}
