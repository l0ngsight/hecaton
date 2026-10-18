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

package uk.org.hecaton.json;

import uk.org.hecaton.aggregation.Aggregators;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MetadataPolicy;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonJsonValue;
import uk.org.hecaton.internal.json.Json;
import uk.org.hecaton.internal.json.JsonArray;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapAggregationJsonTest extends HecatonTestSupport {

    public static final int OBJECT_COUNT = 1000;
    private static final String STRING_PREFIX = "s";

    TestHecatonInstanceFactory factory;
    HecatonInstance instance;

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(1)
    public MetadataPolicy metadataPolicy;

    @Parameterized.Parameters(name = "inMemoryFormat: {0}, metadataPolicy: {1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, MetadataPolicy.OFF},
                {InMemoryFormat.BINARY, MetadataPolicy.CREATE_ON_UPDATE},
                {InMemoryFormat.OBJECT, MetadataPolicy.OFF},
                {InMemoryFormat.OBJECT, MetadataPolicy.CREATE_ON_UPDATE},
        });
    }

    @Before
    public void setup() {
        factory = createHecatonInstanceFactory(3);
        factory.newInstances(getConfig(), 3);
        instance = factory.getAllHecatonInstances().iterator().next();
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.getMapConfig("default")
                .setInMemoryFormat(inMemoryFormat)
                .setMetadataPolicy(metadataPolicy);
        return config;
    }

    @Test
    public void testLongField() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        long maxLongValue = map.aggregate(Aggregators.longMax("longValue"));
        assertEquals(OBJECT_COUNT - 1, maxLongValue);
    }

    @Test
    public void testDoubleField() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        double maxDoubleValue = map.aggregate(Aggregators.doubleMax("doubleValue"));
        assertEquals(OBJECT_COUNT - 0.5, maxDoubleValue, 0.00001);
    }

    @Test
    public void testStringField() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        String maxStringValue = map.aggregate(Aggregators.comparableMax("stringValue"));
        assertEquals(STRING_PREFIX + "999", maxStringValue);
    }

    @Test
    public void testNestedField() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        long maxLongValue = map.aggregate(Aggregators.longMax("nestedObject.nestedLongValue"));
        assertEquals((OBJECT_COUNT - 1) * 10, maxLongValue);
    }

    @Test
    public void testValueIsOmitted_whenObjectIsEmpty() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        map.put(OBJECT_COUNT, new HecatonJsonValue(Json.object().toString()));
        long maxLongValue = map.aggregate(Aggregators.longMax("longValue"));
        assertEquals(OBJECT_COUNT - 1, maxLongValue);
    }

    @Test
    public void testValueIsOmitted_whenAttributePathDoesNotExist() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        map.put(OBJECT_COUNT, new HecatonJsonValue(Json.object().add("someField", "someValue").toString()));
        long maxLongValue = map.aggregate(Aggregators.longMax("longValue"));
        assertEquals(OBJECT_COUNT - 1, maxLongValue);
    }

    @Test
    public void testValueIsOmitted_whenValueIsNotAnObject() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        map.put(OBJECT_COUNT, new HecatonJsonValue(Json.value(5).toString()));
        long maxLongValue = map.aggregate(Aggregators.longMax("longValue"));
        assertEquals(OBJECT_COUNT - 1, maxLongValue);
    }

    @Test
    public void testValueIsOmitted_whenAttributePathIsNotTerminal() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        map.put(OBJECT_COUNT, new HecatonJsonValue(Json.object()
                .add("longValue", Json.object())
                .toString()));
        long count = map.aggregate(Aggregators.longMax("longValue"));
        assertEquals(OBJECT_COUNT - 1, count);
    }

    @Test
    public void testValueIsOmitted_whenAttributePathIsNotTerminal_count() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        map.put(OBJECT_COUNT, new HecatonJsonValue(Json.object()
                .add("longValue", Json.object())
                .toString()));
        long count = map.aggregate(Aggregators.count("longValue"));
        assertEquals(OBJECT_COUNT, count);
    }

    @Test
    public void testValueIsOmitted_whenAttributePathIsNotTerminal_distinct() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        map.put(OBJECT_COUNT, new HecatonJsonValue(Json.object()
                .add("longValue", Json.object())
                .toString()));
        Collection<Object> distinctLongValues = map.aggregate(Aggregators.distinct("longValue"));
        assertEquals(OBJECT_COUNT, distinctLongValues.size());
    }

    @Test
    public void testAny() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        Collection<Object> distinctStrings = map.aggregate(Aggregators.distinct("stringValueArray[any]"));
        assertEquals(OBJECT_COUNT * 2, distinctStrings.size());
        for (int i = 0; i < OBJECT_COUNT; i++) {
            assertContains(distinctStrings, "nested0 " + STRING_PREFIX + i);
            assertContains(distinctStrings, "nested1 " + STRING_PREFIX + i);
        }
    }

    protected IMap<Integer, HecatonJsonValue> getPreloadedMap() {
        IMap<Integer, HecatonJsonValue> map = instance.getMap(randomMapName());
        for (int i = 0; i < OBJECT_COUNT; i++) {
            map.put(i, createHecatonJsonValue(STRING_PREFIX + i, i, (double) i + 0.5, (long) i * 10));
        }
        return map;
    }

    private HecatonJsonValue createHecatonJsonValue(String stringValue, long longValue, double doubleValue, long nestedLongValue) {
        return new HecatonJsonValue(createJsonString(stringValue, longValue, doubleValue, nestedLongValue));
    }

    @Test
    public void testArrayWithNestedField() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        long maxLongValue = map.aggregate(Aggregators.longMax(
                "nestedArray[any].nestedArrayObject.secondary.nestedObjectLongValue"));
        assertEquals((OBJECT_COUNT - 1) * 10, maxLongValue);
    }

    @Test
    public void testArrayWithNestedField_when_field_nonexist() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        Long maxLongValue = map.aggregate(Aggregators.longMax(
                "nestedArray[any].nestedArrayObject.secondary.nestedObjectLongValue.nonExistant"));
        assertNull(maxLongValue);
    }

    @Test
    public void testArrayWithNestedField_when_last_field_array() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        // some aggregations do not care if the path points to scalar or object
        long count = map.aggregate(Aggregators.count(
                "nestedArray[any].nestedArrayObject.secondary"));
        assertEquals(OBJECT_COUNT, count);

        // but other work only for scalar
        assertThatThrownBy(() -> map.aggregate(Aggregators.longMax(
                "nestedArray[any].nestedArrayObject.secondary")))
                .isInstanceOf(ClassCastException.class);
        // Message assertion is not possible due to OmitStackTraceInFastThrow,
        // the message should be "uk.org.hecaton.internal.json.NonTerminalJsonValue cannot be cast to class java.lang.Comparable"
    }

    @Test
    public void testArrayWithNestedField_when_field_name_is_wrongly_written() {
        IMap<Integer, HecatonJsonValue> map = getPreloadedMap();
        Long maxLongValue = map.aggregate(Aggregators.longMax("nestedArray[any].nestedArrayObject.secondary.xnestedObjectLongValue"));
        assertNull(maxLongValue);
    }

    @Test
    public void test_nested_json() {
        IMap<Integer, HecatonJsonValue> map = instance.getMap(randomMapName());
        map.put(1, new HecatonJsonValue(nestedJsonString()));
        Long sum = map.aggregate(Aggregators.longSum("list[any].secondLevelItem.thirdLevelItem"));

        assertEquals(6L, sum.longValue());

        Long countNotScalar = map.aggregate(Aggregators.count("list[any].secondLevelItem"));
        assertEquals(3L, countNotScalar.longValue());
    }

    @Test
    public void test_nested_json_different_levels() {
        IMap<Integer, HecatonJsonValue> map = instance.getMap(randomMapName());
        map.put(1, new HecatonJsonValue(nestedJsonStringDifferentNesting()));
        Long sum = map.aggregate(Aggregators.longSum("list[any].secondLevelItem.thirdLevelItem"));

        assertEquals(1L, sum.longValue());
    }

    @Test
    public void test_nested_json_same_object_nested() {
        IMap<Integer, HecatonJsonValue> map = instance.getMap(randomMapName());
        map.put(1, new HecatonJsonValue(nestedJsonStringNestingOfTheSameObject()));
        Long sum = map.aggregate(Aggregators.longSum("list[any].secondLevelItem.thirdLevelItem"));

        assertEquals(3L, sum.longValue());
    }

    private static String nestedJsonString() {
        return """
                {
                  "list": [
                    {
                      "secondLevelItem": {
                        "thirdLevelItem": 1
                      }
                    },
                    {
                      "secondLevelItem": {
                        "thirdLevelItem": 2
                      }
                    },
                    {
                      "secondLevelItem": {
                        "thirdLevelItem": 3
                      }
                    }
                  ]
                }""";
    }

    private static String nestedJsonStringDifferentNesting() {
        return """
                {
                  "list": [
                    {
                      "secondLevelItem": {
                        "thirdLevelItem": 1
                      }
                    },
                    {
                        "thirdLevelItem": 2
                    },
                    {
                      "secondLevelItem": {
                        "twoAndAHalfLevelItem": {
                          "thirdLevelItem": 3
                        }
                      }
                    }
                  ]
                }""";
    }

    private static String nestedJsonStringNestingOfTheSameObject() {
        return "{\n"
                + "  \"list\": [\n"
                + "    {\n"
                + "      \"secondLevelItem\": {\n"
                + "        \"thirdLevelItem\": 1\n"
                + "      },\n"
                // the object is nested so should not match `secondLevelItem.thirdLevelItem` path
                + "      \"additionallyNested\": {\n"
                + "        \"secondLevelItem\": {\n"
                + "          \"thirdLevelItem\": 200\n"
                + "        }\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"secondLevelItem\": {\n"
                + "        \"thirdLevelItem\": 2\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";
    }

    private String createJsonString(String stringValue, long longValue, double doubleValue, long nestedLongValue) {
        JsonObject object = Json.object();

        JsonArray array = new JsonArray();
        array.add(Json.object().add("nestedArrayObject",
                Json.object().add("secondary",
                        Json.object().add("nestedObjectLongValue", nestedLongValue))));


        object.add("nestedArray", array);

        object.add("stringValue", stringValue)
                .add("longValue", longValue)
                .add("doubleValue", doubleValue)
                .add("nestedObject", Json.object()
                        .add("nestedLongValue", nestedLongValue))
                .add("stringValueArray", Json.array("nested0 " + stringValue, "nested1 " + stringValue));
        return object.toString();
    }
}
