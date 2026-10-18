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

import uk.org.hecaton.config.AttributeConfig;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.query.QueryException;
import uk.org.hecaton.query.ReflectiveAttributeTestObject;
import uk.org.hecaton.query.extractor.ValueCollector;
import uk.org.hecaton.query.extractor.ValueExtractor;
import uk.org.hecaton.query.impl.getters.policy.ReflectiveAttributeLookupPolicy;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Properties;

import static uk.org.hecaton.query.impl.getters.GetterCache.SIMPLE_GETTER_CACHE_SUPPLIER;
import static uk.org.hecaton.test.HecatonTestSupport.assertInstanceOf;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
@SuppressWarnings("unused")
public class ExtractorsTest {

    @Parameters(name = "useClassloader:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {false},
                {true},
        });
    }

    @Parameter
    public boolean useClassloader;

    private final Bond bond = new Bond();

    private InternalSerializationService defaultSerService;

    @Before
    public void setUp() {
        defaultSerService = createSerializationService(ReflectiveAttributeLookupPolicy.Policy.CONTROLLED_ACCESS.getPolicyName());
    }

    @Test
    public void when_getGetterByReflection_then_getterInCache() {
        // GIVEN
        Extractors extractors = createExtractors(null);

        // WHEN
        Getter getterFirstInvocation = extractors.getGetter(bond, "car.power", true);
        Getter getterSecondInvocation = extractors.getGetter(bond, "car.power", true);

        // THEN
        assertThat(getterFirstInvocation)
                .isInstanceOf(FieldGetter.class)
                .isSameAs(getterSecondInvocation);
    }

    @Test
    public void when_extractByReflection_then_correctValue() {
        // WHEN
        Object power = createExtractors(null).extract(bond, "car.power", null);

        // THEN
        assertThat((Integer) power).isEqualTo(550);
    }

    @Test
    public void when_extractAttributeMatchingStaticMethodAndGetter_withRestrictedLookup_then_getterIsUsed() {
        Object result = createExtractors(null).extract(new ReflectiveAttributeTestObject("a"), "name", null);
        assertThat(result).isEqualTo("a");
    }

    @Test
    public void when_extractAttributeMatchingStaticMethodAndGetter_withExtendedLookup_then_staticMethodIsUsed() {
        Object result = createExtractors(null, ReflectiveAttributeLookupPolicy.Policy.FULL_ACCESS)
                .extract(new ReflectiveAttributeTestObject("b"), "name", null);
        assertThat(result).isEqualTo("static");
    }

    @Test
    public void when_extractStaticMethod_withRestrictedLookup_then_throwQueryException() {
        assertThatThrownBy(() -> createExtractors(null).extract(new ReflectiveAttributeTestObject("c"), "getStaticValue", null))
                .isInstanceOf(QueryException.class)
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("cannot be used for attribute extraction");
    }

    @Test
    public void when_extractStaticMethod_withExtendedLookup_then_returnValue() {
        Object result = createExtractors(null, ReflectiveAttributeLookupPolicy.Policy.FULL_ACCESS)
                .extract(new ReflectiveAttributeTestObject("d"), "getStaticValue", null);
        assertThat(result).isEqualTo(123);
    }

    @Test
    public void when_getGetterExtractor_then_getterInCacheWithProperType() {
        // GIVEN
        AttributeConfig config
                = new AttributeConfig("gimmePower", "uk.org.hecaton.query.impl.getters.ExtractorsTest$PowerExtractor");
        Extractors extractors = createExtractors(config);

        // WHEN
        Getter getterFirstInvocation = extractors.getGetter(bond, "gimmePower", true);
        Getter getterSecondInvocation = extractors.getGetter(bond, "gimmePower", true);

        // THEN
        assertThat(getterFirstInvocation).isSameAs(getterSecondInvocation);
        assertThat(getterFirstInvocation).isInstanceOf(ExtractorGetter.class);
    }

    @Test
    public void when_extractExtractor_then_correctValue() {
        // GIVEN
        AttributeConfig config
                = new AttributeConfig("gimmePower", "uk.org.hecaton.query.impl.getters.ExtractorsTest$PowerExtractor");
        Extractors extractors = createExtractors(config);

        // WHEN
        Object power = extractors.extract(bond, "gimmePower", null);

        // THEN
        assertThat((Integer) power).isEqualTo(550);
    }

    @Test
    public void when_extractWithNullTarget_then_nullValue() {
        // WHEN
        Object power = createExtractors(null).extract(null, "gimmePower", null);

        // THEN
        assertNull(power);
    }

    @Test
    public void when_extractWithNullParams_then_nullValue() {
        // WHEN
        Object power = createExtractors(null).extract(null, null, null);

        // THEN
        assertNull(power);
    }

    @Test(expected = NullPointerException.class)
    public void when_extractWithNullAttributeWithNotNullTarget_then_fail() {
        createExtractors(null).extract(bond, null, null);
    }

    @Test
    public void when_creatingWithBuilder_then_evictableCacheIsUsed() {
        assertInstanceOf(EvictableGetterCache.class, Extractors.newBuilder(defaultSerService).build().getterCache);
    }

    @Test
    public void when_creatingWithBuilderWithSimpleGetterCache_then_simpleGetterCacheIsUsed() {
        Extractors extractors = Extractors.newBuilder(defaultSerService)
                                          .setGetterCacheSupplier(SIMPLE_GETTER_CACHE_SUPPLIER).build();
        assertInstanceOf(SimpleGetterCache.class, extractors.getterCache);
    }

    private Extractors createExtractors(AttributeConfig config) {
        return createExtractors(config, ReflectiveAttributeLookupPolicy.Policy.CONTROLLED_ACCESS);
    }

    private Extractors createExtractors(AttributeConfig config, ReflectiveAttributeLookupPolicy.Policy lookupPolicy) {
        Extractors.Builder builder = Extractors.newBuilder(createSerializationService(lookupPolicy.getPolicyName()));
        if (config != null) {
            builder.setAttributeConfigs(singletonList(config));
        }
        if (useClassloader) {
            builder.setClassLoader(this.getClass().getClassLoader());
        }
        return builder.build();
    }

    private InternalSerializationService createSerializationService(String lookupPolicyName) {
        Properties properties = new Properties();
        properties.setProperty(ReflectiveAttributeLookupPolicy.REFLECTIVE_ATTRIBUTE_LOOKUP_POLICY.getName(), lookupPolicyName);
        return new DefaultSerializationServiceBuilder().setVersion(InternalSerializationService.VERSION_1)
                .setProperties(new HecatonProperties(properties))
                .build();
    }

    private static class Bond {
        Car car = new Car();
    }

    private static class Car {
        int power = 550;
    }

    public static class PowerExtractor implements ValueExtractor<Bond, Object> {
        @Override
        public void extract(Bond target, Object arguments, ValueCollector collector) {
            collector.addObject(target.car.power);
        }
    }
}
