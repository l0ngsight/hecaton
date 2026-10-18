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

package uk.org.hecaton.spi.properties;

import tools.jackson.core.type.TypeReference;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.internal.diagnostics.HealthMonitorLevel;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.net.URI;

import static uk.org.hecaton.spi.properties.ClusterProperty.ENTERPRISE_LICENSE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.StringUtils;
import tools.jackson.databind.json.JsonMapper;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class HecatonPropertiesTest {

    private final Config config = new Config();
    private final HecatonProperties defaultProperties = new HecatonProperties(config);

    @Test
    public void testNullProperties() {
        HecatonProperties properties = new HecatonProperties((Properties) null);

        assertTrue(properties.keySet().isEmpty());
    }

    @Test
    public void testKeySet_whenPropertiesAvailable() {
        Properties props = new Properties();
        props.setProperty("key1", "value1");
        props.setProperty("key2", "value2");
        HecatonProperties properties = new HecatonProperties(props);

        assertEquals(props.keySet(), properties.keySet());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKeySet_isImmutable() {
        HecatonProperties properties = new HecatonProperties(config);

        properties.keySet().remove("foo");
    }

    @Test(expected = NullPointerException.class)
    public void testGet_whenKeyNull() {
        HecatonProperties properties = new HecatonProperties(config);

        properties.get(null);
    }

    @Test
    public void testGet_whenKeyNotExisting() {
        Properties props = new Properties();
        props.setProperty("key1", "value1");
        props.setProperty("key2", "value2");
        HecatonProperties properties = new HecatonProperties(props);

        assertNull(properties.get("nonExistingKey"));
    }

    @Test
    public void testGet_whenKeyExisting() {
        Properties props = new Properties();
        props.setProperty("key1", "value1");
        props.setProperty("key2", "value2");
        HecatonProperties properties = new HecatonProperties(props);

        assertEquals("value1", properties.get("key1"));
    }

    @Test
    public void testGet_whenValueNotString() {
        // given a "compromised" Properties object
        Properties props = new Properties();
        props.put("key", 1);

        // HecatonProperties.get returns null
        HecatonProperties hzProperties = new HecatonProperties(props);
        assertNull(hzProperties.get("key"));
    }

    @Test
    public void testGet_whenFunctionAvailable_andNoOtherSettings() {
        Properties props = new Properties();
        HecatonProperty p = new HecatonProperty("key", (Function<HecatonProperties, Integer>) properties -> 23);
        HecatonProperties properties = new HecatonProperties(props);

        assertEquals(23, properties.getInteger(p));
    }

    @Test
    public void testGet_whenFunctionAvailable_andPropertySet() {
        Properties props = new Properties();
        props.setProperty("key", "1");
        HecatonProperty p = new HecatonProperty("key", (Function<HecatonProperties, Integer>) properties -> 23);
        HecatonProperties properties = new HecatonProperties(props);
        assertEquals(1, properties.getInteger(p));
    }

    @Test
    public void setProperty_ensureHighestPriorityOfConfig() {
        config.setProperty(ENTERPRISE_LICENSE_KEY.getName(), "configValue");
        ENTERPRISE_LICENSE_KEY.setSystemProperty("systemValue");

        HecatonProperties properties = new HecatonProperties(config);
        String value = properties.getString(ENTERPRISE_LICENSE_KEY);

        System.clearProperty(ENTERPRISE_LICENSE_KEY.getName());

        assertEquals("configValue", value);
    }

    @Test
    public void setProperty_ensureUsageOfSystemProperty() {
        ENTERPRISE_LICENSE_KEY.setSystemProperty("systemValue");

        HecatonProperties hecatonProperties = new HecatonProperties(config);
        String value = hecatonProperties.getString(ENTERPRISE_LICENSE_KEY);

        System.clearProperty(ENTERPRISE_LICENSE_KEY.getName());

        assertEquals("systemValue", value);
    }

    @Test
    public void setProperty_ensureUsageOfDefaultValue() {
        String value = defaultProperties.getString(ENTERPRISE_LICENSE_KEY);

        assertNull(value);
    }

    @Test
    public void setProperty_inheritDefaultValueOfParentProperty() {
        HecatonProperty parent = new HecatonProperty("parent", 1);
        HecatonProperty child = new HecatonProperty("child", parent);

        assertEquals(1, defaultProperties.getInteger(child));
    }

    @Test
    public void setProperty_inheritActualValueOfParentProperty() {
        config.setProperty(ClusterProperty.IO_THREAD_COUNT.getName(), "1");
        HecatonProperties properties = new HecatonProperties(config);

        String inputIOThreadCount = properties.getString(ClusterProperty.IO_INPUT_THREAD_COUNT);

        assertEquals("1", inputIOThreadCount);
        assertNotEquals(ClusterProperty.IO_THREAD_COUNT.getDefaultValue(), inputIOThreadCount);
    }

    @Test
    public void getSystemProperty() {
        ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.setSystemProperty("12");

        assertEquals("12", ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getSystemProperty());

        System.clearProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName());
    }

    @Test
    public void getBoolean() {
        HecatonProperty property = new HecatonProperty("foo", "true");

        boolean isHumanReadable = defaultProperties.getBoolean(property);

        assertTrue(isHumanReadable);
    }

    @Test
    public void getInteger() {
        HecatonProperty property = new HecatonProperty("key", 3);
        int ioThreadCount = defaultProperties.getInteger(property);

        assertEquals(3, ioThreadCount);
    }

    @Test
    public void getLong() {
        long lockMaxLeaseTimeSeconds = defaultProperties.getLong(ClusterProperty.LOCK_MAX_LEASE_TIME_SECONDS);

        assertEquals(Long.MAX_VALUE, lockMaxLeaseTimeSeconds);
    }

    @Test
    public void getFloat() {
        HecatonProperty property = new HecatonProperty("foo", 10.1F);

        float foo = defaultProperties.getFloat(property);

        assertEquals(10.1F, foo, 0.0001);
    }

    @Test
    public void getDouble() {
        HecatonProperty property = new HecatonProperty("foo", 10.1D);
        double foo = defaultProperties.getDouble(property);

        assertEquals(10.1D, foo, 0.0001);
    }

    @Test
    public void getPositiveMillisOrDefault() {
        String name = ClusterProperty.PARTITION_TABLE_SEND_INTERVAL.getName();
        config.setProperty(name, "-300");
        HecatonProperty property = new HecatonProperty(name, "20", TimeUnit.MILLISECONDS);

        long millis = defaultProperties.getPositiveMillisOrDefault(property);

        assertEquals(20, millis);
    }

    @Test
    public void getPositiveMillisOrDefaultWithManualDefault() {
        String name = ClusterProperty.PARTITION_TABLE_SEND_INTERVAL.getName();
        config.setProperty(name, "-300");
        HecatonProperties properties = new HecatonProperties(config);
        HecatonProperty property = new HecatonProperty(name, "20", TimeUnit.MILLISECONDS);

        long millis = properties.getPositiveMillisOrDefault(property, 50);

        assertEquals(50, millis);
    }

    @Test
    public void getTimeUnit() {
        config.setProperty(ClusterProperty.PARTITION_TABLE_SEND_INTERVAL.getName(), "300");
        HecatonProperties properties = new HecatonProperties(config);

        assertEquals(300, properties.getSeconds(ClusterProperty.PARTITION_TABLE_SEND_INTERVAL));
    }

    @Test
    public void getTimeUnit_default() {
        long expectedSeconds = 15;

        long intervalNanos = defaultProperties.getNanos(ClusterProperty.PARTITION_TABLE_SEND_INTERVAL);
        long intervalMillis = defaultProperties.getMillis(ClusterProperty.PARTITION_TABLE_SEND_INTERVAL);
        long intervalSeconds = defaultProperties.getSeconds(ClusterProperty.PARTITION_TABLE_SEND_INTERVAL);

        assertEquals(TimeUnit.SECONDS.toNanos(expectedSeconds), intervalNanos);
        assertEquals(TimeUnit.SECONDS.toMillis(expectedSeconds), intervalMillis);
        assertEquals(expectedSeconds, intervalSeconds);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getTimeUnit_noTimeUnitProperty() {
        defaultProperties.getMillis(ClusterProperty.EVENT_THREAD_COUNT);
    }

    @Test
    public void getEnum() {
        config.setProperty(ClusterProperty.HEALTH_MONITORING_LEVEL.getName(), "NOISY");
        HecatonProperties properties = new HecatonProperties(config.getProperties());
        HealthMonitorLevel healthMonitorLevel = properties
                .getEnum(ClusterProperty.HEALTH_MONITORING_LEVEL, HealthMonitorLevel.class);

        assertEquals(HealthMonitorLevel.NOISY, healthMonitorLevel);
    }

    @Test
    public void getEnum_default() {
        HecatonProperties properties = new HecatonProperties(config.getProperties());
        HealthMonitorLevel healthMonitorLevel = properties
                .getEnum(ClusterProperty.HEALTH_MONITORING_LEVEL, HealthMonitorLevel.class);

        assertEquals(HealthMonitorLevel.SILENT, healthMonitorLevel);
    }

    @Test
    public void getString_whenDeprecatedNameUsed() {
        Properties props = new Properties();
        props.setProperty("oldname", "10");
        HecatonProperties properties = new HecatonProperties(props);

        HecatonProperty property = new HecatonProperty("newname")
                .setDeprecatedName("oldname");
        String value = properties.getString(property);

        assertEquals("10", value);
    }

    /**
     * Prints to the console any {@link HecatonProperty}'s where an alternative British English version of their
     * {@link HecatonProperty#name} or {@link HecatonProperty#deprecatedName} exists.
     * <p>
     * e.g. {@code hecaton.enterprise.license.key} vs {@code hecaton.enterprise.licence.key}
     *
     * @see <a href="https://github.com/hazelcast/hazelcast/pull/25460">Print a warning when British spelling of "License" is
     *      used</a>
     */
    public static void main(final String[] args) throws Exception {
        // Load translation data
        // https://github.com/hyperreality/American-British-English-Translator/
        URI uri = URI.create(
                "https://raw.githubusercontent.com/hyperreality/American-British-English-Translator/master/data/american_spellings.json");

        Map<String, String> rawMap;
        try (var is = uri.toURL().openStream()) {
            rawMap = new JsonMapper().readValue(is, new TypeReference<>() {
            });
        }

        final Map<String, String> americanToBritish = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        americanToBritish.putAll(rawMap);

        final UnaryOperator<String> translateStringToBritishEnglish = str -> {
            final String translated = americanToBritish.get(str);

            if (translated == null) {
                return str;
            } else {
                // Highlight difference in red
                return "\u001B[31m" + translated + "\u001B[0m";
            }
        };

        final Pattern delimiterPattern = Pattern.compile("([^\\w])");
        final Function<HecatonProperty, String> translatePropertyToBritishEnglish = property -> {
            // Generate a British English version of the properties' name
            final Matcher matcher = delimiterPattern.matcher(property.getName());
            final StringBuffer builder = new StringBuffer();

            int prevEnd = 0;
            while (matcher.find()) {
                builder.append(translateStringToBritishEnglish.apply(property.getName().substring(prevEnd, matcher.start())))
                        .append(matcher.group(1));
                prevEnd = matcher.end();
            }
            builder.append(translateStringToBritishEnglish.apply(property.getName().substring(prevEnd)));

            return builder.toString();
        };

        // Ideally you'd use the Reflections library to do this, but this struggles to return *everything*:
        // https://github.com/ronmamo/reflections/issues/385
        // Instead use Guava:
        // https://www.baeldung.com/java-find-all-classes-in-package#3-google-guava-library
        final Stream<Class<?>> allHecatonClasses = ClassPath.from(ClassLoader.getSystemClassLoader()).getAllClasses()
                .parallelStream().filter(clazz -> clazz.getPackageName().startsWith("uk.org.hecaton")).map(ClassInfo::load);

        final Stream<HecatonProperty> hecatonProperties = allHecatonClasses.map(Class::getDeclaredFields)
                .flatMap(Arrays::stream).filter(field -> HecatonProperty.class.isAssignableFrom(field.getType())
                        && Modifier.isStatic(field.getModifiers()))
                .map(field -> {
                    try {
                        field.setAccessible(true);
                        return field.get(null);
                    } catch (final ReflectiveOperationException e) {
                        throw new RuntimeException(e);
                    }
                }).map(HecatonProperty.class::cast);

        // Print the output
        int columnWidth = 50;
        final String format = "%-" + columnWidth + "s | %-" + columnWidth + "s%n";
        System.out.printf(format, "Property Name", "Suggested British English Property Name");
        System.out.printf(format, StringUtils.repeat("-", columnWidth), StringUtils.repeat("-", columnWidth));

        hecatonProperties.sorted(Comparator.comparing(HecatonProperty::getName)).forEachOrdered(property -> {
            final String britishEnglishPropertyTranslation = translatePropertyToBritishEnglish.apply(property);

            // If the British English translation differs from the properties' name / deprecated name, log it
            if (!StringUtils.equalsIgnoreCase(property.getName(), britishEnglishPropertyTranslation)
                    && !StringUtils.equalsIgnoreCase(property.getDeprecatedName(), britishEnglishPropertyTranslation)) {
                System.out.printf(format, property.toString(), britishEnglishPropertyTranslation);
            }
        });
    }
}
