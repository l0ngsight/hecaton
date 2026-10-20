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

package uk.org.hecaton.test;

import org.reflections.Reflections;
import org.reflections.Store;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.QueryFunction;

import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.List;
import java.util.function.Predicate;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

import static uk.org.hecaton.jet.impl.util.Util.uncheckCall;
import static org.reflections.scanners.Scanners.SubTypes;

/**
 * Initialize the org.reflections library once to avoid duplicate work on scanning the classpath from individual tests.
 */
public final class ReflectionsHelper {

    public static final Reflections REFLECTIONS;

    /**
     * Excludes classes that does not belong to {@code uk.org.hecaton} package or
     * belongs to {@code uk.org.hecaton.internal.json} package.
     */
    public static final Predicate<String> EXCLUDE_NON_HECATON_CLASSES =
            c -> c.startsWith("uk.org.hecaton") && !c.startsWith("uk.org.hecaton.internal.json");

    /**
     * Excludes enums, interfaces, abstract and anonymous classes.
     */
    public static final Predicate<Class<?>> EXCLUDE_NON_CONCRETE_CLASSES =
            c -> !(Enum.class.isAssignableFrom(c) || c.isAnonymousClass()
                    || c.isInterface() || Modifier.isAbstract(c.getModifiers()));

    /**
     * {@link ClassLoader#getResources(String)} returns the latest versioned entry
     * from multi-release JARs, which has prefix {@code META-INF/versions/{n}/}.
     *
     * @see JarFile#getJarEntry
     * @see <a href="https://openjdk.org/jeps/238">JEP 238: Multi-Release JAR Files</a>
     */
    private static final Pattern VERSIONED_CLASS_PREFIX = Pattern.compile("META-INF/versions/\\d+/$");

    static {
        // Obtain all classpath URLs with uk.org.hecaton package classes.
        List<URL> comHecatonPackageURLs = ClasspathHelper.forPackage("uk.org.hecaton").stream()
                .map(URL::toString)
                // Exclude hecaton test artifacts (hecaton-*-tests.jar, **/target/test-classes/)
                // and hecaton-license-extractor artifact from package URLs.
                .filter(url -> !url.contains("-tests.jar") && !url.contains("target/test-classes")
                        && !url.contains("hecaton-license-extractor"))
                // Fix URL for multi-release JARs (hecaton-*.jar!/META-INF/versions/*/ [uk/org/hecaton/**]).
                .map(url -> VERSIONED_CLASS_PREFIX.matcher(url).replaceAll(""))
                .map(url -> uncheckCall(() -> new URL(url)))
                .toList();

        REFLECTIONS = new Reflections(new ConfigurationBuilder().addUrls(comHecatonPackageURLs));
    }

    private ReflectionsHelper() { }

    /**
     * Excludes {@linkplain #EXCLUDE_NON_HECATON_CLASSES non-Hecaton classes} from the results.
     */
    @SuppressWarnings({"ClassGetClass", "RedundantCast", "unchecked"})
    public static <T> QueryFunction<Store, Class<? extends T>> subTypesOf(Class<T> type) {
        return SubTypes.of(type)
                .filter(EXCLUDE_NON_HECATON_CLASSES)
                .as((Class<Class<? extends T>>) type.getClass());
    }

    /**
     * Excludes {@linkplain #EXCLUDE_NON_HECATON_CLASSES non-Hecaton classes} and
     * {@linkplain #EXCLUDE_NON_CONCRETE_CLASSES non-concrete classes} from the results.
     */
    public static <T> QueryFunction<Store, Class<? extends T>> concreteSubTypesOf(Class<T> type) {
        return subTypesOf(type)
                .filter(EXCLUDE_NON_CONCRETE_CLASSES);
    }
}
