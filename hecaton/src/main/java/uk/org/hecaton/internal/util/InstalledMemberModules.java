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
package uk.org.hecaton.internal.util;

import uk.org.hecaton.instance.module.MemberModule;

import java.util.HashSet;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Names of the {@link MemberModule}s on the classpath, for {@link HecatonFeature#isAvailable()},
 * which configuration validation calls before a member exists. Looked up once, in core's class
 * loader and the thread context class loader.
 */
final class InstalledMemberModules {

    private static volatile Set<String> names;

    private InstalledMemberModules() {
    }

    static boolean contains(String module) {
        Set<String> found = names;
        if (found == null) {
            found = find();
            names = found;
        }
        return found.contains(module);
    }

    private static Set<String> find() {
        Set<String> found = new HashSet<>();
        addFrom(InstalledMemberModules.class.getClassLoader(), found);
        ClassLoader context = Thread.currentThread().getContextClassLoader();
        if (context != null) {
            addFrom(context, found);
        }
        return Set.copyOf(found);
    }

    private static void addFrom(ClassLoader classLoader, Set<String> found) {
        try {
            for (MemberModule module : ServiceLoader.load(MemberModule.class, classLoader)) {
                found.add(module.name());
            }
        } catch (ServiceConfigurationError e) {
            // a module that cannot be loaded (e.g. on a JVM older than it needs) is not installed
        }
    }
}
