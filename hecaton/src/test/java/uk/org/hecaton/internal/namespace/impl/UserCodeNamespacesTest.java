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

package uk.org.hecaton.internal.namespace.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JavaSerializationFilterConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.config.UserCodeNamespaceConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.monitor.impl.LocalUserCodeNamespaceStats;
import uk.org.hecaton.internal.namespace.UserCodeNamespaceService;
import uk.org.hecaton.map.MapLoader;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * User Code Namespaces (F17): classes from a namespace's JARs are used by the data structures
 * configured with the namespace, and only by them.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UserCodeNamespacesTest extends HecatonTestSupport {

    private static final String LOADER_CLASS = "ucn.test.Loader";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void mapUsesTheMapLoaderOfItsNamespace() throws IOException {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getNamespacesConfig().setEnabled(true)
                .addNamespaceConfig(new UserCodeNamespaceConfig("ns1").addJar(loaderJar("one"), "loader-one"));
        config.addMapConfig(mapWithLoader("map", "ns1"));
        HecatonInstance member = createHecatonInstance(config);

        assertThatThrownBy(() -> Class.forName(LOADER_CLASS)).isInstanceOf(ClassNotFoundException.class);
        assertThat(member.<String, String>getMap("map").get("key")).isEqualTo("one-key");
    }

    @Test
    public void namespacesIsolateClassesWithTheSameName() throws IOException {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getNamespacesConfig().setEnabled(true)
                .addNamespaceConfig(new UserCodeNamespaceConfig("ns1").addJar(loaderJar("one"), "loader"))
                .addNamespaceConfig(new UserCodeNamespaceConfig("ns2").addJar(loaderJar("two"), "loader"));
        config.addMapConfig(mapWithLoader("map1", "ns1"));
        config.addMapConfig(mapWithLoader("map2", "ns2"));
        HecatonInstance member = createHecatonInstance(config);

        assertThat(member.<String, String>getMap("map1").get("key")).isEqualTo("one-key");
        assertThat(member.<String, String>getMap("map2").get("key")).isEqualTo("two-key");
    }

    @Test
    public void defaultNamespaceAppliesToStructuresWithoutOne() throws IOException {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getNamespacesConfig().setEnabled(true).addNamespaceConfig(
                new UserCodeNamespaceConfig(UserCodeNamespaceService.DEFAULT_NAMESPACE_NAME).addJar(loaderJar("default"), "l"));
        config.addMapConfig(mapWithLoader("map", null));
        HecatonInstance member = createHecatonInstance(config);

        assertThat(member.<String, String>getMap("map").get("key")).isEqualTo("default-key");
    }

    @Test
    public void namespaceAddedAtRuntime() throws IOException {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getNamespacesConfig().setEnabled(true);
        HecatonInstance[] members = createHecatonInstanceFactory(2).newInstances(config, 2);

        members[0].getConfig().getNamespacesConfig()
                .addNamespaceConfig(new UserCodeNamespaceConfig("late").addJar(loaderJar("late"), "loader"));
        members[0].getConfig().addMapConfig(mapWithLoader("map", "late"));

        for (HecatonInstance member : members) {
            assertThat(getNode(member).getNamespaceService().hasNamespace("late")).isTrue();
        }
        assertThat(members[1].<String, String>getMap("map").get("key")).isEqualTo("late-key");
    }

    @Test
    public void classFilterBlocksNamespaceClasses() throws IOException {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        JavaSerializationFilterConfig filter = new JavaSerializationFilterConfig();
        filter.getBlacklist().addClasses(LOADER_CLASS);
        config.getNamespacesConfig().setEnabled(true)
                .addNamespaceConfig(new UserCodeNamespaceConfig("ns1").addJar(loaderJar("one"), "loader"));
        config.getNamespacesConfig().setClassFilterConfig(filter);
        config.addMapConfig(mapWithLoader("map", "ns1"));
        HecatonInstance member = createHecatonInstance(config);

        assertThatThrownBy(() -> member.getMap("map").get("key")).hasStackTraceContaining(LOADER_CLASS);
    }

    @Test
    public void statisticsListTheResources() throws IOException {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getNamespacesConfig().setEnabled(true).addNamespaceConfig(new UserCodeNamespaceConfig("ns1")
                .addJar(loaderJar("one"), "loader").addClass(UserCodeNamespacesTest.class));
        HecatonInstance member = createHecatonInstance(config);

        Map<String, LocalUserCodeNamespaceStats> stats = getNode(member).getNamespaceService().getStats();
        assertThat(stats).containsOnlyKeys("ns1");
        assertThat(stats.get("ns1").getResources()).containsKey("loader");
        assertThat(stats.get("ns1").getResourcesCount()).isGreaterThanOrEqualTo(2);
    }

    private static MapConfig mapWithLoader(String name, String namespace) {
        return new MapConfig(name).setUserCodeNamespace(namespace)
                .setMapStoreConfig(new MapStoreConfig().setEnabled(true).setClassName(LOADER_CLASS));
    }

    /** A JAR with a {@link MapLoader} that loads {@code <prefix>-<key>}, compiled here so that it is not on the classpath. */
    private URL loaderJar(String prefix) throws IOException {
        Path sources = folder.newFolder().toPath();
        Path source = sources.resolve("ucn/test/Loader.java");
        Files.createDirectories(source.getParent());
        Files.writeString(source, """
                package ucn.test;

                import java.util.Collection;
                import java.util.HashMap;
                import java.util.Map;

                public class Loader implements uk.org.hecaton.map.MapLoader<String, String> {
                    public String load(String key) {
                        return "%s-" + key;
                    }

                    public Map<String, String> loadAll(Collection<String> keys) {
                        Map<String, String> values = new HashMap<>();
                        for (String key : keys) {
                            values.put(key, load(key));
                        }
                        return values;
                    }

                    public Iterable<String> loadAllKeys() {
                        return null;
                    }
                }
                """.formatted(prefix), StandardCharsets.UTF_8);
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        String classpath = new File(MapLoader.class.getProtectionDomain().getCodeSource().getLocation().getPath())
                .getAbsolutePath();
        int result = compiler.run(null, null, null, "-classpath", classpath, "-d", sources.toString(), source.toString());
        assertThat(result).isZero();

        File jar = folder.newFile(prefix + ".jar");
        try (OutputStream out = Files.newOutputStream(jar.toPath()); JarOutputStream jarOut = new JarOutputStream(out)) {
            jarOut.putNextEntry(new JarEntry("ucn/test/Loader.class"));
            jarOut.write(Files.readAllBytes(sources.resolve("ucn/test/Loader.class")));
            jarOut.closeEntry();
        }
        return jar.toURI().toURL();
    }
}
