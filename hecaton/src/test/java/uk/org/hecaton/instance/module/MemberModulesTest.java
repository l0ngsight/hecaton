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

package uk.org.hecaton.instance.module;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static uk.org.hecaton.test.Accessors.getNode;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberModulesTest extends HecatonTestSupport {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void findsCapabilitiesOfModules() {
        HdModule hd = new HdModule();
        MemberModules modules = new MemberModules(List.of(hd));

        assertThat(modules.find(NativeMemoryCapability.class)).containsSame(hd);
        assertThat(modules.require(NativeMemoryCapability.class, "NATIVE in-memory format")).isSameAs(hd);
        assertThat(modules.find(PersistenceCapability.class)).isEmpty();
    }

    @Test
    public void missingCapabilityNamesTheModule() {
        MemberModules modules = new MemberModules(List.of());

        assertThatThrownBy(() -> modules.require(PersistenceCapability.class, "Persistence"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Persistence requires uk.org.hecaton:hecaton-persistence on the member's classpath");
    }

    @Test
    public void twoModulesWithTheSameCapabilityFail() {
        assertThatThrownBy(() -> new MemberModules(List.of(new HdModule(), new OtherHdModule())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Member modules hd and other-hd both provide NativeMemoryCapability; remove one of them");
    }

    @Test
    public void twoModulesWithTheSameNameFail() {
        assertThatThrownBy(() -> new MemberModules(List.of(new HdModule(), new HdModule())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Two member modules are named hd");
    }

    @Test
    public void memberLoadsAndInitializesModulesFromItsClassLoader() throws Exception {
        Path services = folder.getRoot().toPath().resolve("META-INF/services");
        Files.createDirectories(services);
        Files.writeString(services.resolve(MemberModule.class.getName()), HdModule.class.getName() + "\n");
        try (URLClassLoader classLoader = new URLClassLoader(new URL[]{folder.getRoot().toURI().toURL()},
                getClass().getClassLoader())) {
            Config config = smallInstanceConfig().setClassLoader(classLoader);
            HecatonInstance instance = createHecatonInstance(config);

            Node node = getNode(instance);
            HdModule module = (HdModule) node.getMemberModules().require(NativeMemoryCapability.class, "test");
            assertThat(module.initializedWith).isSameAs(node);
        }
    }

    public static class HdModule implements MemberModule, NativeMemoryCapability {

        volatile Node initializedWith;

        @Override
        public String name() {
            return "hd";
        }

        @Override
        public void init(Node node) {
            initializedWith = node;
        }
    }

    public static class OtherHdModule implements MemberModule, NativeMemoryCapability {

        @Override
        public String name() {
            return "other-hd";
        }
    }
}
