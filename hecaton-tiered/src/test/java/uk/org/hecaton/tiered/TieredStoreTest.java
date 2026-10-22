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

package uk.org.hecaton.tiered;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.LocalDeviceConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.config.TieredStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.module.TieredStoreCapability;
import uk.org.hecaton.internal.util.HecatonFeature;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.memory.Capacity;
import uk.org.hecaton.memory.MemoryUnit;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class TieredStoreTest extends HecatonTestSupport {

    private static final int COUNT = 3000;
    private static final int VALUE_SIZE = 1000;
    private static final long MEMORY_TIER = 512 * 1024;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final TestHecatonFactory factory = new TestHecatonFactory();
    private File baseDir;

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void featureIsAvailable() {
        assertThat(HecatonFeature.TIERED_STORE.isAvailable()).isTrue();
    }

    @Test
    public void valuesBeyondTheMemoryTierAreOnDisk() throws Exception {
        HecatonInstance member = factory.newHecatonInstance(config());
        IMap<Integer, String> map = member.getMap("tiered");
        fill(map);
        assertThat(map.size()).isEqualTo(COUNT);
        MemoryTierBudget budget = module(member).budget("tiered");
        // the memory tier holds at most one value per partition beyond its capacity
        assertThat(budget.used()).isLessThanOrEqualTo(MEMORY_TIER + 31L * (VALUE_SIZE + 100));
        assertThat(tierFiles()).isNotEmpty();
        for (int i = 0; i < COUNT; i++) {
            assertThat(map.get(i)).isEqualTo(value(i));
        }
        assertThat(map.getAll(Set.of(1, 2, 3))).containsEntry(2, value(2));
        assertThat(map.values(Predicates.sql("this like '7-%'"))).hasSize(map.keySet(Predicates.sql("this like '7-%'")).size())
                .contains(value(7));
        map.executeOnEntries(entry -> entry.setValue(entry.getValue().toUpperCase()));
        assertThat(map.get(COUNT - 1)).isEqualTo(value(COUNT - 1).toUpperCase());
        assertThat(map.get(0)).isEqualTo(value(0).toUpperCase());
    }

    @Test
    public void updatesRemovalsClearAndDestroy() throws Exception {
        HecatonInstance member = factory.newHecatonInstance(config());
        IMap<Integer, String> map = member.getMap("tiered");
        fill(map);
        for (int i = 0; i < COUNT; i += 2) {
            map.put(i, "updated" + i);
        }
        for (int i = 1; i < COUNT; i += 4) {
            map.delete(i);
        }
        assertThat(map.size()).isEqualTo(COUNT - COUNT / 4);
        assertThat(map.get(0)).isEqualTo("updated0");
        assertThat(map.get(3)).isEqualTo(value(3));
        assertThat(map.get(5)).isNull();

        map.clear();
        assertThat(map.size()).isZero();
        assertThat(module(member).budget("tiered").used()).isZero();
        assertThat(tierFiles()).isEmpty();

        fill(map);
        map.destroy();
        assertThat(module(member).budget("tiered").used()).isZero();
        assertThat(tierFiles()).isEmpty();
    }

    @Test
    public void backupsMigrationsAndClients() {
        HecatonInstance first = factory.newHecatonInstance(config());
        HecatonInstance client = factory.newHecatonClient();
        IMap<Integer, String> map = client.getMap("tiered");
        fill(map);
        HecatonInstance second = factory.newHecatonInstance(config());
        HecatonInstance third = factory.newHecatonInstance(config());
        waitAllForSafeState(first, second, third);
        first.getLifecycleService().terminate();
        waitAllForSafeState(second, third);
        assertThat(map.size()).isEqualTo(COUNT);
        for (int i = 0; i < COUNT; i++) {
            assertThat(map.get(i)).isEqualTo(value(i));
        }
    }

    @Test
    public void directoriesAreDeleted() throws Exception {
        HecatonInstance member = factory.newHecatonInstance(config());
        fill(member.getMap("tiered"));
        Path root = baseDir.toPath().resolve("hecaton-tiered");
        Path own = root.resolve(getNode(member).getThisUuid().toString());
        assertThat(own).exists();
        // the directory of a member that crashed
        Path stale = Files.createDirectories(root.resolve("stale-member"));
        Files.writeString(stale.resolve("0-0.tier"), "x");
        member.shutdown();
        assertThat(own).doesNotExist();
        member = factory.newHecatonInstance(config());
        fill(member.getMap("tiered"));
        assertThat(stale).doesNotExist();
    }

    @Test
    public void configurationIsValidated() throws Exception {
        Config config = config();
        config.getMapConfig("tiered").getTieredStoreConfig().getDiskTierConfig().setDeviceName("missing");
        assertThatThrownBy(() -> factory.newHecatonInstance(config).getMap("tiered").put(1, "x"))
                .isInstanceOf(InvalidConfigurationException.class).hasMessageContaining("missing");
    }

    private Config config() {
        if (baseDir == null) {
            try {
                baseDir = folder.newFolder();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "31");
        config.setNativeMemoryConfig(new NativeMemoryConfig().setEnabled(true)
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD)
                .setCapacity(new Capacity(128, MemoryUnit.MEGABYTES)));
        config.addDeviceConfig(new LocalDeviceConfig().setName("disk").setBaseDir(baseDir));
        TieredStoreConfig tiered = new TieredStoreConfig().setEnabled(true);
        tiered.getMemoryTierConfig().setCapacity(Capacity.of(MEMORY_TIER, MemoryUnit.BYTES));
        tiered.getDiskTierConfig().setEnabled(true).setDeviceName("disk");
        config.addMapConfig(new MapConfig("tiered").setInMemoryFormat(InMemoryFormat.NATIVE).setTieredStoreConfig(tiered));
        return config;
    }

    private static TieredModule module(HecatonInstance member) {
        return (TieredModule) getNode(member).getMemberModules().find(TieredStoreCapability.class).orElseThrow();
    }

    private static void fill(IMap<Integer, String> map) {
        Map<Integer, String> batch = new HashMap<>();
        for (int i = 0; i < COUNT; i++) {
            if (i < COUNT / 2) {
                map.put(i, value(i));
            } else {
                batch.put(i, value(i));
            }
        }
        map.putAll(batch);
    }

    private static String value(int i) {
        return (i + "-").repeat(VALUE_SIZE).substring(0, VALUE_SIZE);
    }

    private List<Path> tierFiles() throws Exception {
        try (Stream<Path> files = Files.walk(baseDir.toPath())) {
            return files.filter(path -> path.toString().endsWith(".tier")).toList();
        }
    }
}
