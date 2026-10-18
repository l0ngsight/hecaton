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

package uk.org.hecaton.internal.dynamicconfig;

import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.CardinalityEstimatorConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DurableExecutorConfig;
import uk.org.hecaton.config.ExecutorConfig;
import uk.org.hecaton.config.FlakeIdGeneratorConfig;
import uk.org.hecaton.config.ListConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MultiMapConfig;
import uk.org.hecaton.config.PNCounterConfig;
import uk.org.hecaton.config.QueueConfig;
import uk.org.hecaton.config.ReliableTopicConfig;
import uk.org.hecaton.config.ReplicatedMapConfig;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.config.ScheduledExecutorConfig;
import uk.org.hecaton.config.SetConfig;
import uk.org.hecaton.config.TopicConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.UUID;

import static uk.org.hecaton.spi.properties.ClusterProperty.SEARCH_DYNAMIC_CONFIG_FIRST;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigSearchTest extends HecatonTestSupport {

    private static final String STATIC_NAME = "my.custom.data.*";
    private static final String DYNAMIC_NAME = "my.custom.data.cache";
    private static final int STATIC_MAX_SIZE = 2;
    private static final int DYNAMIC_MAX_SIZE = 3;

    private HecatonInstance hecatonInstance;

    private void testTemplate(TestCase<?> testCase) {
        Config staticHecatonConfig = getConfig();
        if (testCase.isDynamicFirst()) {
            staticHecatonConfig.setProperty(SEARCH_DYNAMIC_CONFIG_FIRST.getName(), "true");
        }
        String uuid = UUID.randomUUID().toString();
        staticHecatonConfig.setInstanceName(uuid);
        staticHecatonConfig.setClusterName(uuid);
        testCase.addStaticConfig(staticHecatonConfig);

        hecatonInstance = createHecatonInstance(staticHecatonConfig);
        testCase.addDynamicConfig(hecatonInstance);

        testCase.asserts();
    }

    @Test
    public void testMapConfig_Static() {
        TestCase<MapConfig> testCase = new TestCase<>(staticMapConfig(), dynamicMapConfig(), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addMapConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addMapConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                MapConfig dataConfig = hecatonInstance.getConfig().findMapConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getMaxIdleSeconds()).isEqualTo(staticMapConfig().getMaxIdleSeconds());
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testMapConfig_Dynamic() {
        TestCase<MapConfig> testCase = new TestCase<>(staticMapConfig(), dynamicMapConfig(), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addMapConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addMapConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                MapConfig dataConfig = hecatonInstance.getConfig().findMapConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getMaxIdleSeconds()).isEqualTo(dynamicMapConfig().getMaxIdleSeconds());
            }
        };
        testTemplate(testCase);
    }

    @Nonnull
    private static MapConfig dynamicMapConfig() {
        return new MapConfig(DYNAMIC_NAME).setMaxIdleSeconds(13);
    }

    @Nonnull
    private static MapConfig staticMapConfig() {
        return new MapConfig(STATIC_NAME).setMaxIdleSeconds(15);
    }

    @Test
    public void testCacheConfig_Static() {
        TestCase<CacheSimpleConfig> testCase = new TestCase<>(new CacheSimpleConfig().setName(STATIC_NAME).setBackupCount(STATIC_MAX_SIZE),
                new CacheSimpleConfig().setName(DYNAMIC_NAME).setBackupCount(DYNAMIC_MAX_SIZE), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addCacheConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addCacheConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                CacheSimpleConfig dataConfig = hecatonInstance.getConfig().findCacheConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getBackupCount()).isEqualTo(STATIC_MAX_SIZE);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testCacheConfig_Dynamic() {
        TestCase<CacheSimpleConfig> testCase = new TestCase<>(new CacheSimpleConfig().setName(STATIC_NAME).setBackupCount(STATIC_MAX_SIZE),
                new CacheSimpleConfig().setName(DYNAMIC_NAME).setBackupCount(DYNAMIC_MAX_SIZE), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addCacheConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addCacheConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                CacheSimpleConfig dataConfig = hecatonInstance.getConfig().findCacheConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getBackupCount()).isEqualTo(DYNAMIC_MAX_SIZE);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testQueueConfig_Static() {
        TestCase<QueueConfig> testCase = new TestCase<>(new QueueConfig().setName(STATIC_NAME).setMaxSize(STATIC_MAX_SIZE),
                new QueueConfig().setName(DYNAMIC_NAME).setMaxSize(DYNAMIC_MAX_SIZE), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addQueueConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addQueueConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                QueueConfig dataConfig = hecatonInstance.getConfig().findQueueConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getMaxSize()).isEqualTo(STATIC_MAX_SIZE);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testQueueConfig_Dynamic() {
        TestCase<QueueConfig> testCase = new TestCase<>(new QueueConfig().setName(STATIC_NAME).setMaxSize(STATIC_MAX_SIZE),
                new QueueConfig().setName(DYNAMIC_NAME).setMaxSize(DYNAMIC_MAX_SIZE), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addQueueConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addQueueConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                QueueConfig dataConfig = hecatonInstance.getConfig().findQueueConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getMaxSize()).isEqualTo(DYNAMIC_MAX_SIZE);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testListConfig_Static() {
        TestCase<ListConfig> testCase = new TestCase<>(new ListConfig().setName(STATIC_NAME).setMaxSize(STATIC_MAX_SIZE),
                new ListConfig().setName(DYNAMIC_NAME).setMaxSize(DYNAMIC_MAX_SIZE), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addListConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addListConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ListConfig dataConfig = hecatonInstance.getConfig().findListConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getMaxSize()).isEqualTo(STATIC_MAX_SIZE);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testListConfig_Dynamic() {
        TestCase<ListConfig> testCase = new TestCase<>(new ListConfig().setName(STATIC_NAME).setMaxSize(STATIC_MAX_SIZE),
                new ListConfig().setName(DYNAMIC_NAME).setMaxSize(DYNAMIC_MAX_SIZE), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addListConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addListConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ListConfig dataConfig = hecatonInstance.getConfig().findListConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getMaxSize()).isEqualTo(DYNAMIC_MAX_SIZE);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testSetConfig_Static() {
        TestCase<SetConfig> testCase = new TestCase<>(new SetConfig().setName(STATIC_NAME),
                new SetConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addSetConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addSetConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                SetConfig dataConfig = hecatonInstance.getConfig().findSetConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(STATIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testSetConfig_Dynamic() {
        TestCase<SetConfig> testCase = new TestCase<>(new SetConfig().setName(STATIC_NAME),
                new SetConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addSetConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addSetConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                SetConfig dataConfig = hecatonInstance.getConfig().findSetConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(DYNAMIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testMultiMapConfig_Static() {
        TestCase<MultiMapConfig> testCase = new TestCase<>(new MultiMapConfig().setName(STATIC_NAME),
                new MultiMapConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addMultiMapConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addMultiMapConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                MultiMapConfig dataConfig = hecatonInstance.getConfig().findMultiMapConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(STATIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testMultiMapConfig_Dynamic() {
        TestCase<MultiMapConfig> testCase = new TestCase<>(new MultiMapConfig().setName(STATIC_NAME),
                new MultiMapConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addMultiMapConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addMultiMapConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                MultiMapConfig dataConfig = hecatonInstance.getConfig().findMultiMapConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(DYNAMIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testReplicatedMapConfig_Static() {
        TestCase<ReplicatedMapConfig> testCase = new TestCase<>(new ReplicatedMapConfig().setName(STATIC_NAME),
                new ReplicatedMapConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addReplicatedMapConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addReplicatedMapConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ReplicatedMapConfig dataConfig = hecatonInstance.getConfig().findReplicatedMapConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(STATIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testReplicatedMapConfig_Dynamic() {
        TestCase<ReplicatedMapConfig> testCase = new TestCase<>(new ReplicatedMapConfig().setName(STATIC_NAME),
                new ReplicatedMapConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addReplicatedMapConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addReplicatedMapConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ReplicatedMapConfig dataConfig = hecatonInstance.getConfig().findReplicatedMapConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(DYNAMIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testRingbufferConfig_Static() {
        TestCase<RingbufferConfig> testCase = new TestCase<>(new RingbufferConfig().setName(STATIC_NAME),
                new RingbufferConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addRingBufferConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addRingBufferConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                RingbufferConfig dataConfig = hecatonInstance.getConfig().findRingbufferConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(STATIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testRingbufferConfig_Dynamic() {
        TestCase<RingbufferConfig> testCase = new TestCase<>(new RingbufferConfig().setName(STATIC_NAME),
                new RingbufferConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addRingBufferConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addRingBufferConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                RingbufferConfig dataConfig = hecatonInstance.getConfig().findRingbufferConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(DYNAMIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testTopicConfig_Static() {
        TestCase<TopicConfig> testCase = new TestCase<>(new TopicConfig().setName(STATIC_NAME),
                new TopicConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addTopicConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addTopicConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                TopicConfig dataConfig = hecatonInstance.getConfig().findTopicConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(STATIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testTopicConfig_Dynamic() {
        TestCase<TopicConfig> testCase = new TestCase<>(new TopicConfig().setName(STATIC_NAME),
                new TopicConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addTopicConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addTopicConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                TopicConfig dataConfig = hecatonInstance.getConfig().findTopicConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(DYNAMIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testReliableTopicConfig_Static() {
        TestCase<ReliableTopicConfig> testCase = new TestCase<>(new ReliableTopicConfig(STATIC_NAME),
                new ReliableTopicConfig(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addReliableTopicConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addReliableTopicConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ReliableTopicConfig dataConfig = hecatonInstance.getConfig().findReliableTopicConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(STATIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testReliableTopicConfig_Dynamic() {
        TestCase<ReliableTopicConfig> testCase = new TestCase<>(new ReliableTopicConfig(STATIC_NAME),
                new ReliableTopicConfig(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addReliableTopicConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addReliableTopicConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ReliableTopicConfig dataConfig = hecatonInstance.getConfig().findReliableTopicConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(DYNAMIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testExecutorConfig_Static() {
        TestCase<ExecutorConfig> testCase = new TestCase<>(new ExecutorConfig(STATIC_NAME),
                new ExecutorConfig(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addExecutorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addExecutorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ExecutorConfig dataConfig = hecatonInstance.getConfig().findExecutorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(STATIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testExecutorConfig_Dynamic() {
        TestCase<ExecutorConfig> testCase = new TestCase<>(new ExecutorConfig(STATIC_NAME),
                new ExecutorConfig(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addExecutorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addExecutorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ExecutorConfig dataConfig = hecatonInstance.getConfig().findExecutorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(DYNAMIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testDurableExecutorConfig_Static() {
        TestCase<DurableExecutorConfig> testCase = new TestCase<>(
                new DurableExecutorConfig().setName(STATIC_NAME),
                new DurableExecutorConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addDurableExecutorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addDurableExecutorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                DurableExecutorConfig dataConfig = hecatonInstance.getConfig().findDurableExecutorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(STATIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testDurableExecutorConfig_Dynamic() {
        TestCase<DurableExecutorConfig> testCase = new TestCase<>(
                new DurableExecutorConfig().setName(STATIC_NAME), new DurableExecutorConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addDurableExecutorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addDurableExecutorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                DurableExecutorConfig dataConfig = hecatonInstance.getConfig().findDurableExecutorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(DYNAMIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testScheduledExecutorConfig_Static() {
        TestCase<ScheduledExecutorConfig> testCase = new TestCase<>(
                new ScheduledExecutorConfig(STATIC_NAME), new ScheduledExecutorConfig(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addScheduledExecutorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addScheduledExecutorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ScheduledExecutorConfig dataConfig = hecatonInstance.getConfig().findScheduledExecutorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(STATIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testScheduledExecutorConfig_Dynamic() {
        TestCase<ScheduledExecutorConfig> testCase = new TestCase<>(
                new ScheduledExecutorConfig(STATIC_NAME), new ScheduledExecutorConfig(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addScheduledExecutorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addScheduledExecutorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ScheduledExecutorConfig dataConfig = hecatonInstance.getConfig().findScheduledExecutorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(DYNAMIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testCardinalityEstimatorConfig_Static() {
        TestCase<CardinalityEstimatorConfig> testCase = new TestCase<>(
                new CardinalityEstimatorConfig().setName(STATIC_NAME),
                new CardinalityEstimatorConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addCardinalityEstimatorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addCardinalityEstimatorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                CardinalityEstimatorConfig dataConfig
                        = hecatonInstance.getConfig().findCardinalityEstimatorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(STATIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testCardinalityEstimatorConfig_Dynamic() {
        TestCase<CardinalityEstimatorConfig> testCase = new TestCase<>(
                new CardinalityEstimatorConfig().setName(STATIC_NAME),
                new CardinalityEstimatorConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addCardinalityEstimatorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addCardinalityEstimatorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                CardinalityEstimatorConfig dataConfig
                        = hecatonInstance.getConfig().findCardinalityEstimatorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(DYNAMIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testReliableIdGeneratorConfig_Static() {
        TestCase<FlakeIdGeneratorConfig> testCase = new TestCase<>(new FlakeIdGeneratorConfig(STATIC_NAME),
                new FlakeIdGeneratorConfig(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addFlakeIdGeneratorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addFlakeIdGeneratorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                FlakeIdGeneratorConfig dataConfig = hecatonInstance.getConfig().findFlakeIdGeneratorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(STATIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testReliableIdGeneratorConfig_Dynamic() {
        TestCase<FlakeIdGeneratorConfig> testCase = new TestCase<>(new FlakeIdGeneratorConfig(STATIC_NAME),
                new FlakeIdGeneratorConfig(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addFlakeIdGeneratorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addFlakeIdGeneratorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                FlakeIdGeneratorConfig dataConfig = hecatonInstance.getConfig().findFlakeIdGeneratorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(DYNAMIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testPNCounterConfig_Static() {
        TestCase<PNCounterConfig> testCase = new TestCase<>(new PNCounterConfig(STATIC_NAME),
                new PNCounterConfig(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addPNCounterConfig(this.staticConfig);

            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addPNCounterConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                PNCounterConfig dataConfig = hecatonInstance.getConfig().findPNCounterConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(STATIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testPNCounterConfig_Dynamic() {
        TestCase<PNCounterConfig> testCase = new TestCase<>(new PNCounterConfig(STATIC_NAME),
                new PNCounterConfig(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addPNCounterConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HecatonInstance hecatonInstance) {
                hecatonInstance.getConfig().addPNCounterConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                PNCounterConfig dataConfig = hecatonInstance.getConfig().findPNCounterConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName()).isEqualTo(DYNAMIC_NAME);
            }
        };
        testTemplate(testCase);
    }

    abstract static class TestCase<T> {

        final T staticConfig;
        final T dynamicConfig;

        private final boolean isDynamicFirst;

        TestCase(T staticConfig, T dynamicConfig, boolean isDynamicFirst) {
            this.staticConfig = staticConfig;
            this.dynamicConfig = dynamicConfig;
            this.isDynamicFirst = isDynamicFirst;
        }

        boolean isDynamicFirst() {
            return isDynamicFirst;
        }

        abstract void addStaticConfig(Config config);

        abstract void addDynamicConfig(HecatonInstance hecatonInstance);

        abstract void asserts();
    }
}
