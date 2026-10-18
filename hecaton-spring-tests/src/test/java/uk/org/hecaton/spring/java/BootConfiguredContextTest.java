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

package uk.org.hecaton.spring.java;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.spring.CustomSpringExtension;
import uk.org.hecaton.spring.ExposeHecatonObjects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static uk.org.hecaton.spring.ConfigCreator.createConfig;
import static uk.org.hecaton.test.HecatonTestSupport.assertEqualsEventually;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = BootConfiguredContextTest.SpringHecatonBootConfiguration.class)
@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
public class BootConfiguredContextTest
        extends AppContextTestBase {

    @Test
    @Override
    public void testMap() {
        assertThat((Object) map1).isNotNull();
        assertThat((Object) testMap).isNotNull();

        testMap.set("key1", "value1");
        assertEqualsEventually(() -> testMap.get("key1"), "value1");
    }

    @SpringBootApplication(scanBasePackages = "uk.org.hecaton.non.existing")
    @ExposeHecatonObjects
    public static class SpringHecatonBootConfiguration {

        public static void main(String[] args) {
            SpringApplication.run(SpringHecatonBootConfiguration.class, args);
        }

        @Bean(destroyMethod = "shutdown")
        public HecatonInstance hecatonInstance() {
            Config config = createConfig();
            config.setClusterName("spring-hecaton-cluster-from-java");

            var mapConfig = new MapConfig("testMap");
            mapConfig.setBackupCount(2);
            mapConfig.setReadBackupData(true);
            config.addMapConfig(mapConfig);
            var map1Config = new MapConfig("map1");
            map1Config.setBackupCount(2);
            map1Config.setReadBackupData(true);
            config.addMapConfig(map1Config);
            return HecatonInstanceFactory.newHecatonInstance(config);
        }
    }
}
