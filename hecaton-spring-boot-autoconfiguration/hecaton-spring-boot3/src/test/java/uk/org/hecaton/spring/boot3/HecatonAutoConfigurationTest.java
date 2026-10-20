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
package uk.org.hecaton.spring.boot3;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spring.context.SpringManagedContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class HecatonAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(HecatonAutoConfiguration.class));

    @AfterEach
    void shutdown() {
        Hecaton.shutdownAll();
    }

    @Test
    void memberFromConfigBean() {
        runner.withUserConfiguration(ConfigBean.class).run(context -> {
            HecatonInstance instance = context.getBean(HecatonInstance.class);
            assertThat(instance.getConfig().getClusterName()).isEqualTo("autoconfig-bean");
            assertThat(instance.getConfig().getManagedContext()).isInstanceOf(SpringManagedContext.class);
        });
    }

    @Test
    void memberFromYamlFile() {
        runner.withPropertyValues("spring.hecaton.config=classpath:autoconfig/member.yaml").run(context ->
                assertThat(context.getBean(HecatonInstance.class).getConfig().getClusterName())
                        .isEqualTo("autoconfig-yaml"));
    }

    @Test
    void memberFromXmlFile() {
        runner.withPropertyValues("spring.hecaton.config=classpath:autoconfig/member.xml").run(context ->
                assertThat(context.getBean(HecatonInstance.class).getConfig().getClusterName())
                        .isEqualTo("autoconfig-xml"));
    }

    @Test
    void clientFromFile(@TempDir Path dir) throws IOException {
        Config memberConfig = new Config().setClusterName("autoconfig-client");
        memberConfig.getNetworkConfig().setPort(5985);
        memberConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        HecatonInstance member = Hecaton.newHecatonInstance(memberConfig);
        int port = member.getCluster().getLocalMember().getAddress().getPort();
        Path clientConfig = Files.writeString(dir.resolve("hecaton-client.yaml"), String.format(
                "hecaton-client:%n  cluster-name: autoconfig-client%n  network:%n    cluster-members:%n"
                        + "      - 127.0.0.1:%d%n", port));

        runner.withPropertyValues("spring.hecaton.config=" + clientConfig.toUri()).run(context -> {
            HecatonInstance client = context.getBean(HecatonInstance.class);
            assertThat(client.getName()).startsWith("hc.client");
            client.getMap("m").put("k", "v");
            assertThat(client.getMap("m").get("k")).isEqualTo("v");
        });
    }

    @Test
    void memberConfigWinsOverClientConfig() {
        runner.withUserConfiguration(ConfigBean.class, ClientConfigBean.class).run(context ->
                assertThat(context.getBean(HecatonInstance.class).getConfig().getClusterName())
                        .isEqualTo("autoconfig-bean"));
    }

    @Test
    void nothingWithoutConfiguration() {
        runner.run(context -> assertThat(context).doesNotHaveBean(HecatonInstance.class));
    }

    @Test
    void backsOffForApplicationInstance() {
        runner.withUserConfiguration(ConfigBean.class, InstanceBean.class).run(context -> {
            assertThat(context).hasSingleBean(HecatonInstance.class);
            assertThat(context.getBean(HecatonInstance.class).getName()).isEqualTo("application-instance");
        });
    }

    @Test
    void missingFileFails() {
        runner.withPropertyValues("spring.hecaton.config=classpath:autoconfig/missing.yaml").run(context ->
                assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                        .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining("missing.yaml"));
    }

    @Test
    void failoverConfigFails() {
        runner.withPropertyValues("spring.hecaton.config=classpath:autoconfig/failover.yaml").run(context ->
                assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                        .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining("failover"));
    }

    @Configuration(proxyBeanMethods = false)
    static class ConfigBean {
        @Bean
        Config config() {
            Config config = new Config().setClusterName("autoconfig-bean");
            config.getNetworkConfig().setPort(5991);
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            return config;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ClientConfigBean {
        @Bean
        ClientConfig clientConfig() {
            return new ClientConfig().setClusterName("autoconfig-client");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InstanceBean {
        @Bean(destroyMethod = "shutdown")
        HecatonInstance application(Config config) {
            config.setInstanceName("application-instance");
            return Hecaton.newHecatonInstance(config);
        }
    }
}
