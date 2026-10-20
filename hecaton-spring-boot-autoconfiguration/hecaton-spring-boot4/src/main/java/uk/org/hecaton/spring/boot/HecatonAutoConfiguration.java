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
package uk.org.hecaton.spring.boot;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spring.autoconfigure.HecatonAutoConfigurationSupport;
import uk.org.hecaton.spring.autoconfigure.HecatonClientConfigCondition;
import uk.org.hecaton.spring.autoconfigure.HecatonMemberConfigCondition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Creates a {@link HecatonInstance} bean, a member or a client, when the application declares
 * none, from a {@link Config} or {@link ClientConfig} bean or from the configuration file named
 * by {@code spring.hecaton.config} or found by default (see
 * {@link HecatonAutoConfigurationSupport}). Compatible with Spring Boot 4.
 */
@AutoConfiguration
@ConditionalOnClass(HecatonInstance.class)
@ConditionalOnMissingBean(HecatonInstance.class)
public class HecatonAutoConfiguration {

    /** A member from a {@link Config} bean or member configuration. */
    @Configuration(proxyBeanMethods = false)
    @Conditional(HecatonMemberConfigCondition.class)
    static class MemberConfiguration {

        @Bean(destroyMethod = "shutdown")
        HecatonInstance hecatonInstance(ObjectProvider<Config> config, ApplicationContext context) {
            return HecatonAutoConfigurationSupport.newMember(config.getIfUnique(), context);
        }
    }

    /** A client from a {@link ClientConfig} bean or client configuration, when there is no member configuration. */
    @Configuration(proxyBeanMethods = false)
    @Conditional(HecatonClientConfigCondition.class)
    static class ClientConfiguration {

        @Bean(destroyMethod = "shutdown")
        HecatonInstance hecatonInstance(ObjectProvider<ClientConfig> config, ApplicationContext context) {
            return HecatonAutoConfigurationSupport.newClient(config.getIfUnique(), context);
        }
    }
}
