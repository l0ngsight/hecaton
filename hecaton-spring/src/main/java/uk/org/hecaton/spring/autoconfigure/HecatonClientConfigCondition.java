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
package uk.org.hecaton.spring.autoconfigure;

import uk.org.hecaton.client.config.ClientConfig;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.ConfigurationCondition;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Matches when there is no member configuration ({@link HecatonMemberConfigCondition}) and the
 * application declares a {@link ClientConfig} bean or has client configuration (see
 * {@link HecatonAutoConfigurationSupport}).
 */
public class HecatonClientConfigCondition implements ConfigurationCondition {

    /** Evaluated when beans are registered, so that the application's own bean definitions are visible. */
    @Override
    public ConfigurationPhase getConfigurationPhase() {
        return ConfigurationPhase.REGISTER_BEAN;
    }

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        if (HecatonMemberConfigCondition.hasMemberConfig(context)) {
            return false;
        }
        ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
        if (beanFactory != null && beanFactory.getBeanNamesForType(ClientConfig.class, true, false).length > 0) {
            return true;
        }
        return HecatonAutoConfigurationSupport.hasClientConfig(context.getEnvironment(), context.getResourceLoader());
    }
}
