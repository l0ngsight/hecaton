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

package uk.org.hecaton.spring.boot;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.spring.ConfigCreator;
import uk.org.hecaton.spring.ExposeHecatonObjects;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.ClassPathResource;

@SpringBootApplication
@ExposeHecatonObjects
public class BootHecatonApplication {

    public static void main(String[] args) {
        SpringApplication.run(BootHecatonApplication.class, args);
    }

    @Bean
    public PropertySourcesPlaceholderConfigurer propertyPlaceholderConfigurer() {
        var propertyPlaceholderConfigurer = new PropertySourcesPlaceholderConfigurer();
        propertyPlaceholderConfigurer.setLocation(new ClassPathResource("hecaton-default.properties"));
        return propertyPlaceholderConfigurer;
    }

    @Bean
    public Config config() {
        return ConfigCreator.createConfigWithStructures();
    }
}
