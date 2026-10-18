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
package uk.org.hecaton.spring.boot3;

import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.spring.HecatonObjectExtractionConfiguration;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Auto Configuration for {@link HecatonObjectExtractionConfiguration}, compatible with Spring Boot 3.
 * @since 5.7
 */
@Configuration(proxyBeanMethods = false)
@AutoConfiguration
@AutoConfigureAfter(name = {
        // Boot 3.x, avoid direct class usage due to JavaDoc generation error - it does not see actuator 3.x when
        // applying javadoc:aggregate, as it sees 4.x from the boot 4 module.
        "org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration",
        "uk.org.hecaton.spring.HecatonExposeObjectRegistrar" })
@Import(HecatonObjectExtractionConfiguration.class)
public class HecatonBoot3ObjectExtractionAutoConfiguration implements InitializingBean {

    private static final int REQUIRED_SPRING_BOOT_VERSION = 3;

    @Override
    public void afterPropertiesSet() {
        String version = SpringBootVersion.getVersion();
        int detectedSpringMajorVersion = Integer.parseInt(version.split("\\.")[0]);

        if (detectedSpringMajorVersion != REQUIRED_SPRING_BOOT_VERSION) {
            String msg = "You're using hecaton-spring-boot3 for Spring Boot 3.x, "
                    + "but your Spring Boot version is " + SpringBootVersion.getVersion() + ". "
                    + "Please use hecaton-spring-boot" + detectedSpringMajorVersion + " instead.";
            throw new HecatonException(msg);
        }
    }
}
