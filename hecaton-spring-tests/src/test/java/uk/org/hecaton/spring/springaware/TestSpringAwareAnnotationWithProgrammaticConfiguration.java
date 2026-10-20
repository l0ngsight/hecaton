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

package uk.org.hecaton.spring.springaware;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spring.CustomSpringExtension;
import uk.org.hecaton.spring.context.SpringAware;
import uk.org.hecaton.spring.context.SpringManagedContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(CustomSpringExtension.class)
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
class TestSpringAwareAnnotationWithProgrammaticConfiguration {

    @BeforeAll
    @AfterAll
    static void cleanup() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Test
    void testSpringManagedContextManuallySet()
            throws ExecutionException, InterruptedException, TimeoutException {
        ApplicationContext applicationContext = new AnnotationConfigApplicationContext(SpringConf.class);
        Config config = new Config();
        config.setManagedContext(new SpringManagedContext(applicationContext));
        config.setClusterName("second-spring-cluster");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        HecatonInstance instance = Hecaton.newHecatonInstance(config);

        Future<String> future = instance.getExecutorService("default").submit(new MyTask());
        String result = future.get(2, TimeUnit.MINUTES);
        assertThat(result).isEqualTo("Hello World!");
    }

    public static class DummyService {
        public String hello() {
            return "Hello World!";
        }
    }

    @Configuration
    public static class SpringConf {
        @Bean
        public DummyService dummyService() {
            return new DummyService();
        }
    }

    @SpringAware
    public static class MyTask implements Callable<String>, Serializable {

        @Autowired
        private transient DummyService service;

        @Override
        public String call() {
            return service.hello();
        }
    }
}
