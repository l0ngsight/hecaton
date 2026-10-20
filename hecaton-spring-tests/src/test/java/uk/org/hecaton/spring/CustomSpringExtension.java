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

package uk.org.hecaton.spring;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.spring.config.ConfigFactoryAccessor;
import uk.org.hecaton.test.JmxLeakHelper;
import uk.org.hecaton.test.TestLoggingUtils;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;

/**
 * This Junit5 extension is used per test class. It is similar to a JUnit4 test class runner
 */
public class CustomSpringExtension implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback, InvocationInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomSpringExtension.class);

    // Specifies the default time out value for a test method, unless a test timeout annotation is not provided
    private static final int DEFAULT_TEST_TIMEOUT_IN_SECONDS = Integer.getInteger("hecaton.test.defaultTestTimeoutInSeconds", 300);

    private static final int MAX_CLIENT_CONNECT_TIMEOUT_MS = 30_000;

    static {
        TestLoggingUtils.initializeLogging();
        ConfigFactoryAccessor.setConfigSupplier(() -> {
            Config config = smallInstanceConfig();
            config.setProperty("java.net.preferIPv4Stack", "true");
            config.setProperty("hecaton.local.localAddress", "127.0.0.1");
            config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "1");
            config.setProperty(ClusterProperty.ASYNC_JOIN_STRATEGY_ENABLED.getName(), "false");
            return config;
        });
        ConfigFactoryAccessor.setClientConfigSupplier(() -> {
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig()
                    .setClusterConnectTimeoutMillis(MAX_CLIENT_CONNECT_TIMEOUT_MS);
            return clientConfig;
        });
    }

    @Override
    public void beforeAll(@Nonnull ExtensionContext context) {
        // Code to run before all tests
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        Method testMethod = context.getRequiredTestMethod();
        String testName = testMethod.getName();
        TestLoggingUtils.setThreadLocalTestMethodName(testName);
    }

    @Override
    public void afterEach(@Nonnull ExtensionContext context) {
        TestLoggingUtils.removeThreadLocalTestMethodName();
    }

    @Override
    public void afterAll(@Nonnull ExtensionContext context) {
        var applicationContext = (AbstractApplicationContext) SpringExtension.getApplicationContext(context);
        if (!applicationContext.isRunning() || applicationContext.isActive()) {
            // context was closed, but instance(s) are running, something went wrong.
            Set<HecatonInstance> instances = HecatonInstanceFactory.getAllHecatonInstances();
            if (!instances.isEmpty()) {
                LOGGER.warn("Instances haven't been shut down: {}", instances);
                HecatonInstanceFactory.terminateAll();
            }
            applicationContext.stop();
        } else {
            // Normal Hecaton beans are declared with destroy-method = shutdown, but it makes graceful shutdown,
            // which takes time and is not necessary in tests.
            // instead, terminate Hz instances
            HecatonInstanceFactory.terminateAll();
        }
        JmxLeakHelper.checkJmxBeans();
    }

    @Override
    public void interceptTestMethod(@Nonnull Invocation<Void> invocation, @Nonnull ReflectiveInvocationContext<Method> invocationContext,
                                    @Nonnull ExtensionContext extensionContext) throws Throwable {
        Method testMethod = invocationContext.getExecutable();
        long finalTimeoutSeconds = getTimeoutSeconds(testMethod);
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                invocation.proceed();
            } catch (Throwable throwable) {
                error.set(throwable);
            }
        });
        thread.setDaemon(true);
        thread.start();
        thread.join(finalTimeoutSeconds * 1000);
        if (thread.isAlive()) {
            thread.interrupt();
            throw new IllegalStateException("Test method " + testMethod.getName() + " timed out after " + finalTimeoutSeconds + " seconds");
        }
        if (error.get() != null) {
            throw error.get();
        }
    }

    private long getTimeoutSeconds(Method testMethod) {
        Timeout timeout = testMethod.getAnnotation(Timeout.class);
        long finalTimeoutSeconds;
        if (timeout == null) {
            finalTimeoutSeconds = DEFAULT_TEST_TIMEOUT_IN_SECONDS;
        } else {
            finalTimeoutSeconds = timeout.value();
            if (timeout.unit() != TimeUnit.SECONDS) {
                finalTimeoutSeconds = timeout.unit().toSeconds(timeout.value());
            }
        }
        return finalTimeoutSeconds;
    }
}
