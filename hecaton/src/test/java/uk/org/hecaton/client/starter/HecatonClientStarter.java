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

package uk.org.hecaton.client.starter;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.starter.HecatonAPIDelegatingClassloader;
import uk.org.hecaton.test.starter.HecatonStarter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static uk.org.hecaton.test.starter.HecatonProxyFactory.proxyObjectForStarter;
import static uk.org.hecaton.test.starter.HecatonStarter.getConfig;
import static uk.org.hecaton.test.starter.HecatonStarter.getTargetVersionClassloader;
import static uk.org.hecaton.test.starter.HecatonStarterUtils.rethrowGuardianException;
import static java.lang.Thread.currentThread;

public class HecatonClientStarter {

    private HecatonClientStarter() {
    }

    public static HecatonInstance newHecatonClient(String version, boolean enterprise) {
        return newHecatonClient(version, null, enterprise);
    }

    public static HecatonInstance newHecatonClient(String version, ClientConfig clientConfig, boolean enterprise) {
        return newHecatonClient(version, clientConfig, enterprise, Collections.emptyList());
    }

    @SuppressWarnings("unchecked")
    public static HecatonInstance newHecatonClient(String version, ClientConfig clientConfig, boolean enterprise, List<URL> additionalJars) {
        ClassLoader classLoader = clientConfig == null ? null : clientConfig.getClassLoader();
        HecatonAPIDelegatingClassloader classloader = getTargetVersionClassloader(version, enterprise,
                classLoader, additionalJars);
        ClassLoader contextClassLoader = currentThread().getContextClassLoader();
        currentThread().setContextClassLoader(null);
        try {
            Class<HecatonClient> hecatonClass
                    = (Class<HecatonClient>) classloader.loadClass("uk.org.hecaton.client.HecatonClient");
            System.out.println(hecatonClass + " loaded by " + hecatonClass.getClassLoader());
            Class<?> configClass = classloader.loadClass("uk.org.hecaton.client.config.ClientConfig");
            Object config = getConfig(classloader, configClass, clientConfig);

            Method newHecatonInstanceMethod = hecatonClass.getMethod("newHecatonClient", configClass);
            Object delegate = newHecatonInstanceMethod.invoke(null, config);
            return (HecatonInstance) proxyObjectForStarter(HecatonStarter.class.getClassLoader(), delegate);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw rethrowGuardianException(e);
        } finally {
            if (contextClassLoader != null) {
                currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }
}
