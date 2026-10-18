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

package uk.org.hecaton.test.starter.hz3;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.impl.deployment.ChildFirstClassLoader;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static uk.org.hecaton.test.starter.HecatonVersionLocator.Artifact.OS_JAR;
import static uk.org.hecaton.test.starter.HecatonVersionLocator.locateVersion;

/**
 * Creates new Hecaton 3 proxy
 */
public class Hecaton3Proxy {

    private static final Map<String, String> TYPES_4_TO_3 = new HashMap<>() {{
        put("uk.org.hecaton.topic.ITopic", "uk.org.hecaton.core.ITopic");
        put("uk.org.hecaton.topic.MessageListener", "uk.org.hecaton.core.MessageListener");
        put("uk.org.hecaton.topic.Message", "uk.org.hecaton.core.Message");
    }};

    private static final Map<String, String> TYPES_3_TO_4 = new HashMap<>() {{
        put("uk.org.hecaton.core.ITopic", "uk.org.hecaton.topic.ITopic");
        put("uk.org.hecaton.core.MessageListener", "uk.org.hecaton.topic.MessageListener");
        put("uk.org.hecaton.core.Message", "uk.org.hecaton.topic.Message");
    }};

    public HecatonInstance newHecatonInstance(String xmlConfig) {
        try {
            File hecatonJar = locateVersion("3.12.13", false).get(OS_JAR);
            URLClassLoader classLoader = new ChildFirstClassLoader(
                    new URL[]{hecatonJar.toURI().toURL()},
                    Hecaton3Proxy.class.getClassLoader()
            );
            Object config = buildHz3Config(xmlConfig, classLoader);
            Object hz3Instance = newHecaton3Instance(classLoader, config);

            return proxyInstance(hz3Instance, HecatonInstance.class);

        } catch (MalformedURLException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private Object buildHz3Config(String xmlConfig, URLClassLoader classLoader) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        ByteArrayInputStream is = new ByteArrayInputStream(xmlConfig.getBytes(StandardCharsets.UTF_8));
        Class<?> xmlConfigBuilderClass = classLoader.loadClass("uk.org.hecaton.config.XmlConfigBuilder");
        Constructor<?> constructor = xmlConfigBuilderClass.getDeclaredConstructor(InputStream.class);
        Object configBuilder = constructor.newInstance(is);
        Object config = invokeMethod(configBuilder, "build");
        return config;
    }

    private Object newHecaton3Instance(URLClassLoader classLoader, Object config) throws ClassNotFoundException {
        Class<?> hz3InstanceClass = classLoader.loadClass("uk.org.hecaton.core.Hecaton");
        Object hz3Instance = invokeStaticMethod(hz3InstanceClass, "newHecatonInstance", config);
        return hz3Instance;
    }

    private Object invokeMethod(Object object, String methodName) {
        try {
            Method method = object.getClass().getMethod(methodName);
            return method.invoke(object);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method on class ", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Could not invoke method on class ", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Could not access method on class ", e);
        }
    }

    private Object invokeStaticMethod(Class<?> clazz, String methodName, Object... params) {
        try {
            Class<?>[] paramTypes = paramsToTypes(params);
            Method method = clazz.getMethod(methodName, paramTypes);
            return method.invoke(null, params);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method on class ", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Could not invoke method on class ", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Could not access method on class ", e);
        }
    }

    private Class<?>[] paramsToTypes(Object[] params) {
        if (params == null) {
            return null;
        }
        Class<?>[] paramTypes = new Class[params.length];
        for (int i = 0; i < params.length; i++) {
            paramTypes[i] = params[i].getClass();
        }
        return paramTypes;
    }

    public <T> T proxyInstance(Object target, Class<T> iface) {
        Object proxy = Mockito.mock(iface, new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Method method = invocation.getMethod();
                Object[] objects = invocation.getArguments();

                Class<?>[] translatedTypes = translateParameterTypes(method.getParameterTypes());
                Object[] proxiedParams = proxyParameters(objects, translatedTypes);
                Method targetMethod = target.getClass().getMethod(method.getName(), translatedTypes);
                Object result = targetMethod.invoke(target, proxiedParams);

                if (method.getReturnType().getPackage() != null
                    && method.getReturnType().getPackage().getName().startsWith("uk.org.hecaton")) {

                    return proxyInstance(result, method.getReturnType());
                } else {
                    if (method.getReturnType().equals(UUID.class) && targetMethod.getReturnType().equals(String.class)) {
                        return UUID.fromString((String) result);
                    } else {
                        return result;
                    }
                }
            }

            private Class<?>[] translateParameterTypes(Class<?>[] types) throws ClassNotFoundException {
                Class<?>[] translated = new Class<?>[types.length];
                for (int i = 0; i < types.length; i++) {
                    if (types[i].getPackage() != null
                            && types[i].getPackage().getName().startsWith("uk.org.hecaton")) {

                        String typeName = types[i].getName();
                        String name = TYPES_4_TO_3.get(typeName);
                        if (name == null) {
                            name = TYPES_3_TO_4.get(typeName);
                        }
                        if (name == null) {
                            name = typeName;
                        }
                        translated[i] = target.getClass().getClassLoader().loadClass(name);
                    } else {
                        translated[i] = types[i];
                    }
                }
                return translated;
            }

            private Object[] proxyParameters(Object[] params, Class<?>[] types) {
                assert params.length == types.length;

                Object[] proxied = new Object[params.length];
                for (int i = 0; i < params.length; i++) {
                    if (types[i].getPackage() != null
                            && types[i].getPackage().getName().startsWith("uk.org.hecaton")) {
                        proxied[i] = proxyInstance(params[i], types[i]);
                    } else {
                        proxied[i] = params[i];
                    }
                }
                return proxied;
            }
        });

        return (T) proxy;
    }
}
