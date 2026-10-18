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

package uk.org.hecaton.test.starter.test;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import uk.org.hecaton.test.starter.HecatonAPIDelegatingClassloader;
import uk.org.hecaton.test.starter.HecatonProxyFactory;
import uk.org.hecaton.test.starter.HecatonStarter;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class HecatonProxyFactoryTest {

    @Test
    public void testReturnedProxyImplements_sameInterfaceByNameOnTargetClassLoader() throws Exception {
        ProxiedInterface delegate = new ProxiedInterface() {
            @Override
            public void get() {
            }
        };

        // HecatonAPIDelegatingClassloader will reload the bytes of ProxiedInterface as a new class
        // as happens with every uk.org.hecaton class that contains "test"
        HecatonAPIDelegatingClassloader targetClassLoader
                = new HecatonAPIDelegatingClassloader(new URL[]{}, HecatonProxyFactoryTest.class.getClassLoader());

        Object proxy = HecatonProxyFactory.proxyObjectForStarter(targetClassLoader, delegate);

        assertNotNull(proxy);
        Class<?>[] ifaces = proxy.getClass().getInterfaces();
        assertEquals(1, ifaces.length);

        Class<?> proxyInterface = ifaces[0];
        // it is not the same class but has the same name on a different classloader
        assertNotEquals(ProxiedInterface.class, proxyInterface);
        assertEquals(ProxiedInterface.class.getName(), proxyInterface.getName());
        assertEquals(targetClassLoader, proxyInterface.getClassLoader());
    }

    @Test
    public void testProxyHecatonInstanceClasses_ofSameVersion_areSame() {
        HecatonInstance hz1 = HecatonStarter.newHecatonInstance("5.0");
        HecatonInstance hz2 = HecatonStarter.newHecatonInstance("5.0");
        try {
            assertEquals(hz1.getClass(), hz2.getClass());
        } finally {
            hz1.shutdown();
            hz2.shutdown();
        }
    }

    public interface ProxiedInterface {
        void get();
    }
}
