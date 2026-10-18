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

package uk.org.hecaton.internal.servicemanager.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ConfigAccessor;
import uk.org.hecaton.config.ServiceConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.services.ConfigurableService;
import uk.org.hecaton.internal.services.ManagedService;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.servicemanager.ServiceInfo;
import uk.org.hecaton.spi.impl.servicemanager.impl.ServiceManagerImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.swing.*;
import java.util.List;
import java.util.Properties;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ServiceManagerImplTest extends HecatonTestSupport {

    private ServiceManagerImpl serviceManager;

    @Before
    public void setup() {
        Config config = new Config();

        ServiceConfig serviceConfig = new ServiceConfig()
                .setClassName(FooService.class.getName())
                .setEnabled(true)
                .setName("fooService");
        ConfigAccessor.getServicesConfig(config).addServiceConfig(serviceConfig);

        HecatonInstance hz = createHecatonInstance(config);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        serviceManager = (ServiceManagerImpl) nodeEngine.getServiceManager();
    }

    static class FooService implements ManagedService, ConfigurableService {
        volatile boolean initCalled;
        volatile boolean configureCalled;

        @Override
        public void configure(Object configObject) {
            this.configureCalled = true;
        }

        @Override
        public void init(NodeEngine nodeEngine, Properties properties) {
            this.initCalled = true;
        }

        @Override
        public void reset() {

        }

        @Override
        public void shutdown(boolean terminate) {

        }
    }

    // ===================== getServiceInfo ================================


    @Test
    public void getServiceInfo() {
        ServiceInfo result = serviceManager.getServiceInfo(MapService.SERVICE_NAME);
        assertNotNull(result);
        assertEquals(MapService.SERVICE_NAME, result.getName());
        assertInstanceOf(MapService.class, result.getService());
    }

    @Test
    public void getServiceInfo_notExisting() {
        ServiceInfo result = serviceManager.getServiceInfo("notexisting");
        assertNull(result);
    }

    // ===================== getServiceInfos ================================

    @Test
    public void getServiceInfos() {
        List<ServiceInfo> result = serviceManager.getServiceInfos(MapService.class);
        assertNotNull(result);
        assertEquals(1, result.size());

        ServiceInfo serviceInfo = result.get(0);
        assertEquals(MapService.SERVICE_NAME, serviceInfo.getName());
        assertInstanceOf(MapService.class, serviceInfo.getService());
    }

    @Test
    public void getServiceInfos_notExisting() {
        List<ServiceInfo> result = serviceManager.getServiceInfos(JPanel.class);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    // ==================== getService =====================================

    @Test
    public void getService() {
        Object result = serviceManager.getService(MapService.SERVICE_NAME);
        assertInstanceOf(MapService.class, result);
    }

    @Test
    public void getService_notExisting() {
        Object result = serviceManager.getService("notexisting");
        assertNull(result);
    }

    // ========================= getServices =====================================

    @Test
    public void getServices() {
        List<MapService> result = serviceManager.getServices(MapService.class);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertInstanceOf(MapService.class, result.get(0));
    }

    @Test
    public void getServices_notExisting() {
        List<JPanel> result = serviceManager.getServices(JPanel.class);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    // ========================= userService =====================================

    @Test
    public void userService() {
        Object result = serviceManager.getService("fooService");
        FooService fooService = assertInstanceOf(FooService.class, result);

        assertTrue(fooService.initCalled);
        assertTrue(fooService.configureCalled);
    }
}
