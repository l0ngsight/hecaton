/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.cp.internal;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.CPMap;
import uk.org.hecaton.cp.CPSubsystem;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPSubsystemImplTest extends HecatonTestSupport {

    private TestHecatonInstanceFactory factory;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void init() {
        factory = createHecatonInstanceFactory();
    }

    @Test
    public void test_atomicLong_whenCPSubsystemNotConfigured() {
        HecatonInstance instance = factory.newHecatonInstance();
        assertNotNull(instance.getCPSubsystem().getAtomicLong("long"));
    }

    @Test
    public void test_atomicReference_whenCPSubsystemNotConfigured() {
        HecatonInstance instance = factory.newHecatonInstance();
        assertNotNull(instance.getCPSubsystem().getAtomicReference("ref"));
    }

    @Test
    public void test_lock_whenCPSubsystemNotConfigured() {
        HecatonInstance instance = factory.newHecatonInstance();
        assertNotNull(instance.getCPSubsystem().getLock("lock"));
    }

    @Test
    public void test_semaphore_whenCPSubsystemNotConfigured() {
        HecatonInstance instance = factory.newHecatonInstance();
        assertNotNull(instance.getCPSubsystem().getSemaphore("semaphore"));
    }

    @Test
    public void test_countDownLatch_whenCPSubsystemNotConfigured() {
        HecatonInstance instance = factory.newHecatonInstance();
        assertNotNull(instance.getCPSubsystem().getCountDownLatch("latch"));
    }

    @Test
    public void test_cpSubsystemManagementService_whenCPSubsystemNotConfigured() {
        HecatonInstance instance = factory.newHecatonInstance();
        thrown.expect(HecatonException.class);

        instance.getCPSubsystem().getCPSubsystemManagementService();
    }

    @Test
    public void test_cpSessionManagementService_whenCPSubsystemNotConfigured() {
        HecatonInstance instance = factory.newHecatonInstance();
        thrown.expect(HecatonException.class);

        instance.getCPSubsystem().getCPSessionManagementService();
    }

    @Test
    public void test_atomicLong_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
        HecatonInstance instance = factory.newHecatonInstance(config);

        assertNotNull(instance.getCPSubsystem().getAtomicLong("long"));
    }

    @Test
    public void test_atomicReference_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
        HecatonInstance instance = factory.newHecatonInstance(config);

        assertNotNull(instance.getCPSubsystem().getAtomicReference("ref"));
    }

    @Test
    public void test_lock_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
        HecatonInstance instance = factory.newHecatonInstance(config);

        assertNotNull(instance.getCPSubsystem().getLock("lock"));
    }

    @Test
    public void test_semaphore_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
        HecatonInstance instance = factory.newHecatonInstance(config);

        assertNotNull(instance.getCPSubsystem().getSemaphore("semaphore"));
    }

    @Test
    public void test_countDownLatch_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
        HecatonInstance instance = factory.newHecatonInstance(config);

        assertNotNull(instance.getCPSubsystem().getCountDownLatch("latch"));
    }

    @Test
    public void test_cpSubsystemManagementService_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
        HecatonInstance instance = factory.newHecatonInstance(config);

        assertNotNull(instance.getCPSubsystem().getCPSubsystemManagementService());
    }

    @Test
    public void test_cpSessionManagementService_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
        HecatonInstance instance = factory.newHecatonInstance(config);

        assertNotNull(instance.getCPSubsystem().getCPSessionManagementService());
    }

    @Test
    public void testCPMap() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
        HecatonInstance instance = factory.newHecatonInstance(config);
        CPMap<String, String> map = instance.getCPSubsystem().getMap("myMap");
        map.set("key", "value");
        assertEquals("value", map.get("key"));
    }

    @Test
    public void testGetCPGroupIdsWhenSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
        HecatonInstance instance = factory.newHecatonInstance(config);

        CPSubsystem cp = instance.getCPSubsystem();

        cp.getAtomicLong("long");
        cp.getAtomicLong("long@group1");
        cp.getAtomicLong("long@group2");

        Collection<CPGroupId> cpGroupIds = cp.getCPGroupIds();
        assertThat(cpGroupIds).extracting(CPGroupId::getName)
                .containsExactlyInAnyOrder("METADATA", "default", "group1", "group2");
    }

}
