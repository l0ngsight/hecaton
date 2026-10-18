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

package uk.org.hecaton.internal.cluster.impl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.internal.util.FutureUtil;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.proxyservice.impl.ProxyServiceImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.OverridePropertyRule;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.spi.properties.ClusterProperty.ASYNC_JOIN_STRATEGY_ENABLED;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.OverridePropertyRule.set;
import static uk.org.hecaton.test.TestEnvironment.HECATON_TEST_USE_NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class})
public class EventRegistrationTest extends HecatonTestSupport {

    @Rule
    public final OverridePropertyRule overridePropertyRule = set(HECATON_TEST_USE_NETWORK, "true");
    @Rule
    public final OverridePropertyRule overridePropertyRule2 = set(ASYNC_JOIN_STRATEGY_ENABLED.getName(), "false");
    private final HecatonInstance[] batchedMembers = new HecatonInstance[6];

    @Before
    public void prepare() throws InterruptedException {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        // Create a cluster that goes through a batched join, starting with a master instance (always index 0)
        batchedMembers[0] = factory.newHecatonInstance(config);
        // Start 3 members simultaneously (so their join requests are batched)
        List<Future<?>> futures = new ArrayList<>(3);
        for (int k = 1; k < 4; k++) {
            int finalK = k;
            futures.add(spawn(() -> batchedMembers[finalK] = factory.newHecatonInstance(config)));
        }

        // Wait for all batched member instances to be created
        FutureUtil.waitWithDeadline(futures, 30, TimeUnit.SECONDS);

        // Ensure the batched join period has elapsed
        ClusterJoinManager joinManager = ((ClusterServiceImpl) getNodeEngineImpl(batchedMembers[0])
                .getClusterService()).getClusterJoinManager();
        assertTrueEventually(() -> assertFalse(joinManager.isBatchingJoins(Clock.currentTimeMillis())));

        // Then start 2 members sequentially (so their join requests are not batched)
        for (int k = 4; k < 6; k++) {
            batchedMembers[k] = factory.newHecatonInstance(config);
        }

        // Ensure cluster size is 6
        assertClusterSizeEventually(6, batchedMembers);
    }

    /**
     * <a href="https://hazelcast.atlassian.net/browse/HZ-2797">Related to HZ-2797</a>
     */
    @Test
    public void testEventRegistrationsForAllMembers_AfterJoin() {
        // Gather a set of expected addresses to use in comparisons
        Set<Address> expectedAddresses = new HashSet<>(batchedMembers.length);
        for (HecatonInstance member : batchedMembers) {
            expectedAddresses.add(getNodeEngineImpl(member).getThisAddress());
        }

        // Iterate over all members of the cluster and assert that it has listeners registered for
        //  every member of the cluster (including the local member itself)
        Set<Address> localAddresses = new HashSet<>(batchedMembers.length);
        for (HecatonInstance member : batchedMembers) {
            Collection<EventRegistration> registrations = getNodeEngineImpl(member).getEventService().getRegistrations(
                    ProxyServiceImpl.SERVICE_NAME, ProxyServiceImpl.SERVICE_NAME);

            localAddresses.clear();
            registrations.forEach(registration -> localAddresses.add(registration.getSubscriber()));

            assertEquals(String.format("Expected: %s, Actual: %s", expectedAddresses, localAddresses),
                    expectedAddresses.size(), localAddresses.size());
            assertContainsAll(localAddresses, expectedAddresses);
        }
    }
}
