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

package uk.org.hecaton.spring.hibernate;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.hibernate.HecatonCacheRegionFactory;
import uk.org.hecaton.hibernate.HecatonLocalCacheRegionFactory;
import uk.org.hecaton.spring.CustomSpringExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.net.InetSocketAddress;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"hibernate-applicationContext-hecaton.xml"})
class TestHibernateApplicationContext {

    @Autowired
    private HecatonInstance instance;

    @Autowired
    private HecatonCacheRegionFactory regionFactory;

    @Autowired
    private HecatonLocalCacheRegionFactory localRegionFactory;

    @Autowired
    private HecatonLocalCacheRegionFactory localRegionFactory2;

    @BeforeAll
    @AfterAll
    static void start() {
        Hecaton.shutdownAll();
    }

    @Test
    void testInstance() {
        assertNotNull(instance);
        Set<Member> members = instance.getCluster().getMembers();
        assertEquals(1, members.size());
        Member member = members.iterator().next();
        InetSocketAddress inetSocketAddress = member.getSocketAddress();
        assertEquals(5700, inetSocketAddress.getPort());
    }

    @Test
    void testRegionFactory() {
        assertNotNull(regionFactory);
        assertEquals(regionFactory.getHecatonInstance(), instance);

        assertNotNull(localRegionFactory);
        assertEquals(localRegionFactory.getHecatonInstance(), instance);

        assertNotNull(localRegionFactory2);
        assertEquals(localRegionFactory2.getHecatonInstance(), instance);
    }
}
