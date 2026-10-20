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

package uk.org.hecaton.cluster;

import uk.org.hecaton.cluster.memberselector.MemberSelectors;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static uk.org.hecaton.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static uk.org.hecaton.cluster.memberselector.MemberSelectors.LITE_MEMBER_SELECTOR;
import static uk.org.hecaton.cluster.memberselector.MemberSelectors.LOCAL_MEMBER_SELECTOR;
import static uk.org.hecaton.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static uk.org.hecaton.test.Accessors.getClusterService;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterServiceMemberListTest
        extends HecatonTestSupport {

    private final Config liteConfig = new Config().setLiteMember(true);

    private TestHecatonInstanceFactory factory;

    private HecatonInstance liteInstance;

    private HecatonInstance dataInstance;

    private HecatonInstance dataInstance2;

    @Before
    public void before() {
        factory = createHecatonInstanceFactory(3);
        liteInstance = factory.newHecatonInstance(liteConfig);
        dataInstance = factory.newHecatonInstance();
        dataInstance2 = factory.newHecatonInstance();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testGetMembersWithMemberSelector() {
        assertTrueEventually(() -> {
            verifyMembersFromLiteMember(liteInstance);
            verifyMembersFromDataMember(dataInstance);
            verifyMembersFromDataMember(dataInstance2);
        });
    }

    @Test
    public void testSizeWithMemberSelector() {
        assertTrueEventually(() -> {
            verifySizeFromLiteMember(liteInstance);
            verifySizeFromDataMember(dataInstance);
            verifySizeFromDataMember(dataInstance2);
        });
    }

    private void verifyMembersFromLiteMember(final HecatonInstance instance) {
        final Member localMember = getLocalMember(instance);
        final ClusterService clusterService = getClusterService(instance);
        final Collection<Member> liteMembers = clusterService.getMembers(LITE_MEMBER_SELECTOR);
        final Collection<Member> dataMembers = clusterService.getMembers(DATA_MEMBER_SELECTOR);

        assertContains(liteMembers, localMember);
        assertNotContains(dataMembers, localMember);
        final Collection<Member> liteMembersWithoutThis = clusterService
                .getMembers(MemberSelectors.and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertNotContains(liteMembersWithoutThis, localMember);
        final Collection<Member> dataMembersWithThis = clusterService
                .getMembers(MemberSelectors.or(DATA_MEMBER_SELECTOR, LOCAL_MEMBER_SELECTOR));
        assertContains(dataMembersWithThis, localMember);
    }

    private void verifyMembersFromDataMember(final HecatonInstance instance) {
        final Member localMember = getLocalMember(instance);
        final ClusterService clusterService = getClusterService(instance);
        final Collection<Member> liteMembers = clusterService.getMembers(LITE_MEMBER_SELECTOR);
        final Collection<Member> dataMembers = clusterService.getMembers(DATA_MEMBER_SELECTOR);

        assertContains(dataMembers, localMember);
        assertNotContains(liteMembers, localMember);
        final Collection<Member> dataMembersWithoutThis = clusterService
                .getMembers(MemberSelectors.and(DATA_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertNotContains(dataMembersWithoutThis, localMember);
        final Collection<Member> liteMembersWithThis = clusterService
                .getMembers(MemberSelectors.or(LITE_MEMBER_SELECTOR, LOCAL_MEMBER_SELECTOR));
        assertContains(liteMembersWithThis, localMember);
    }

    private void verifySizeFromLiteMember(final HecatonInstance instance) {
        final ClusterService clusterService = getClusterService(instance);

        assertEquals(1, clusterService.getSize(MemberSelectors.LITE_MEMBER_SELECTOR));
        assertEquals(2, clusterService.getSize(MemberSelectors.DATA_MEMBER_SELECTOR));
        assertEquals(0, clusterService.getSize(MemberSelectors.and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR)));
        assertEquals(3, clusterService.getSize(MemberSelectors.or(DATA_MEMBER_SELECTOR, LOCAL_MEMBER_SELECTOR)));
    }

    private void verifySizeFromDataMember(final HecatonInstance instance) {
        final ClusterService clusterService = getClusterService(instance);

        assertEquals(1, clusterService.getSize(MemberSelectors.LITE_MEMBER_SELECTOR));
        assertEquals(2, clusterService.getSize(MemberSelectors.DATA_MEMBER_SELECTOR));
        assertEquals(1, clusterService.getSize(MemberSelectors.and(DATA_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR)));
        assertEquals(2, clusterService.getSize(MemberSelectors.or(LITE_MEMBER_SELECTOR, LOCAL_MEMBER_SELECTOR)));
    }

    private Member getLocalMember(HecatonInstance instance) {
        return getNode(instance).getLocalMember();
    }

}
