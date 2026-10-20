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

package uk.org.hecaton.internal.util.iterator;

import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RestartingMemberIteratorTest extends HecatonTestSupport {

    private ClusterService mockClusterService;

    @Before
    public void setUp() {
        Set<Member> memberSet = new LinkedHashSet<>();
        mockClusterService = mock(ClusterService.class);
        when(mockClusterService.getMembers())
                .thenReturn(memberSet);
    }

    private Member addClusterMember() {
        Set<Member> currentMemberSet = mockClusterService.getMembers();
        Set<Member> newMemberSet = new LinkedHashSet<>(currentMemberSet);

        Member mockMember = mock(Member.class);
        newMemberSet.add(mockMember);

        when(mockClusterService.getMembers())
                .thenReturn(newMemberSet);
        return mockMember;
    }

    @Test
    public void testIteratorOverSingleEntry() {
        int maxRetries = 0;
        Member mockMember = addClusterMember();

        RestartingMemberIterator iterator = new RestartingMemberIterator(mockClusterService, maxRetries);
        assertTrue(iterator.hasNext());

        Member member = iterator.next();
        assertSame(mockMember, member);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testRestart() {
        int maxRetries = 1;
        Member mockMember = addClusterMember();

        RestartingMemberIterator iterator = new RestartingMemberIterator(mockClusterService, maxRetries);
        assertTrue(iterator.hasNext());

        Member member = iterator.next();
        assertSame(mockMember, member);

        //topologoy change -> this should restart iteration
        Member anotherMockMember = addClusterMember();

        member = iterator.next();
        assertSame(mockMember, member);
        member = iterator.next();
        assertSame(anotherMockMember, member);
        assertFalse(iterator.hasNext());
    }

    @Test(expected = HecatonException.class)
    public void testRestart_withMaxRetriesExhausted_nextThrowsException() {
        int maxRetries = 0;
        addClusterMember();

        RestartingMemberIterator iterator = new RestartingMemberIterator(mockClusterService, maxRetries);
        // start iterating
        iterator.next();

        // topologoy change -> this should restart iteration, but there are no retry attempt left
        addClusterMember();

        // this has to throw the Exception
        iterator.next();
    }

    @Test(expected = HecatonException.class)
    public void testRestart_withMaxRetriesExhausted_hasNextThrowsException() {
        int maxRetries = 0;
        addClusterMember();

        RestartingMemberIterator iterator = new RestartingMemberIterator(mockClusterService, maxRetries);
        iterator.next();

        addClusterMember();

        iterator.hasNext();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeThrowUOE() {
        int maxRetries = 0;
        addClusterMember();

        RestartingMemberIterator iterator = new RestartingMemberIterator(mockClusterService, maxRetries);
        iterator.next();
        iterator.remove();
    }

    @Test(expected = NoSuchElementException.class)
    public void give() {
        int maxRetries = 0;
        addClusterMember();

        RestartingMemberIterator iterator = new RestartingMemberIterator(mockClusterService, maxRetries);
        iterator.next();

        //this should throw NoSuchElementException
        iterator.next();
    }
}
