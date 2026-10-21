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

package uk.org.hecaton.cp.internal.datastructures.semaphore;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.internal.util.ThreadUtil;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.cp.internal.session.ProxySessionManagerService.NO_SESSION_ID;
import static org.junit.Assert.assertNotEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SessionAwareSemaphoreFailureTest extends SemaphoreFailureTest {

    @Override
    boolean isJDKCompatible() {
        return false;
    }

    @Override
    long getSessionId(HecatonInstance semaphoreInstance, RaftGroupId groupId) {
        long sessionId = sessionManagerService.getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        return sessionId;
    }

    @Override
    long getThreadId(RaftGroupId groupId) {
        return ThreadUtil.getThreadId();
    }


}
