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

package uk.org.hecaton.internal.crdt.pncounter;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.core.ConsistencyLostException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.crdt.pncounter.PNCounter;
import uk.org.hecaton.instance.impl.TestUtil;
import uk.org.hecaton.test.HecatonTestSupport;
import org.junit.Test;

import static uk.org.hecaton.test.Accessors.getNode;

/**
 * Base test for testing behaviour of {@link ConsistencyLostException} in the case of CRDTs.
 */
public abstract class AbstractPNCounterConsistencyLostTest extends HecatonTestSupport {

    @Test(expected = ConsistencyLostException.class)
    public void consistencyLostExceptionIsThrownWhenTargetReplicaDisappears() {
        final PNCounter driver = getCounter();
        mutate(driver);
        assertState(driver);

        final Address currentTarget = getCurrentTargetReplicaAddress(driver);

        terminateMember(currentTarget);

        mutate(driver);
    }

    @Test
    public void driverCanContinueSessionByCallingReset() {
        final PNCounter driver = getCounter();
        mutate(driver);
        assertState(driver);
        final Address currentTarget = getCurrentTargetReplicaAddress(driver);
        terminateMember(currentTarget);

        driver.reset();
        mutate(driver);
    }

    private void terminateMember(Address memberAddress) {
        for (HecatonInstance member : getMembers()) {
            if (getNode(member).getThisAddress().equals(memberAddress)) {
                TestUtil.terminateInstance(member);
            }
        }
    }

    protected abstract Address getCurrentTargetReplicaAddress(PNCounter driver);

    protected abstract void assertState(PNCounter driver);

    protected abstract void mutate(PNCounter driver);

    protected abstract PNCounter getCounter();

    protected abstract HecatonInstance[] getMembers();
}
