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

package uk.org.hecaton.internal.crdt.pncounter.operations;

import uk.org.hecaton.cluster.impl.VectorClock;
import uk.org.hecaton.cluster.memberselector.MemberSelectors;
import uk.org.hecaton.core.ConsistencyLostException;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.crdt.pncounter.PNCounter;
import uk.org.hecaton.internal.crdt.CRDTDataSerializerHook;
import uk.org.hecaton.crdt.TargetNotReplicaException;
import uk.org.hecaton.internal.crdt.pncounter.PNCounterImpl;
import uk.org.hecaton.internal.crdt.pncounter.PNCounterService;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.NamedOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import static uk.org.hecaton.internal.crdt.pncounter.PNCounterService.SERVICE_NAME;

/**
 * Base class for {@link PNCounter} query and
 * mutation operation implementations. It will throw an exception on all
 * serialization and deserialization invocations as CRDT operations must
 * be invoked locally on a member.
 */
public abstract class AbstractPNCounterOperation extends Operation implements IdentifiedDataSerializable, NamedOperation {
    protected String name;
    private PNCounterImpl counter;

    AbstractPNCounterOperation() {
    }

    AbstractPNCounterOperation(String name) {
        this.name = name;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    PNCounterImpl getPNCounter(VectorClock observedTimestamps) {
        if (counter != null) {
            return counter;
        }
        final PNCounterService service = getService();
        if (observedTimestamps != null && !observedTimestamps.isEmpty() && !service.containsCounter(name)) {
            throw new ConsistencyLostException("This replica cannot provide the session guarantees for "
                    + "the PN counter since it's state is stale");
        }
        final int maxConfiguredReplicaCount = getNodeEngine().getConfig().findPNCounterConfig(name).getReplicaCount();
        if (!isCRDTReplica(maxConfiguredReplicaCount)) {
            throw new TargetNotReplicaException("This member is not a CRDT replica for the " + name + " + PN counter");
        }
        this.counter = service.getCounter(name);
        return counter;
    }

    /**
     * Returns {@code true} if this member is a CRDT replica.
     *
     * @param configuredReplicaCount the configured max replica count
     */
    private boolean isCRDTReplica(int configuredReplicaCount) {
        final Collection<Member> dataMembers = getNodeEngine().getClusterService()
                                                              .getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);
        final Iterator<Member> dataMemberIterator = dataMembers.iterator();
        final Address thisAddress = getNodeEngine().getThisAddress();

        for (int i = 0; i < Math.min(configuredReplicaCount, dataMembers.size()); i++) {
            final Address dataMemberAddress = dataMemberIterator.next().getAddress();
            if (thisAddress.equals(dataMemberAddress)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", name=").append(name);
    }

    @Override
    public int getFactoryId() {
        return CRDTDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
    }
}
