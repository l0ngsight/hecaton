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

package uk.org.hecaton.internal.monitor;

import uk.org.hecaton.internal.management.dto.ClientEndPointDTO;
import uk.org.hecaton.internal.management.dto.ClusterHotRestartStatusDTO;
import uk.org.hecaton.json.internal.JsonSerializable;

import java.util.Collection;
import java.util.UUID;

/**
 * Local statistics for a Hecaton member.
 */
public interface MemberState extends JsonSerializable {

    String getAddress();

    /**
     * Returns the UUID of this member.
     *
     * @return the UUID of this member.
     */
    UUID getUuid();

    /**
     * Returns the local CP member's UUID if this Hecaton
     * member is part of the CP subsystem, returns null otherwise.
     *
     * @return local CP member's UUID if available, null otherwise
     */
    UUID getCpMemberUuid();

    /**
     * Returns the name of a Hecaton member.
     * @return the name of a Hecaton member.
     */
    String getName();

    Collection<ClientEndPointDTO> getClients();

    /**
     * Returns the local operation statistics.
     *
     * @return LocalOperationStats statistics
     */
    LocalOperationStats getOperationStats();

    MemberPartitionState getMemberPartitionState();

    NodeState getNodeState();

    HotRestartState getHotRestartState();

    ClusterHotRestartStatusDTO getClusterHotRestartStatus();
}
