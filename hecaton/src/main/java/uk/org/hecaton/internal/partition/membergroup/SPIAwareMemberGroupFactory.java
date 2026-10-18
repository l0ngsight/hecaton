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

package uk.org.hecaton.internal.partition.membergroup;


import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.properties.ValidationException;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.internal.partition.impl.PartitionStateManager;
import uk.org.hecaton.spi.discovery.DiscoveryStrategy;
import uk.org.hecaton.spi.discovery.impl.DefaultDiscoveryService;
import uk.org.hecaton.spi.discovery.integration.DiscoveryService;
import uk.org.hecaton.spi.partitiongroup.MemberGroup;
import uk.org.hecaton.spi.partitiongroup.PartitionGroupStrategy;

import java.util.Collection;
import java.util.Set;

import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static uk.org.hecaton.internal.util.SetUtil.createHashSet;

/**
 * SPIAwareMemberGroupFactory is responsible for providing custom MemberGroups
 * implemented by the user in {@link DiscoveryStrategy#getPartitionGroupStrategy()}
 * to the {@link PartitionStateManager}.
 *
 * @since 3.7
 */
public class SPIAwareMemberGroupFactory extends BackupSafeMemberGroupFactory implements MemberGroupFactory {

    private final DiscoveryService discoveryService;

    public SPIAwareMemberGroupFactory(DiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    @Override
    protected Set<MemberGroup> createInternalMemberGroups(Collection<? extends Member> allMembers) {
        Set<MemberGroup> memberGroups = createHashSet(allMembers.size());

        try {
            DefaultDiscoveryService defaultDiscoveryService = (DefaultDiscoveryService) discoveryService;
            // If no discovery strategy is found fail-fast
            if (!defaultDiscoveryService.getDiscoveryStrategies().iterator().hasNext()) {
                throw new RuntimeException("Could not load any Discovery Strategy, please "
                        + "check service definitions under META_INF.services folder. ");
            } else {
                for (DiscoveryStrategy discoveryStrategy : defaultDiscoveryService.getDiscoveryStrategies()) {
                    PartitionGroupStrategy groupStrategy = discoveryStrategy.getPartitionGroupStrategy(allMembers);
                    if (groupStrategy == null) {
                        groupStrategy = discoveryStrategy.getPartitionGroupStrategy();
                    }
                    checkNotNull(groupStrategy);
                    for (MemberGroup group : groupStrategy.getMemberGroups()) {
                        memberGroups.add(group);
                    }
                    return memberGroups;
                }
            }
        } catch (ValidationException e) {
            throw new InvalidConfigurationException("Invalid configuration", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure discovery strategies", e);
        }

        return memberGroups;
    }

}
