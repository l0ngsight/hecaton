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

package uk.org.hecaton.map.impl.nearcache;

import uk.org.hecaton.internal.nearcache.impl.invalidation.MinimalPartitionService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.partition.IPartitionService;

/**
 * Member side implementation of {@link MinimalPartitionService}
 */
public class MemberMinimalPartitionService implements MinimalPartitionService {

    private final IPartitionService partitionService;

    public MemberMinimalPartitionService(IPartitionService partitionService) {
        this.partitionService = partitionService;
    }

    @Override
    public int getPartitionId(Data key) {
        return partitionService.getPartitionId(key);
    }

    @Override
    public int getPartitionId(Object key) {
        return partitionService.getPartitionId(key);
    }

    @Override
    public int getPartitionCount() {
        return partitionService.getPartitionCount();
    }
}
