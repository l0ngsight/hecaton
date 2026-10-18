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

package uk.org.hecaton.internal.jmx;

import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.partition.PartitionService;

import java.util.Map;

import static uk.org.hecaton.internal.jmx.ManagementService.quote;
import static uk.org.hecaton.internal.util.MapUtil.createHashMap;

/**
 * Management bean for {@link PartitionService}
 */
@ManagedDescription("HecatonInstance.PartitionServiceMBean")
public class PartitionServiceMBean extends HecatonMBean<InternalPartitionService> {

    private static final int INITIAL_CAPACITY = 3;
    private final HecatonInstanceImpl hecatonInstance;

    public PartitionServiceMBean(HecatonInstanceImpl hecatonInstance, InternalPartitionService partitionService,
                                 ManagementService service) {
        super(partitionService, service);

        this.hecatonInstance = hecatonInstance;
        final Map<String, String> properties = createHashMap(INITIAL_CAPACITY);
        properties.put("type", quote("HecatonInstance.PartitionServiceMBean"));
        properties.put("name", quote(hecatonInstance.getName()));
        properties.put("instance", quote(hecatonInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("partitionCount")
    @ManagedDescription("Number of partitions")
    public int getPartitionCount() {
        return managedObject.getPartitionCount();
    }

    @ManagedAnnotation("activePartitionCount")
    @ManagedDescription("Number of active partitions")
    public int getActivePartitionCount() {
        Address thisAddress = hecatonInstance.getCluster().getLocalMember().getAddress();
        return managedObject.getMemberPartitionsIfAssigned(thisAddress).size();
    }

    @ManagedAnnotation("isClusterSafe")
    @ManagedDescription("Is the cluster in a safe state")
    public boolean isClusterSafe() {
        return hecatonInstance.getPartitionService().isClusterSafe();
    }

    @ManagedAnnotation("isLocalMemberSafe")
    @ManagedDescription("Is the local member safe to shutdown")
    public boolean isLocalMemberSafe() {
        return hecatonInstance.getPartitionService().isLocalMemberSafe();
    }
}
