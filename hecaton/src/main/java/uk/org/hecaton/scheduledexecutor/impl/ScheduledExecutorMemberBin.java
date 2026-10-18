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

package uk.org.hecaton.scheduledexecutor.impl;

import uk.org.hecaton.config.ScheduledExecutorConfig;
import uk.org.hecaton.internal.util.ConstructorFunction;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.NodeEngine;

import static uk.org.hecaton.config.ScheduledExecutorConfig.CapacityPolicy.PER_PARTITION;
import static uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService.NOOP_PERMIT;

public class ScheduledExecutorMemberBin
        extends AbstractScheduledExecutorContainerHolder {

    private final ILogger logger;

    private final ConstructorFunction<String, ScheduledExecutorContainer> containerConstructorFunction;

    public ScheduledExecutorMemberBin(NodeEngine nodeEngine, DistributedScheduledExecutorService service) {
        super(nodeEngine);
        this.logger = nodeEngine.getLogger(getClass());
        this.containerConstructorFunction = name -> {
            if (logger.isFinestEnabled()) {
                logger.finest("[Partition: -1] Create new scheduled executor container with name: %s", name);
            }

            ScheduledExecutorConfig config = nodeEngine.getConfig().findScheduledExecutorConfig(name);
            return new ScheduledExecutorMemberOwnedContainer(name, newPermitFor(name, service, config),
                    nodeEngine, config.isStatisticsEnabled(), config.getUserCodeNamespace());
        };
    }

    CapacityPermit newPermitFor(String name, DistributedScheduledExecutorService service,
                                ScheduledExecutorConfig config) {
        if (config.getCapacity() == 0 || config.getCapacityPolicy() == PER_PARTITION) {
            return NOOP_PERMIT;
        }

        return service.permitFor(name, config);
    }

    @Override
    public ConstructorFunction<String, ScheduledExecutorContainer> getContainerConstructorFunction() {
        return containerConstructorFunction;
    }

}
