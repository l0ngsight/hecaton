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

package uk.org.hecaton.jet.impl;

import uk.org.hecaton.internal.cluster.MemberInfo;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.impl.execution.init.ExecutionPlan;
import uk.org.hecaton.jet.impl.operation.InitExecutionOperation;
import uk.org.hecaton.spi.annotation.Beta;

import java.util.Map;
import java.util.Set;

/**
 * Jet job metadata observer used primarily for testing.
 * It is invoked exactly before {@link InitExecutionOperation}.
 * <p>
 * The internal state of the observer supposed to be mutable.
 * {@link #onLightJobInvocation} and {@link #onJobInvocation} implementations must not use blocking code.
 *
 * @since 5.4
 */
@Beta
public interface JobInvocationObserver {

    void onJobInvocation(long jobId, Map<MemberInfo, ExecutionPlan> executionPlanMap, DAG dag, JobConfig jobConfig);

    void onLightJobInvocation(long jobId, Set<MemberInfo> members, DAG dag, JobConfig jobConfig);
}
