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

package uk.org.hecaton.jet;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.partition.NoDataMemberInClusterException;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static uk.org.hecaton.jet.core.JobAssertions.assertThat;
import static uk.org.hecaton.jet.core.TestProcessors.batchDag;
import static uk.org.hecaton.jet.core.TestProcessors.streamingDag;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonSerialClassRunner.class)
public class LightJob_StandaloneClusterTest extends JetTestSupport {

    @Test
    public void test_submittedFromLiteMember() {
        HecatonInstance nonLiteInst = createHecatonInstance();
        HecatonInstance liteInst = createHecatonInstance(smallInstanceConfig().setLiteMember(true));
        // lite members can be coordinators, though they won't execute processors
        Job job = liteInst.getJet().newLightJob(streamingDag());

        assertTrueEventually(() -> assertThat(job).isExecutingOn(nonLiteInst));
        assertThat(job).isNotExecutingOn(liteInst);
    }

    @Test
    public void test_submittedFromLiteMember_noDataMember() {
        HecatonInstance liteInst = createHecatonInstance(smallInstanceConfig().setLiteMember(true));

        assertThatThrownBy(() -> liteInst.getJet().newLightJob(batchDag()).join())
                .hasRootCauseInstanceOf(NoDataMemberInClusterException.class)
                .hasRootCauseMessage("Partitions can't be assigned since all nodes in the cluster are lite members");
    }

    @Test
    public void when_coordinatorFails_then_jobNotRetriedWithAnotherCoordinator() {
        HecatonInstance coordinatorInst = createHecatonInstance();
        createHecatonInstance();
        HecatonInstance coordinatorClient = createHecatonClient(configForSingleMemberClientConnectingTo(coordinatorInst));

        Job job = coordinatorClient.getJet().newLightJob(streamingDag());
        assertTrueEventually(() -> assertThat(job).isExecutingOn(coordinatorInst));

        coordinatorInst.shutdown();

        assertThatThrownBy(job::join)
                .hasMessageContaining("uk.org.hecaton.spi.exception.TargetDisconnectedException: Mocked Remote socket closed");
    }
}
